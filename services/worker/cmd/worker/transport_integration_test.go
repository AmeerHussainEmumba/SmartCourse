//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport/transporttest"
)

// transportModes are the two event transports (ADR-0029). The worker's event
// tests run once per mode: the same handlers must produce the same effects.
var transportModes = []string{config.TransportInProcess, config.TransportKafka}

// useTransport sets cfg.Kafka for mode. For kafka it starts a real broker and
// the registry stand-in (not the real Schema Registry, see transporttest).
func useTransport(t *testing.T, cfg *config.Config, mode string) {
	t.Helper()
	if mode != config.TransportKafka {
		cfg.Kafka = config.KafkaConfig{Transport: config.TransportInProcess}
		return
	}
	kb := testenv.SetupKafka(t)
	reg := transporttest.NewRegistry(t)
	cfg.Kafka = config.KafkaConfig{
		Transport: config.TransportKafka, Brokers: kb.Brokers, SchemaRegistryURL: reg.URL,
		ConsumerMaxInFlight: 4, ConsumerMaxAttempts: 5,
		ConsumerBackoffBase: 50 * time.Millisecond, ConsumerBackoffMax: time.Second,
		ConsumerShutdownTimeout: 5 * time.Second, ProduceTimeout: 10 * time.Second,
	}
}

// recordingSink counts what the transport parked.
type recordingSink struct {
	mu  sync.Mutex
	got []relay.DeadLetter
}

func (s *recordingSink) Park(_ context.Context, d relay.DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, d)
	return nil
}

func (s *recordingSink) parked() []relay.DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]relay.DeadLetter(nil), s.got...)
}

// runEvents starts the worker's event side the way main does for cfg's mode:
// the transport, the relay built from it, and the consumers. The returned func
// stops all of it and waits.
func runEvents(t *testing.T, ctx context.Context, cfg *config.Config, deps *infra.Dependencies) (rl *relay.Relay, sink *recordingSink, stop func()) {
	t.Helper()
	sink = &recordingSink{}
	tr, err := newEventTransport(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, sink, nil)
	if err != nil {
		t.Fatalf("newEventTransport: %v", err)
	}
	rl = buildRelay(deps, cfg, tr.RelayOptions()...)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); rl.Run(ctx) }()
	go func() { defer wg.Done(); tr.Run(ctx, rl, deps.DB) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			tr.Close()
		})
	}
	t.Cleanup(stop)
	return rl, sink, stop
}

// pollUntil polls problems until it reports none or the deadline passes, and
// fails with the last list.
func pollUntil(t *testing.T, d time.Duration, problems func() []string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		p := problems()
		if len(p) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not converged after %s: %v", d, p)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func scalarOf(db *gorm.DB, q string, args ...any) int64 {
	var n int64
	if err := db.Raw(q, args...).Row().Scan(&n); err != nil {
		return -1
	}
	return n
}

// Kafka mode fails at start, with a message naming the problem, when the
// registry lacks a subject or cannot be reached, instead of failing row by row
// later. A healthy start provisions the topics and exposes the readiness check.
func TestNewEventTransport_KafkaStartup(t *testing.T) {
	kb := testenv.SetupKafka(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := relayTestConfig()
	base := config.KafkaConfig{
		Transport: config.TransportKafka, Brokers: kb.Brokers, ProduceTimeout: 5 * time.Second,
	}

	t.Run("healthy", func(t *testing.T) {
		cfg.Kafka = base
		cfg.Kafka.SchemaRegistryURL = transporttest.NewRegistry(t).URL
		tr, err := newEventTransport(context.Background(), logger, cfg, nil, nil)
		if err != nil {
			t.Fatalf("newEventTransport: %v", err)
		}
		defer tr.Close()
		if len(tr.RelayOptions()) != 1 {
			t.Errorf("kafka mode must hand the relay its publisher, got %d options", len(tr.RelayOptions()))
		}
		check := tr.HealthCheck()
		if check == nil {
			t.Fatal("kafka mode must expose the readiness check")
		}
		if err := check(context.Background()); err != nil {
			t.Errorf("readiness check against a live broker: %v", err)
		}
	})
	t.Run("subject missing", func(t *testing.T) {
		reg := transporttest.NewRegistry(t)
		reg.Remove("smartcourse.progress.v1.CourseCompleted")
		cfg.Kafka = base
		cfg.Kafka.SchemaRegistryURL = reg.URL
		_, err := newEventTransport(context.Background(), logger, cfg, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "smartcourse.progress.v1.CourseCompleted") {
			t.Fatalf("want an error naming the missing subject, got %v", err)
		}
	})
	t.Run("registry unreachable", func(t *testing.T) {
		cfg.Kafka = base
		cfg.Kafka.SchemaRegistryURL = "http://127.0.0.1:1"
		_, err := newEventTransport(context.Background(), logger, cfg, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "127.0.0.1:1") {
			t.Fatalf("want an error naming the registry, got %v", err)
		}
	})
}
