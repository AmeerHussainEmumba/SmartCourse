package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type nopSink struct{}

func (nopSink) Park(context.Context, relay.DeadLetter) error { return nil }

func TestTransportMode_FollowsConfig(t *testing.T) {
	for _, m := range []string{config.TransportInProcess, config.TransportKafka} {
		if got := transportMode(&config.Config{Kafka: config.KafkaConfig{Transport: m}}); got != m {
			t.Errorf("transportMode = %q, want %q", got, m)
		}
	}
}

// In-process mode adds nothing to the relay but the optional dead-letter sink,
// has no readiness check, and Run returns at once (the consumers are kafka-only).
func TestEventTransport_InProcessIsInert(t *testing.T) {
	cfg := relayTestConfig()
	cfg.Kafka = config.KafkaConfig{Transport: config.TransportInProcess}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tr, err := newEventTransport(context.Background(), logger, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(tr.RelayOptions()); n != 0 {
		t.Errorf("RelayOptions without a sink = %d, want 0", n)
	}
	if tr.HealthCheck() != nil {
		t.Error("in-process mode has no Kafka health check")
	}
	tr.Run(context.Background(), nil, nil) // must not touch the relay or the database
	tr.Close()

	withSink, err := newEventTransport(context.Background(), logger, cfg, nopSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(withSink.RelayOptions()); n != 1 {
		t.Errorf("RelayOptions with a sink = %d, want 1 (the sink)", n)
	}
	if buildRelay(&infra.Dependencies{}, cfg, withSink.RelayOptions()...).Publishing() {
		t.Error("in-process mode must not publish")
	}
}
