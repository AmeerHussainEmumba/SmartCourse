//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/archive"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mongoCfg(uri string) *config.Config {
	return &config.Config{Mongo: config.MongoConfig{
		URI: uri, Database: "wire_test", ConnectTimeout: time.Second, ServerSelectionTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	}}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func sampleEvent() relay.DecodedEvent {
	return relay.DecodedEvent{EventID: uuid.New(), AggregateID: uuid.New(), EventType: "identity.user_registered", Payload: []byte(`{}`), OccurredAt: time.Now()}
}

// MONGO_URI unset: run returns at once, every call reports "not connected",
// and nothing panics.
func TestMongoWiring_NotConfigured_DegradesQuietly(t *testing.T) {
	mg := newMongoWiring(&config.Config{})
	done := make(chan struct{})
	go func() { mg.run(context.Background(), quietLogger()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run blocked although MongoDB is not configured")
	}
	if err := mg.Ping(context.Background()); err == nil {
		t.Error("Ping succeeded without MongoDB")
	}
	h := mg.ArchiveHandlers()[0]
	if err := h.Handle(context.Background(), nil, sampleEvent()); !errors.Is(err, errMongoNotReady) {
		t.Errorf("archiver without MongoDB: want errMongoNotReady, got %v", err)
	}
	if err := mg.Sink().Park(context.Background(), deadletter.Parked{Event: sampleEvent(), Consumer: "c", Err: errors.New("x")}); !errors.Is(err, errMongoNotReady) {
		t.Errorf("sink without MongoDB: want errMongoNotReady, got %v", err)
	}
}

// MongoDB configured but unreachable: the worker's goroutine keeps retrying
// without blocking anything and stops promptly when told to.
func TestMongoWiring_Unreachable_RetriesAndStopsOnCancel(t *testing.T) {
	mg := newMongoWiring(mongoCfg("mongodb://" + freeAddr(t)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { mg.run(ctx, quietLogger()); close(done) }()
	time.Sleep(1500 * time.Millisecond)
	if _, err := mg.get(); !errors.Is(err, errMongoNotReady) {
		t.Errorf("get with MongoDB unreachable: want errMongoNotReady, got %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

// MongoDB reachable: the wiring connects, the archiver and sink work through it.
func TestMongoWiring_Connects_ArchivesAndParks(t *testing.T) {
	mongoEnv := testenv.SetupMongo(t)
	mg := newMongoWiring(mongoCfg(mongoEnv.URI))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); mg.run(ctx, quietLogger()) }()
	defer func() { cancel(); wg.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for mg.Ping(context.Background()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("never connected")
		}
		time.Sleep(50 * time.Millisecond)
	}

	evt := sampleEvent()
	h := mg.ArchiveHandlers()[0]
	if h.ConsumerName != archive.Consumer {
		t.Errorf("consumer name = %q", h.ConsumerName)
	}
	for range 2 { // redelivery
		if err := h.Handle(ctx, nil, evt); err != nil {
			t.Fatalf("archive: %v", err)
		}
	}
	got, err := mg.ArchiveEvent(ctx, mongostore.Event{ID: evt.EventID.String(), EventType: evt.EventType})
	if err != nil || got {
		t.Errorf("second archive of the same id: inserted=%v err=%v, want false and nil", got, err)
	}
	if err := mg.Sink().Park(ctx, deadletter.Parked{Event: evt, Consumer: "c", Err: errors.New("x"), Attempts: 5}); err != nil {
		t.Fatalf("park: %v", err)
	}
	if _, err := mg.GetFailedEvent(ctx, evt.EventID.String(), "c"); err != nil {
		t.Errorf("parked event not readable: %v", err)
	}
}

// The admin server runs beside the rest of the worker: it serves health on its
// own port with MongoDB unreachable (degraded, not down) and shuts down
// gracefully on cancel.
func TestServeAdmin_HealthWithMongoDown_AndGracefulShutdown(t *testing.T) {
	pg := testenv.Setup(t)
	cfg := mongoCfg("mongodb://" + freeAddr(t))
	cfg.Env, cfg.MaxBodyBytes = "test", 1<<20
	cfg.JWT = config.JWTConfig{SigningKey: "test-signing-key-0123456789-abcdefghij", AccessTokenTTL: time.Hour, Issuer: "smartcourse"}
	cfg.Admin.Addr = freeAddr(t)
	cfg.HTTPReadTimeout, cfg.HTTPWriteTimeout, cfg.HTTPIdleTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	mg := newMongoWiring(cfg)
	deps := &infra.Dependencies{DB: pg.DB, RDB: pg.RDB}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { serveAdmin(ctx, quietLogger(), deps, cfg, mg, consumerHandlers()); close(done) }()

	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = http.Get("http://" + cfg.Admin.Addr + "/health/ready")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("admin never came up: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("ready = %d %s, want 200 (MongoDB is non-fatal)", resp.StatusCode, body)
	}
	if want := fmt.Sprintf(`"mongodb":"%s"`, "failed"); !strings.Contains(string(body), want) {
		t.Errorf("ready body %s should report %s", body, want)
	}
	// The failed-events API answers 401 without a token: reachable, protected.
	if r, err := http.Get("http://" + cfg.Admin.Addr + "/api/v1/admin/failed-events"); err != nil || r.StatusCode != 401 {
		t.Errorf("unauthenticated list: %v %v", r, err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("serveAdmin did not stop after cancel")
	}
	if _, err := http.Get("http://" + cfg.Admin.Addr + "/health/live"); err == nil {
		t.Error("admin still serving after shutdown")
	}
}
