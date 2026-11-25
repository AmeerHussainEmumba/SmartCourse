//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// recordingPublisher stands in for the Kafka producer: it records what the
// relay publishes and acks it.
type recordingPublisher struct {
	mu   sync.Mutex
	evts []relay.DecodedEvent
}

func (p *recordingPublisher) Publish(_ context.Context, e relay.DecodedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evts = append(p.evts, e)
	return nil
}

func (p *recordingPublisher) types() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, e := range p.evts {
		out = append(out, e.EventType)
	}
	return out
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

func get(url string) (int, string) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startWorker runs the worker's real wiring (runtime.Run + wireWith) against real
// Postgres (as svc_identity) and Redis; only the Kafka producer is replaced.
// Kafka itself is unreachable (127.0.0.1:1), which must degrade readiness and
// nothing else. It returns the internal listener's base URL.
func startWorker(t *testing.T, env *testenv.Env, newPublisher publisherFactory) string {
	t.Helper()
	intAddr := freeAddr(t)
	t.Setenv("POSTGRES_DSN", env.ServiceDSN(t, "identity"))
	t.Setenv("POSTGRES_SEARCH_PATH", "identity,public")
	t.Setenv("REDIS_ADDR", env.RDB.Options().Addr)
	t.Setenv("MINIO_ENDPOINT", "unused:9000")
	t.Setenv("MINIO_ACCESS_KEY_ID", "unused")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "unused")
	t.Setenv("JWT_SIGNING_KEY", "integration-test-signing-key-32-bytes!!")
	t.Setenv("ARGON2_MEMORY_KIB", "8192")
	t.Setenv("ARGON2_ITERATIONS", "1")
	t.Setenv("ARGON2_PARALLELISM", "1")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:1")
	t.Setenv("OUTBOX_RETENTION", "1h")
	t.Setenv("METRICS_HTTP_ADDR", intAddr)
	t.Setenv("OTEL_SDK_DISABLED", "true")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "identity-worker", func(p *runtime.Process) error { return wireWith(p, newPublisher) }, options()...)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(40 * time.Second):
			t.Error("identity-worker did not stop")
		}
	})
	base := "http://" + intAddr
	eventually(t, "readiness to answer", func() bool { code, _ := get(base + "/health/ready"); return code == 200 })
	return base
}

func writeEvent(t *testing.T, db *gorm.DB, eventType string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	err := db.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{AggregateType: "user", AggregateID: id, EventType: eventType, SchemaVersion: 1,
			Payload: map[string]any{"user_id": id, "role": "student", "full_name": "W"}})
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// All three components run in one process: the relay publishes identity.outbox
// through its (publish-only) relay, the re-seeder repairs a flushed Redis key,
// and an enqueued PruneOutbox task deletes an old published row. Kafka being
// unreachable shows up as degraded, never unready.
func TestIdentityWorker_RunsRelayReseederAndJobs(t *testing.T) {
	env := testenv.Setup(t)
	db := env.ServiceDB(t, "identity")
	pub := &recordingPublisher{}

	// Fixtures: a user whose token version Redis does not have, and an old published outbox row.
	uid := uuid.New()
	if err := db.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status, token_version)
		VALUES (?, 'tv@example.com', 'x', 'TV', 'instructor', 'active', 4)`, uid).Error; err != nil {
		t.Fatal(err)
	}
	oldID := writeEvent(t, db, "identity.user_registered")
	if err := db.Exec(`UPDATE outbox SET published_at = now() - interval '2 days' WHERE aggregate_id = ?`, oldID).Error; err != nil {
		t.Fatal(err)
	}

	base := startWorker(t, env, func(context.Context) (relay.Publisher, func(), error) { return pub, func() {}, nil })

	// Re-seeder.
	key := auth.TokenVersionRedisKey(uid.String())
	eventually(t, "the re-seeder to write the token version", func() bool {
		v, err := env.RDB.Get(context.Background(), key).Int()
		return err == nil && v == 4
	})

	// Relay: a new row reaches the publisher and is marked published (in identity.outbox).
	newID := writeEvent(t, db, "identity.user_role_changed")
	eventually(t, "the relay to publish the new row", func() bool {
		var n int
		env.Owner.Raw(`SELECT count(*) FROM identity.outbox WHERE aggregate_id = ? AND published_at IS NOT NULL`, newID).Row().Scan(&n)
		return n == 1
	})
	if got := pub.types(); len(got) == 0 || got[len(got)-1] != "identity.user_role_changed" {
		t.Errorf("published event types = %v", got)
	}

	// Jobs: the Asynq servers are up and run PruneOutbox when its task is enqueued.
	c := asynqx.NewClient(asynqx.RedisClient(env.RDB.Options().Addr, "", 0))
	defer c.Close()
	if _, err := c.Enqueue(context.Background(), asynqx.Job{Type: jobs.TypeOutboxPrune, ID: "wiring-prune", Queue: asynqx.QueueLow}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "PruneOutbox to delete the old published row", func() bool {
		var n int
		env.Owner.Raw(`SELECT count(*) FROM identity.outbox WHERE aggregate_id = ?`, oldID).Row().Scan(&n)
		return n == 0
	})
	var kept int
	env.Owner.Raw(`SELECT count(*) FROM identity.outbox WHERE aggregate_id = ?`, newID).Row().Scan(&kept)
	if kept != 1 {
		t.Error("PruneOutbox deleted a recent row")
	}

	// Readiness: Postgres and Redis ok (fatal), Kafka unreachable but non-fatal.
	code, body := get(base + "/health/ready")
	if code != 200 || !strings.Contains(body, `"degraded"`) || !strings.Contains(body, `"kafka":"failed"`) ||
		!strings.Contains(body, `"postgres":"ok"`) || !strings.Contains(body, `"redis":"ok"`) {
		t.Errorf("ready = %d %s", code, body)
	}
	if code, m := get(base + "/metrics"); code != 200 || !strings.Contains(m, "outbox_backlog") {
		t.Errorf("internal /metrics = %d, lacks outbox_backlog", code)
	}
}

// A component failing is logged and shown degraded and never stops the others:
// the relay component panics (a publisher that explodes at build time) while the
// re-seeder and the jobs keep working.
func TestIdentityWorker_AFailingRelayDoesNotStopTheReseederOrJobs(t *testing.T) {
	env := testenv.Setup(t)
	db := env.ServiceDB(t, "identity")
	uid := uuid.New()
	if err := db.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status, token_version)
		VALUES (?, 'tv2@example.com', 'x', 'TV', 'instructor', 'active', 2)`, uid).Error; err != nil {
		t.Fatal(err)
	}
	oldID := writeEvent(t, db, "identity.user_registered")
	if err := db.Exec(`UPDATE outbox SET published_at = now() - interval '2 days' WHERE aggregate_id = ?`, oldID).Error; err != nil {
		t.Fatal(err)
	}

	base := startWorker(t, env, func(context.Context) (relay.Publisher, func(), error) { panic(errors.New("boom")) })

	eventually(t, "the relay to be reported failed", func() bool {
		code, body := get(base + "/health/ready")
		return code == 200 && strings.Contains(body, "component:relay") && strings.Contains(body, "failed")
	})
	eventually(t, "the re-seeder to keep working", func() bool {
		v, err := env.RDB.Get(context.Background(), auth.TokenVersionRedisKey(uid.String())).Int()
		return err == nil && v == 2
	})
	c := asynqx.NewClient(asynqx.RedisClient(env.RDB.Options().Addr, "", 0))
	defer c.Close()
	if _, err := c.Enqueue(context.Background(), asynqx.Job{Type: jobs.TypeOutboxPrune, ID: "wiring-prune-2", Queue: asynqx.QueueLow}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the jobs component to keep working", func() bool {
		var n int
		env.Owner.Raw(`SELECT count(*) FROM identity.outbox WHERE aggregate_id = ?`, oldID).Row().Scan(&n)
		return n == 0
	})
}
