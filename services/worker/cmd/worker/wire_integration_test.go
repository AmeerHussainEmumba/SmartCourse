//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/jobs"
)

// The worker as deployed: buildRelay over a connection with the worker's real
// POSTGRES_SEARCH_PATH (platform,course,analytics,public - no identity), all
// three modules' handlers registered together, fed real outbox rows. Every
// event must be processed without a single give-up, and every module's own
// tables must show the effect.
//
// It runs once per event transport (ADR-0029): in kafka mode the same handlers
// run in consumer groups, and every effect must be the same.
func TestBuildRelay_EndToEnd_OnWorkerSearchPath(t *testing.T) {
	for _, mode := range transportModes {
		t.Run(mode, func(t *testing.T) { testBuildRelayEndToEnd(t, mode) })
	}
}

func testBuildRelayEndToEnd(t *testing.T, mode string) {
	env := testenv.Setup(t)
	conn := env.OpenWithSearchPath(t, "platform,course,analytics,public")
	cfg := relayTestConfig()
	useTransport(t, cfg, mode)
	deps := &infra.Dependencies{DB: conn}

	instructor, student := uuid.New(), uuid.New()
	courseID, enrollmentID := uuid.New(), uuid.New()
	// A real course row for the enrollment archive handler to lock.
	versionID := uuid.New()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if err := env.DB.Exec(q, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, 'worker-e2e')`, courseID, instructor)
	mustExec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'T', '{}')`, versionID, courseID)
	mustExec(`UPDATE courses SET published_version_id = ?, enrollment_count = 1 WHERE id = ?`, versionID, courseID)
	mustExec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`, enrollmentID, student, courseID)

	write := func(typ string, payload map[string]any) {
		t.Helper()
		if err := env.DB.Transaction(func(tx *gorm.DB) error {
			return outbox.Write(context.Background(), tx, outbox.Event{AggregateType: "x", AggregateID: uuid.New(), EventType: typ, SchemaVersion: 1, Payload: payload})
		}); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().UTC().Add(-time.Hour)
	write("identity.user_registered", map[string]any{"user_id": instructor, "role": "instructor", "full_name": "Worker Instr"})
	write("identity.user_profile_updated", map[string]any{"user_id": instructor, "full_name": "Worker Instr 2"})
	write("identity.user_role_changed", map[string]any{"user_id": student, "role": "student", "status": "active"})
	write("course.course_published", map[string]any{"course_id": courseID})
	write("enrollment.student_enrolled", map[string]any{"enrollment_id": enrollmentID, "student_id": student, "course_id": courseID, "enrolled_at": at})
	write("course.course_archived", map[string]any{"course_id": courseID})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rl, sink, stop := runEvents(t, ctx, cfg, deps)

	// Published is not processed in kafka mode (the consumers run the handlers
	// afterwards), so the effects themselves are what is waited for, in both.
	pollUntil(t, 90*time.Second, func() []string {
		var p []string
		if n := scalarOf(env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
			p = append(p, "events still unpublished")
		}
		if scalarOf(env.DB, `SELECT count(*) FROM instructor_mirror WHERE user_id = ?`, instructor) != 1 {
			p = append(p, "course instructor_mirror not fed")
		}
		if scalarOf(env.DB, `SELECT count(*) FROM user_role_mirror`) != 2 || scalarOf(env.DB, `SELECT total_courses_published FROM platform_snapshot`) != 1 {
			p = append(p, "analytics mirrors/snapshot not fed")
		}
		if scalarOf(env.DB, `SELECT count(*) FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID) != 1 {
			p = append(p, "analytics enrollment_mirror not fed")
		}
		if scalarOf(env.DB, `SELECT count(*) FROM enrollments WHERE id = ? AND status = 'withdrawn'`, enrollmentID) != 1 || scalarOf(env.DB, `SELECT enrollment_count FROM courses WHERE id = ?`, courseID) != 0 {
			p = append(p, "enrollment archive handler did not withdraw the enrollment / fix the counter")
		}
		return p
	})
	stop()

	if p, f, g := rl.Stats(); g != 0 {
		t.Fatalf("relay gave up on %d events (processed=%d failed=%d)", g, p, f)
	}
	if d := sink.parked(); len(d) != 0 {
		t.Fatalf("%d events were dead-lettered: %+v", len(d), d)
	}
}

// startAsynq as deployed, against a real Redis and Postgres: the servers come up
// through the wiring, a maintenance task enqueued on the low queue reaches its
// handler and does its work, and cancelling the context stops it.
func TestStartAsynq_RunsAJobTaskThroughTheWiringAndStopsOnCancel(t *testing.T) {
	env := testenv.Setup(t)
	cfg := asynqTestConfig(env.RDB.Options().Addr)
	if err := env.DB.Exec(`INSERT INTO idempotency_keys (key, user_id, endpoint, request_hash, expires_at)
		VALUES ('stale', ?, 'POST /x', '\x00', now() - interval '1 hour')`, uuid.New()).Error; err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		startAsynq(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), &infra.Dependencies{DB: env.DB, RDB: env.RDB}, cfg)
	}()

	c := asynqx.NewClient(asynqx.RedisClient(cfg.Redis.Addr, "", 0))
	defer c.Close()
	if _, err := c.Enqueue(ctx, asynqx.Job{Type: jobs.TypeIdempotencyPrune, ID: "wiring-1", Queue: asynqx.QueueLow}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		env.DB.Raw(`SELECT count(*) FROM idempotency_keys`).Row().Scan(&n)
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the idempotency prune task was never run by the worker's servers")
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("startAsynq did not return after its context was cancelled")
	}
}

// An Asynq or scheduler failure must not stop the relay (ADR-0009's
// independence, carried over): here startAsynq is given a Redis nothing
// listens on, and then a config it refuses outright, while the relay publishes
// an event.
func TestStartAsynq_FailureDoesNotStopTheRelay(t *testing.T) {
	cases := map[string]func(*config.Config){
		"redis unreachable": func(c *config.Config) { c.Redis.Addr = "127.0.0.1:1" },
		"jobs refused":      func(c *config.Config) { c.Jobs.OutboxRetention = 0 },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			env := testenv.Setup(t)
			conn := env.OpenWithSearchPath(t, "platform,course,analytics,public")
			deps := &infra.Dependencies{DB: conn, RDB: env.RDB}
			cfg := asynqTestConfig(env.RDB.Options().Addr)
			breakIt(cfg)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); buildRelay(deps, relayTestConfig()).Run(ctx) }()
			go func() {
				defer wg.Done()
				startAsynq(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), deps, cfg)
			}()

			if err := env.DB.Transaction(func(tx *gorm.DB) error {
				return outbox.Write(ctx, tx, outbox.Event{AggregateType: "x", AggregateID: uuid.New(), EventType: "identity.user_registered", SchemaVersion: 1,
					Payload: map[string]any{"user_id": uuid.New(), "role": "student", "full_name": "Isolated"}})
			}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				var n int64
				env.DB.Raw(`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Row().Scan(&n)
				if n == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the relay stopped publishing while Asynq was failing")
				}
				time.Sleep(20 * time.Millisecond)
			}
			cancel()
			stopped := make(chan struct{})
			go func() { wg.Wait(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(30 * time.Second):
				t.Fatal("the worker's goroutines did not stop after cancel")
			}
		})
	}
}
