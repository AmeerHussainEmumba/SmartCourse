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

	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/notification"
)

// The worker's event side end to end, on the worker's real search path and
// with real Redis: outbox events go through the relay (analytics, course,
// enrollment, certificate and notification handlers registered together), the
// notification sweeper enqueues the pending deliveries, the Asynq delivery task
// sends them through a capturing provider, and the completion event has issued
// exactly one certificate. Nothing here calls a handler directly.
//
// It runs once per event transport (ADR-0029): with EVENT_TRANSPORT=kafka the
// relay publishes through a real broker and consumer groups run the same
// handlers, and the certificate and the mail must come out the same.
func TestWorker_EventsToCertificateAndMail_EndToEnd(t *testing.T) {
	for _, mode := range transportModes {
		t.Run(mode, func(t *testing.T) { testWorkerEventsEndToEnd(t, mode) })
	}
}

func testWorkerEventsEndToEnd(t *testing.T, mode string) {
	env := testenv.Setup(t)
	conn := env.OpenWithSearchPath(t, "platform,course,analytics,public")
	deps := &infra.Dependencies{DB: conn, RDB: env.RDB}
	cfg := asynqTestConfig(env.RDB.Options().Addr)
	cfg.Relay.Workers = 3
	useTransport(t, cfg, mode)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	student, instructor := uuid.New(), uuid.New()
	courseID, enrollmentID, versionID := uuid.New(), uuid.New(), uuid.New()
	exec := func(q string, args ...any) {
		t.Helper()
		if err := env.DB.Exec(q, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, 'worker-mail-e2e')`, courseID, instructor)
	exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'Distributed Systems', '{}')`, versionID, courseID)
	exec(`UPDATE courses SET published_version_id = ?, enrollment_count = 1 WHERE id = ?`, versionID, courseID)
	exec(`INSERT INTO enrollments (id, student_id, course_id, status, completed_at) VALUES (?, ?, ?, 'completed', now())`, enrollmentID, student, courseID)

	write := func(typ string, payload map[string]any) {
		t.Helper()
		if err := env.DB.Transaction(func(tx *gorm.DB) error {
			return outbox.Write(context.Background(), tx, outbox.Event{
				AggregateType: "x", AggregateID: uuid.New(), EventType: typ, SchemaVersion: 1, Payload: payload,
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
	write("identity.user_registered", map[string]any{"user_id": student, "email": "ada@example.com", "role": "student", "full_name": "Ada Lovelace"})
	write("identity.user_registered", map[string]any{"user_id": instructor, "email": "grace@example.com", "role": "instructor", "full_name": "Grace Hopper"})
	write("enrollment.student_enrolled", map[string]any{"enrollment_id": enrollmentID, "student_id": student, "course_id": courseID, "enrolled_at": time.Now().UTC().Add(-time.Hour)})
	write("progress.course_completed", map[string]any{"enrollment_id": enrollmentID, "student_id": student, "completed_at": time.Now().UTC()})

	var mu sync.Mutex
	var sent []notification.Message
	provider := notification.ProviderFunc(func(_ context.Context, m notification.Message) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, m)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers, scheduler, err := buildAsynq(cfg, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeNotify := wireNotifications(ctx, logger, servers, deps, cfg, provider)
	defer closeNotify()
	if err := servers.Start(); err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()
	defer servers.Shutdown()

	rl, sink, stopEvents := runEvents(t, ctx, cfg, deps)

	count := func(q string, args ...any) int64 {
		var n int64
		if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if count(`SELECT count(*) FROM notification.deliveries WHERE status = 'sent'`) >= 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deliveries never reached 'sent' (pending=%d sent=%d)",
				count(`SELECT count(*) FROM notification.deliveries WHERE status = 'pending'`),
				count(`SELECT count(*) FROM notification.deliveries WHERE status = 'sent'`))
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopEvents()

	if p, f, g := rl.Stats(); g != 0 {
		t.Fatalf("relay gave up on %d events (processed=%d failed=%d)", g, p, f)
	}
	if d := sink.parked(); len(d) != 0 {
		t.Fatalf("%d events were dead-lettered: %+v", len(d), d)
	}
	if n := count(`SELECT count(*) FROM certificates WHERE enrollment_id = ?`, enrollmentID); n != 1 {
		t.Errorf("want exactly 1 certificate for the completed enrollment, got %d", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if int64(len(sent)) != count(`SELECT count(*) FROM notification.deliveries WHERE status = 'sent'`) {
		t.Errorf("provider saw %d messages but %d deliveries are marked sent",
			len(sent), count(`SELECT count(*) FROM notification.deliveries WHERE status = 'sent'`))
	}
	to := map[string]int{}
	for _, m := range sent {
		to[m.To]++
	}
	if to["ada@example.com"] < 3 || to["grace@example.com"] < 1 {
		t.Errorf("unexpected recipients: %v (want >=3 for the student: welcome, enrollment, completion; >=1 for the instructor)", to)
	}
}
