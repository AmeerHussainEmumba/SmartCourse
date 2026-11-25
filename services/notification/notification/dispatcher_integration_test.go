//go:build integration

// What this file checks, against real Postgres (RFC §18.2) and the real
// relay: that the handlers write exactly one delivery per event however the
// event is delivered, that the sweep and the delivery task turn those rows
// into exactly the expected sends (including through provider outages and a
// crash between commit and enqueue), and that the recipient mirror survives
// out-of-order events. Mail goes to an in-process fake SMTP server or a fake
// Provider; the real Mailpit container is in mailpit_integration_test.go.
package notification

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// memEnqueuer is an Enqueuer that records tasks; fail makes it error.
type memEnqueuer struct {
	mu    sync.Mutex
	tasks [][]byte
	fail  atomic.Bool
}

func (e *memEnqueuer) Enqueue(_ context.Context, taskType string, payload []byte) error {
	if taskType != TaskTypeDeliver {
		return errors.New("unexpected task type " + taskType)
	}
	if e.fail.Load() {
		return errors.New("redis down")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tasks = append(e.tasks, payload)
	return nil
}

func (e *memEnqueuer) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.tasks)
}

func (e *memEnqueuer) drain() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.tasks
	e.tasks = nil
	return t
}

// recordingProvider counts sends; fn (optional) decides the result.
type recordingProvider struct {
	mu    sync.Mutex
	sent  []Message
	calls int
	fn    func(call int) error
}

func (p *recordingProvider) Send(_ context.Context, m Message) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	fn := p.fn
	p.mu.Unlock()
	if fn != nil {
		if err := fn(call); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, m)
	return nil
}

func (p *recordingProvider) sends() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.sent...)
}

func handlerFor(t *testing.T, eventType string) relay.Handler {
	t.Helper()
	for _, h := range Handlers() {
		if h.EventTypes[eventType] {
			return h
		}
	}
	t.Fatalf("no handler registered for %s", eventType)
	return relay.Handler{}
}

func evt(eventType string, outboxID int64, payload map[string]any) relay.DecodedEvent {
	raw, _ := json.Marshal(payload)
	return relay.DecodedEvent{OutboxID: outboxID, EventID: uuid.New(), EventType: eventType, Payload: raw}
}

// apply runs the handler in its own transaction, the way the relay does.
func apply(db *gorm.DB, e relay.DecodedEvent) error {
	var h relay.Handler
	for _, c := range Handlers() {
		if c.EventTypes[e.EventType] {
			h = c
		}
	}
	return db.Transaction(func(tx *gorm.DB) error { return h.Handle(context.Background(), tx, e) })
}

func mustApply(t *testing.T, db *gorm.DB, e relay.DecodedEvent) {
	t.Helper()
	if err := apply(db, e); err != nil {
		t.Fatalf("handle %s: %v", e.EventType, err)
	}
}

func count(t *testing.T, db *gorm.DB, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(q, args...).Scan(&n).Error; err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func register(t *testing.T, db *gorm.DB, id uuid.UUID, email, name string, outboxID int64) {
	t.Helper()
	mustApply(t, db, evt("identity.user_registered", outboxID,
		map[string]any{"user_id": id, "email": email, "role": "student", "full_name": name}))
}

type deliveryInfo struct {
	ID       uuid.UUID
	Status   string
	Attempts int
	Kind     string
}

func deliveries(t *testing.T, db *gorm.DB) []deliveryInfo {
	t.Helper()
	var rows []deliveryInfo
	if err := db.Raw(`SELECT id, status, attempts, kind FROM notification.deliveries ORDER BY created_at, id`).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestHandlers_OneDeliveryPerEvent_UnderRepeatedAndConcurrentDelivery(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	base := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`) // the welcome mail

	e := evt("enrollment.student_enrolled", 2, map[string]any{
		"enrollment_id": uuid.New(), "student_id": student, "course_id": uuid.New()})

	// The same event delivered again and again.
	for i := 0; i < 3; i++ {
		mustApply(t, env.DB, e)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE event_id = ?`, e.EventID); n != 1 {
		t.Fatalf("sequential redelivery: want 1 delivery, got %d", n)
	}

	// The same event handled by many transactions at once (two relay
	// replicas, or a replay racing the original).
	e2 := evt("enrollment.student_enrolled", 3, map[string]any{
		"enrollment_id": uuid.New(), "student_id": student, "course_id": uuid.New()})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- apply(env.DB, e2) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent handler errored: %v", err)
		}
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE event_id = ?`, e2.EventID); n != 1 {
		t.Fatalf("concurrent delivery: want 1 delivery, got %d", n)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`); n != base+2 {
		t.Fatalf("total deliveries %d, want %d", n, base+2)
	}
}

func TestHandlers_RollbackLeavesNoDeliveryAndNoEnqueue(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	before := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`)

	e := evt("progress.course_completed", 5, map[string]any{"enrollment_id": uuid.New(), "student_id": student})
	h := handlerFor(t, e.EventType)
	boom := errors.New("simulated failure after the handler ran")
	err := env.DB.Transaction(func(tx *gorm.DB) error {
		if err := h.Handle(context.Background(), tx, e); err != nil {
			return err
		}
		return boom // the relay's marker insert failing, say
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want the simulated error, got %v", err)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`); n != before {
		t.Fatalf("rolled-back handler left %d new rows", n-before)
	}
	// Handlers have no Enqueuer at all: nothing can be enqueued for a
	// transaction that did not commit. Only rows that exist can be swept.
	enq := &memEnqueuer{}
	if _, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range enq.drain() {
		if strings.Contains(string(p), e.EventID.String()) {
			t.Fatal("rolled-back event was enqueued")
		}
	}
}

func TestEnqueuePending_RecoversFromCrashBetweenCommitAndEnqueue(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	mustApply(t, env.DB, evt("progress.course_completed", 2, map[string]any{"enrollment_id": uuid.New(), "student_id": student}))
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE status = 'pending'`); n != 2 {
		t.Fatalf("want 2 pending deliveries committed, got %d", n)
	}

	// The process "crashes" at the enqueue: Redis is unreachable.
	enq := &memEnqueuer{}
	enq.fail.Store(true)
	n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{})
	if err == nil || n != 0 {
		t.Fatalf("want a failed sweep with 0 enqueued, got n=%d err=%v", n, err)
	}
	if got := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE status = 'pending' AND enqueued_at IS NULL`); got != 2 {
		t.Fatalf("rows must stay pending and un-stamped, got %d", got)
	}

	// Redis is back: the next sweep finds the same rows and enqueues them.
	enq.fail.Store(false)
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 2 {
		t.Fatalf("recovery sweep: n=%d err=%v, want 2", n, err)
	}
	// And does not enqueue them again straight away.
	if n, _ := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); n != 0 {
		t.Fatalf("second sweep re-enqueued %d fresh rows", n)
	}
	// After RequeueAfter a still-pending row is enqueued again (lost task).
	time.Sleep(60 * time.Millisecond)
	if n, _ := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{RequeueAfter: 50 * time.Millisecond}); n != 2 {
		t.Fatalf("stale rows should be re-enqueued, got %d", n)
	}
}

func TestDeliveryHandler_ProviderDownThenUp_SingleSend(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	enq := &memEnqueuer{}
	if _, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil {
		t.Fatal(err)
	}
	tasks := enq.drain()
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}

	var down atomic.Bool
	down.Store(true)
	prov := &recordingProvider{fn: func(int) error {
		if down.Load() {
			return errors.New("connection refused")
		}
		return nil
	}}
	handle := NewDeliveryHandler(env.DB, prov)

	// Provider down: each run fails (so the queue retries with backoff),
	// the row goes back to pending and records the cause.
	for i := 1; i <= 3; i++ {
		err := handle(context.Background(), tasks[0])
		if err == nil || errors.Is(err, ErrPermanent) {
			t.Fatalf("run %d: want a retryable error, got %v", i, err)
		}
		d := deliveries(t, env.DB)[0]
		if d.Status != "pending" || d.Attempts != i {
			t.Fatalf("run %d: status=%s attempts=%d", i, d.Status, d.Attempts)
		}
	}
	if len(prov.sends()) != 0 {
		t.Fatal("nothing should have been sent while the provider was down")
	}

	// Provider back up: the retry sends once and marks it sent.
	down.Store(false)
	if err := handle(context.Background(), tasks[0]); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	// A duplicate task, a re-run, the sweep: no second send.
	for i := 0; i < 3; i++ {
		if err := handle(context.Background(), tasks[0]); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{RequeueAfter: time.Millisecond}); n != 0 {
		t.Fatalf("a sent delivery was swept again (%d)", n)
	}
	if got := len(prov.sends()); got != 1 {
		t.Fatalf("want exactly 1 send, got %d", got)
	}
	if d := deliveries(t, env.DB)[0]; d.Status != "sent" {
		t.Fatalf("status %s, want sent", d.Status)
	}
	if m := prov.sends()[0]; m.To != "ada@example.com" || m.ID != deliveries(t, env.DB)[0].ID.String() {
		t.Fatalf("unexpected message: %+v", m)
	}
}

func TestDeliveryHandler_ConcurrentTasksSendOnce(t *testing.T) {
	env := testenv.Setup(t)
	register(t, env.DB, uuid.New(), "ada@example.com", "Ada", 1)
	id := deliveries(t, env.DB)[0].ID

	prov := &recordingProvider{fn: func(int) error { time.Sleep(150 * time.Millisecond); return nil }}
	handle := NewDeliveryHandler(env.DB, prov)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handle(context.Background(), DeliveryPayload(id)); err != nil {
				t.Errorf("handle: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := len(prov.sends()); got != 1 {
		t.Fatalf("8 concurrent tasks sent %d mails, want 1", got)
	}
}

func TestDeliveryHandler_CrashedClaimIsTakenOverAfterLease(t *testing.T) {
	env := testenv.Setup(t)
	register(t, env.DB, uuid.New(), "ada@example.com", "Ada", 1)
	id := deliveries(t, env.DB)[0].ID
	// A worker claimed the row and died: 'sending' with an old lease.
	if err := env.DB.Exec(`UPDATE notification.deliveries SET status='sending', claimed_at = now() - interval '10 minutes', attempts = 1 WHERE id = ?`, id).Error; err != nil {
		t.Fatal(err)
	}
	prov := &recordingProvider{}
	// A fresh lease is honoured (another worker may be mid-send)...
	if err := env.DB.Exec(`UPDATE notification.deliveries SET claimed_at = now() WHERE id = ?`, id).Error; err != nil {
		t.Fatal(err)
	}
	if err := NewDeliveryHandler(env.DB, prov)(context.Background(), DeliveryPayload(id)); err != nil || len(prov.sends()) != 0 {
		t.Fatalf("live claim must be left alone: err=%v sends=%d", err, len(prov.sends()))
	}
	// ...an expired one is recovered, by the sweep and then the task.
	if err := env.DB.Exec(`UPDATE notification.deliveries SET claimed_at = now() - interval '10 minutes' WHERE id = ?`, id).Error; err != nil {
		t.Fatal(err)
	}
	enq := &memEnqueuer{}
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 1 {
		t.Fatalf("sweep should re-enqueue the stale claim: n=%d err=%v", n, err)
	}
	if err := NewDeliveryHandler(env.DB, prov)(context.Background(), enq.drain()[0]); err != nil {
		t.Fatal(err)
	}
	if len(prov.sends()) != 1 {
		t.Fatalf("want 1 send after takeover, got %d", len(prov.sends()))
	}
}

func TestDeliveryHandler_PermanentFailureIsNotRetried(t *testing.T) {
	env := testenv.Setup(t)
	register(t, env.DB, uuid.New(), "ada@example.com", "Ada", 1)
	id := deliveries(t, env.DB)[0].ID
	prov := &recordingProvider{fn: func(int) error { return ErrPermanent }}
	handle := NewDeliveryHandler(env.DB, prov)
	if err := handle(context.Background(), DeliveryPayload(id)); !errors.Is(err, ErrPermanent) {
		t.Fatalf("want ErrPermanent, got %v", err)
	}
	if d := deliveries(t, env.DB)[0]; d.Status != "failed" {
		t.Fatalf("status %s, want failed", d.Status)
	}
	// Later runs and sweeps leave it alone.
	if err := handle(context.Background(), DeliveryPayload(id)); err != nil {
		t.Fatalf("a failed delivery should be a no-op, got %v", err)
	}
	if prov.calls != 1 {
		t.Fatalf("provider called %d times", prov.calls)
	}
}

func TestDeliveryHandler_BadPayloadIsPermanent(t *testing.T) {
	env := testenv.Setup(t)
	handle := NewDeliveryHandler(env.DB, &recordingProvider{})
	for _, p := range [][]byte{nil, []byte("not json"), []byte(`{"delivery_id":"nope"}`), []byte(`{}`)} {
		if err := handle(context.Background(), p); !errors.Is(err, ErrPermanent) {
			t.Errorf("payload %q: want ErrPermanent, got %v", p, err)
		}
	}
	if err := handle(context.Background(), DeliveryPayload(uuid.New())); !errors.Is(err, ErrPermanent) {
		t.Errorf("unknown delivery: want ErrPermanent, got %v", err)
	}
}

func TestRecipientMirror_OutOfOrderProfileUpdateDoesNotRegress(t *testing.T) {
	env := testenv.Setup(t)
	user := uuid.New()
	name := func() (string, *string) {
		var r struct {
			Name  string
			Email *string
		}
		if err := env.DB.Raw(`SELECT name, email FROM notification.recipients WHERE user_id = ?`, user).Scan(&r).Error; err != nil {
			t.Fatal(err)
		}
		return r.Name, r.Email
	}

	// The profile update (outbox id 20) commits BEFORE the registration (10).
	mustApply(t, env.DB, evt("identity.user_profile_updated", 20, map[string]any{"user_id": user, "full_name": "Ada Lovelace"}))
	if n, e := name(); n != "Ada Lovelace" || e != nil {
		t.Fatalf("after the early update: name=%q email=%v", n, e)
	}
	mustApply(t, env.DB, evt("identity.user_registered", 10, map[string]any{"user_id": user, "email": "ada@example.com", "role": "student", "full_name": "Ada"}))
	if n, e := name(); n != "Ada Lovelace" || e == nil || *e != "ada@example.com" {
		t.Fatalf("the older registration regressed the name or lost the email: name=%q email=%v", n, e)
	}
	// And a stale update arriving last is ignored too.
	mustApply(t, env.DB, evt("identity.user_profile_updated", 15, map[string]any{"user_id": user, "full_name": "Stale"}))
	if n, _ := name(); n != "Ada Lovelace" {
		t.Fatalf("stale update applied: %q", n)
	}
	// A newer one wins.
	mustApply(t, env.DB, evt("identity.user_profile_updated", 30, map[string]any{"user_id": user, "full_name": "Ada King"}))
	if n, _ := name(); n != "Ada King" {
		t.Fatalf("newer update lost: %q", n)
	}
}

func TestUnknownRecipient_IsParkedThenDeliveredWhenItArrives(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New() // never registered yet
	prov := &recordingProvider{}
	handle := NewDeliveryHandler(env.DB, prov)
	enq := &memEnqueuer{}

	// Does not crash, does not fail the event, does not lose the mail.
	mustApply(t, env.DB, evt("enrollment.student_enrolled", 1, map[string]any{
		"enrollment_id": uuid.New(), "student_id": student, "course_id": uuid.New()}))
	d := deliveries(t, env.DB)
	if len(d) != 1 || d[0].Status != "parked" {
		t.Fatalf("want 1 parked delivery, got %+v", d)
	}
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 0 {
		t.Fatalf("a parked row must not be enqueued: n=%d err=%v", n, err)
	}
	// Even if a task for it runs anyway, it stays parked and sends nothing.
	if err := handle(context.Background(), DeliveryPayload(d[0].ID)); err != nil {
		t.Fatalf("parked delivery should be a no-op: %v", err)
	}
	if len(prov.sends()) != 0 {
		t.Fatal("sent to an unknown recipient")
	}

	// The recipient's registration arrives: parked mail becomes sendable.
	register(t, env.DB, student, "late@example.com", "Late", 5)
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 2 { // enrollment + welcome
		t.Fatalf("sweep after registration: n=%d err=%v", n, err)
	}
	for _, task := range enq.drain() {
		if err := handle(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	sends := prov.sends()
	if len(sends) != 2 || sends[0].To != "late@example.com" {
		t.Fatalf("want 2 sends to late@example.com, got %+v", sends)
	}
}

// A parked row inserted just AFTER its recipient's own promotion ran would
// otherwise stay parked forever; the sweep promotes it.
func TestSweep_PromotesParkedRowWhoseRecipientAlreadyExists(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	mustApply(t, env.DB, evt("enrollment.student_enrolled", 1, map[string]any{
		"enrollment_id": uuid.New(), "student_id": student, "course_id": uuid.New()}))
	if err := env.DB.Exec(`INSERT INTO notification.recipients (user_id, email, name) VALUES (?, 'x@example.com', 'X')`, student).Error; err != nil {
		t.Fatal(err)
	}
	enq := &memEnqueuer{}
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want the parked row promoted and enqueued", n, err)
	}
}

func TestSweep_FailsRowsThatExhaustedTheirAttempts(t *testing.T) {
	env := testenv.Setup(t)
	register(t, env.DB, uuid.New(), "ada@example.com", "Ada", 1)
	if err := env.DB.Exec(`UPDATE notification.deliveries SET attempts = 3`).Error; err != nil {
		t.Fatal(err)
	}
	enq := &memEnqueuer{}
	if n, _ := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{MaxAttempts: 3}); n != 0 {
		t.Fatalf("exhausted row enqueued (%d)", n)
	}
	if d := deliveries(t, env.DB)[0]; d.Status != "failed" {
		t.Fatalf("status %s, want failed", d.Status)
	}
}

// End to end: outbox -> real relay -> handlers -> sweep -> delivery task ->
// real SMTP provider -> fake SMTP server. The recipient's name carries a
// header-injection attempt.
func TestEndToEnd_OutboxToSMTP_WithHeaderInjectionInName(t *testing.T) {
	env := testenv.Setup(t)
	fake := newFakeSMTP(t)
	host, port := fake.hostPort()
	prov, err := NewSMTPProvider(config.SMTPConfig{Host: host, Port: port, From: "no-reply@smartcourse.local",
		ConnectTimeout: time.Second, SendTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	for _, h := range Handlers() {
		rl.Register(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	student, instructor, course, enrollment := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	write := func(aggType string, aggID uuid.UUID, eventType string, payload map[string]any) {
		t.Helper()
		if err := env.DB.Transaction(func(tx *gorm.DB) error {
			return outbox.Write(context.Background(), tx, outbox.Event{
				AggregateType: aggType, AggregateID: aggID, EventType: eventType, SchemaVersion: 1, Payload: payload})
		}); err != nil {
			t.Fatal(err)
		}
	}
	evil := "Mallory\r\nBcc: victim@evil.test\r\n\r\n<script>x</script>"
	write("user", instructor, "identity.user_registered", map[string]any{"user_id": instructor, "email": "teacher@example.com", "role": "instructor", "full_name": "Grace"})
	write("user", student, "identity.user_registered", map[string]any{"user_id": student, "email": "mallory@example.com", "role": "student", "full_name": evil})
	write("course", course, "course.course_published", map[string]any{"course_id": course, "version_id": uuid.New(), "title": "Go 101", "instructor_id": instructor})
	write("enrollment", enrollment, "enrollment.student_enrolled", map[string]any{"enrollment_id": enrollment, "student_id": student, "course_id": course})
	write("enrollment", enrollment, "progress.course_completed", map[string]any{"enrollment_id": enrollment, "student_id": student})
	const wantMails = 5 // 2 welcome + published + enrolled + completed

	// The Enqueuer here is a queue of its own: tasks run on another goroutine
	// (a task run inline would wait on the row lock the sweep still holds).
	handle := NewDeliveryHandler(env.DB, prov)
	enq := enqueuerFunc(func(ctx context.Context, _ string, payload []byte) error {
		go func() {
			if err := handle(context.WithoutCancel(ctx), payload); err != nil {
				t.Logf("delivery task: %v", err)
			}
		}()
		return nil
	})
	deadline := time.After(30 * time.Second)
	for len(fake.Messages()) < wantMails {
		if _, err := EnqueuePending(ctx, env.DB, enq, SweepOptions{RequeueAfter: time.Hour}); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d mails arrived", len(fake.Messages()), wantMails)
		case <-time.After(100 * time.Millisecond):
		}
	}
	time.Sleep(300 * time.Millisecond) // a duplicate would show up now
	msgs := fake.Messages()
	if len(msgs) != wantMails {
		t.Fatalf("want exactly %d mails, got %d", wantMails, len(msgs))
	}
	for _, m := range msgs {
		if len(m.to) != 1 {
			t.Fatalf("recipient list widened: %v", m.to)
		}
		if strings.Contains(m.data, "\r\nBcc:") {
			t.Fatalf("injected Bcc header: %q", m.data)
		}
	}
	var sawTitle bool
	for _, m := range msgs {
		if strings.Contains(m.data, "Go 101") {
			sawTitle = true
		}
	}
	if !sawTitle {
		t.Fatal("no mail mentions the course title")
	}
	for _, d := range deliveries(t, env.DB) {
		if d.Status != "sent" {
			t.Fatalf("delivery %s is %s, want sent", d.ID, d.Status)
		}
	}
}

type enqueuerFunc func(ctx context.Context, taskType string, payload []byte) error

func (f enqueuerFunc) Enqueue(ctx context.Context, taskType string, payload []byte) error {
	return f(ctx, taskType, payload)
}

// The migration's down path drops the schema and the up path rebuilds it
// (testenv applies every migration first).
func TestMigration_DownThenUp_RoundTrip(t *testing.T) {
	env := testenv.Setup(t)
	read := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "notification", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	schemaExists := func() bool {
		return count(t, env.DB, `SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'notification'`) == 1
	}
	if !schemaExists() {
		t.Fatal("notification schema missing after testenv setup")
	}
	if err := env.DB.Exec(read("000001_notification.down.sql")).Error; err != nil {
		t.Fatalf("down: %v", err)
	}
	if schemaExists() {
		t.Fatal("down left the notification schema behind")
	}
	if err := env.DB.Exec(read("000001_notification.up.sql")).Error; err != nil {
		t.Fatalf("up after down: %v", err)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'notification'`); n != 4 {
		t.Fatalf("want 4 tables after up, got %d", n)
	}
	// The dedupe invariant lives in the schema, not only in the handlers.
	id := uuid.New()
	ins := `INSERT INTO notification.deliveries (id, event_id, kind, recipient_id) VALUES (?, ?, 'welcome', ?)`
	ev, rc := uuid.New(), uuid.New()
	if err := env.DB.Exec(ins, uuid.New(), ev, rc).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(ins, id, ev, rc).Error; err == nil {
		t.Fatal("the UNIQUE (event_id, kind, recipient_id) constraint did not reject a duplicate")
	}
}
