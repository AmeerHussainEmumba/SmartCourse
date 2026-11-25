//go:build integration

// The trace seam of the notification path (ADR-0032), against real Postgres:
// the handler that writes a delivery row stores the traceparent of the span it
// runs under, the sweeper enqueues each row under that stored trace (a row
// without one is enqueued in no trace), the payload the queue carries is
// unchanged, and migration 000002 reverses cleanly.
package notification

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func enableTracing(t *testing.T) {
	t.Helper()
	tp, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "test", SampleRatio: 1}, tracetest.NewInMemoryExporter())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
}

func storedTraceparent(t *testing.T, env *testenv.Env, id uuid.UUID) *string {
	t.Helper()
	var rows []struct{ Traceparent *string }
	if err := env.DB.Raw(`SELECT traceparent FROM notification.deliveries WHERE id = ?`, id).Scan(&rows).Error; err != nil || len(rows) != 1 {
		t.Fatalf("read traceparent: %v (%d rows)", err, len(rows))
	}
	return rows[0].Traceparent
}

func TestDelivery_StoresTheTraceparentOfTheWritingSpan(t *testing.T) {
	enableTracing(t)
	env := setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1) // written with no span in ctx

	enrollment := evt("progress.course_completed", 2, map[string]any{"enrollment_id": uuid.New(), "student_id": student})
	ctx, span := telemetry.StartConsume(context.Background(), "", "notification-dispatcher", "progress.events", enrollment.EventType, enrollment.EventID.String())
	defer span.End()
	_, _, want := telemetry.InjectTraceContext(ctx)
	if want == "" {
		t.Fatal("test setup: no traceparent for the consume span")
	}
	h := handlerFor(t, "progress.course_completed")
	if err := env.DB.Transaction(func(tx *gorm.DB) error { return h.Handle(ctx, tx, enrollment) }); err != nil {
		t.Fatal(err)
	}

	var withTrace, without int
	for _, d := range deliveries(t, env.DB) {
		got := storedTraceparent(t, env, d.ID)
		switch d.Kind {
		case KindCourseCompleted:
			withTrace++
			if got == nil || *got != want {
				t.Errorf("delivery %s stored traceparent %v, want %s", d.Kind, got, want)
			}
		default:
			without++
			if got != nil {
				t.Errorf("delivery %s written with no span stored traceparent %q, want NULL", d.Kind, *got)
			}
		}
	}
	if withTrace != 1 || without != 1 {
		t.Fatalf("want one traced and one untraced delivery, got %d and %d", withTrace, without)
	}
}

func TestEnqueuePending_EnqueuesUnderTheStoredTrace(t *testing.T) {
	env := setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	mustApply(t, env.DB, evt("progress.course_completed", 2, map[string]any{"enrollment_id": uuid.New(), "student_id": student}))

	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	stored := telemetry.TraceparentFromIDs(tid, sid)
	if err := env.DB.Exec(`UPDATE notification.deliveries SET traceparent = ? WHERE kind = ?`, stored, KindCourseCompleted).Error; err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	got := map[string]trace.SpanContext{} // payload -> span context of the enqueue call
	enq := enqueuerFunc(func(ctx context.Context, _ string, payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		got[string(payload)] = trace.SpanContextFromContext(ctx)
		return nil
	})
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 2 {
		t.Fatalf("sweep: n=%d err=%v, want 2", n, err)
	}
	for _, d := range deliveries(t, env.DB) {
		sc := got[string(DeliveryPayload(d.ID))]
		if d.Kind == KindCourseCompleted {
			if sc.TraceID().String() != tid || sc.SpanID().String() != sid || !sc.IsRemote() {
				t.Errorf("traced delivery enqueued under %v, want remote %s/%s", sc, tid, sid)
			}
		} else if sc.IsValid() {
			t.Errorf("untraced delivery enqueued under %v, want no trace", sc)
		}
	}
}

// The queue payload is the contract of NewDeliveryHandler and old queued tasks;
// the trace rides in the task headers, never in it.
func TestDeliveryPayload_IsUnchanged(t *testing.T) {
	id := uuid.MustParse("0192f3a0-0000-7000-8000-000000000001")
	if got, want := string(DeliveryPayload(id)), `{"delivery_id":"0192f3a0-0000-7000-8000-000000000001"}`; got != want {
		t.Errorf("DeliveryPayload = %s, want %s", got, want)
	}
}

func TestMigration000002_DownDropsTheColumnUpRestoresIt(t *testing.T) {
	env := testenv.Setup(t) // the owner: this test runs DDL
	read := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "notification", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	hasColumn := func() bool {
		return count(t, env.DB, `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'notification' AND table_name = 'deliveries' AND column_name = 'traceparent'`) == 1
	}
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	if !hasColumn() {
		t.Fatal("traceparent missing after testenv applied every migration")
	}
	if err := env.DB.Exec(read("000002_delivery_traceparent.down.sql")).Error; err != nil {
		t.Fatalf("down: %v", err)
	}
	if hasColumn() {
		t.Fatal("down left the traceparent column behind")
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.recipients`); n != 1 {
		t.Fatalf("down touched other data: %d recipients, want 1", n)
	}
	if err := env.DB.Exec(read("000002_delivery_traceparent.up.sql")).Error; err != nil {
		t.Fatalf("up after down: %v", err)
	}
	if !hasColumn() {
		t.Fatal("up did not restore the column")
	}
	if err := env.DB.Exec(read("000002_delivery_traceparent.up.sql")).Error; err != nil {
		t.Fatalf("up is not idempotent: %v", err)
	}
}
