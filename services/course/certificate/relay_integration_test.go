//go:build integration

// What this file checks, through the real libs/relay worker pool rather than
// by calling the handler directly: a progress.course_completed outbox row
// becomes one certificate; replaying the row (marker present) and replaying
// it with the marker gone (UNIQUE alone) both leave one row; many outbox rows
// for the same enrollment, claimed by parallel workers under different
// event_ids so processed_events cannot help, still yield one certificate; and
// a poison row for an unknown enrollment does not stop the rows behind it.
package certificate_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay/relaytest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

func writeCompleted(t *testing.T, db *gorm.DB, enrollmentID, studentID uuid.UUID) {
	t.Helper()
	err := db.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "enrollment", AggregateID: enrollmentID,
			EventType: "progress.course_completed", SchemaVersion: 1,
			Payload: map[string]any{"enrollment_id": enrollmentID, "student_id": studentID, "completed_at": time.Now().UTC()},
		})
	})
	if err != nil {
		t.Fatalf("write outbox event: %v", err)
	}
}

// runRelayUntilDrained runs a relay with the certificate handlers until every
// outbox row is published, or fails the test after 10s.
func runRelayUntilDrained(t *testing.T, db *gorm.DB, workers int) {
	t.Helper()
	rl := relay.New(db, relaytest.DeliverTo(db, certificate.Handlers()...), relay.WithWorkerCount(workers), relay.WithPollInterval(20*time.Millisecond), relay.WithMaxAttempts(2))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var pending int64
		db.Raw(`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Row().Scan(&pending)
		if pending == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("relay did not drain the outbox within 10s")
}

func TestRelay_CompletionEvent_IssuesOnceAndReplayIsNoOp(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)

	writeCompleted(t, env.DB, f.enrollmentID, f.studentID)
	runRelayUntilDrained(t, env.DB, 2)
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("expected 1 certificate after the first delivery, got %d", n)
	}
	var marks int64
	env.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = 'certificate-issuer'`).Row().Scan(&marks)
	if marks != 1 {
		t.Fatalf("expected the processed_events marker to be written with the certificate, got %d", marks)
	}

	// Replay 1: the outbox row is redelivered, the marker is present.
	mustExec(t, env.DB, `UPDATE outbox SET published_at = NULL`)
	runRelayUntilDrained(t, env.DB, 2)
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("replay with marker produced %d certificates", n)
	}

	// Replay 2: marker lost (e.g. pruned), UNIQUE alone must hold the line.
	mustExec(t, env.DB, `DELETE FROM processed_events WHERE consumer_name = 'certificate-issuer'`)
	mustExec(t, env.DB, `UPDATE outbox SET published_at = NULL`)
	runRelayUntilDrained(t, env.DB, 2)
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("replay without marker produced %d certificates", n)
	}
}

func TestRelay_ManyDuplicateEventsAcrossWorkers_OneCertificate(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	for i := 0; i < 20; i++ {
		writeCompleted(t, env.DB, f.enrollmentID, f.studentID) // 20 distinct event_ids, one enrollment
	}
	runRelayUntilDrained(t, env.DB, 8)
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("expected exactly 1 certificate from 20 duplicate events, got %d", n)
	}
	var published, failed int64
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'progress.course_completed' AND published_at IS NOT NULL AND last_error IS NULL`).Row().Scan(&published)
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE last_error IS NOT NULL`).Row().Scan(&failed)
	if published != 20 || failed != 0 {
		t.Fatalf("every duplicate must be a clean no-op: published=%d with_error=%d", published, failed)
	}
}

func TestRelay_UnknownAndWithdrawnEnrollmentDoNotBlockTheStream(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	withdrawn := seedEnrollment(t, env, "withdrawn", "Ada Lovelace")
	good := seedCompletedEnrollment(t, env)

	writeCompleted(t, env.DB, uuid.New(), uuid.New())
	writeCompleted(t, env.DB, withdrawn.enrollmentID, withdrawn.studentID)
	writeCompleted(t, env.DB, good.enrollmentID, good.studentID)
	runRelayUntilDrained(t, env.DB, 1)

	if n := countCertificates(t, env.DB, `enrollment_id = ?`, good.enrollmentID); n != 1 {
		t.Fatalf("the valid event behind the skipped ones must still be issued, got %d", n)
	}
	if n := countCertificates(t, env.DB, `true`); n != 1 {
		t.Fatalf("only the valid enrollment may get a certificate, got %d in total", n)
	}
	var withErr int64
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE last_error IS NOT NULL`).Row().Scan(&withErr)
	if withErr != 0 {
		t.Fatalf("skipped events must be acknowledged, not retried into last_error; %d rows have errors", withErr)
	}
}
