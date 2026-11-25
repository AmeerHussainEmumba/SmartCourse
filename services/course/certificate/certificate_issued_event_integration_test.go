//go:build integration

// What this file checks, against real Postgres: progress.certificate_issued is
// written in the same transaction as the certificate row and only when the
// outcome is `issued`: a replay, a concurrent delivery that lost the race, a
// skipped event and a rolled-back transaction write none. The payload carries
// no verification code.
package certificate_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

func issuedEvents(t *testing.T, db *gorm.DB, enrollmentID uuid.UUID) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(`SELECT count(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?`,
		certificate.EventCertificateIssued, enrollmentID).Row().Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIssue_WritesCertificateIssuedWithTheCertificate(t *testing.T) {
	env := testenv.Setup(t)
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatal(err)
	}
	if n := issuedEvents(t, env.DB, f.enrollmentID); n != 1 {
		t.Fatalf("certificate_issued events: %d, want 1", n)
	}
	var raw []byte
	var aggregateType string
	if err := env.DB.Raw(`SELECT payload, aggregate_type FROM outbox WHERE event_type = ? AND aggregate_id = ?`,
		certificate.EventCertificateIssued, f.enrollmentID).Row().Scan(&raw, &aggregateType); err != nil {
		t.Fatal(err)
	}
	if aggregateType != "enrollment" {
		t.Errorf("aggregate_type = %q, want enrollment", aggregateType)
	}
	var p certificate.CertificateIssuedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	list, err := certificate.NewService(certificate.NewRepository(env.DB)).ListMine(context.Background(), f.studentID)
	if err != nil || len(list) != 1 {
		t.Fatalf("certificates: %d (err %v)", len(list), err)
	}
	c := list[0]
	if p.CertificateID != c.ID || p.EnrollmentID != f.enrollmentID || p.StudentID != f.studentID || p.CourseID != f.courseID ||
		p.CourseTitle != "Go Concurrency" || !p.IssuedAt.Equal(c.IssuedAt) {
		t.Errorf("payload %+v does not match certificate %+v", p, c)
	}
	if strings.Contains(string(raw), c.VerificationCode) || strings.Contains(string(raw), "verification") {
		t.Errorf("payload leaks the verification code: %s", raw)
	}
}

func TestIssue_ReplayAndSkippedEventsWriteNoCertificateIssued(t *testing.T) {
	env := testenv.Setup(t)
	f := seedCompletedEnrollment(t, env)
	evt := completedEvent(t, f.enrollmentID, f.studentID)
	for range 3 {
		if err := deliver(t, env.DB, evt); err != nil {
			t.Fatal(err)
		}
	}
	// A producer-side duplicate with a new event id is also a no-op.
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatal(err)
	}
	if n := issuedEvents(t, env.DB, f.enrollmentID); n != 1 {
		t.Fatalf("certificate_issued events after 4 deliveries: %d, want 1", n)
	}

	w := seedEnrollment(t, env, "withdrawn", "Grace Hopper")
	if err := deliver(t, env.DB, completedEvent(t, w.enrollmentID, w.studentID)); err != nil {
		t.Fatal(err)
	}
	if err := deliver(t, env.DB, completedEvent(t, uuid.New(), uuid.New())); err != nil {
		t.Fatal(err)
	}
	var total int64
	if err := env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = ?`, certificate.EventCertificateIssued).Row().Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("certificate_issued events in the outbox: %d, want 1 (skipped events must write none)", total)
	}
}

func TestIssue_TwentyConcurrentDeliveriesWriteOneCertificateIssued(t *testing.T) {
	env := testenv.Setup(t)
	f := seedCompletedEnrollment(t, env)
	evt := completedEvent(t, f.enrollmentID, f.studentID)

	const n = 20
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := deliver(t, env.DB, evt); err != nil {
				t.Errorf("deliver: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := issuedEvents(t, env.DB, f.enrollmentID); got != 1 {
		t.Fatalf("certificate_issued events from %d concurrent deliveries: %d, want 1", n, got)
	}
}

// The event and the certificate commit together: a transaction that fails after
// issuing leaves neither (ADR-0006).
func TestIssue_RollbackLeavesNeitherCertificateNorEvent(t *testing.T) {
	env := testenv.Setup(t)
	f := seedCompletedEnrollment(t, env)
	hs := certificate.Handlers()
	boom := errors.New("later step failed")
	err := env.DB.Transaction(func(tx *gorm.DB) error {
		if err := hs[0].Handle(context.Background(), tx, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 0 {
		t.Errorf("certificates after rollback: %d", n)
	}
	if n := issuedEvents(t, env.DB, f.enrollmentID); n != 0 {
		t.Errorf("certificate_issued events after rollback: %d", n)
	}
}
