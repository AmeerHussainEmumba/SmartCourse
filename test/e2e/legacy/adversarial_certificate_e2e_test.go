//go:build e2e_legacy

package e2e_test

import (
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestAdversarial_CertificateNeedsNameMirror_SurvivesAMirrorStall
//
// This one HELD in the run recorded in the register (a 90 s stall; not bisected
// further): it is a regression guard, not a defect reproducer.
//
// The certificate-issuer needs the student's display name from
// course.instructor_mirror, which a DIFFERENT consumer group
// (course-instructor-mirror) fills from identity.events. The two groups read
// different topics with no ordering between them (ADR-0029 "Known gaps" (2)).
// After any worker downtime every group replays its backlog at once, so the
// issuer can reach course_completed before the mirror has the student.
//
// The issuer treats the missing name as a retryable error, but the retry
// budget is 8 attempts with 500 ms to 30 s backoff (the defaults), about a
// minute. The test holds the mirror back for 90 s (an uncommitted conflicting row makes the
// mirror's insert wait), which a lagging group, a slow query or a restart
// reproduces without help. The issuer exhausts its budget, parks the event and
// moves on; nobody is told and the student never receives the certificate
// unless an admin replays the parked event by hand.
//
// Expected: once the mirror has the student the certificate is issued without
// operator action.
// Run: cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -v -run TestAdversarial_CertificateNeedsNameMirror .
func TestAdversarial_CertificateNeedsNameMirror_SurvivesAMirrorStall(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	course := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)

	s.KillWorker() // downtime: events pile up in the outbox
	student := s.NewStudent(t)
	enrollment := s.Enroll(t, student, course)
	s.CompleteAll(t, student, enrollment)

	// The mirror's insert for this student will wait on this uncommitted row.
	lock := s.HoldRowLock(t, `INSERT INTO course.instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'held', 0) RETURNING 1`, student.ID)
	s.StartWorker(t)
	time.Sleep(90 * time.Second)
	lock.Release() // the mirror catches up now

	harness.Eventually(t, 2*time.Minute, "the student's certificate without operator action", func() error {
		if n := len(s.Certificates(t, student)); n != 1 {
			failed, _ := s.Mongo.Count("failed_events", nil)
			return harness.Errf("%d certificates; %d events parked in failed_events (the issuer gave up before the mirror caught up)", n, failed)
		}
		return nil
	})
}
