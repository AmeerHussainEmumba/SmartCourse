//go:build e2e_legacy && bugrepro

package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestAdversarial_MongoOutage_BeyondDefaultRetryBudget_LosesEvents (B-06, B-07)
//
// ADR-0031: "A parked event is never dropped" and "A MongoDB outage degrades
// the archiver and the failed-events view only". E13 proves the outage case
// only with a retry budget of 12 attempts (about 19 s) that the scenario sets
// itself so that it outlasts the outage, and E04 parks with MongoDB up. This
// test uses the production defaults (5 attempts, 200 ms to 10 s backoff) and a
// 45 s outage:
//
//	archive: the audit-archiver's handler fails on every attempt, the record
//	  exhausts its budget, the sink (also MongoDB) refuses it, and the runner
//	  advances the offset: the event is in neither events nor failed_events
//	  after MongoDB returns.
//	park: a poison event handled while MongoDB is down is lost the same way.
//
// Expected: after MongoDB returns, every event is either archived or parked.
// Run: cd test/e2e && GOWORK=off go test -tags='e2e bugrepro' -count=1 -v -run TestAdversarial_MongoOutage .
func TestAdversarial_MongoOutage_BeyondDefaultRetryBudget_LosesEvents(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	student := s.NewStudent(t)
	course := s.PublishedCourse(t, inst, 1)
	enrollment := s.Enroll(t, student, course)
	s.WaitSettled(t, time.Minute)

	s.StopMongo(t)
	late := s.NewStudent(t)
	enrLate := s.Enroll(t, late, course)
	var enrolledEvent string
	harness.Eventually(t, time.Minute, "the StudentEnrolled event in the outbox", func() error {
		var err error
		enrolledEvent, err = s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrLate.String())
		return err
	})
	ghostEnrollment := uuid.New()
	payload := fmt.Sprintf(`{"enrollment_id":%q,"student_id":%q,"completed_at":%q}`,
		ghostEnrollment, uuid.New(), time.Now().UTC().Format(time.RFC3339))
	poisonID := s.NewEventProducer(t).Produce(t, "progress.course_completed", enrollment, payload, 1<<40)

	time.Sleep(150 * time.Second) // far longer than the default retry budget plus parking tries
	s.StartMongo(t)
	s.WaitSettled(t, 2*time.Minute)
	t.Logf("worker log: exhausted=%d handler_failed=%d could_not_park=%d parked=%d", s.Worker.LogCount("exhausted its retry budget"),
		s.Worker.LogCount("handler failed"), s.Worker.LogCount("could not park"), s.Worker.LogCount("parked a record"))

	t.Run("archive", func(t *testing.T) {
		harness.Eventually(t, 90*time.Second, "the event archived or parked after MongoDB returned", func() error {
			a, _ := s.Mongo.ArchivedEvents(enrolledEvent)
			f, _ := s.Mongo.FailedEvents(enrolledEvent)
			if a == 0 && len(f) == 0 {
				return harness.Errf("event %s is neither archived nor parked: lost during the outage", enrolledEvent)
			}
			return nil
		})
	})
	t.Run("park", func(t *testing.T) {
		harness.Eventually(t, 90*time.Second, "the poison event parked after MongoDB returned", func() error {
			f, _ := s.Mongo.FailedEvents(poisonID)
			if len(f) == 0 {
				return harness.Errf("poison event %s is not in failed_events: dropped while the sink was down", poisonID)
			}
			return nil
		})
	})
}
