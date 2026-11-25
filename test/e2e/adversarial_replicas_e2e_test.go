//go:build e2e

package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestAdversarial_TwoWorkerReplicas_KilledInTurn_KeepEveryInvariant
//
// Every Week 3 E2E scenario runs ONE worker. This one runs two replicas (the
// harness worker and a second process on the same Postgres, Kafka, Redis and
// MongoDB: two relays on one outbox, two members of each consumer group, two
// Asynq servers, two schedulers, two notification sweepers), kills each with
// SIGKILL in turn while enrollments and lesson completions are flowing, and
// then checks the invariants the ADRs claim:
//
//   - no event is lost: the outbox drains, every event id is archived once;
//   - no effect is doubled: analytics counts, one processed_events row per
//     (consumer, event), exactly one certificate per completed enrollment;
//   - no half state: no failed_events, no stuck notification deliveries;
//   - the duplicate-send window stays bounded: at most one enrollment mail per
//     student is expected (a kill between SMTP accept and the delivery mark may
//     legitimately send a second; the test records but does not allow more
//     than that).
//
// Run: cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -v -run TestAdversarial_TwoWorkerReplicas .
func TestAdversarial_TwoWorkerReplicas_KilledInTurn_KeepEveryInvariant(t *testing.T) {
	const students = 18
	s := harness.Start(t, harness.Options{})
	b := s.NewWorkerProc(t, nil)
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	harness.Eventually(t, 2*time.Minute, "replica B started its publish worker", func() error {
		if b.LogCount("publish worker started") == 0 {
			return harness.Errf("not started")
		}
		return nil
	})

	inst := s.NewInstructor(t)
	course := s.PublishedCourse(t, inst, 2)
	users := s.NewStudents(t, students, 6)
	s.WaitSettled(t, time.Minute)

	enrollments := make([]uuid.UUID, students)
	for i, u := range users {
		enrollments[i] = s.Enroll(t, u, course)
		switch i {
		case 5:
			s.KillWorker() // replica A dies with events in flight
		case 11:
			if err := s.Worker.Start(); err != nil { // A returns, B dies
				t.Fatal(err)
			}
			b.Kill()
		}
	}
	for i, u := range users {
		s.CompleteAll(t, u, enrollments[i])
		if i == 8 {
			if err := b.Start(); err != nil { // B returns, A dies again
				t.Fatal(err)
			}
			s.KillWorker()
		}
		if i == 14 {
			if err := s.Worker.Start(); err != nil {
				t.Fatal(err)
			}
		}
	}

	harness.Eventually(t, 6*time.Minute, "the outbox to drain and every group to catch up", func() error {
		n, err := s.OutboxUnpublished()
		if err != nil || n != 0 {
			return harness.Errf("%d unpublished outbox rows (err %v)", n, err)
		}
		for _, g := range harness.ConsumerGroups {
			if lag, err := s.ConsumerLag(g); err != nil || lag != 0 {
				return harness.Errf("group %s lag %d (err %v)", g, lag, err)
			}
		}
		return nil
	})

	t.Run("analytics counts are exact", func(t *testing.T) {
		harness.Eventually(t, 2*time.Minute, "course stats", func() error {
			cs := s.CourseStatsFor(t, inst, course)
			if cs.TotalEnrollments != students || cs.Completions != students {
				return harness.Errf("course stats %+v, want %d enrollments and %d completions", cs, students, students)
			}
			return nil
		})
	})
	t.Run("one certificate per student", func(t *testing.T) {
		for i, u := range users {
			if got := len(s.Certificates(t, u)); got != 1 {
				t.Errorf("student %d has %d certificates, want 1", i, got)
			}
		}
	})
	t.Run("archive has every event once and nothing is parked", func(t *testing.T) {
		harness.Eventually(t, 2*time.Minute, "archive complete", func() error {
			rows, err := s.DB().Raw(`SELECT event_id::text FROM platform.outbox`).Rows()
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			missing, doubled := 0, 0
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				n, err := s.Mongo.ArchivedEvents(id)
				if err != nil {
					return err
				}
				if n == 0 {
					missing++
				} else if n > 1 {
					doubled++
				}
			}
			if missing != 0 || doubled != 0 {
				return harness.Errf("%d events missing from the archive, %d archived twice", missing, doubled)
			}
			return nil
		})
		if n, err := s.Mongo.Count("failed_events", nil); err != nil || n != 0 {
			t.Errorf("failed_events holds %d documents (err %v), want 0", n, err)
		}
	})
	t.Run("enrollment mails are not doubled beyond the documented window", func(t *testing.T) {
		harness.Eventually(t, 3*time.Minute, "every student has a mail", func() error {
			for _, u := range users {
				msgs, err := s.Mailpit.MessagesToWithSubject(u.Email, "enrolled")
				if err != nil || len(msgs) == 0 {
					return harness.Errf("%s: %d mails (err %v)", u.Email, len(msgs), err)
				}
			}
			return nil
		})
		dups := 0
		for _, u := range users {
			msgs, _ := s.Mailpit.MessagesToWithSubject(u.Email, "enrolled")
			if len(msgs) > 2 {
				t.Errorf("%s got %d enrollment mails, want 1 (2 only inside the documented duplicate-send window)", u.Email, len(msgs))
			}
			if len(msgs) == 2 {
				dups++
			}
		}
		t.Logf("students with a duplicate enrollment mail: %d of %d", dups, students)
	})
	t.Run("no delivery is stuck", func(t *testing.T) {
		harness.Eventually(t, 7*time.Minute, "no pending or sending delivery", func() error {
			n, err := s.Count(`SELECT count(*) FROM notification.deliveries WHERE status IN ('pending','sending')`)
			if err != nil || n != 0 {
				return harness.Errf("%d deliveries pending or sending (err %v)", n, err)
			}
			return nil
		})
	})
	t.Run("processed_events has one row per consumer and event", func(t *testing.T) {
		n := s.MustCount(t, `SELECT count(*) FROM (SELECT consumer_name, event_id FROM platform.processed_events GROUP BY 1,2 HAVING count(*) > 1) d`)
		if n != 0 {
			t.Errorf("%d (consumer, event) pairs have more than one processed_events row", n)
		}
	})
	_ = fmt.Sprint
}
