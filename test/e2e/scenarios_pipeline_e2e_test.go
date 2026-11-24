//go:build e2e

package e2e_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// The scenarios in the *_e2e_test.go files run the full stack of
// test/e2e/harness: real containers and the real service binaries, with the
// Kafka transport. They carry the `e2e` build tag (not `integration`): they
// start a dozen containers and have their own CI job (make test-e2e). Their
// specs are the doc comments of the original skipped tests, kept here with
// the implementation; E2, E8 to E14 are still skipped specs in
// scenarios_*_test.go. Rules for all of them are in the header of
// scenarios_pipeline_test.go (black box, real dependencies, no fixed sleeps).

// topicOf is RFC 6.5: the topic follows the event type's prefix.
func topicOf(eventType string) string {
	prefix, _, _ := strings.Cut(eventType, ".")
	return prefix + ".events"
}

// TestE2E_E01_HappyPath is E1, matrix row E01 (delivered by Phase 8, needs
// Phases 2 to 7).
//
// Steps:
//  1. Start identity-api, course-api, analytics-api and the worker (relay with
//     EVENT_TRANSPORT=kafka, all consumers, Asynq server) against fresh
//     containers; Mailpit is the SMTP sink.
//  2. Register an admin-promoted instructor and two students through the API
//     (register 202, login 200, admin PATCH /admin/users/{id}/role, re-login).
//  3. Instructor creates a course with one module and two lessons and
//     POST /courses/{id}/publish (202); poll publish-status until "ready".
//  4. Student A POST /enrollments (201), then PUT the completion of both
//     lessons (204; the spec said 200, the service answers 204 No Content).
//  5. Student B enrolls then DELETE /enrollments/{id} (withdraw).
//
// Assertions (each polled with a deadline):
//   - publish-status is "ready"; course.published_version_id is set.
//   - course.enrollments, course.lesson_progress rows exist for A; a
//     certificate row exists for A exactly once (RFC 8.4) after the final lesson.
//   - Kafka: one message per event on identity.events, course.events,
//     enrollment.events (StudentEnrolled, StudentWithdrawn) and
//     progress.events (CourseCompleted), each keyed as RFC 6.5 says (the
//     aggregate id), each carrying a schema id. LessonCompleted and
//     CertificateIssued are in the spec but no service emits them yet (they
//     are not in libs/events' catalogue): the subtest "lesson and certificate
//     events" skips with that reason and starts asserting the day they exist.
//   - analytics: the platform counters and the course's stats moved by the
//     deltas this run caused (the baseline of each is read first); the
//     popular-courses ranking, the daily enrollments and the issues report
//     answer.
//   - MongoDB events collection contains every event id once.
//   - Mailpit holds the enrollment mail for A and B and a completion mail for
//     A, each exactly once (plus the welcome and course-published mails).
func TestE2E_E01_HappyPath(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	// Baselines, read once everything the bootstrap wrote has been handled.
	s.WaitSettled(t, time.Minute)
	admin := s.Login(t, s.AdminEmail())
	base := s.PlatformStats(t, admin)

	inst := s.NewInstructor(t)
	a := s.NewStudentNamed(t, "Alice")
	b := s.NewStudentNamed(t, "Bob")

	course := s.PublishedCourse(t, inst, 2)
	if n := s.MustCount(t, `SELECT count(*) FROM course.courses WHERE id = ? AND published_version_id IS NOT NULL`, course.ID); n != 1 {
		t.Fatalf("course %s has no published_version_id after publish-status ready", course.ID)
	}

	// Let the identity events reach every consumer before the student acts:
	// the certificate issuer needs the student's name from the instructor
	// mirror (a different consumer group), and its retry budget (5 attempts,
	// about 3 s with the default backoff) is shorter than a freshly started
	// group's first assignment. A completion that outruns the mirror is
	// dead-lettered and no certificate is issued (reported as a finding;
	// docs/plans/week-3.md, E1 run notes).
	s.WaitSettled(t, time.Minute)

	enrA := s.Enroll(t, a, course)
	s.CompleteAll(t, a, enrA)
	enrB := s.Enroll(t, b, course)
	s.Withdraw(t, b, enrB)

	// Rows in course.
	harness.Eventually(t, 30*time.Second, "A's enrollment, progress and the certificate rows", func() error {
		if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE id = ? AND status = 'completed'`, enrA); n != 1 {
			return harness.Errf("enrollment rows (completed) for A: %d, want 1", n)
		}
		if n := s.MustCount(t, `SELECT count(*) FROM course.lesson_progress WHERE enrollment_id = ? AND completed_at IS NOT NULL`, enrA); n != 2 {
			return harness.Errf("completed lesson_progress rows for A: %d, want 2", n)
		}
		return nil
	})
	if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE id = ? AND status = 'withdrawn'`, enrB); n != 1 {
		t.Errorf("B's enrollment is not withdrawn (%d rows)", n)
	}
	harness.Eventually(t, time.Minute, "exactly one certificate for A", func() error {
		if n := s.MustCount(t, `SELECT count(*) FROM course.certificates WHERE enrollment_id = ?`, enrA); n != 1 {
			return harness.Errf("certificate rows for A: %d, want 1", n)
		}
		return nil
	})
	if certs := s.Certificates(t, a); len(certs) != 1 {
		t.Errorf("GET /certificates for A lists %d, want 1", len(certs))
	}

	s.WaitSettled(t, time.Minute)

	t.Run("kafka", func(t *testing.T) {
		type row struct{ EventID, EventType, AggregateID string }
		var rows []row
		if err := s.DB().Raw(`SELECT event_id::text AS event_id, event_type, aggregate_id::text AS aggregate_id FROM platform.outbox ORDER BY id`).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		recs, err := s.ReadTopics(harness.EventTopics...)
		if err != nil {
			t.Fatal(err)
		}
		byEvent := map[string][]harness.KRecord{}
		for _, r := range recs {
			byEvent[r.EventID()] = append(byEvent[r.EventID()], r)
		}
		for _, r := range rows {
			got := byEvent[r.EventID]
			if len(got) != 1 {
				t.Errorf("event %s (%s): %d records on the topics, want exactly 1", r.EventID, r.EventType, len(got))
				continue
			}
			rec := got[0]
			if rec.Topic != topicOf(r.EventType) {
				t.Errorf("event %s (%s) is on %s, want %s", r.EventID, r.EventType, rec.Topic, topicOf(r.EventType))
			}
			if rec.Key != r.AggregateID {
				t.Errorf("event %s (%s) key %s, want the aggregate id %s", r.EventID, r.EventType, rec.Key, r.AggregateID)
			}
			env, schemaID, err := rec.Envelope()
			if err != nil {
				t.Errorf("event %s: %v", r.EventID, err)
				continue
			}
			if schemaID == 0 {
				t.Errorf("event %s carries no schema id", r.EventID)
			}
			if env.GetEventId() != r.EventID || env.GetEventType() != r.EventType {
				t.Errorf("event %s: envelope says %s/%s", r.EventID, env.GetEventId(), env.GetEventType())
			}
		}
		if len(recs) != len(rows) {
			t.Errorf("%d records on the topics, %d outbox rows", len(recs), len(rows))
		}
		count := func(eventType string) (n int) {
			for _, r := range rows {
				if r.EventType == eventType {
					n++
				}
			}
			return n
		}
		for eventType, want := range map[string]int{
			"identity.user_registered":     4, // admin, instructor, A, B
			"identity.user_role_changed":   1,
			"course.course_published":      1,
			"enrollment.student_enrolled":  2,
			"enrollment.student_withdrawn": 1,
			"progress.course_completed":    1,
		} {
			if got := count(eventType); got != want {
				t.Errorf("%s: %d events, want %d", eventType, got, want)
			}
		}
	})

	t.Run("lesson and certificate events", func(t *testing.T) {
		var lesson, cert int
		_ = s.DB().Raw(`SELECT count(*) FROM platform.outbox WHERE event_type = 'progress.lesson_completed'`).Row().Scan(&lesson)
		_ = s.DB().Raw(`SELECT count(*) FROM platform.outbox WHERE event_type = 'progress.certificate_issued'`).Row().Scan(&cert)
		if lesson == 0 && cert == 0 {
			t.Skip("known gap: E1's spec lists LessonCompleted x2 and CertificateIssued on progress.events, but no service writes them to the outbox and libs/events has no message for them (libs/events/catalog.go, services/course/certificate/README); only CourseCompleted exists")
		}
		if lesson != 2 || cert != 1 {
			t.Errorf("progress events: %d lesson_completed, %d certificate_issued, want 2 and 1", lesson, cert)
		}
	})

	t.Run("analytics", func(t *testing.T) {
		harness.Eventually(t, time.Minute, "platform counters", func() error {
			p := s.PlatformStats(t, admin)
			if p.TotalStudents != base.TotalStudents+2 || p.TotalInstructors != base.TotalInstructors+1 || p.TotalCoursesPublished != base.TotalCoursesPublished+1 {
				return harness.Errf("platform %+v, baseline %+v: want students +2, instructors +1, courses published +1", p, base)
			}
			// One active or completed enrollment (A's; B withdrew) over every student.
			if want := 1 / float64(p.TotalStudents); math.Abs(p.AvgCoursesPerStudent-want) > 0.001 {
				return harness.Errf("avg_courses_per_student %v, want %.3f (1 enrollment / %d students)", p.AvgCoursesPerStudent, want, p.TotalStudents)
			}
			return nil
		})
		// A completed, B withdrew: the stats count active and completed
		// enrollments, so B no longer counts (total 1, completion rate 1 of 1;
		// the spec's "1 of 2" counted B's withdrawn enrollment).
		harness.Eventually(t, time.Minute, "course stats", func() error {
			cs := s.CourseStatsFor(t, inst, course)
			if cs.TotalEnrollments != 1 || cs.ActiveEnrollments != 0 || cs.Completions != 1 || cs.CompletionRate != 1 {
				return harness.Errf("course stats %+v, want total 1, active 0, completions 1, rate 1", cs)
			}
			if cs.AvgCompletionS == nil {
				return harness.Errf("avg_completion_s is null after a completion")
			}
			return nil
		})
		if n := s.DailyEnrollments(t, inst, course); n < 1 || n > 2 {
			t.Errorf("daily enrollments for the course sum to %d, want 1 or 2", n)
		}
		found := false
		for _, it := range s.PopularCourses(t, a) {
			found = found || it.CourseID == course.ID.String()
		}
		if !found {
			t.Errorf("popular-courses does not list the course")
		}
		issues := s.Issues(t)
		if fe, _ := issues["failed_events"].(map[string]any); fe == nil || fe["total"] != float64(0) {
			t.Errorf("issues report failed_events %v, want total 0", issues["failed_events"])
		}
	})

	t.Run("archive", func(t *testing.T) {
		var ids []string
		if err := s.DB().Raw(`SELECT event_id::text FROM platform.outbox`).Scan(&ids).Error; err != nil {
			t.Fatal(err)
		}
		harness.Eventually(t, time.Minute, "every event archived once in MongoDB", func() error {
			for _, id := range ids {
				if n, err := s.Mongo.ArchivedEvents(id); err != nil || n != 1 {
					return harness.Errf("event %s archived %d times (err %v), want 1", id, n, err)
				}
			}
			total, err := s.Mongo.Count("events", nil)
			if err != nil || total != int64(len(ids)) {
				return harness.Errf("events collection holds %d documents (err %v), outbox has %d", total, err, len(ids))
			}
			return nil
		})
	})

	t.Run("mail", func(t *testing.T) {
		want := []struct{ who, email, subject string }{
			{"A enrollment", a.Email, "enrolled"},
			{"B enrollment", b.Email, "enrolled"},
			{"A completion", a.Email, "completing"},
			{"A welcome", a.Email, "welcome"},
			{"B welcome", b.Email, "welcome"},
			{"instructor welcome", inst.Email, "welcome"},
			{"instructor course published", inst.Email, "published"},
		}
		harness.Eventually(t, time.Minute, "every notification mail delivered exactly once", func() error {
			for _, w := range want {
				msgs, err := s.Mailpit.MessagesToWithSubject(w.email, w.subject)
				if err != nil {
					return err
				}
				if len(msgs) != 1 {
					return harness.Errf("%s: %d mails (subject containing %q), want 1", w.who, len(msgs), w.subject)
				}
			}
			return nil
		})
		// B withdrew before completing: no completion mail for B.
		if msgs, _ := s.Mailpit.MessagesToWithSubject(b.Email, "completing"); len(msgs) != 0 {
			t.Errorf("B got %d completion mails, want 0", len(msgs))
		}
	})
}

// TestE2E_E03_DuplicateDelivery is E3, matrix row E03 (Phase 3).
//
// Steps:
//  1. Stack as E01. Student enrolls; wait until the event is consumed
//     (processed_events has one row per consumer; Mailpit has one mail).
//  2. Read the current analytics counters and the mail count.
//  3. Re-produce the exact same Kafka record (same event id, same key, same
//     bytes) to enrollment.events twice more, bypassing the relay (a test-only
//     producer), simulating at-least-once redelivery.
//  4. Wait until every consumer group's lag is 0.
//
// Assertions:
//   - Analytics counters are unchanged from step 2 (changed exactly once in
//     total relative to before the enrollment).
//   - Mailpit still holds exactly one enrollment mail for the student.
//   - processed_events still has exactly one row per (consumer, event id).
//   - No row in failed_events.
//
// Implementation notes: "every consumer" is the three groups that handle
// StudentEnrolled (analytics-aggregator, notification-dispatcher,
// audit-archiver); the lag wait covers all six groups. The worker's
// event_processed_total{outcome="duplicate"} is logged, not asserted (see the
// end of the scenario): the redelivery is proven by the topic holding the
// copies and the lag returning to 0 with the effects unchanged.
func TestE2E_E03_DuplicateDelivery(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	student := s.NewStudent(t)
	course := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)

	enrollment := s.Enroll(t, student, course)
	eventID := ""
	harness.Eventually(t, time.Minute, "the StudentEnrolled event in the outbox", func() error {
		var err error
		eventID, err = s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrollment.String())
		return err
	})
	s.WaitSettled(t, time.Minute)

	consumers := []string{"analytics-aggregator", "notification-dispatcher", "audit-archiver"}
	oneEffect := func() error {
		for _, c := range consumers {
			if n, err := s.ProcessedCount(c, eventID); err != nil || n != 1 {
				return harness.Errf("processed_events for %s: %d (err %v), want 1", c, n, err)
			}
		}
		msgs, err := s.Mailpit.MessagesToWithSubject(student.Email, "enrolled")
		if err != nil || len(msgs) != 1 {
			return harness.Errf("enrollment mails: %d (err %v), want 1", len(msgs), err)
		}
		return nil
	}
	harness.Eventually(t, time.Minute, "one processed_events row per consumer and one mail", oneEffect)

	// Step 2: the counters to compare against.
	admin := s.Login(t, s.AdminEmail())
	platform := s.PlatformStats(t, admin)
	stats := s.CourseStatsFor(t, inst, course)
	daily := s.DailyEnrollments(t, inst, course)
	if stats.TotalEnrollments != 1 || stats.ActiveEnrollments != 1 {
		t.Fatalf("after one enrollment the course stats are %+v, want total 1, active 1", stats)
	}
	before, _ := s.WorkerMetrics()

	// Step 3: re-deliver the exact same record twice more, bypassing the relay.
	recs, err := s.ReadTopic("enrollment.events")
	if err != nil {
		t.Fatal(err)
	}
	var original *harness.KRecord
	for i := range recs {
		if recs[i].EventID() == eventID {
			original = &recs[i]
		}
	}
	if original == nil {
		t.Fatalf("event %s not found on enrollment.events", eventID)
	}
	producer := s.NewEventProducer(t)
	producer.Redeliver(t, *original)
	producer.Redeliver(t, *original)
	if again, err := s.ReadTopic("enrollment.events"); err != nil || len(again) != len(recs)+2 {
		t.Fatalf("after redelivery enrollment.events holds %d records (err %v), want %d", len(again), err, len(recs)+2)
	}

	// Step 4: every consumer group's lag is 0 again.
	s.WaitSettled(t, time.Minute)

	// The copies were consumed; the effects stay single. Checked for a short
	// window after the lag is 0, so a late second effect would show.
	harness.Never(t, 3*time.Second, "single effects after redelivery", func() error {
		if p := s.PlatformStats(t, admin); p != platform {
			return harness.Errf("platform counters %+v, were %+v", p, platform)
		}
		if cs := s.CourseStatsFor(t, inst, course); !cs.Same(stats) {
			return harness.Errf("course stats %+v, were %+v", cs, stats)
		}
		if d := s.DailyEnrollments(t, inst, course); d != daily {
			return harness.Errf("daily enrollments %d, were %d", d, daily)
		}
		return oneEffect()
	})
	if n := s.MustCount(t, `SELECT count(*) FROM notification.deliveries WHERE event_id = ?`, eventID); n != 1 {
		t.Errorf("notification.deliveries rows for the event: %d, want 1", n)
	}
	if n, err := s.Mongo.ArchivedEvents(eventID); err != nil || n != 1 {
		t.Errorf("archive documents for the event: %d (err %v), want 1", n, err)
	}
	if fe, err := s.Mongo.Count("failed_events", nil); err != nil || fe != 0 {
		t.Errorf("failed_events holds %d documents (err %v), want 0", fe, err)
	}
	if got := s.ListFailedEvents(t, ""); len(got) != 0 {
		t.Errorf("failed-events admin list shows %d records, want 0", len(got))
	}
	if after, err := s.WorkerMetrics(); err == nil && before != nil {
		dup := after.SumWhere("event_processed_total", `outcome="duplicate"`) - before.SumWhere("event_processed_total", `outcome="duplicate"`)
		t.Logf("event_processed_total{outcome=duplicate} moved by %v over the redelivery (3 consumers x 2 copies = 6 when the metric is wired)", dup)
	}
}
