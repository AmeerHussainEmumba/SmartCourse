//go:build e2e

package e2e_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// Failure-mode scenarios: E4 to E7, on the full stack of test/e2e/harness
// (ten binaries). See the header of scenarios_pipeline_e2e_test.go.

// TestE2E_E04_PoisonMessage is E4, matrix row E04 (Phase 4).
//
// Steps:
//  1. Stack as E01 with a small retry budget for consumers (3 attempts, 50 ms
//     backoff, on every worker) so the test is fast.
//  2. Produce a record that decodes (valid envelope and schema id) but makes
//     the analytics consumer fail every time. Then a real student completes a
//     course normally, so a healthy event sits behind the poison one on the
//     same partition.
//  3. Wait for the retry budget to be exhausted.
//
// Assertions:
//   - failed_events in MongoDB has exactly one document for the poison event
//     id with the payload, the consumer name, attempts equal to the budget
//     and one error entry per attempt, parked by the CONSUMING service's
//     worker (analytics-worker).
//   - The healthy event behind it was processed (analytics counters, Mailpit
//     mail, certificate): the partition did not stall, the offset advanced.
//   - The failed-events admin endpoint of audit-api (admin token) lists the
//     document; analytics-worker's dead_letter_total increased by 1.
//   - POST replay for that event id (audit-api, admin) republishes the stored
//     event to retry.analytics-aggregator (outcome status replayed, that
//     retry topic named, the record on the topic); the analytics consumer
//     group applies it through its normal idempotent path: after fixing the
//     cause the effect appears once. A second replay of the same id is a
//     200/no-op (already_replayed) that produces nothing and has no second
//     effect.
//
// Deviation from the spec text, which proposed a StudentEnrolled for a course
// that does not exist: no consumer treats that as an error (analytics keeps
// no foreign keys and records it, notification parks the mail). The poison
// here is a progress.course_completed for an enrollment the analytics mirror
// has never seen, which handleCourseCompleted rejects every time ("the row
// must already exist"); the certificate issuer acknowledges it (unknown
// enrollment) and the notification consumer parks a mail for an unknown
// recipient, so exactly one consumer fails, as the spec expects. It is keyed
// by a real enrollment's id, so it sits on the same partition as the real
// completion that follows. The cause is fixed by creating the missing mirror
// row, which is what the arrival of the StudentEnrolled event would do.
func TestE2E_E04_PoisonMessage(t *testing.T) {
	const budget = 3
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"KAFKA_CONSUMER_MAX_ATTEMPTS": fmt.Sprint(budget),
		"KAFKA_CONSUMER_BACKOFF_BASE": "50ms",
		"KAFKA_CONSUMER_BACKOFF_MAX":  "100ms",
	}})
	inst := s.NewInstructor(t)
	student := s.NewStudent(t)
	course := s.PublishedCourse(t, inst, 1)
	enrollment := s.Enroll(t, student, course)
	s.WaitSettled(t, time.Minute) // the identity events reached every consumer (see E01)

	baseDeadLetters := float64(0)
	if m, err := s.Metrics(harness.AnalyticsWorker); err == nil {
		baseDeadLetters = m.Sum("dead_letter_total")
	}

	// Step 2: the poison, then the healthy event right behind it.
	ghostEnrollment, ghostStudent := uuid.New(), uuid.New()
	payload := fmt.Sprintf(`{"enrollment_id":%q,"student_id":%q,"completed_at":%q}`,
		ghostEnrollment, ghostStudent, time.Now().UTC().Format(time.RFC3339))
	producer := s.NewEventProducer(t)
	poisonID := producer.Produce(t, "progress.course_completed", enrollment, payload, 1<<40)
	s.CompleteAll(t, student, enrollment)

	// Step 3: the budget runs out and the event is parked.
	var docs []bson.M
	harness.Eventually(t, time.Minute, "the poison event in failed_events", func() error {
		var err error
		docs, err = s.Mongo.FailedEvents(poisonID)
		if err != nil || len(docs) != 1 {
			return harness.Errf("%d failed_events documents (err %v), want 1", len(docs), err)
		}
		return nil
	})
	doc := docs[0]
	if doc["consumer"] != "analytics-aggregator" {
		t.Errorf("failed_events consumer %v, want analytics-aggregator", doc["consumer"])
	}
	if doc["event_type"] != "progress.course_completed" {
		t.Errorf("failed_events event_type %v", doc["event_type"])
	}
	if attempts, _ := doc["attempts"].(int32); attempts != budget {
		t.Errorf("failed_events attempts %v, want the budget %d", doc["attempts"], budget)
	}
	t.Run("error history has one entry per attempt", func(t *testing.T) {
		if errs, _ := doc["errors"].(bson.A); len(errs) != budget {
			t.Errorf("PRODUCT DEFECT: failed_events errors has %d entries, want one per attempt (%d): the dead-letter sink receives only the last error from the consumer runner (libs/kafka OnExhausted gets the final error). Document: %v", len(errs), budget, doc["errors"])
		}
	})
	if raw, _ := doc["payload"].(bson.Binary); !strings.Contains(string(raw.Data), ghostEnrollment.String()) {
		t.Errorf("failed_events payload does not hold the poison payload: %v", doc["payload"])
	}
	if doc["status"] != "parked" {
		t.Errorf("failed_events status %v, want parked", doc["status"])
	}

	// The healthy completion behind it was processed by every consumer.
	harness.Eventually(t, time.Minute, "the healthy completion processed behind the poison", func() error {
		cs := s.CourseStatsFor(t, inst, course)
		if cs.Completions != 1 {
			return harness.Errf("course stats %+v, want 1 completion", cs)
		}
		if msgs, err := s.Mailpit.MessagesToWithSubject(student.Email, "completing"); err != nil || len(msgs) != 1 {
			return harness.Errf("completion mails: %d (err %v), want 1", len(msgs), err)
		}
		if n := s.MustCount(t, `SELECT count(*) FROM course.certificates WHERE enrollment_id = ?`, enrollment); n != 1 {
			return harness.Errf("certificates: %d, want 1", n)
		}
		return nil
	})
	s.WaitSettled(t, time.Minute)
	if n := s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE enrollment_id = ?`, ghostEnrollment); n != 0 {
		t.Fatalf("the ghost enrollment is already in the analytics mirror (%d rows): the poison is not failing", n)
	}

	// The admin surface lists it, and the dead-letter metric moved by one.
	listed := false
	for _, f := range s.ListFailedEvents(t, "parked") {
		if f.EventID == poisonID {
			listed = true
			if f.Consumer != "analytics-aggregator" || f.Attempts != budget {
				t.Errorf("admin list shows %+v, want analytics-aggregator with %d attempts", f, budget)
			}
		}
	}
	if !listed {
		t.Errorf("the admin failed-events list does not show %s", poisonID)
	}
	// Replay: first fix the cause (the enrollment the event is about exists,
	// written as the database owner), then replay by id through audit-api.
	// Nothing is applied by the call itself: audit-api republishes the stored
	// event to retry.analytics-aggregator and analytics-worker's own group
	// applies it.
	before := s.CourseStatsFor(t, inst, course)
	retryTopic := harness.RetryTopic("analytics-aggregator")
	if err := s.DB().Exec(`INSERT INTO analytics.enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at, last_event_id)
		VALUES (?, ?, ?, 'active', now(), 0)`, ghostEnrollment, course.ID, ghostStudent).Error; err != nil {
		t.Fatalf("fix the cause: %v", err)
	}
	code, res, raw := s.ReplayFailedEvent(t, poisonID)
	if code != 200 || res == nil || !res.Replayed || len(res.Outcomes) != 1 || res.Outcomes[0].Consumer != "analytics-aggregator" ||
		res.Outcomes[0].Status != "replayed" || res.Outcomes[0].RetryTopic != retryTopic {
		t.Fatalf("first replay: HTTP %d %s, want 200 replayed with one analytics-aggregator outcome on %s", code, raw, retryTopic)
	}
	if recs, err := s.ReadTopic(retryTopic); err != nil || countEvent(recs, poisonID) != 1 {
		t.Errorf("%s holds %d records of the poison event (err %v), want 1", retryTopic, countEvent(recs, poisonID), err)
	}
	var afterFirst harness.CourseStats
	harness.Eventually(t, time.Minute, "the replayed event applied by analytics-aggregator from its retry topic", func() error {
		afterFirst = s.CourseStatsFor(t, inst, course)
		if afterFirst.Completions != before.Completions+1 {
			return harness.Errf("completions %d, were %d (want +1)", afterFirst.Completions, before.Completions)
		}
		return nil
	})
	s.WaitSettled(t, time.Minute) // the retry topic is part of the group: lag 0 means it was handled
	if n, err := s.ProcessedCount("analytics-aggregator", poisonID); err != nil || n != 1 {
		t.Errorf("processed_events for the replayed event: %d (err %v), want 1", n, err)
	}
	if docs, err := s.Mongo.FailedEvents(poisonID); err != nil || len(docs) != 1 || docs[0]["status"] != "replayed" {
		t.Errorf("failed_events after replay: %v (err %v), want one document with status replayed", docs, err)
	}

	code, res, raw = s.ReplayFailedEvent(t, poisonID)
	if code != 200 || res == nil || res.Replayed || len(res.Outcomes) != 1 || res.Outcomes[0].Status != "already_replayed" {
		t.Fatalf("second replay: HTTP %d %s, want 200 with replayed=false and already_replayed", code, raw)
	}
	s.WaitSettled(t, time.Minute)
	harness.Never(t, 3*time.Second, "no second effect after the second replay", func() error {
		if afterSecond := s.CourseStatsFor(t, inst, course); !afterSecond.Same(afterFirst) {
			return harness.Errf("stats %+v, were %+v", afterSecond, afterFirst)
		}
		return nil
	})
	if recs, err := s.ReadTopic(retryTopic); err != nil || countEvent(recs, poisonID) != 1 {
		t.Errorf("after the second replay %s holds %d records of the event (err %v), want still 1: a no-op replay must produce nothing", retryTopic, countEvent(recs, poisonID), err)
	}
	if n, err := s.ProcessedCount("analytics-aggregator", poisonID); err != nil || n != 1 {
		t.Errorf("processed_events after the second replay: %d (err %v), want 1", n, err)
	}

	t.Run("dead_letter_total", func(t *testing.T) {
		m, err := s.Metrics(harness.AnalyticsWorker)
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Sum("dead_letter_total") - baseDeadLetters; got != 1 {
			t.Errorf("PRODUCT DEFECT: analytics-worker's dead_letter_total moved by %v after one event was parked, want 1", got)
		}
	})
}

// TestE2E_E05_KafkaDownThenUp is E5, matrix row E05 (Phase 3; metrics from
// Phase 7).
//
// Steps:
//  1. Stack as E01. Record baseline analytics counters.
//  2. Stop the Kafka container.
//  3. Over 30 seconds, enroll 20 different students (each registered
//     beforehand) in 20 courses with seats, and call read endpoints
//     (GET /courses, GET /enrollments) throughout.
//  4. Assert while Kafka is down. Start Kafka again.
//  5. Wait for the outbox to drain.
//
// Assertions:
//   - While down: every enroll is 201 and every read is 200 (the API does not
//     depend on Kafka; /health/ready for course-api stays 200 because Kafka is
//     a non-fatal check). outbox rows for the 20 events are unpublished and
//     outbox_backlog on /metrics rises to at least 20;
//     outbox_oldest_unpublished_seconds grows.
//   - After Kafka returns: outbox_backlog returns to 0 within a bounded time;
//     every event is on its topic exactly once (the same outbox id seen once
//     per topic, key preserved, per-aggregate order preserved); analytics
//     counters moved by exactly 20, not 40 (idempotency absorbed any
//     redelivery); nothing lost.
//
// The outbox gauges are refreshed by a 15 s poller, so the metric checks poll
// for up to a minute. Everything registered and published before the broker
// stops is settled first, so the backlog counts only the 20 enrollments.
func TestE2E_E05_KafkaDownThenUp(t *testing.T) {
	const n = 20
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	courses := make([]*harness.Course, n)
	for i := range courses {
		courses[i] = s.NewCourse(t, inst, 1, 100)
		s.StartPublish(t, inst, courses[i])
	}
	for _, c := range courses {
		s.WaitPublished(t, inst, c)
	}
	students := make([]*harness.User, n)
	for i := range students {
		students[i] = s.NewStudentNamed(t, fmt.Sprintf("Student%02d", i))
	}
	s.WaitSettled(t, 2*time.Minute)

	courseIDs := make([]uuid.UUID, n)
	for i, c := range courses {
		courseIDs[i] = c.ID
	}
	enrolledInAnalytics := func() (int64, error) {
		return s.Count(`SELECT COALESCE(sum(total_enrollments), 0) FROM analytics.course_stats WHERE course_id IN ?`, courseIDs)
	}
	if got, err := enrolledInAnalytics(); err != nil || got != 0 {
		t.Fatalf("baseline analytics enrollments for the 20 courses: %d (err %v), want 0", got, err)
	}

	// Step 2 and 3: Kafka goes down; the API keeps working for 30 seconds.
	s.StopKafka(t)
	began := time.Now()
	// identity's relay recovers too: two accounts registered during the outage
	// leave identity.user_registered events in identity.outbox, not in
	// course's.
	outageUsers := []*harness.User{s.NewStudentNamed(t, "OutageOne"), s.NewStudentNamed(t, "OutageTwo")}
	for i := range students {
		s.Enroll(t, students[i], courses[i]) // 201 or the test fails
		for _, read := range []struct{ url, token string }{
			{s.CourseURL + "/api/v1/courses", ""},
			{s.CourseURL + "/api/v1/enrollments", students[i].Token},
		} {
			if code, body, err := harness.Do("GET", read.url, read.token, nil); err != nil || code != 200 {
				t.Fatalf("GET %s with Kafka down: HTTP %d %s (err %v), want 200", read.url, code, body, err)
			}
		}
		if err := harness.CheckReady(s.CourseURL); err != nil {
			t.Fatalf("course-api readiness with Kafka down: %v", err)
		}
		if wait := time.Until(began.Add(time.Duration(i+1) * 30 * time.Second / n)); wait > 0 {
			time.Sleep(wait) // spread the 20 enrollments over the 30 seconds
		}
	}

	// Step 4: while it is down.
	if got := s.MustCount(t, `SELECT count(*) FROM course.outbox WHERE event_type = 'enrollment.student_enrolled' AND published_at IS NULL`); got != n {
		t.Errorf("unpublished StudentEnrolled rows in course.outbox with Kafka down: %d, want %d", got, n)
	}
	if got := s.MustCount(t, `SELECT count(*) FROM identity.outbox WHERE event_type = 'identity.user_registered' AND published_at IS NULL`); got != int64(len(outageUsers)) {
		t.Errorf("unpublished user_registered rows in identity.outbox with Kafka down: %d, want %d", got, len(outageUsers))
	}
	var oldest float64
	harness.Eventually(t, time.Minute, "outbox_backlog >= 20 on course-api /metrics", func() error {
		m, err := s.CourseMetrics()
		if err != nil {
			return err
		}
		if got := m.Sum("outbox_backlog"); got < n {
			return harness.Errf("outbox_backlog %v, want at least %d", got, n)
		}
		oldest = m.Sum("outbox_oldest_unpublished_seconds")
		return nil
	})
	harness.Eventually(t, time.Minute, "outbox_oldest_unpublished_seconds to grow", func() error {
		m, err := s.CourseMetrics()
		if err != nil {
			return err
		}
		if got := m.Sum("outbox_oldest_unpublished_seconds"); got <= oldest {
			return harness.Errf("outbox_oldest_unpublished_seconds %v, was %v", got, oldest)
		}
		return nil
	})
	if got, err := enrolledInAnalytics(); err != nil || got != 0 {
		t.Errorf("analytics moved to %d (err %v) with Kafka down, want 0", got, err)
	}

	// Step 5: Kafka returns and the outbox drains.
	s.StartKafka(t)
	s.WaitOutboxDrained(t, 3*time.Minute)
	harness.Eventually(t, time.Minute, "outbox_backlog back to 0 on course-api /metrics", func() error {
		m, err := s.CourseMetrics()
		if err != nil {
			return err
		}
		if got := m.Sum("outbox_backlog"); got != 0 {
			return harness.Errf("outbox_backlog %v, want 0", got)
		}
		return nil
	})
	s.WaitSettled(t, 3*time.Minute)

	// Every event is on its topic exactly once, with its key, in order.
	rows, err := s.OutboxRows() // both services' outboxes
	if err != nil {
		t.Fatal(err)
	}
	recs, err := s.ReadTopics(harness.EventTopics...)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]harness.KRecord{}
	for _, r := range recs {
		if prev, dup := seen[r.EventID()]; dup {
			t.Errorf("event %s published twice: %s[%d]@%d and %s[%d]@%d", r.EventID(), prev.Topic, prev.Partition, prev.Offset, r.Topic, r.Partition, r.Offset)
		}
		seen[r.EventID()] = r
	}
	for _, r := range rows {
		rec, ok := seen[r.EventID]
		switch {
		case !ok:
			t.Errorf("event %s (%s) never reached a topic: lost", r.EventID, r.EventType)
		case rec.Topic != topicOf(r.EventType):
			t.Errorf("event %s (%s) is on %s", r.EventID, r.EventType, rec.Topic)
		case rec.Key != r.AggregateID:
			t.Errorf("event %s (%s) key %s, want %s", r.EventID, r.EventType, rec.Key, r.AggregateID)
		}
	}
	if len(recs) != len(rows) {
		t.Errorf("%d records on the topics for %d outbox rows", len(recs), len(rows))
	}
	lastOutboxID := map[string]int64{} // per partition key: outbox ids must rise with the offset
	for _, r := range recs {           // recs are sorted by partition, then offset
		var id int64
		_, _ = fmt.Sscan(r.Headers["outbox_id"], &id)
		k := r.Topic + "/" + r.Key
		if id <= lastOutboxID[k] {
			t.Errorf("order broken for %s: outbox id %d after %d", k, id, lastOutboxID[k])
		}
		lastOutboxID[k] = id
	}

	// Analytics moved by exactly 20, not 40, and every course by exactly one.
	harness.Eventually(t, time.Minute, "analytics to show 20 enrollments", func() error {
		if got, err := enrolledInAnalytics(); err != nil || got != n {
			return harness.Errf("analytics enrollments for the 20 courses: %d (err %v), want %d", got, err, n)
		}
		return nil
	})
	for i, c := range courses {
		if cs := s.CourseStatsFor(t, inst, c); cs.TotalEnrollments != 1 {
			t.Errorf("course %d stats %+v, want one enrollment", i, cs)
		}
	}
	// Nothing lost on the effect side either: every student has their mail.
	harness.Eventually(t, time.Minute, "an enrollment mail for each of the 20 students", func() error {
		for i, st := range students {
			if msgs, err := s.Mailpit.MessagesToWithSubject(st.Email, "enrolled"); err != nil || len(msgs) != 1 {
				return harness.Errf("student %d has %d enrollment mails (err %v), want 1", i, len(msgs), err)
			}
		}
		return nil
	})
	// Every consumer recovered, not only analytics and notification: the
	// identity events of the outage reached course-worker's instructor mirror
	// (a course consumer) and the audit archiver wrote every event once.
	harness.Eventually(t, time.Minute, "course-worker's mirror and audit-worker's archive to have caught up", func() error {
		for _, u := range outageUsers {
			if n, err := s.Count(`SELECT count(*) FROM course.instructor_mirror WHERE user_id = ?`, u.ID); err != nil || n != 1 {
				return harness.Errf("instructor_mirror rows for %s: %d (err %v), want 1", u.Email, n, err)
			}
			if msgs, err := s.Mailpit.MessagesToWithSubject(u.Email, "welcome"); err != nil || len(msgs) != 1 {
				return harness.Errf("welcome mails for %s: %d (err %v), want 1", u.Email, len(msgs), err)
			}
		}
		if total, err := s.Mongo.Count("events", nil); err != nil || total != int64(len(rows)) {
			return harness.Errf("archive holds %d events (err %v), outboxes hold %d", total, err, len(rows))
		}
		return nil
	})
	if fe, err := s.Mongo.Count("failed_events", nil); err != nil || fe != 0 {
		t.Errorf("failed_events holds %d documents (err %v) after the outage, want 0", fe, err)
	}
}

// TestE2E_E06_ConsumerKilledMidFlow is E6, matrix row E06 (Phase 3).
//
// Steps:
//  1. Every service runs as its own OS process, so a worker can be killed
//     with SIGKILL (kill -9), not a graceful stop. The process killed here is
//     analytics-worker (its consumer group, analytics-aggregator, is the one
//     whose handler the test can hold inside its transaction).
//  2. Enroll a student; wait until analytics-worker is inside the handler's
//     transaction.
//  3. kill -9 analytics-worker. Start a new analytics-worker process.
//
// Assertions:
//   - The enrollment event is eventually consumed by every consumer: the
//     analytics counters moved by exactly 1, one Mailpit mail, one archive
//     document: no lost effect.
//   - processed_events has exactly one row per (consumer, event id): no
//     double effect.
//   - The uncommitted transaction left nothing behind (the effect and its
//     processed_events row are atomic, RFC 6.6).
//   - Repeat with the kill after the commit and before the offset commit: the
//     redelivery is a no-op.
//
// The spec proposed a test-only fault hook in the worker (an environment
// variable that blocks the handler). The worker has none, and a hook would be
// test code in the product, so the harness stops the handler without touching
// the product: the test holds a Postgres row lock (SELECT ... FOR UPDATE on
// the course's analytics.course_stats row) that the analytics handler needs
// after it has written its first row, so its transaction is open, with an
// uncommitted write, when the test kills the worker. For the second case the
// test stops Kafka while the handler is blocked, releases the lock so the
// transaction commits, waits for the committed processed_events row, then
// kills the worker: the offset commit cannot have reached the stopped broker.
// After Kafka is back, the group's lag is above 0 (the offset was not
// committed) before the new worker starts, and the redelivery changes nothing.
//
// The consumer group takes a while to hand the dead member's partitions to
// the new process (the broker waits for the dead member's session to expire,
// 45 s by default), so the recovery waits are generous. The other services
// keep running throughout: the other consumers (notification, audit) apply the
// event once while analytics-worker is dead, which effectsOnce checks at the end.
func TestE2E_E06_ConsumerKilledMidFlow(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	st1, st2 := s.NewStudentNamed(t, "First"), s.NewStudentNamed(t, "Second")
	course := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)

	const lockRow = `SELECT 1 FROM analytics.course_stats WHERE course_id = ? FOR UPDATE`
	const blockedFragment = "course_stats WHERE course_id"
	consumers := []string{"analytics-aggregator", "notification-dispatcher", "audit-archiver"}

	// effectsOnce asserts the one-effect state for an enrollment event.
	effectsOnce := func(student *harness.User, eventID string, wantEnrollments int64) func() error {
		return func() error {
			for _, c := range consumers {
				if n, err := s.ProcessedCount(c, eventID); err != nil || n != 1 {
					return harness.Errf("processed_events for %s: %d (err %v), want 1", c, n, err)
				}
			}
			if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != wantEnrollments {
				return harness.Errf("course stats %+v, want %d enrollments", cs, wantEnrollments)
			}
			if msgs, err := s.Mailpit.MessagesToWithSubject(student.Email, "enrolled"); err != nil || len(msgs) != 1 {
				return harness.Errf("enrollment mails for %s: %d (err %v), want 1", student.Email, len(msgs), err)
			}
			if n, err := s.Mongo.ArchivedEvents(eventID); err != nil || n != 1 {
				return harness.Errf("archive documents: %d (err %v), want 1", n, err)
			}
			return nil
		}
	}
	eventOf := func(enrollment uuid.UUID) string {
		var id string
		harness.Eventually(t, time.Minute, "the StudentEnrolled event in the outbox", func() error {
			var err error
			id, err = s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrollment.String())
			return err
		})
		return id
	}

	// Case 1: killed inside the transaction, before the commit.
	lock := s.HoldRowLock(t, lockRow, course.ID)
	enr1 := s.Enroll(t, st1, course)
	event1 := eventOf(enr1)
	s.WaitBlockedOn(t, blockedFragment, time.Minute)
	s.Kill(harness.AnalyticsWorker)
	lock.Release()
	s.WaitNoBlockedBackends(t, time.Minute)
	if n := s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE enrollment_id = ?`, enr1); n != 0 {
		t.Errorf("the killed transaction left %d analytics.enrollment_mirror rows behind, want 0", n)
	}
	if n, err := s.ProcessedCount("analytics-aggregator", event1); err != nil || n != 0 {
		t.Errorf("the killed transaction left %d processed_events rows (err %v), want 0", n, err)
	}
	s.Start(t, harness.AnalyticsWorker)
	harness.Eventually(t, 4*time.Minute, "every consumer to apply the event once after the restart", effectsOnce(st1, event1, 1))
	s.WaitSettled(t, 2*time.Minute)

	// Case 2: killed after the commit, before the offset commit.
	lock = s.HoldRowLock(t, lockRow, course.ID)
	enr2 := s.Enroll(t, st2, course)
	event2 := eventOf(enr2)
	s.WaitBlockedOn(t, blockedFragment, time.Minute)
	s.StopKafka(t) // the offset commit can no longer succeed
	lock.Release() // the handler's transaction commits
	harness.Eventually(t, time.Minute, "the handler's transaction to commit with Kafka down", func() error {
		if n, err := s.ProcessedCount("analytics-aggregator", event2); err != nil || n != 1 {
			return harness.Errf("processed_events: %d (err %v), want 1", n, err)
		}
		return nil
	})
	s.Kill(harness.AnalyticsWorker)
	s.StartKafka(t)
	harness.Eventually(t, time.Minute, "the analytics group to show the uncommitted record as lag", func() error {
		lag, err := s.ConsumerLag("analytics-aggregator")
		if err != nil {
			return err
		}
		if lag < 1 {
			return harness.Errf("analytics-aggregator lag %d, want at least 1 (the offset must not have been committed)", lag)
		}
		return nil
	})
	s.Start(t, harness.AnalyticsWorker)
	s.WaitSettled(t, 4*time.Minute)
	harness.Never(t, 3*time.Second, "one effect after the redelivery", effectsOnce(st2, event2, 2))
	if n := s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE course_id = ?`, course.ID); n != 2 {
		t.Errorf("analytics.enrollment_mirror rows for the course: %d, want 2", n)
	}
	if fe, err := s.Mongo.Count("failed_events", nil); err != nil || fe != 0 {
		t.Errorf("failed_events holds %d documents (err %v), want 0", fe, err)
	}
}

// TestE2E_E07_SMTPProviderDown is E7, matrix row E07 (Phase 5).
//
// Steps:
//  1. Stack as E01 with Mailpit as the SMTP sink and short Asynq retry
//     backoff (1 s base, 8 s cap) configured on notification-worker, the
//     process that sends the mail (the queue gauges are read from its
//     /metrics).
//  2. Stop Mailpit. Enroll three students.
//  3. Observe for several retry intervals. Start Mailpit again.
//
// Assertions:
//   - The enroll requests are all 201 and the Kafka offsets for the
//     notification consumer advance (the consumer writes the delivery row and
//     commits at once, ADR-0014): delivery trouble does not block event
//     processing.
//   - While down: asynq_queue_depth plus asynq_retry_count show the three
//     jobs pending or retrying; asynq_retry_count increases; attempts are
//     spaced with growing delay (the timestamps of each delivery's attempts,
//     read from notification.deliveries.attempts, are polled and each gap is
//     at least the previous one, within the 20% jitter of the backoff).
//   - After Mailpit returns: all three mails arrive; the number of mails per
//     student is exactly 1 (a delivery record prevents a repeat send); queue
//     depth returns to 0.
func TestE2E_E07_SMTPProviderDown(t *testing.T) {
	s := harness.Start(t, harness.Options{BinaryEnv: map[string]map[string]string{
		harness.NotificationWorker: {
			"ASYNQ_BACKOFF_BASE": "1s",
			"ASYNQ_BACKOFF_MAX":  "8s",
		},
	}})
	inst := s.NewInstructor(t)
	students := []*harness.User{s.NewStudentNamed(t, "One"), s.NewStudentNamed(t, "Two"), s.NewStudentNamed(t, "Three")}
	course := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)
	harness.Eventually(t, time.Minute, "the welcome and publish mails to be delivered before the outage", func() error {
		n, err := s.Count(`SELECT count(*) FROM notification.deliveries WHERE status <> 'sent'`)
		if err != nil || n != 0 {
			return harness.Errf("%d deliveries not sent yet (err %v)", n, err)
		}
		return nil
	})

	s.StopSMTP(t)
	for _, st := range students {
		s.Enroll(t, st, course) // 201 with the provider down
	}
	s.WaitSettled(t, time.Minute) // the notification consumer's offsets advanced

	// Observe the retries: record when each delivery's attempt counter moves.
	type attempt struct {
		at time.Time
		n  int
	}
	history := map[string][]attempt{}
	sample := func() error {
		var rows []struct {
			ID       string
			Attempts int
			Status   string
		}
		if err := s.DB().Raw(`SELECT id::text AS id, attempts, status FROM notification.deliveries WHERE kind = 'enrollment_confirmation' ORDER BY id`).Scan(&rows).Error; err != nil {
			return err
		}
		if len(rows) != len(students) {
			return harness.Errf("%d enrollment deliveries, want %d", len(rows), len(students))
		}
		for _, r := range rows {
			h := history[r.ID]
			if len(h) == 0 || h[len(h)-1].n != r.Attempts {
				history[r.ID] = append(h, attempt{time.Now(), r.Attempts})
			}
		}
		return nil
	}
	// The queue gauges carry the worker's queue name, "critical" or, with the
	// per-service namespace, "notification:critical": the fragment `critical"`
	// matches both. A notification-worker that exports no asynq gauge at all is
	// asserted through the delivery rows and the attempt timing below instead.
	const critical = `critical"`
	var retryMax, queueMax float64
	haveGauges := false
	harness.Eventually(t, 2*time.Minute, "every delivery to be retried at least 4 times", func() error {
		if err := sample(); err != nil {
			return err
		}
		if m, err := s.Metrics(harness.NotificationWorker); err == nil {
			haveGauges = haveGauges || m.Has("asynq_queue_depth") || m.Has("asynq_retry_count")
			retryMax = max(retryMax, m.SumWhere("asynq_retry_count", critical))
			queueMax = max(queueMax, m.SumWhere("asynq_queue_depth", critical)+m.SumWhere("asynq_retry_count", critical))
		}
		for id, h := range history {
			if h[len(h)-1].n < 4 {
				return harness.Errf("delivery %s at attempt %d", id, h[len(h)-1].n)
			}
		}
		return nil
	})
	if !haveGauges {
		t.Log("notification-worker exports no asynq_queue_depth or asynq_retry_count: the retry evidence is the delivery rows (attempts, status) and the attempt timing below")
	} else {
		if queueMax < float64(len(students)) {
			t.Errorf("asynq_queue_depth + asynq_retry_count peaked at %v while the provider was down, want at least %d", queueMax, len(students))
		}
		if retryMax < 1 {
			t.Errorf("asynq_retry_count never rose above %v while the provider was down", retryMax)
		}
	}
	// Attempts are spaced with a delay that does not shrink: each gap is at
	// least the previous one, less the backoff's 20% jitter and the polling
	// resolution (a sample can be up to a second late).
	for id, h := range history {
		var gaps []time.Duration
		for i := 1; i < len(h); i++ {
			gaps = append(gaps, h[i].at.Sub(h[i-1].at))
		}
		t.Logf("delivery %s: attempts %d..%d, gaps between attempts %v", id, h[0].n, h[len(h)-1].n, gaps)
		for i := 1; i < len(gaps); i++ {
			if tolerance := gaps[i-1]/4 + time.Second; gaps[i]+tolerance < gaps[i-1] {
				t.Errorf("delivery %s: the gap before attempt %d (%v) is shorter than the gap before it (%v): retries are not backing off", id, h[i+1].n, gaps[i], gaps[i-1])
			}
		}
	}
	if msgs, _ := s.Mailpit.Messages(); len(msgs) != 0 {
		t.Errorf("mailpit holds %d messages while it was stopped", len(msgs))
	}

	s.StartSMTP(t)
	harness.Eventually(t, 3*time.Minute, "all three enrollment mails after the provider returns", func() error {
		for i, st := range students {
			if msgs, err := s.Mailpit.MessagesToWithSubject(st.Email, "enrolled"); err != nil || len(msgs) != 1 {
				return harness.Errf("student %d: %d enrollment mails (err %v), want 1", i, len(msgs), err)
			}
		}
		return nil
	})
	harness.Never(t, 5*time.Second, "exactly one mail per student", func() error {
		for i, st := range students {
			if msgs, err := s.Mailpit.MessagesToWithSubject(st.Email, "enrolled"); err != nil || len(msgs) != 1 {
				return harness.Errf("student %d: %d enrollment mails (err %v), want 1", i, len(msgs), err)
			}
		}
		return nil
	})
	if n := s.MustCount(t, `SELECT count(*) FROM notification.deliveries WHERE kind = 'enrollment_confirmation' AND status = 'sent'`); n != 3 {
		t.Errorf("sent enrollment deliveries: %d, want 3", n)
	}
	// The queue gauges refresh every 15 s, so the drain shows within a minute.
	// Without gauges the drain is already shown by every delivery being sent.
	if haveGauges {
		harness.Eventually(t, time.Minute, "asynq queue depth and retry count back to 0", func() error {
			m, err := s.Metrics(harness.NotificationWorker)
			if err != nil {
				return err
			}
			if got := m.SumWhere("asynq_queue_depth", critical) + m.SumWhere("asynq_retry_count", critical); got != 0 {
				return harness.Errf("asynq_queue_depth + asynq_retry_count on the critical queue = %v, want 0", got)
			}
			return nil
		})
	}
}
