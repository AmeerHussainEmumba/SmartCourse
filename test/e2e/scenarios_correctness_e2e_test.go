//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// Correctness scenarios E08 and E09 on the ten-binary harness. E08's jobs run
// in analytics-worker (platform and course-stats reconcile, popular ranking)
// and course-worker (enrollment_count); both read the JOBS_* variables, which
// WorkerEnv gives to every worker.

// TestE2E_E08_CounterDrift is E8, matrix row E08 (Phase 5 job, Phase 6 metrics).
//
// Steps:
//  1. Stack as E01 with the scheduled jobs at their shortest interval,
//     JOBS_ANALYTICS_RECONCILE_INTERVAL=1m and JOBS_ENROLLMENT_COUNT_INTERVAL=1m
//     (production: 1 m and 1 h). The cron scheduler cannot go below one minute,
//     so the spec's 5 s is not configurable; the test waits up to two minutes.
//  2. Create real data: two students, one published course, one enrollment.
//     Wait until the analytics platform snapshot has seen both students
//     (GET /analytics/platform).
//  3. Corrupt state by hand: analytics.platform_snapshot (total_students 9999,
//     avg_courses_per_student 42), analytics.course_stats for the course
//     (total_enrollments 500), the Redis popular-courses ranking (999 for the
//     course) and the enrollment counter course.courses.enrollment_count (77).
//     Nothing is done through the API after this point, so no event can repair
//     anything: only the scheduled jobs can.
//  4. Wait up to two reconciliation intervals.
//
// Assertions:
//   - Immediately after step 3 the APIs show the corrupted values (proves the
//     test changed what the services serve).
//   - Within two intervals GET /analytics/platform, the course stats, the
//     popular-courses ranking and enrollment_count are back to the values a
//     direct SQL recompute from the mirrors and the enrollment rows gives, and
//     not the corrupted ones.
//   - The jobs ran as scheduled Asynq tasks: completed cron ticks of both job
//     types are in the low queue's completed set in Redis, and course-worker logged the
//     enrollment-count repair (stored 77, actual 1). The analytics reconciler
//     logs nothing when it repairs (observed; not asserted).
//
// A second enrollment is not made after the corruption: it would trigger the
// event-side recompute and heal the aggregates for the wrong reason.
func TestE2E_E08_CounterDrift(t *testing.T) {
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"JOBS_ANALYTICS_RECONCILE_INTERVAL": "1m",
		"JOBS_ENROLLMENT_COUNT_INTERVAL":    "1m",
	}})
	inst := s.NewInstructor(t)
	a, b := s.NewStudentNamed(t, "Alice"), s.NewStudentNamed(t, "Bob")
	course := s.PublishedCourse(t, inst, 1)
	s.Enroll(t, a, course)
	s.WaitSettled(t, time.Minute)
	admin := s.Login(t, s.AdminEmail())

	harness.Eventually(t, time.Minute, "analytics to have seen both students and the enrollment", func() error {
		if p := s.PlatformStats(t, admin); p.TotalStudents < 2 {
			return harness.Errf("platform %+v, want at least 2 students", p)
		}
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != 1 {
			return harness.Errf("course stats %+v, want 1 enrollment", cs)
		}
		return nil
	})
	_ = b

	// What a recompute from the source tables gives.
	type truth struct {
		students, instructors, published int64
		avg                              float64
		enrollments                      int64
	}
	recompute := func() truth {
		var tr truth
		tr.students = s.MustCount(t, `SELECT count(*) FROM analytics.user_role_mirror WHERE role = 'student'`)
		tr.instructors = s.MustCount(t, `SELECT count(*) FROM analytics.user_role_mirror WHERE role = 'instructor'`)
		tr.published = s.MustCount(t, `SELECT count(*) FROM analytics.course_mirror WHERE published`)
		tr.enrollments = s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE course_id = ? AND status IN ('active','completed')`, course.ID)
		all := s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE status IN ('active','completed')`)
		if tr.students > 0 {
			tr.avg = float64(all) / float64(tr.students)
		}
		return tr
	}
	want := recompute()
	if want.students < 2 || want.enrollments != 1 {
		t.Fatalf("source tables hold %+v before the corruption, want at least 2 students and 1 enrollment", want)
	}
	if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != 1 {
		t.Fatalf("enrollment_count is %d before the corruption, want 1", n)
	}

	// Step 3: corrupt by hand.
	ctx := context.Background()
	if err := s.DB().Exec(`UPDATE analytics.platform_snapshot SET total_students = 9999, avg_courses_per_student = 42`).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec(`UPDATE analytics.course_stats SET total_enrollments = 500 WHERE course_id = ?`, course.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec(`UPDATE course.courses SET enrollment_count = 77 WHERE id = ?`, course.ID).Error; err != nil {
		t.Fatal(err)
	}
	const popularKey = "analytics:popular_courses"
	if err := s.Env.RDB.ZAdd(ctx, popularKey, redisZ(999, course.ID.String())).Err(); err != nil {
		t.Fatal(err)
	}

	// The services serve the corruption.
	if p := s.PlatformStats(t, admin); p.TotalStudents != 9999 || p.AvgCoursesPerStudent != 42 {
		t.Fatalf("GET /analytics/platform after the corruption: %+v, want total_students 9999 and avg 42 (the test did not change what is served, or a job already ran)", p)
	}
	if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != 500 {
		t.Fatalf("course stats after the corruption: %+v, want 500 enrollments", cs)
	}
	popularFor := func() int64 {
		for _, it := range s.PopularCourses(t, a) {
			if it.CourseID == course.ID.String() {
				return it.Enrollments
			}
		}
		return -1
	}
	if got := popularFor(); got != 999 {
		t.Fatalf("popular-courses shows %d enrollments for the course after the corruption, want 999", got)
	}
	corrupted := time.Now()

	// Step 4: the scheduled jobs repair it.
	harness.Eventually(t, 2*time.Minute+30*time.Second, "the scheduled jobs to repair every corrupted value", func() error {
		p := s.PlatformStats(t, admin)
		if p.TotalStudents != want.students || p.TotalInstructors != want.instructors || p.TotalCoursesPublished != want.published || abs(p.AvgCoursesPerStudent-want.avg) > 0.001 {
			return harness.Errf("platform %+v, want students %d instructors %d published %d avg %.3f", p, want.students, want.instructors, want.published, want.avg)
		}
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != want.enrollments || cs.ActiveEnrollments != 1 {
			return harness.Errf("course stats %+v, want total %d active 1", cs, want.enrollments)
		}
		if got := popularFor(); got != want.enrollments {
			return harness.Errf("popular-courses shows %d for the course, want %d", got, want.enrollments)
		}
		if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != 1 {
			return harness.Errf("courses.enrollment_count %d, want 1", n)
		}
		return nil
	})
	t.Logf("every corrupted value was repaired %s after the corruption", time.Since(corrupted).Round(time.Second))

	// The repairs came from scheduled Asynq tasks.
	harness.Eventually(t, 30*time.Second, "course-worker to log the enrollment_count repair", func() error {
		if !s.Proc(harness.CourseWorker).LogContains("enrollment_count drift repaired") {
			return harness.Errf("no enrollment_count drift repaired line in the course-worker log")
		}
		return nil
	})
	if log := s.Proc(harness.CourseWorker).Logs(); !strings.Contains(log, `"stored":77`) || !strings.Contains(log, `"actual":1`) {
		t.Errorf("the repair log line does not say stored 77 and actual 1")
	}
	ticks := completedTicks(t, s)
	for _, job := range []string{"maintenance:analytics-reconcile", "maintenance:enrollment-counts"} {
		if ticks[job] == 0 {
			t.Errorf("no completed scheduler tick of %s in the low queue (ticks seen: %v)", job, ticks)
		}
	}
	t.Logf("completed scheduler ticks retained in Redis: %v", ticks)
}

// TestE2E_E09_EnrollmentUnderLoad is E9, matrix row E09 (Phase 3).
//
// Steps:
//  1. Stack as E01. Instructor publishes a course with enrollment_limit 25.
//  2. Register and log in 150 students in advance (outside the timed part).
//  3. Release 150 goroutines together (a start barrier) each doing
//     POST /enrollments for the course, each with its own Idempotency-Key and
//     its own client address, through the real course-api. Record each status
//     code and latency.
//
// Assertions:
//   - Exactly 25 responses are 201; the other 125 are 409 course_full; none is
//     a 5xx, a 429 or a timeout.
//   - course.enrollments has exactly 25 active rows for the course, one per
//     student (the partial unique index), and enrollment_count is 25.
//   - After the consumers drain, analytics shows 25 enrollments for the course
//     (not 150, not 24), the archive has exactly 25 StudentEnrolled events for
//     it, and Mailpit has exactly 25 enrollment mails, one to each winner.
//   - Backpressure is visible: sampled every 20 ms during the burst, the outbox
//     backlog, the notification delivery queue or the consumer lag rose above
//     0, and all of them are back at 0 afterwards (and the worker's lag and
//     queue gauges read 0 once they refresh).
//   - Retrying a winner's request with the same Idempotency-Key replays the
//     201 (Idempotent-Replayed: true) and takes no second seat; a loser's retry
//     is refused again (4xx responses are not stored).
//
// Ported to the ten-binary harness: Kafka only (the in-process transport and
// its subtest are gone); the outbox is course.outbox; the delivery-queue and
// lag gauges are read from notification-worker and the consumer workers.
func TestE2E_E09_EnrollmentUnderLoad(t *testing.T) {
	const (
		students = 150
		seats    = 25
	)
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	course := s.NewCourse(t, inst, 1, seats)
	s.StartPublish(t, inst, course)
	s.WaitPublished(t, inst, course)
	users := s.NewStudents(t, students, 16)
	s.WaitSettled(t, 2*time.Minute)
	// The welcome mails of the 150 registrations are not part of the burst:
	// wait until every delivery so far is sent, so the queue the sampler sees
	// is the burst's.
	harness.Eventually(t, 3*time.Minute, "every earlier delivery to be sent", func() error {
		n, err := s.Count(`SELECT count(*) FROM notification.deliveries WHERE status <> 'sent'`)
		if err != nil || n != 0 {
			return harness.Errf("%d deliveries not sent yet (err %v)", n, err)
		}
		return nil
	})

	// Sample the pipeline while the burst runs.
	var peak struct {
		sync.Mutex
		backlog, queue, lag int64
	}
	stopSampling := make(chan struct{})
	var sampling sync.WaitGroup
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		for {
			select {
			case <-stopSampling:
				return
			case <-time.After(20 * time.Millisecond):
			}
			backlog, _ := s.OutboxUnpublished()
			queue, _ := s.Count(`SELECT count(*) FROM notification.deliveries WHERE status <> 'sent'`)
			var lag int64
			for _, g := range harness.ConsumerGroups {
				if l, err := s.ConsumerLag(g); err == nil {
					lag += l
				}
			}
			peak.Lock()
			peak.backlog, peak.queue, peak.lag = max(peak.backlog, backlog), max(peak.queue, queue), max(peak.lag, lag)
			peak.Unlock()
		}
	}()

	type result struct {
		status  int
		code    string
		elapsed time.Duration
		key     string
		reqID   string
		err     error
	}
	results := make([]result, students)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, u := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key := fmt.Sprintf("e2e-load-%d-%s", i, u.ID)
			began := time.Now()
			resp, err := harness.DoFull("POST", s.CourseURL+"/api/v1/enrollments", u.Token,
				map[string]string{"course_id": course.ID.String()}, map[string]string{"Idempotency-Key": key})
			r := result{elapsed: time.Since(began), key: key, err: err}
			if err == nil {
				r.status, r.code, r.reqID = resp.Status, resp.ErrorCode(), resp.Header.Get("X-Request-Id")
			}
			results[i] = r
		}()
	}
	burstStart := time.Now()
	close(start)
	wg.Wait()
	burst := time.Since(burstStart)

	var created, full, other int
	var latencies []time.Duration
	var winners, losers []int
	for i, r := range results {
		latencies = append(latencies, r.elapsed)
		switch {
		case r.err != nil:
			t.Errorf("student %d: %v (a timeout or connection error)", i, r.err)
			other++
		case r.status == http.StatusCreated:
			created++
			winners = append(winners, i)
		case r.status == http.StatusConflict && r.code == "course_full":
			full++
			losers = append(losers, i)
		default:
			t.Errorf("student %d: HTTP %d code %q, want 201 or 409 course_full", i, r.status, r.code)
			other++
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("%d requests in %s: %d created, %d course_full, %d other; latency p50 %s p95 %s max %s",
		students, burst.Round(time.Millisecond), created, full, other,
		latencies[len(latencies)/2].Round(time.Millisecond), latencies[len(latencies)*95/100].Round(time.Millisecond), latencies[len(latencies)-1].Round(time.Millisecond))
	if created != seats || full != students-seats {
		t.Errorf("%d created and %d course_full, want exactly %d and %d", created, full, seats, students-seats)
	}

	// Rows and counters.
	if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE course_id = ? AND status = 'active'`, course.ID); n != seats {
		t.Errorf("active enrollment rows: %d, want %d", n, seats)
	}
	if n := s.MustCount(t, `SELECT count(DISTINCT student_id) FROM course.enrollments WHERE course_id = ?`, course.ID); n != seats {
		t.Errorf("distinct students with an enrollment row: %d, want %d", n, seats)
	}
	if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE course_id = ?`, course.ID); n != seats {
		t.Errorf("enrollment rows in total: %d, want %d", n, seats)
	}
	if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != seats {
		t.Errorf("courses.enrollment_count: %d, want %d", n, seats)
	}

	// Idempotency across the burst.
	if len(winners) > 0 && len(losers) > 0 {
		w, l := results[winners[0]], results[losers[0]]
		resp, err := harness.DoFull("POST", s.CourseURL+"/api/v1/enrollments", users[winners[0]].Token,
			map[string]string{"course_id": course.ID.String()}, map[string]string{"Idempotency-Key": w.key})
		if err != nil || resp.Status != http.StatusCreated || resp.Header.Get("Idempotent-Replayed") != "true" {
			t.Errorf("retry of a winner's key: %+v (err %v), want the stored 201 with Idempotent-Replayed: true", resp, err)
		}
		resp, err = harness.DoFull("POST", s.CourseURL+"/api/v1/enrollments", users[losers[0]].Token,
			map[string]string{"course_id": course.ID.String()}, map[string]string{"Idempotency-Key": l.key})
		if err != nil || resp.Status != http.StatusConflict || resp.ErrorCode() != "course_full" || resp.Header.Get("Idempotent-Replayed") == "true" {
			t.Errorf("retry of a loser's key: %+v (err %v), want a fresh 409 course_full", resp, err)
		}
	}

	// Downstream: the events were consumed once each.
	var eventIDs []string
	harness.Eventually(t, time.Minute, "25 StudentEnrolled events in the outbox", func() error {
		eventIDs = nil
		if err := s.DB().Raw(`SELECT event_id::text FROM course.outbox WHERE event_type = 'enrollment.student_enrolled'
			AND convert_from(payload, 'UTF8')::jsonb ->> 'course_id' = ?`, course.ID.String()).Scan(&eventIDs).Error; err != nil {
			return err
		}
		if len(eventIDs) != seats {
			return harness.Errf("%d StudentEnrolled events for the course, want %d", len(eventIDs), seats)
		}
		return nil
	})
	s.WaitOutboxDrained(t, 2*time.Minute)
	harness.Eventually(t, 3*time.Minute, "analytics, the archive and Mailpit to show exactly 25", func() error {
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != seats || cs.ActiveEnrollments != seats {
			return harness.Errf("course stats %+v, want total %d active %d", cs, seats, seats)
		}
		wantArchived := int64(seats)
		archived, err := s.Mongo.Count("events", bson.M{"_id": bson.M{"$in": eventIDs}})
		if err != nil || archived != wantArchived {
			return harness.Errf("archived StudentEnrolled events for the course: %d (err %v), want %d", archived, err, wantArchived)
		}
		for _, i := range winners {
			msgs, err := s.Mailpit.MessagesToWithSubject(users[i].Email, "enrolled")
			if err != nil || len(msgs) != 1 {
				return harness.Errf("winner %d has %d enrollment mails (err %v), want 1", i, len(msgs), err)
			}
		}
		return nil
	})
	s.WaitSettled(t, 2*time.Minute)
	harness.Never(t, 3*time.Second, "exactly 25 of each after the drain", func() error {
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != seats {
			return harness.Errf("course stats %+v", cs)
		}
		n := 0
		for i := range users {
			msgs, err := s.Mailpit.MessagesToWithSubject(users[i].Email, "enrolled")
			if err != nil {
				return err
			}
			n += len(msgs)
		}
		if n != seats {
			return harness.Errf("%d enrollment mails across the %d students, want %d", n, students, seats)
		}
		return nil
	})
	for _, i := range losers {
		if msgs, _ := s.Mailpit.MessagesToWithSubject(users[i].Email, "enrolled"); len(msgs) != 0 {
			t.Errorf("a refused student (%d) got %d enrollment mails", i, len(msgs))
		}
	}
	if n := s.MustCount(t, `SELECT count(*) FROM notification.deliveries WHERE kind = 'enrollment_confirmation' AND status = 'sent'`); n != seats {
		t.Errorf("sent enrollment deliveries: %d, want %d", n, seats)
	}
	if fe, err := s.Mongo.Count("failed_events", nil); err != nil || fe != 0 {
		t.Errorf("failed_events holds %d documents (err %v), want 0", fe, err)
	}

	// Backpressure.
	close(stopSampling)
	sampling.Wait()
	peak.Lock()
	t.Logf("peak outbox backlog %d, peak notification queue (unsent deliveries) %d, peak consumer lag %d", peak.backlog, peak.queue, peak.lag)
	if peak.backlog == 0 && peak.queue == 0 && peak.lag == 0 {
		t.Errorf("no backpressure was visible during the burst: the outbox backlog, the delivery queue and the consumer lag never rose above 0 in 20 ms samples")
	}
	peak.Unlock()
	if backlog, _ := s.OutboxUnpublished(); backlog != 0 {
		t.Errorf("outbox backlog %d after the drain, want 0", backlog)
	}
	harness.Eventually(t, time.Minute, "the workers' lag and queue gauges to read 0", func() error {
		var got float64
		for _, name := range []string{harness.CourseWorker, harness.AnalyticsWorker, harness.NotificationWorker, harness.AuditWorker} {
			m, err := s.Metrics(name)
			if err != nil {
				return err
			}
			got += m.Sum("kafka_consumer_lag") + m.Sum("asynq_queue_depth") + m.Sum("asynq_retry_count")
		}
		if got != 0 {
			return harness.Errf("kafka_consumer_lag + asynq_queue_depth + asynq_retry_count over the workers = %v, want 0", got)
		}
		return nil
	})
}
