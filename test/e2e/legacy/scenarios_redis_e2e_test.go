//go:build e2e_legacy

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestE2E_E10_RedisFlushWithNewServices is E10, matrix row E10 (Phase 3).
//
// The existing test TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks
// (redis_flush_e2e_test.go, tag integration) stays as it is and keeps running.
// This scenario re-runs its steps against the four real binaries with the Week
// 3 pipeline (Kafka, consumers, Asynq, Temporal) running over the same Redis.
//
// Steps:
//  1. Stack as E01. An instructor and a second admin are promoted through the
//     admin API (token version 2); the bootstrap admin is the one created
//     straight in the database (token version 1, like the seed tool's).
//  2. A background client enrolls a dozen students in a course the whole time,
//     and one student completes a lesson, so consumers and Asynq are active
//     across the flushes; its status codes (and those of every request below)
//     are collected: none may be a 5xx.
//  3. FLUSHALL. The pre-flush instructor token is refused with token_stale on
//     a course write (never a 5xx, never accepted); the same flush with the
//     second admin's token on the worker's admin API gives token_stale; the
//     analytics read and the public routes keep working.
//  4. Log in again: both tokens work (course write 201, worker admin 200) and
//     the cached version is re-seeded. FLUSHALL again, refresh instead of
//     login: the new access token works.
//  5. Demote the instructor, FLUSHALL, log the demoted user in: the old
//     instructor token stays refused, the demoted user's own token is 403.
//  6. Demote the bootstrap admin (version 1 to 2), then FLUSHALL without any
//     login: the old admin token must still be refused by the worker admin
//     API (revocation survives a flush).
//  7. The rate limit on the public certificate verification route is exhausted
//     (429), FLUSHALL, and the route answers 404 again (the state is gone, the
//     answer is not a 5xx). An Idempotency-Key replay after the flush is a 201
//     replay or a clean 409, not a 5xx.
//
// Assertions beyond the existing test's:
//   - The worker process did not exit; a student who enrolls after the flushes
//     gets their delivery mail (the Asynq server recovered its queues), and the
//     scheduled jobs still tick.
//   - No event is lost or applied twice across the flushes: course.enrollments
//     rows, analytics course stats, the archive and the mails all equal the
//     number of enrollments made.
func TestE2E_E10_RedisFlushWithNewServices(t *testing.T) {
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"JOBS_ANALYTICS_RECONCILE_INTERVAL": "1m",
	}})
	ctx := context.Background()
	flush := func() {
		t.Helper()
		if err := s.Env.RDB.FlushAll(ctx).Err(); err != nil {
			t.Fatal(err)
		}
	}

	var obs statusLog
	call := func(method, url, token string, body any, headers map[string]string) *harness.Response {
		t.Helper()
		resp, err := harness.DoFull(method, url, token, body, headers)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}
		obs.add(method+" "+url, resp.Status)
		return resp
	}
	stale := func(resp *harness.Response) bool { return resp.Status == 401 && resp.ErrorCode() == "token_stale" }

	inst := s.NewInstructor(t)
	admin2 := promoted(t, s, "admin")
	bootstrap := s.Login(t, s.AdminEmail()) // token version 1
	course := s.PublishedCourse(t, inst, 1)
	students := s.NewStudents(t, 14, 7)
	s.WaitSettled(t, time.Minute)

	createCourse := func(token string) *harness.Response {
		return call("POST", s.CourseURL+"/api/v1/courses", token, map[string]any{
			"title": "E10 Course", "slug": "e10-" + uuid.NewString()[:10], "description": "flush", "enrollment_limit": 10,
		}, nil)
	}
	workerAdmin := func(token string) *harness.Response {
		return call("GET", s.WorkerAdminURL+"/api/v1/admin/failed-events", token, nil, nil)
	}
	if r := createCourse(inst.Token); r.Status != 201 {
		t.Fatalf("instructor creates a course before any flush: HTTP %d %s", r.Status, r.Body)
	}
	if r := workerAdmin(admin2.Token); r.Status != 200 {
		t.Fatalf("second admin on the worker admin API before any flush: HTTP %d %s", r.Status, r.Body)
	}
	if v, err := s.Env.RDB.Get(ctx, auth.TokenVersionRedisKey(inst.ID.String())).Result(); err != nil || v != "2" {
		t.Fatalf("cached token version of the instructor before the flush: %q (err %v), want 2", v, err)
	}

	// Step 2: traffic that spans the flushes. students[0..11] enroll one after
	// the other; students[12] completes a lesson; students[13] enrolls last.
	var bg sync.WaitGroup
	var enrolled sync.Map // email -> struct{}
	stopBG := make(chan struct{})
	bg.Add(1)
	go func() {
		defer bg.Done()
		for _, u := range students[:12] {
			select {
			case <-stopBG:
				return
			case <-time.After(250 * time.Millisecond):
			}
			resp, err := harness.DoFull("POST", s.CourseURL+"/api/v1/enrollments", u.Token,
				map[string]string{"course_id": course.ID.String()}, map[string]string{"Idempotency-Key": "e10-" + u.ID.String()})
			if err != nil {
				obs.add("background enroll error: "+err.Error(), 599)
				continue
			}
			obs.add("POST /enrollments", resp.Status)
			if resp.Status == 201 {
				enrolled.Store(u.Email, struct{}{})
			} else {
				obs.add(fmt.Sprintf("background enroll answered %d %s", resp.Status, resp.Body), 598)
			}
		}
	}()
	completer := students[12]
	completerEnrollment := s.Enroll(t, completer, course)
	enrolled.Store(completer.Email, struct{}{})
	keys := s.LessonKeys(t, completer, completerEnrollment)

	// Step 3: the flush.
	flush()
	if _, err := s.Env.RDB.Get(ctx, auth.TokenVersionRedisKey(inst.ID.String())).Result(); err == nil {
		t.Fatal("precondition: the cached version should be gone after the flush")
	}
	if r := createCourse(inst.Token); !stale(r) {
		t.Fatalf("after a flush the pre-flush instructor token must be refused as token_stale on a course write: HTTP %d %s", r.Status, r.Body)
	}
	if r := workerAdmin(admin2.Token); !stale(r) {
		t.Fatalf("after a flush the pre-flush admin token must be refused as token_stale by the worker admin API: HTTP %d %s", r.Status, r.Body)
	}
	if r := call("GET", s.AnalyticsURL+"/api/v1/analytics/platform", inst.Token, nil, nil); r.Status != 200 {
		t.Errorf("analytics platform read with the pre-flush token after the flush: HTTP %d %s, want 200 (it has no token-version check)", r.Status, r.Body)
	}
	if r := call("GET", s.IdentityURL+"/api/v1/me", inst.Token, nil, nil); r.Status != 200 {
		t.Errorf("identity /me with the pre-flush token after the flush: HTTP %d %s, want 200 (Postgres fallback re-seeds the version)", r.Status, r.Body)
	}
	if r := call("GET", s.CourseURL+"/api/v1/courses", "", nil, nil); r.Status != 200 {
		t.Errorf("public catalogue after the flush: HTTP %d, want 200", r.Status)
	}
	s.CompleteLesson(t, completer, completerEnrollment, keys[0]) // progress keeps working across the flush

	// Step 4: logging in again restores access; so does a refresh.
	inst = s.Login(t, inst.Email)
	admin2 = s.Login(t, admin2.Email)
	if v, err := s.Env.RDB.Get(ctx, auth.TokenVersionRedisKey(inst.ID.String())).Result(); err != nil || v != "2" {
		t.Errorf("login after the flush should re-seed the cached version (want 2): %q (err %v)", v, err)
	}
	if r := createCourse(inst.Token); r.Status != 201 {
		t.Errorf("instructor after a re-login: HTTP %d %s, want 201", r.Status, r.Body)
	}
	if r := workerAdmin(admin2.Token); r.Status != 200 {
		t.Errorf("worker admin API after the admin's re-login: HTTP %d %s, want 200", r.Status, r.Body)
	}
	flush()
	refreshed := call("POST", s.IdentityURL+"/api/v1/auth/refresh", "", map[string]string{"refresh_token": inst.RefreshToken}, nil)
	if refreshed.Status != 200 {
		t.Fatalf("refresh after the flush: HTTP %d %s", refreshed.Status, refreshed.Body)
	}
	var rt struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(refreshed.Body, &rt); err != nil {
		t.Fatal(err)
	}
	if r := createCourse(rt.AccessToken); r.Status != 201 {
		t.Errorf("instructor after a refresh: HTTP %d %s, want 201", r.Status, r.Body)
	}

	// Step 5: demotion survives a flush.
	if r := call("PATCH", s.IdentityURL+"/api/v1/admin/users/"+inst.ID.String()+"/role", admin2.Token,
		map[string]string{"role": "student", "status": "active"}, nil); r.Status != 200 {
		t.Fatalf("demote the instructor: HTTP %d %s", r.Status, r.Body)
	}
	flush()
	demoted := s.Login(t, inst.Email)
	if r := createCourse(rt.AccessToken); !stale(r) {
		t.Errorf("the old instructor token must stay refused after a demotion and a flush: HTTP %d %s", r.Status, r.Body)
	}
	if r := createCourse(demoted.Token); r.Status != 403 {
		t.Errorf("the demoted user's own token: HTTP %d %s, want 403", r.Status, r.Body)
	}

	// Step 6: the bootstrap admin (token version 1) is demoted; a flush must
	// not bring its old token back.
	if r := call("PATCH", s.IdentityURL+"/api/v1/admin/users/"+bootstrap.ID.String()+"/role", admin2.Token,
		map[string]string{"role": "student", "status": "active"}, nil); r.Status != 200 {
		t.Fatalf("demote the bootstrap admin: HTTP %d %s", r.Status, r.Body)
	}
	if r := workerAdmin(bootstrap.Token); !stale(r) {
		t.Fatalf("the demoted admin's token on the worker admin API before any flush: HTTP %d %s, want token_stale", r.Status, r.Body)
	}
	flush()
	t.Run("a demoted admin's token stays refused by the worker admin API after a flush", func(t *testing.T) {
		if r := workerAdmin(bootstrap.Token); r.Status == 200 {
			t.Errorf("PRODUCT DEFECT: after FLUSHALL the worker admin API (RequireFreshPrivilege without a Postgres source, services/worker/admin/admin.go) accepts the old token of an admin demoted from token version 1 to 2: a cache miss reads as version 1. The demoted admin can list failed events and replay them until they log in. HTTP %d", r.Status)
		} else if !stale(r) {
			t.Errorf("worker admin API with the demoted admin's token after a flush: HTTP %d %s, want 401 token_stale", r.Status, r.Body)
		}
	})

	// Step 7: rate-limit and idempotency state.
	const code = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	same := map[string]string{"X-Forwarded-For": "203.0.113.77"}
	limited := false
	for i := 0; i < 40 && !limited; i++ {
		r := call("GET", s.CourseURL+"/api/v1/certificates/verify/"+code, "", nil, same)
		limited = r.Status == 429
	}
	if !limited {
		t.Errorf("40 verifications from one address were never rate limited (429)")
	}
	flush()
	if r := call("GET", s.CourseURL+"/api/v1/certificates/verify/"+code, "", nil, same); r.Status != 404 {
		t.Errorf("verification right after a flush: HTTP %d, want 404 (the limit state is gone, the code is unknown)", r.Status)
	}
	if r := call("POST", s.CourseURL+"/api/v1/enrollments", students[0].Token, map[string]string{"course_id": course.ID.String()},
		map[string]string{"Idempotency-Key": "e10-" + students[0].ID.String()}); r.Status >= 500 {
		t.Errorf("a retried enrollment after the flush: HTTP %d %s, want a replay or a clean 4xx", r.Status, r.Body)
	}

	// Let the background enrollments finish while another flush lands in them.
	flush()
	bg.Wait()
	close(stopBG)

	// The worker survived and Asynq recovered: a student enrolling now gets mail.
	last := students[13]
	s.Enroll(t, last, course)
	enrolled.Store(last.Email, struct{}{})
	if !s.Worker.Running() {
		t.Fatalf("the worker process exited during the flushes")
	}
	harness.Eventually(t, 2*time.Minute, "the enrollment mail of a student who enrolled after the flushes", func() error {
		msgs, err := s.Mailpit.MessagesToWithSubject(last.Email, "enrolled")
		if err != nil || len(msgs) != 1 {
			return harness.Errf("%d mails (err %v), want 1", len(msgs), err)
		}
		return nil
	})
	harness.Eventually(t, 3*time.Minute, "the scheduled jobs to tick after the flushes", func() error {
		if completedTicks(t, s)["maintenance:analytics-reconcile"] == 0 {
			return harness.Errf("no completed analytics-reconcile tick in Redis")
		}
		return nil
	})

	// No event lost or applied twice.
	var want int64
	enrolled.Range(func(_, _ any) bool { want++; return true })
	s.WaitSettled(t, 3*time.Minute)
	if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE course_id = ?`, course.ID); n != want {
		t.Errorf("enrollment rows %d, want %d (one per successful enrollment)", n, want)
	}
	if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != want {
		t.Errorf("courses.enrollment_count %d, want %d", n, want)
	}
	harness.Eventually(t, 3*time.Minute, "analytics, the archive and Mailpit to match the enrollments made", func() error {
		if cs := s.CourseStatsFor(t, s.Login(t, admin2.Email), course); cs.TotalEnrollments != want {
			return harness.Errf("course stats %+v, want %d enrollments", cs, want)
		}
		var ids []string
		if err := s.DB().Raw(`SELECT event_id::text FROM platform.outbox`).Scan(&ids).Error; err != nil {
			return err
		}
		for _, id := range ids {
			if n, err := s.Mongo.ArchivedEvents(id); err != nil || n != 1 {
				return harness.Errf("event %s archived %d times (err %v), want 1", id, n, err)
			}
		}
		total := 0
		for _, u := range students {
			msgs, err := s.Mailpit.MessagesToWithSubject(u.Email, "enrolled")
			if err != nil {
				return err
			}
			total += len(msgs)
		}
		if int64(total) != want {
			return harness.Errf("%d enrollment mails, want %d", total, want)
		}
		return nil
	})
	if fe, err := s.Mongo.Count("failed_events", nil); err != nil || fe != 0 {
		t.Errorf("failed_events holds %d documents (err %v), want 0", fe, err)
	}
	if n := obs.fiveXX(); len(n) > 0 {
		t.Errorf("%d responses were 5xx, a transport error or an unexpected non-201 enrollment (shown as 598 and 599) across the flushes: %v", len(n), n)
	}
	t.Logf("%d requests observed across the flushes, none a 5xx", obs.total())
}

// promoted registers a user and gives it role through the admin API (token
// version 2), then logs in again so the token carries the role.
func promoted(t *testing.T, s *harness.Stack, role string) *harness.User {
	t.Helper()
	email := fmt.Sprintf("%s-%s@e2e.example.test", role, uuid.NewString()[:8])
	s.Register(t, email, role+" E2E")
	u := s.Login(t, email)
	code, body, err := harness.Do("PATCH", s.IdentityURL+"/api/v1/admin/users/"+u.ID.String()+"/role", s.AdminToken(t),
		map[string]string{"role": role, "status": "active"})
	if err != nil || code != http.StatusOK {
		t.Fatalf("promote %s to %s: HTTP %d %s (err %v)", email, role, code, body, err)
	}
	return s.Login(t, email)
}

// statusLog collects the status of every request a scenario sends, so "no 5xx
// anywhere" is one assertion at the end.
type statusLog struct {
	mu   sync.Mutex
	seen []string
	bad  []string
}

func (l *statusLog) add(what string, status int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, what)
	if status >= 500 {
		l.bad = append(l.bad, fmt.Sprintf("%s -> %d", what, status))
	}
}

func (l *statusLog) fiveXX() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.bad...)
}

func (l *statusLog) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.seen)
}
