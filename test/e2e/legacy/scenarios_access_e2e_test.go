//go:build e2e_legacy

package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// accessRow is one request of the access matrix: who calls what, and the
// status and error code the real binary must answer.
type accessRow struct {
	name   string
	method string
	url    string
	token  string
	body   any
	status int
	code   string // error code for a 4xx; "" for a 2xx
}

// runMatrix sends every row through the real binaries and checks the status,
// the error code, the standard {code, message} shape of every error and that
// nothing is a 5xx. It returns the number of rows that failed.
func runMatrix(t *testing.T, rows []accessRow) int {
	t.Helper()
	failed := 0
	for _, r := range rows {
		resp, err := harness.DoFull(r.method, r.url, r.token, r.body, nil)
		if err != nil {
			t.Errorf("%s: %s %s: %v", r.name, r.method, r.url, err)
			failed++
			continue
		}
		switch {
		case resp.Status >= 500:
			t.Errorf("%s: HTTP %d %s: a 5xx", r.name, resp.Status, resp.Body)
			failed++
		case resp.Status != r.status:
			t.Errorf("%s: HTTP %d %s, want %d %s", r.name, resp.Status, resp.Body, r.status, r.code)
			failed++
		case r.status >= 400:
			var e struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(resp.Body, &e); err != nil || e.Code == "" || e.Message == "" {
				t.Errorf("%s: the %d body %s is not the standard {code, message} shape", r.name, resp.Status, resp.Body)
				failed++
			} else if r.code != "" && e.Code != r.code {
				t.Errorf("%s: error code %q, want %q", r.name, e.Code, r.code)
				failed++
			}
		}
	}
	return failed
}

// TestE2E_E14_AuthAndAccessOnNewSurface is E14, matrix row E14 (Phase 4 for
// failed-events and replay, Phase 6 for most popular and failed-events metrics).
//
// Steps:
//  1. Stack as E01 with one parked failed event (as in E04) and one published
//     course with an enrolled student who completed it (so a certificate exists).
//  2. Tokens for: no token, a student enrolled in the course, a student not
//     enrolled, an instructor who owns the course, an instructor who does not
//     own it, an admin, and an admin token made stale by a role change.
//  3. Each new endpoint is called with each token, through the real binaries:
//     the worker's failed-events list, get and replay; the analytics platform,
//     popular-courses, issues, per-course and daily-enrollment routes;
//     certificates (list, by id, public verify); the admin role route; and the
//     Idempotency-Key behaviour on POST /courses and POST /enrollments.
//
// Assertions (the policy is the one in DR-022 to DR-025 and ADR-0031):
//   - Failed-events list, get and replay: admin 200; student and instructor 403;
//     no token 401; a stale admin token 401 token_stale. A non-admin replay
//     changes nothing (the document stays parked with its attempt count and no
//     effect appears). A malformed event id is 400, an unknown one 404. The
//     admin's replay of the parked event is 200, once.
//   - Popular courses and the platform totals: any authenticated user 200, no
//     token 401. Issues (the failed-events metric): admin 200 and counts the
//     parked event; student and instructor 403; no token 401.
//   - Per-course analytics: the owning instructor 200, an instructor who does
//     not own it 404, an enrolled student 200, another student 404, the admin
//     200, a malformed id 400.
//   - Certificates: the owner 200 on its list and by id, a non-owner 404 on the
//     id, no token 401; the public verification returns 200 with only the
//     course title, the student name and the issue date, 400 for a malformed
//     code, 404 for an unknown one, and answers 429 (rate_limited) after 30
//     requests a minute from one address while another address is unaffected.
//   - Idempotency-Key: the same key and body replays the stored 201 with
//     Idempotent-Replayed: true; the same key with another body is 422
//     idempotency_key_reused; an invalid key is 400; another user's identical
//     key is independent; a refused request (4xx) does not store the key.
//   - A token made stale by a role change is refused as token_stale on the
//     instructor's course write, the admin role route and the worker admin API
//     (never accepted, never a 5xx).
//   - Every error uses the standard {code, message} shape and none is a 5xx.
//
// Observed, not asserted: GET /analytics/issues has no token-version check, so
// a stale admin token still reads it (200); the spec only requires the failed-
// events routes and replay to refuse it.
func TestE2E_E14_AuthAndAccessOnNewSurface(t *testing.T) {
	const budget = 3
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"KAFKA_CONSUMER_MAX_ATTEMPTS": fmt.Sprint(budget),
		"KAFKA_CONSUMER_BACKOFF_BASE": "50ms",
		"KAFKA_CONSUMER_BACKOFF_MAX":  "100ms",
	}})
	owner := s.NewInstructor(t)
	other := s.NewInstructor(t)
	enrolled := s.NewStudentNamed(t, "Enrolled")
	outsider := s.NewStudentNamed(t, "Outsider")
	admin := s.Login(t, s.AdminEmail())
	staleAdmin := promoted(t, s, "admin")
	staleInstructor := promoted(t, s, "instructor")
	course := s.PublishedCourse(t, owner, 1)
	s.WaitSettled(t, time.Minute)
	enrollment := s.Enroll(t, enrolled, course)
	s.CompleteAll(t, enrolled, enrollment)

	// A parked event, as in E04.
	ghostEnrollment, ghostStudent := uuid.New(), uuid.New()
	payload := fmt.Sprintf(`{"enrollment_id":%q,"student_id":%q,"completed_at":%q}`, ghostEnrollment, ghostStudent, time.Now().UTC().Format(time.RFC3339))
	poisonID := s.NewEventProducer(t).Produce(t, "progress.course_completed", enrollment, payload, 1<<40)
	harness.Eventually(t, time.Minute, "the poison event parked", func() error {
		if docs, err := s.Mongo.FailedEvents(poisonID); err != nil || len(docs) != 1 {
			return harness.Errf("%d failed_events documents (err %v), want 1", len(docs), err)
		}
		return nil
	})
	s.WaitSettled(t, 2*time.Minute)
	var cert struct{ ID, Code string }
	harness.Eventually(t, time.Minute, "the student's certificate", func() error {
		certs := s.Certificates(t, enrolled)
		if len(certs) != 1 {
			return harness.Errf("%d certificates, want 1", len(certs))
		}
		cert.ID, _ = certs[0]["id"].(string)
		cert.Code, _ = certs[0]["verification_code"].(string)
		return nil
	})
	harness.Eventually(t, time.Minute, "analytics to know the owner of the course", func() error {
		if code, _, err := harness.Do("GET", s.AnalyticsURL+"/api/v1/analytics/courses/"+course.ID.String(), owner.Token, nil); err != nil || code != 200 {
			return harness.Errf("the owner reads the course stats: HTTP %d (err %v)", code, err)
		}
		return nil
	})

	// Make staleAdmin and staleInstructor stale by demoting them.
	for _, u := range []*harness.User{staleAdmin, staleInstructor} {
		code, body, err := harness.Do("PATCH", s.IdentityURL+"/api/v1/admin/users/"+u.ID.String()+"/role", admin.Token, map[string]string{"role": "student", "status": "active"})
		if err != nil || code != 200 {
			t.Fatalf("demote %s: HTTP %d %s (err %v)", u.Email, code, body, err)
		}
	}

	W := s.WorkerAdminURL + "/api/v1/admin/failed-events"
	A := s.AnalyticsURL + "/api/v1/analytics"
	C := s.CourseURL + "/api/v1"
	I := s.IdentityURL + "/api/v1"
	unknown := uuid.NewString()
	failures := 0

	t.Run("failed-events routes", func(t *testing.T) {
		var rows []accessRow
		for _, tc := range []struct {
			who, token string
			status     int
			code       string
		}{
			{"no token", "", 401, "unauthorized"},
			{"enrolled student", enrolled.Token, 403, "forbidden"},
			{"outsider student", outsider.Token, 403, "forbidden"},
			{"owner instructor", owner.Token, 403, "forbidden"},
			{"other instructor", other.Token, 403, "forbidden"},
			{"stale admin", staleAdmin.Token, 401, "token_stale"},
			{"admin", admin.Token, 200, ""},
		} {
			rows = append(rows,
				accessRow{"list as " + tc.who, "GET", W, tc.token, nil, tc.status, tc.code},
				accessRow{"get as " + tc.who, "GET", W + "/" + poisonID, tc.token, nil, tc.status, tc.code},
			)
			if tc.status != 200 { // the real replay is the last step, with the cause fixed
				rows = append(rows, accessRow{"replay as " + tc.who, "POST", W + "/" + poisonID + "/replay", tc.token, nil, tc.status, tc.code})
			}
		}
		rows = append(rows,
			accessRow{"get, malformed id", "GET", W + "/not-a-uuid", admin.Token, nil, 400, "validation_failed"},
			accessRow{"get, unknown id", "GET", W + "/" + unknown, admin.Token, nil, 404, "not_found"},
			accessRow{"replay, malformed id", "POST", W + "/not-a-uuid/replay", admin.Token, nil, 400, "validation_failed"},
			accessRow{"replay, unknown id", "POST", W + "/" + unknown + "/replay", admin.Token, nil, 404, "not_found"},
			accessRow{"list, bad status filter", "GET", W + "?status=bogus", admin.Token, nil, 400, "validation_failed"},
		)
		failures += runMatrix(t, rows)

		// The refused replays changed nothing.
		docs, err := s.Mongo.FailedEvents(poisonID)
		if err != nil || len(docs) != 1 {
			t.Fatalf("failed_events documents: %d (err %v)", len(docs), err)
		}
		if docs[0]["status"] != "parked" {
			t.Errorf("a refused replay changed the status to %v", docs[0]["status"])
		}
		if attempts, _ := docs[0]["attempts"].(int32); attempts != budget {
			t.Errorf("a refused replay changed the attempt count to %v, want %d", docs[0]["attempts"], budget)
		}
		if n, err := s.ProcessedCount("analytics-aggregator", poisonID); err != nil || n != 0 {
			t.Errorf("a refused replay left %d processed_events rows (err %v), want 0", n, err)
		}
	})

	t.Run("analytics routes", func(t *testing.T) {
		stats := A + "/courses/" + course.ID.String()
		daily := stats + "/daily-enrollments"
		var rows []accessRow
		for _, tc := range []struct {
			who, token string
			platform   int
			issues     int
			course     int
		}{
			{"no token", "", 401, 401, 401},
			{"enrolled student", enrolled.Token, 200, 403, 200},
			{"outsider student", outsider.Token, 200, 403, 404},
			{"owner instructor", owner.Token, 200, 403, 200},
			{"other instructor", other.Token, 200, 403, 404},
			{"admin", admin.Token, 200, 200, 200},
		} {
			code := func(status int) string {
				switch status {
				case 401:
					return "unauthorized"
				case 403:
					return "forbidden"
				case 404:
					return "not_found"
				}
				return ""
			}
			rows = append(rows,
				accessRow{"platform as " + tc.who, "GET", A + "/platform", tc.token, nil, tc.platform, code(tc.platform)},
				accessRow{"popular-courses as " + tc.who, "GET", A + "/popular-courses", tc.token, nil, tc.platform, code(tc.platform)},
				accessRow{"issues as " + tc.who, "GET", A + "/issues", tc.token, nil, tc.issues, code(tc.issues)},
				accessRow{"course stats as " + tc.who, "GET", stats, tc.token, nil, tc.course, code(tc.course)},
				accessRow{"daily enrollments as " + tc.who, "GET", daily, tc.token, nil, tc.course, code(tc.course)},
			)
		}
		rows = append(rows,
			accessRow{"course stats, malformed id", "GET", A + "/courses/not-a-uuid", admin.Token, nil, 400, "validation_failed"},
			accessRow{"course stats of an unknown course, instructor", "GET", A + "/courses/" + unknown, owner.Token, nil, 404, "not_found"},
		)
		failures += runMatrix(t, rows)

		// The failed-events metric counts the parked event.
		resp, err := harness.DoFull("GET", A+"/issues", admin.Token, nil, nil)
		if err != nil || resp.Status != 200 {
			t.Fatalf("issues: %+v (err %v)", resp, err)
		}
		var issues struct {
			FailedEvents struct {
				Total      int `json:"total"`
				Unresolved int `json:"unresolved"`
			} `json:"failed_events"`
		}
		if err := json.Unmarshal(resp.Body, &issues); err != nil || issues.FailedEvents.Total != 1 || issues.FailedEvents.Unresolved != 1 {
			t.Errorf("issues report %s (err %v), want 1 failed event, 1 unresolved", resp.Body, err)
		}
		// Observation: a stale admin token on the issues route.
		if resp, err := harness.DoFull("GET", A+"/issues", staleAdmin.Token, nil, nil); err == nil {
			t.Logf("OBSERVED: GET /analytics/issues with a stale admin token answers HTTP %d (the route has no token-version check)", resp.Status)
		}
		// Popular courses lists the course with ids and counts only.
		if items := s.PopularCourses(t, outsider); len(items) == 0 {
			t.Errorf("popular-courses is empty although a course has an enrollment")
		}
	})

	t.Run("certificates", func(t *testing.T) {
		rows := []accessRow{
			{"list, no token", "GET", C + "/certificates", "", nil, 401, "unauthorized"},
			{"list, student", "GET", C + "/certificates", enrolled.Token, nil, 200, ""},
			{"by id, no token", "GET", C + "/certificates/" + cert.ID, "", nil, 401, "unauthorized"},
			{"by id, owner", "GET", C + "/certificates/" + cert.ID, enrolled.Token, nil, 200, ""},
			{"by id, other student", "GET", C + "/certificates/" + cert.ID, outsider.Token, nil, 404, "not_found"},
			{"by id, instructor", "GET", C + "/certificates/" + cert.ID, owner.Token, nil, 404, "not_found"},
			{"by id, admin", "GET", C + "/certificates/" + cert.ID, admin.Token, nil, 404, "not_found"},
			{"by id, unknown", "GET", C + "/certificates/" + unknown, enrolled.Token, nil, 404, "not_found"},
			{"by id, malformed", "GET", C + "/certificates/not-a-uuid", enrolled.Token, nil, 400, "validation_failed"},
			{"verify, no token", "GET", C + "/certificates/verify/" + cert.Code, "", nil, 200, ""},
			{"verify, malformed code", "GET", C + "/certificates/verify/" + strings.ToLower(cert.Code), "", nil, 400, "validation_failed"},
			{"verify, unknown code", "GET", C + "/certificates/verify/ABCDEFGHIJKLMNOPQRSTUVWXYZ", "", nil, 404, "not_found"},
		}
		failures += runMatrix(t, rows)

		resp, err := harness.DoFull("GET", C+"/certificates/verify/"+cert.Code, "", nil, nil)
		if err != nil || resp.Status != 200 {
			t.Fatalf("verify: %+v (err %v)", resp, err)
		}
		var fields map[string]any
		_ = json.Unmarshal(resp.Body, &fields)
		keys := keysOf(fields)
		sort.Strings(keys)
		if strings.Join(keys, ",") != "course_title,issued_at,student_name" {
			t.Errorf("the public verification returns fields %v, want exactly course_title, issued_at and student_name", keys)
		}

		// 30 a minute per address: the 31st from one address is refused, another
		// address is not.
		same := map[string]string{"X-Forwarded-For": "203.0.113.50"}
		refused := -1
		for i := 1; i <= 40; i++ {
			r, err := harness.DoFull("GET", C+"/certificates/verify/"+cert.Code, "", nil, same)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status == 429 {
				refused = i
				if r.ErrorCode() != "rate_limited" {
					t.Errorf("the 429 has code %q, want rate_limited", r.ErrorCode())
				}
				t.Logf("verification refused from request %d; Retry-After %q", i, r.Header.Get("Retry-After"))
				break
			}
			if r.Status != 200 {
				t.Fatalf("verification %d from one address: HTTP %d %s", i, r.Status, r.Body)
			}
		}
		if refused < 25 || refused > 33 {
			t.Errorf("the verification route refused request number %d from one address (never if -1), want about the 31st (30 a minute)", refused)
		}
		if r, err := harness.DoFull("GET", C+"/certificates/verify/"+cert.Code, "", nil, map[string]string{"X-Forwarded-For": "203.0.113.51"}); err != nil || r.Status != 200 {
			t.Errorf("another address is refused while the first is limited: %+v (err %v)", r, err)
		}
	})

	t.Run("idempotency keys", func(t *testing.T) {
		body := func(slug string) map[string]any {
			return map[string]any{"title": "Idem " + slug, "slug": slug, "description": "d", "enrollment_limit": 5}
		}
		slug := "idem-" + uuid.NewString()[:8]
		key := "e14-" + uuid.NewString()
		hdr := map[string]string{"Idempotency-Key": key}
		first, err := harness.DoFull("POST", C+"/courses", owner.Token, body(slug), hdr)
		if err != nil || first.Status != 201 {
			t.Fatalf("create with a key: %+v (err %v)", first, err)
		}
		replay, err := harness.DoFull("POST", C+"/courses", owner.Token, body(slug), hdr)
		if err != nil || replay.Status != 201 || replay.Header.Get("Idempotent-Replayed") != "true" {
			t.Errorf("same key, same body: %+v (err %v), want the stored 201 with Idempotent-Replayed: true", replay, err)
		}
		var a, b struct {
			Course struct {
				ID string `json:"id"`
			} `json:"course"`
		}
		_ = json.Unmarshal(first.Body, &a)
		_ = json.Unmarshal(replay.Body, &b)
		if a.Course.ID == "" || a.Course.ID != b.Course.ID {
			t.Errorf("the replay returned course %q, the first request created %q", b.Course.ID, a.Course.ID)
		}
		if n := s.MustCount(t, `SELECT count(*) FROM course.courses WHERE slug = ?`, slug); n != 1 {
			t.Errorf("%d courses with the slug after a replayed create, want 1", n)
		}
		if r, err := harness.DoFull("POST", C+"/courses", owner.Token, body(slug+"-changed"), hdr); err != nil || r.Status != 422 || r.ErrorCode() != "idempotency_key_reused" {
			t.Errorf("same key, different body: %+v (err %v), want 422 idempotency_key_reused", r, err)
		}
		if r, err := harness.DoFull("POST", C+"/courses", owner.Token, body(slug+"-bad"), map[string]string{"Idempotency-Key": strings.Repeat("k", 300)}); err != nil || r.Status != 400 || r.ErrorCode() != "idempotency_key_invalid" {
			t.Errorf("a 300-character key: %+v (err %v), want 400 idempotency_key_invalid", r, err)
		}
		// Scoped per user: the other instructor's identical key is independent.
		if r, err := harness.DoFull("POST", C+"/courses", other.Token, body(slug+"-other"), hdr); err != nil || r.Status != 201 || r.Header.Get("Idempotent-Replayed") == "true" {
			t.Errorf("another user's identical key: %+v (err %v), want a fresh 201", r, err)
		}
		// A refused request does not store the key: the retry runs again.
		refusedKey := map[string]string{"Idempotency-Key": "e14-refused-" + uuid.NewString()}
		for i := 0; i < 2; i++ {
			if r, err := harness.DoFull("POST", C+"/enrollments", outsider.Token, map[string]string{"course_id": unknown}, refusedKey); err != nil || r.Status != 404 || r.Header.Get("Idempotent-Replayed") == "true" {
				t.Errorf("enrollment in an unknown course with a key, attempt %d: %+v (err %v), want a fresh 404", i+1, r, err)
			}
		}
		enrollKey := map[string]string{"Idempotency-Key": "e14-enroll-" + uuid.NewString()}
		for i, wantReplay := range []bool{false, true} {
			r, err := harness.DoFull("POST", C+"/enrollments", outsider.Token, map[string]string{"course_id": course.ID.String()}, enrollKey)
			if err != nil || r.Status != 201 || (r.Header.Get("Idempotent-Replayed") == "true") != wantReplay {
				t.Errorf("enrollment with a key, attempt %d: %+v (err %v), want 201 (replayed: %v)", i+1, r, err, wantReplay)
			}
		}
		if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE course_id = ? AND student_id = ?`, course.ID, outsider.ID); n != 1 {
			t.Errorf("%d enrollment rows for the student after a replayed enroll, want 1", n)
		}
	})

	t.Run("token_stale after a role change", func(t *testing.T) {
		rows := []accessRow{
			{"stale instructor creates a course", "POST", C + "/courses", staleInstructor.Token,
				map[string]any{"title": "x", "slug": "stale-" + uuid.NewString()[:8], "description": "d"}, 401, "token_stale"},
			{"stale admin changes a role", "PATCH", I + "/admin/users/" + outsider.ID.String() + "/role", staleAdmin.Token,
				map[string]string{"role": "instructor", "status": "active"}, 401, "token_stale"},
			{"stale admin reads /me", "GET", I + "/me", staleAdmin.Token, nil, 401, "token_stale"},
			{"student changes a role", "PATCH", I + "/admin/users/" + outsider.ID.String() + "/role", outsider.Token,
				map[string]string{"role": "instructor", "status": "active"}, 403, "forbidden"},
			{"no token changes a role", "PATCH", I + "/admin/users/" + outsider.ID.String() + "/role", "",
				map[string]string{"role": "instructor", "status": "active"}, 401, "unauthorized"},
			{"admin changes a role of an unknown user", "PATCH", I + "/admin/users/" + unknown + "/role", admin.Token,
				map[string]string{"role": "instructor", "status": "active"}, 404, "not_found"},
			{"admin, malformed user id", "PATCH", I + "/admin/users/not-a-uuid/role", admin.Token,
				map[string]string{"role": "instructor", "status": "active"}, 400, "validation_failed"},
		}
		failures += runMatrix(t, rows)
		if role := s.MustCount(t, `SELECT count(*) FROM identity.users WHERE id = ? AND role = 'student'`, outsider.ID); role != 1 {
			t.Errorf("the refused role changes changed the outsider's role")
		}
	})

	// The one admin replay: fix the cause, then replay once (200), then a no-op.
	if err := s.DB().Exec(`INSERT INTO analytics.enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at, last_event_id)
		VALUES (?, ?, ?, 'active', now(), 0)`, ghostEnrollment, course.ID, ghostStudent).Error; err != nil {
		t.Fatalf("fix the cause of the poison event: %v", err)
	}
	code, res, raw := s.ReplayFailedEvent(t, poisonID)
	if code != http.StatusOK || res == nil || !res.Replayed {
		t.Errorf("the admin's replay: HTTP %d %s, want 200 replayed", code, raw)
	}
	if code, res, raw := s.ReplayFailedEvent(t, poisonID); code != http.StatusOK || res == nil || res.Replayed {
		t.Errorf("the second replay: HTTP %d %s, want 200 with replayed=false", code, raw)
	}
	if failures > 0 {
		t.Logf("%d matrix rows failed; see the errors above", failures)
	}
}
