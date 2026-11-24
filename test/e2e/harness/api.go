package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Password is the password of every user the harness creates.
const Password = "correct-horse-battery-1"

// User is a registered, logged-in account.
type User struct {
	Email        string
	ID           uuid.UUID
	Name         string
	Role         string
	Token        string // access token of the latest login
	RefreshToken string
}

// Course is a course the harness created.
type Course struct {
	ID        uuid.UUID
	Slug      string
	Title     string
	Module    uuid.UUID
	LessonIDs []uuid.UUID
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

// ipSeq numbers the client addresses sent as X-Forwarded-For: the identity
// service rate-limits register and login per client IP (5 and 20 a minute),
// and every request from the test comes from 127.0.0.1. The services trust the
// loopback proxy (TRUSTED_PROXIES), so each request claims its own address.
var ipSeq atomic.Uint32

func nextIP() string {
	n := ipSeq.Add(1)
	return fmt.Sprintf("10.%d.%d.%d", (n>>16)&255, (n>>8)&255, n&255)
}

// Do sends one JSON request and returns the status and body. token may be
// empty; body may be nil.
func Do(method, url, token string, body any) (int, []byte, error) {
	r, err := DoFull(method, url, token, body, nil)
	if err != nil {
		return 0, nil, err
	}
	return r.Status, r.Body, nil
}

// Response is a full HTTP response: status, headers and body.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// ErrorCode is the `code` of the standard {code, message} error body, "" when
// the body is not one.
func (r *Response) ErrorCode() string {
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(r.Body, &e)
	return e.Code
}

// DoFull is Do with extra request headers (an Idempotency-Key, a traceparent,
// an X-Forwarded-For of the caller's choosing) and the response headers back.
func DoFull(method, url, token string, body any, headers map[string]string) (*Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("X-Forwarded-For", nextIP())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Response{Status: resp.StatusCode, Header: resp.Header, Body: raw}, nil
}

// call is Do that fails the test unless the status is want.
func call(t testing.TB, want int, method, url, token string, body any) []byte {
	t.Helper()
	code, raw, err := Do(method, url, token, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	if code != want {
		t.Fatalf("%s %s: HTTP %d, want %d: %s", method, url, code, want, raw)
	}
	return raw
}

func decode(t testing.TB, raw []byte, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

// ---- identity ----

// Register registers email (202) without logging in.
func (s *Stack) Register(t testing.TB, email, name string) {
	t.Helper()
	call(t, http.StatusAccepted, "POST", s.IdentityURL+"/api/v1/auth/register", "", map[string]string{
		"email": email, "password": Password, "full_name": name,
	})
}

// Login logs email in (200) and returns the user with a fresh access token.
func (s *Stack) Login(t testing.TB, email string) *User {
	t.Helper()
	raw := call(t, http.StatusOK, "POST", s.IdentityURL+"/api/v1/auth/login", "", map[string]string{
		"email": email, "password": Password,
	})
	var r struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		User         struct {
			ID       string `json:"id"`
			FullName string `json:"full_name"`
			Role     string `json:"role"`
		} `json:"user"`
	}
	decode(t, raw, &r)
	id, err := uuid.Parse(r.User.ID)
	if err != nil || r.AccessToken == "" {
		t.Fatalf("login %s: no user id or token in %s", email, raw)
	}
	return &User{Email: email, ID: id, Name: r.User.FullName, Role: r.User.Role, Token: r.AccessToken, RefreshToken: r.RefreshToken}
}

func uniqueEmail(prefix string) string {
	return fmt.Sprintf("%s-%s@e2e.example.test", prefix, uuid.NewString()[:8])
}

// NewStudent registers a fresh student (register 202, login 200). Email
// verification is not required to log in (the verification mail is a
// notification; the account works at once), so there is no verify step.
func (s *Stack) NewStudent(t testing.TB) *User { return s.NewStudentNamed(t, "Student") }

// NewStudentNamed is NewStudent with a name prefix.
func (s *Stack) NewStudentNamed(t testing.TB, name string) *User {
	t.Helper()
	email := uniqueEmail("student")
	s.Register(t, email, name+" E2E")
	return s.Login(t, email)
}

// NewInstructor registers a user, promotes it to instructor through the admin
// API (PATCH /admin/users/{id}/role), and logs in again so the token carries
// the new role and token version.
func (s *Stack) NewInstructor(t testing.TB) *User {
	t.Helper()
	email := uniqueEmail("instructor")
	s.Register(t, email, "Instructor E2E")
	u := s.Login(t, email)
	call(t, http.StatusOK, "PATCH", s.IdentityURL+"/api/v1/admin/users/"+u.ID.String()+"/role", s.AdminToken(t),
		map[string]string{"role": "instructor", "status": "active"})
	return s.Login(t, email)
}

// AdminToken returns a fresh admin access token. The admin account is
// bootstrapped by Start (see bootstrapAdmin): there is no API to create the
// first admin.
func (s *Stack) AdminToken(t testing.TB) string {
	t.Helper()
	return s.Login(t, s.adminEmail).Token
}

// ---- course ----

// NewCourse creates a draft course with one module and `lessons` lessons.
func (s *Stack) NewCourse(t testing.TB, inst *User, lessons, enrollmentLimit int) *Course {
	t.Helper()
	slug := "e2e-" + uuid.NewString()[:12]
	c := &Course{Slug: slug, Title: "E2E Course " + slug}
	raw := call(t, http.StatusCreated, "POST", s.CourseURL+"/api/v1/courses", inst.Token, map[string]any{
		"title": c.Title, "slug": slug, "description": "created by the e2e harness", "enrollment_limit": enrollmentLimit,
	})
	var cr struct {
		Course struct {
			ID string `json:"id"`
		} `json:"course"`
	}
	decode(t, raw, &cr)
	c.ID = mustUUID(t, cr.Course.ID)

	raw = call(t, http.StatusCreated, "POST", s.CourseURL+"/api/v1/courses/"+c.ID.String()+"/modules", inst.Token,
		map[string]any{"title": "Module 1", "position": 1})
	var mr struct {
		ID string `json:"id"`
	}
	decode(t, raw, &mr)
	c.Module = mustUUID(t, mr.ID)

	for i := 1; i <= lessons; i++ {
		raw = call(t, http.StatusCreated, "POST", s.CourseURL+"/api/v1/modules/"+c.Module.String()+"/lessons", inst.Token, map[string]any{
			"title": fmt.Sprintf("Lesson %d", i), "position": i, "content_type": "text",
			"content": map[string]string{"body": "hello"}, "duration_s": 60,
		})
		var lr struct {
			ID string `json:"id"`
		}
		decode(t, raw, &lr)
		c.LessonIDs = append(c.LessonIDs, mustUUID(t, lr.ID))
	}
	return c
}

// StartPublish asks to publish (202) and returns without waiting.
func (s *Stack) StartPublish(t testing.TB, inst *User, c *Course) {
	t.Helper()
	call(t, http.StatusAccepted, "POST", s.CourseURL+"/api/v1/courses/"+c.ID.String()+"/publish", inst.Token, nil)
}

// PublishState is the state publish-status reports ("running", "ready", ...).
func (s *Stack) PublishState(t testing.TB, inst *User, c *Course) string {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.CourseURL+"/api/v1/courses/"+c.ID.String()+"/publish-status", inst.Token, nil)
	var r struct {
		State string `json:"state"`
	}
	decode(t, raw, &r)
	return r.State
}

// WaitPublished polls publish-status until "ready" (a Temporal workflow ran).
func (s *Stack) WaitPublished(t testing.TB, inst *User, c *Course) {
	t.Helper()
	last := ""
	Eventually(t, 2*time.Minute, "publish-status ready for "+c.Slug, func() error {
		last = s.PublishState(t, inst, c)
		switch last {
		case "ready":
			return nil
		case "publish_failed":
			t.Fatalf("publish of %s failed (publish-status publish_failed)", c.Slug)
		}
		return Errf("publish-status %q", last)
	})
}

// PublishedCourse creates a course with `lessons` lessons and publishes it.
func (s *Stack) PublishedCourse(t testing.TB, inst *User, lessons int) *Course {
	t.Helper()
	c := s.NewCourse(t, inst, lessons, 100)
	s.StartPublish(t, inst, c)
	s.WaitPublished(t, inst, c)
	return c
}

// ---- enrollment and progress ----

// Enroll enrolls student in c (201) and returns the enrollment id.
func (s *Stack) Enroll(t testing.TB, student *User, c *Course) uuid.UUID {
	t.Helper()
	raw := call(t, http.StatusCreated, "POST", s.CourseURL+"/api/v1/enrollments", student.Token, map[string]string{"course_id": c.ID.String()})
	var r struct {
		ID string `json:"id"`
	}
	decode(t, raw, &r)
	return mustUUID(t, r.ID)
}

// Withdraw withdraws an enrollment (DELETE /enrollments/{id}, 2xx).
func (s *Stack) Withdraw(t testing.TB, student *User, enrollment uuid.UUID) {
	t.Helper()
	code, raw, err := Do("DELETE", s.CourseURL+"/api/v1/enrollments/"+enrollment.String(), student.Token, nil)
	if err != nil || code/100 != 2 {
		t.Fatalf("withdraw %s: HTTP %d %s (err %v)", enrollment, code, raw, err)
	}
}

// LessonKeys lists the lesson keys of an enrollment's progress.
func (s *Stack) LessonKeys(t testing.TB, student *User, enrollment uuid.UUID) []uuid.UUID {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.CourseURL+"/api/v1/enrollments/"+enrollment.String()+"/progress", student.Token, nil)
	var r struct {
		Items []struct {
			LessonKey string `json:"lesson_key"`
		} `json:"items"`
	}
	decode(t, raw, &r)
	var keys []uuid.UUID
	for _, it := range r.Items {
		keys = append(keys, mustUUID(t, it.LessonKey))
	}
	return keys
}

// CompleteLesson marks a lesson complete (PUT .../complete; the service
// answers 204 No Content).
func (s *Stack) CompleteLesson(t testing.TB, student *User, enrollment, lessonKey uuid.UUID) {
	t.Helper()
	call(t, http.StatusNoContent, "PUT", s.CourseURL+"/api/v1/enrollments/"+enrollment.String()+"/lessons/"+lessonKey.String()+"/complete", student.Token, nil)
}

// CompleteAll completes every lesson of the enrollment, one after the other.
func (s *Stack) CompleteAll(t testing.TB, student *User, enrollment uuid.UUID) {
	t.Helper()
	for _, k := range s.LessonKeys(t, student, enrollment) {
		s.CompleteLesson(t, student, enrollment, k)
	}
}

// Certificates lists the caller's certificates (GET /certificates).
func (s *Stack) Certificates(t testing.TB, student *User) []map[string]any {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.CourseURL+"/api/v1/certificates", student.Token, nil)
	var r struct {
		Items []map[string]any `json:"items"`
	}
	decode(t, raw, &r)
	return r.Items
}

// ---- analytics ----

// Platform is GET /analytics/platform.
type Platform struct {
	TotalStudents         int64   `json:"total_students"`
	TotalInstructors      int64   `json:"total_instructors"`
	TotalCoursesPublished int64   `json:"total_courses_published"`
	AvgCoursesPerStudent  float64 `json:"avg_courses_per_student"`
}

// CourseStats is GET /analytics/courses/{id}.
type CourseStats struct {
	TotalEnrollments  int64   `json:"total_enrollments"`
	ActiveEnrollments int64   `json:"active_enrollments"`
	Completions       int64   `json:"completions"`
	CompletionRate    float64 `json:"completion_rate"`
	AvgCompletionS    *int64  `json:"avg_completion_s"`
}

// Same compares two readings by value (AvgCompletionS is a pointer, so ==
// would compare addresses).
func (c CourseStats) Same(o CourseStats) bool {
	if (c.AvgCompletionS == nil) != (o.AvgCompletionS == nil) {
		return false
	}
	if c.AvgCompletionS != nil && *c.AvgCompletionS != *o.AvgCompletionS {
		return false
	}
	return c.TotalEnrollments == o.TotalEnrollments && c.ActiveEnrollments == o.ActiveEnrollments &&
		c.Completions == o.Completions && c.CompletionRate == o.CompletionRate
}

// PlatformStats reads the platform snapshot as user.
func (s *Stack) PlatformStats(t testing.TB, user *User) Platform {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.AnalyticsURL+"/api/v1/analytics/platform", user.Token, nil)
	var p Platform
	decode(t, raw, &p)
	return p
}

// CourseStatsFor reads a course's stats as user (the instructor, an enrolled
// student or an admin; others get 404).
func (s *Stack) CourseStatsFor(t testing.TB, user *User, c *Course) CourseStats {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.AnalyticsURL+"/api/v1/analytics/courses/"+c.ID.String(), user.Token, nil)
	var cs CourseStats
	decode(t, raw, &cs)
	return cs
}

// PopularCourses reads GET /analytics/popular-courses as user.
func (s *Stack) PopularCourses(t testing.TB, user *User) []struct {
	CourseID    string `json:"course_id"`
	Enrollments int64  `json:"enrollments"`
} {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.AnalyticsURL+"/api/v1/analytics/popular-courses", user.Token, nil)
	var r struct {
		Items []struct {
			CourseID    string `json:"course_id"`
			Enrollments int64  `json:"enrollments"`
		} `json:"items"`
	}
	decode(t, raw, &r)
	return r.Items
}

// DailyEnrollments reads the per-day enrollment counts of a course.
func (s *Stack) DailyEnrollments(t testing.TB, user *User, c *Course) int64 {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.AnalyticsURL+"/api/v1/analytics/courses/"+c.ID.String()+"/daily-enrollments", user.Token, nil)
	var r struct {
		Items []struct {
			Count int64 `json:"count"`
		} `json:"items"`
	}
	decode(t, raw, &r)
	var n int64
	for _, it := range r.Items {
		n += it.Count
	}
	return n
}

// Issues reads GET /analytics/issues (admin) and returns the raw document.
func (s *Stack) Issues(t testing.TB) map[string]any {
	t.Helper()
	raw := call(t, http.StatusOK, "GET", s.AnalyticsURL+"/api/v1/analytics/issues", s.AdminToken(t), nil)
	var m map[string]any
	decode(t, raw, &m)
	return m
}

func mustUUID(t testing.TB, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("not a uuid: %q", s)
	}
	return id
}
