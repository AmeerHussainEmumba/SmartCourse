package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type harness struct {
	f      *fake
	cfg    config
	logs   *bytes.Buffer
	out    *bytes.Buffer
	sleeps []time.Duration
}

func newHarness(t *testing.T) *harness {
	f := newFake(t)
	return &harness{f: f, logs: &bytes.Buffer{}, out: &bytes.Buffer{}, cfg: config{
		IdentityURL: f.identity.URL, CourseURL: f.course.URL, AnalyticsURL: f.analytics.URL,
		AdminEmail: testAdminEmail, AdminPassword: testAdminPW,
		ReadyWait: 100 * time.Millisecond, PublishTimeout: time.Minute, PublishPoll: time.Millisecond,
		PublishStartTries: 3, PublishRetryStep: time.Millisecond,
	}}
}

// run does one seed run with sleeping replaced by a recorder.
func (h *harness) run(t *testing.T) int {
	t.Helper()
	h.logs.Reset()
	h.out.Reset()
	s := newSeeder(h.cfg, slog.New(slog.NewTextHandler(h.logs, nil)), h.out)
	for _, c := range []*client{s.identity, s.course, s.analytic} {
		c.sleep = func(ctx context.Context, d time.Duration) error { h.sleeps = append(h.sleeps, d); return ctx.Err() }
	}
	return s.run(context.Background())
}

func (h *harness) roles() map[string]string {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	m := map[string]string{}
	for e, u := range h.f.users {
		m[e] = u.role
	}
	return m
}

func (h *harness) state(slug string) string {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if c := h.f.courses[slug]; c != nil {
		return c.state
	}
	return "(absent)"
}

func (h *harness) totals() (users, courses, enrollments, done int) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	for _, e := range h.f.enrollments {
		done += len(e.done)
	}
	return len(h.f.users), len(h.f.courses), len(h.f.enrollments), done
}

func TestRun_BuildsTheWholeDatasetOverHTTP(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t); code != 0 {
		t.Fatalf("exit %d\nlogs: %s", code, h.logs)
	}
	roles := h.roles()
	for _, n := range instructorNames {
		if roles[instructorEmail(n)] != "instructor" {
			t.Errorf("%s role = %q, want instructor (promoted through the admin endpoint)", n, roles[instructorEmail(n)])
		}
	}
	if roles["student12@smartcourse.dev"] != "student" || roles[testAdminEmail] != "admin" {
		t.Errorf("roles: %v", roles)
	}
	want := map[string]string{
		"introduction-to-go": "ready", "advanced-postgresql": "ready", "distributed-systems-essentials": "ready",
		"security-fundamentals": "ready", "kubernetes-in-production": "draft", "legacy-shell-scripting": "archived",
	}
	for slug, st := range want {
		if got := h.state(slug); got != st {
			t.Errorf("course %s state = %s, want %s", slug, got, st)
		}
	}
	users, courses, enrollments, done := h.totals()
	// admin + 3 instructors + 12 students; 6 enrollments (3+2+1); completions:
	// Go 5 (finished) + 2 (40%), PostgreSQL 1 (50%), Security 1 (finished).
	if users != 16 || courses != 6 || enrollments != 6 || done != 9 {
		t.Errorf("users=%d courses=%d enrollments=%d completions=%d, want 16/6/6/9", users, courses, enrollments, done)
	}
	for _, want := range []string{seedPassword, "student1@smartcourse.dev", "irfan-instructor@smartcourse.dev"} {
		if !strings.Contains(h.out.String(), want) {
			t.Errorf("summary lacks %q:\n%s", want, h.out)
		}
	}
}

func TestRun_NeverLogsOrPrintsSecrets(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	all := h.logs.String() + h.out.String()
	for _, secret := range []string{testAdminPW, "tok-"} {
		if strings.Contains(all, secret) {
			t.Errorf("output contains %q", secret)
		}
	}
}

func TestRun_SecondRunCreatesNothing(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t); code != 0 {
		t.Fatalf("first run exit %d\n%s", code, h.logs)
	}
	u1, c1, e1, d1 := h.totals()
	h.f.mu.Lock()
	h.f.counts = map[string]int{}
	h.f.mu.Unlock()

	if code := h.run(t); code != 0 {
		t.Fatalf("second run exit %d\n%s", code, h.logs)
	}
	u2, c2, e2, d2 := h.totals()
	if u1 != u2 || c1 != c2 || e1 != e2 || d1 != d2 {
		t.Errorf("second run changed the data: %d/%d/%d/%d -> %d/%d/%d/%d", u1, c1, e1, d1, u2, c2, e2, d2)
	}
	if n := h.f.counts["POST /api/v1/auth/register"]; n != 0 {
		t.Errorf("second run registered %d users, want 0", n)
	}
	if n := h.f.counts["POST /api/v1/courses"]; n != len(dataset) {
		t.Errorf("second run attempted %d course creates, want %d (each answered 409)", n, len(dataset))
	}
	if !strings.Contains(h.out.String(), "0 created, 15 already existed") {
		t.Errorf("summary does not report the no-op:\n%s", h.out)
	}
}

func TestRun_TopsUpAPartialDataset(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t); code != 0 {
		t.Fatalf("exit %d", code)
	}
	// An enrollment lost between runs is restored; nothing else is duplicated.
	h.f.mu.Lock()
	h.f.enrollments = h.f.enrollments[1:]
	h.f.mu.Unlock()
	if code := h.run(t); code != 0 {
		t.Fatalf("second run exit %d\n%s", code, h.logs)
	}
	if _, _, e, _ := h.totals(); e != 6 {
		t.Errorf("enrollments = %d, want 6", e)
	}
}

func TestRun_WaitsForThePublishWorkflowToBeReady(t *testing.T) {
	h := newHarness(t)
	h.f.pendingPolls = 3
	if code := h.run(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, h.logs)
	}
	if got := h.state("introduction-to-go"); got != "ready" {
		t.Errorf("state = %s", got)
	}
	if _, _, e, _ := h.totals(); e != 6 {
		t.Errorf("enrollments = %d: students were enrolled before the course was ready", e)
	}
}

func TestRun_DependencyDownFailsClearly(t *testing.T) {
	h := newHarness(t)
	h.f.course.Close()
	if code := h.run(t); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(h.logs.String(), "course-api") || !strings.Contains(h.logs.String(), "did not become ready") {
		t.Errorf("message does not name the dependency:\n%s", h.logs)
	}
	if n := h.f.counts["POST /api/v1/auth/login"]; n != 0 {
		t.Errorf("seeded while a dependency was down (%d logins)", n)
	}
}

func TestRun_AnalyticsDownAlsoFails(t *testing.T) {
	h := newHarness(t)
	h.f.analytics.Close()
	if code := h.run(t); code != 1 || !strings.Contains(h.logs.String(), "analytics-api") {
		t.Fatalf("exit %d, logs %s", code, h.logs)
	}
}

func TestRun_WrongAdminCredentialsPointToIdentityAdmin(t *testing.T) {
	h := newHarness(t)
	h.cfg.AdminPassword = "nope-nope-nope"
	if code := h.run(t); code != 1 || !strings.Contains(h.logs.String(), "identity-admin") {
		t.Fatalf("exit %d, logs %s", code, h.logs)
	}
	if strings.Contains(h.logs.String(), "nope-nope-nope") {
		t.Error("the admin password was logged")
	}
}

func TestRun_PublishingUnavailableStillCreatesTheRest(t *testing.T) {
	h := newHarness(t)
	h.f.publish503 = true
	if code := h.run(t); code != 2 {
		t.Fatalf("exit %d, want 2 (partial)\n%s", code, h.logs)
	}
	if got := h.state("kubernetes-in-production"); got != "draft" {
		t.Errorf("the draft course was not created (state %s)", got)
	}
	if got := h.state("distributed-systems-essentials"); got != "(absent)" {
		t.Errorf("a course that needs publishing was created anyway (state %s)", got)
	}
	if _, _, e, _ := h.totals(); e != 0 {
		t.Errorf("enrollments = %d, want 0", e)
	}
	for _, want := range []string{"WARNING", "make dev-workflow", "Introduction to Go"} {
		if !strings.Contains(h.out.String(), want) {
			t.Errorf("summary lacks %q:\n%s", want, h.out)
		}
	}
}

func TestRun_SkipPublishIsExplicitAndExitsZero(t *testing.T) {
	h := newHarness(t)
	h.cfg.SkipPublish = true
	if code := h.run(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, h.logs)
	}
	if h.state("kubernetes-in-production") != "draft" || h.state("introduction-to-go") != "(absent)" {
		t.Error("only the draft course should exist")
	}
	if !strings.Contains(h.out.String(), "SEED_SKIP_PUBLISH=1") {
		t.Errorf("summary does not say publishing was skipped:\n%s", h.out)
	}
}

func TestRun_PublishTimeoutIsReportedAsUnavailable(t *testing.T) {
	h := newHarness(t)
	h.f.pendingPolls = 1 << 30
	h.cfg.PublishTimeout = 20 * time.Millisecond
	if code := h.run(t); code != 2 {
		t.Fatalf("exit %d, want 2\n%s", code, h.logs)
	}
}

func TestRun_RateLimitedRegistrationWaitsAndRetries(t *testing.T) {
	h := newHarness(t)
	h.f.registerLimits = 2
	if code := h.run(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, h.logs)
	}
	long := 0
	for _, d := range h.sleeps {
		if d == 20*time.Second {
			long++
		}
	}
	if long != 2 {
		t.Errorf("waited %d times for the rate limit, want 2 (sleeps %v)", long, h.sleeps)
	}
	if users, _, _, _ := h.totals(); users != 16 {
		t.Errorf("users = %d after the retries, want 16", users)
	}
}

func TestRun_EveryRequestClaimsItsOwnClientAddress(t *testing.T) {
	h := newHarness(t)
	h.run(t) // fake.note fails the test when an X-Forwarded-For repeats
	if h.f.seenXFF == 0 {
		t.Error("no X-Forwarded-For header was sent")
	}
}

func TestClient_GivesUpAfterTheRetryBudget(t *testing.T) {
	h := newHarness(t)
	h.f.registerLimits = 1000
	s := newSeeder(h.cfg, slog.New(slog.NewTextHandler(h.logs, nil)), h.out)
	s.identity.sleep = func(context.Context, time.Duration) error { return nil }
	err := s.identity.do(context.Background(), "POST", "/api/v1/auth/register", "", map[string]string{"email": "a@b.c"}, nil)
	if statusOf(err) != 429 {
		t.Fatalf("err = %v, want a 429", err)
	}
}

func TestCheckTargets(t *testing.T) {
	cases := []struct {
		name  string
		urls  [3]string
		allow bool
		ok    bool
	}{
		{"dev defaults", [3]string{"http://localhost:8090", "http://localhost:8091", "http://localhost:8092"}, false, true},
		{"127.0.0.1 and ::1", [3]string{"http://127.0.0.1:1", "http://[::1]:2", "http://127.9.9.9:3"}, false, true},
		{"a real host", [3]string{"https://smartcourse.example.com", "http://localhost:8091", "http://localhost:8092"}, false, false},
		{"a private LAN address is not loopback", [3]string{"http://localhost:1", "http://10.0.0.5:8091", "http://localhost:3"}, false, false},
		{"a host that only starts with localhost", [3]string{"http://localhost.evil.test", "http://localhost:2", "http://localhost:3"}, false, false},
		{"allowed explicitly", [3]string{"https://staging.example.com", "https://staging.example.com", "https://staging.example.com"}, true, true},
		{"not a URL", [3]string{"localhost:8090", "http://localhost:2", "http://localhost:3"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTargets(config{IdentityURL: tc.urls[0], CourseURL: tc.urls[1], AnalyticsURL: tc.urls[2], Allow: tc.allow})
			if (err == nil) != tc.ok {
				t.Errorf("checkTargets = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg := loadConfig(func(string) string { return "" })
	if cfg.IdentityURL != "http://localhost:8090" || cfg.CourseURL != "http://localhost:8091" || cfg.AnalyticsURL != "http://localhost:8092" {
		t.Errorf("urls: %+v", cfg)
	}
	if cfg.AdminEmail != "admin@smartcourse.local" || cfg.Allow || cfg.SkipPublish {
		t.Errorf("config: %+v", cfg)
	}
	env := map[string]string{"SEED_ALLOW": "1", "IDENTITY_URL": "http://h:1/", "SEED_ADMIN_EMAIL": "x@y.z"}
	cfg = loadConfig(func(k string) string { return env[k] })
	if !cfg.Allow || cfg.IdentityURL != "http://h:1" || cfg.AdminEmail != "x@y.z" {
		t.Errorf("config: %+v", cfg)
	}
}
