//go:build e2e

package e2e_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestE2E_Adversarial_Surface is the security tester's whole-stack pass over
// the Week 3 surface (metrics endpoints, header and trace-context handling,
// error bodies, admin exposure), on the ten binaries. One stack, several
// attacks. In the four-binary build the first subtest (S-11) reproduced a
// finding of docs/plans/week-3-findings-security.md; the split binaries serve
// /metrics only on their internal listener, so it is now a guard like the others.
//
//	cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -run TestE2E_Adversarial_Surface ./
func TestE2E_Adversarial_Surface(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	student := s.NewStudent(t)
	inst := s.NewInstructor(t)
	course := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)

	// S-11: /metrics must not be served on the port of a public API (anything
	// that routes the API publicly would expose route templates, traffic counts,
	// DB pool state and process statistics to anyone); it lives on the internal
	// listener (METRICS_HTTP_ADDR), which a scenario can still scrape.
	t.Run("Guard_MetricsNotOnPublicAPIPort", func(t *testing.T) {
		for name, base := range map[string]string{"identity": s.IdentityURL, "course": s.CourseURL, "analytics": s.AnalyticsURL, "audit": s.AuditURL} {
			resp, err := harness.DoFull("GET", base+"/metrics", "", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Status == 200 {
				t.Errorf("%s: GET /metrics with no credentials on the API port answered 200 (%d bytes)", name, len(resp.Body))
			}
		}
	})

	// HELD: scraped metrics never carry request data: emails, ids, tokens,
	// request ids or raw paths from hostile requests (label cardinality guard).
	t.Run("Guard_MetricsCarryNoRequestData", func(t *testing.T) {
		secretEmail := "victim-" + uuid.NewString()[:8] + "@example.com"
		secretID := uuid.NewString()
		rid := "rid-" + uuid.NewString()
		_, _, _ = harness.Do("POST", s.IdentityURL+"/api/v1/auth/login", "", map[string]string{"email": secretEmail, "password": "x"})
		_, _ = harness.DoFull("GET", s.CourseURL+"/api/v1/courses/"+secretID+"/zzz", student.Token, nil, map[string]string{"X-Request-Id": rid})
		_, _, _ = harness.Do("GET", s.AnalyticsURL+"/api/v1/analytics/courses/"+secretID, student.Token, nil)
		_, _, _ = harness.Do("GET", s.AuditURL+"/api/v1/admin/failed-events/"+secretID, student.Token, nil)
		for _, name := range harness.Binaries {
			u := s.MetricsURL(name)
			resp, err := harness.DoFull("GET", u+"/metrics", "", nil, nil)
			if err != nil || resp.Status != 200 {
				t.Errorf("%s metrics: %v %v", name, err, resp)
				continue
			}
			body := string(resp.Body)
			for _, secret := range []string{secretEmail, secretID, rid, student.Token, student.Email, "Bearer ", "postgres://", "password", "JWT_SIGNING_KEY", "e2e-signing-key"} {
				if strings.Contains(body, secret) {
					t.Errorf("%s /metrics leaks %q", name, secret)
				}
			}
		}
	})

	// HELD: hostile X-Request-Id / traceparent / tracestate values (control
	// characters, obs-fold, oversized, duplicated, malformed) never produce a 5xx,
	// are never echoed back, and never create a forged log line.
	t.Run("Guard_TraceAndRequestIDHeadersCannotInjectOrCrash", func(t *testing.T) {
		hostile := []string{
			"X-Request-Id: ok\r\n X-Folded: yes",
			"X-Request-Id: " + strings.Repeat("a", 100000),
			"X-Request-Id: \x01\x02\x03",
			"X-Request-Id: FORGED-LOG-LINE level=ERROR msg=pwned",
			"traceparent: 00-00000000000000000000000000000000-0000000000000000-01",
			"traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
			"traceparent: ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"traceparent: 00-ZZZZ2f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"traceparent: " + strings.Repeat("0", 70000),
			"tracestate: " + strings.Repeat("k=v,", 5000),
			"tracestate: a=b\r\ntracestate: c=d",
			"baggage: " + strings.Repeat("a=b,", 5000),
		}
		if code, _ := rawGET(t, strings.TrimPrefix(s.CourseURL, "http://"), "/health/live", "X-Control: 1"); code != 200 {
			t.Fatalf("control request over the raw client answered %d, the subtest would prove nothing", code)
		}
		answered := 0
		defer func() { t.Logf("hostile requests answered with a status: %d of %d", answered, len(hostile)) }()
		for _, h := range hostile {
			code, head := rawGET(t, strings.TrimPrefix(s.CourseURL, "http://"), "/health/live", h)
			if code > 0 {
				answered++
			}
			if code >= 500 {
				t.Errorf("%.50q: status %d", h, code)
			}
			if strings.Contains(strings.ToLower(head), "x-folded") || strings.Contains(head, "FORGED-LOG-LINE") || strings.Contains(head, "\x01") {
				t.Errorf("%.50q: hostile value echoed in response headers", h)
			}
		}
		for _, name := range harness.Binaries {
			p := s.Proc(name)
			for _, line := range strings.Split(p.Logs(), "\n") {
				if strings.HasPrefix(line, "FORGED-LOG-LINE") || strings.HasPrefix(line, "\x01") {
					t.Errorf("%s: forged log line: %.100q", name, line)
				}
			}
		}
	})

	// HELD: malformed input never yields an error body with SQL, DSNs, stack
	// traces, file paths or driver names, on any service.
	t.Run("Guard_ErrorBodiesLeakNothing", func(t *testing.T) {
		admin := s.AdminToken(t)
		cid := course.ID.String()
		type req struct{ method, url, tok, body string }
		reqs := []req{
			{"POST", s.IdentityURL + "/api/v1/auth/register", "", `{"email":"a@b.c","password":"x","full_name":"` + strings.Repeat("é", 5000) + `"}`},
			{"POST", s.IdentityURL + "/api/v1/auth/login", "", `{"email":{"$ne":""},"password":[1]}`},
			{"POST", s.IdentityURL + "/api/v1/auth/login", "", `{"email":"a\u0000b@c.d","password":"x"}`},
			{"GET", s.CourseURL + "/api/v1/courses/search?q=%00&limit=99999999999999999999&cursor=%ff", student.Token, ""},
			{"GET", s.CourseURL + "/api/v1/courses/search?category=%27%3B--", student.Token, ""},
			{"POST", s.CourseURL + "/api/v1/enrollments", student.Token, `{"course_id":"` + strings.Repeat("9", 5000) + `"}`},
			{"POST", s.CourseURL + "/api/v1/enrollments", student.Token, `{"course_id":"` + cid + `","extra":{"a":{"a":{"a":{}}}}}`},
			{"GET", s.CourseURL + "/api/v1/certificates/verify/%00", "", ""},
			{"POST", s.CourseURL + "/api/v1/assets/upload-url", inst.Token, `{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":1e999}`},
			{"GET", s.AnalyticsURL + "/api/v1/analytics/popular-courses?limit=-1", student.Token, ""},
			{"GET", s.AnalyticsURL + "/api/v1/analytics/courses/%00", admin, ""},
			{"GET", s.AuditURL + "/api/v1/admin/failed-events?cursor=%00&limit=1e9&status[$ne]=x", admin, ""},
			{"POST", s.AuditURL + "/api/v1/admin/failed-events/%00/replay", admin, ""},
			{"GET", s.AuditURL + "/health/ready", "", ""},
		}
		banned := []string{"SQLSTATE", "pq:", "pgx", "gorm", "postgres://", "mongodb://", "redis://", "goroutine ", "panic:", "runtime error", ".go:", "/Users/", "/home/", "bson", "ERROR:  ", "JWT_SIGNING_KEY", "e2e-signing-key"}
		for _, r := range reqs {
			var body any
			if r.body != "" {
				body = rawJSON(r.body)
			}
			resp, err := harness.DoFull(r.method, r.url, r.tok, body, nil)
			if err != nil {
				t.Errorf("%s %.60s: %v", r.method, r.url, err)
				continue
			}
			if resp.Status >= 500 && resp.Status != 503 {
				t.Errorf("%s %.80s: status %d %.200s", r.method, r.url, resp.Status, resp.Body)
			}
			for _, b := range banned {
				if strings.Contains(string(resp.Body), b) {
					t.Errorf("%s %.80s: body leaks %q: %.200s", r.method, r.url, b, resp.Body)
				}
			}
		}
	})

	// The failed-events surface (audit-api) and the internal metrics listener
	// are separate ports; admin routes need a fresh admin token; a student or
	// instructor token, a garbage token and /metrics on the API port all fail.
	t.Run("Guard_AuditAPIPortSeparationAndRoles", func(t *testing.T) {
		for _, tok := range []string{"", student.Token, inst.Token, "garbage"} {
			code, _, err := harness.Do("GET", s.AuditURL+"/api/v1/admin/failed-events", tok, nil)
			if err != nil || (code != 401 && code != 403) {
				t.Errorf("token %.12q: %d %v", tok, code, err)
			}
		}
		if code, _, _ := harness.Do("GET", s.AuditURL+"/metrics", "", nil); code == 200 {
			t.Errorf("/metrics is served on the audit-api port")
		}
		if code, _, _ := harness.Do("GET", s.MetricsURL(harness.AuditAPI)+"/api/v1/admin/failed-events", s.AdminToken(t), nil); code == 200 {
			t.Errorf("admin routes are served on the metrics port of audit-api")
		}
		for _, p := range []string{"/debug/pprof/", "/debug/vars", "/swagger", "/admin", "/.env", "/actuator/env"} {
			bases := []string{s.IdentityURL, s.CourseURL, s.AnalyticsURL, s.AuditURL}
			for _, name := range harness.Binaries {
				bases = append(bases, s.MetricsURL(name))
			}
			for _, base := range bases {
				if code, _, _ := harness.Do("GET", base+p, "", nil); code == 200 {
					t.Errorf("%s%s answers 200 (debug surface)", base, p)
				}
			}
		}
	})
}

type rawJSON string

func (r rawJSON) MarshalJSON() ([]byte, error) { return []byte(r), nil }

// rawGET sends a hand-built HTTP/1.1 request with one hostile header line (so
// Go's client cannot sanitise it) and returns the status and response head.
func rawGET(t *testing.T, hostport, path, headerLine string) (int, string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", hostport, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(conn, fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\n%s\r\nConnection: close\r\n\r\n", path, url.PathEscape(hostport), headerLine))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return 0, "" // the server closed or refused the connection: acceptable for hostile input
	}
	defer func() { _ = resp.Body.Close() }()
	var b strings.Builder
	for k, v := range resp.Header {
		b.WriteString(k + ": " + strings.Join(v, ",") + "\n")
	}
	return resp.StatusCode, b.String()
}
