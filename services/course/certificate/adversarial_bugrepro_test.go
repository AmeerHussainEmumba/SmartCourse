//go:build integration && bugrepro

// Adversarial security tests for certificates (security tester, Week 3).
// TestAdversarial_* passed when written (attack HELD); TestBugrepro_* fail
// today and reproduce a finding in docs/plans/week-3-findings-security.md.
//
//	cd services/course && go test -tags='integration bugrepro' -run 'Bugrepro|Adversarial' ./certificate/
package certificate_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

// productionEngine mounts the certificate routes on the production router
// options (no trusted proxies), with the production limit of 30 a minute.
func productionEngine(t *testing.T, env *testenv.Env) *gin.Engine {
	t.Helper()
	r, err := router.NewWithOptions(router.Options{Env: "test", MaxBodyBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	svc := certificate.NewService(certificate.NewRepository(env.DB))
	certificate.RegisterRoutes(r.Group("/api/v1"), svc, testJWTConfig(),
		ratelimit.New(env.RDB).Middleware("cv-adv-"+uuid.NewString(), ratelimit.PerMinute(30)))
	return r
}

func getFrom(r *gin.Engine, path string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = "203.0.113.9:4444"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// HELD: rotating X-Forwarded-For / X-Real-IP / Forwarded does not reset the
// per-IP budget of the public verification route, so the 128-bit code cannot be
// ground down faster than 30 a minute per real peer.
func TestAdversarial_Verify_RateLimitNotBypassedBySpoofedHeaders(t *testing.T) {
	env := testenv.Setup(t)
	r := productionEngine(t, env)
	limited := 0
	for i := 0; i < 80; i++ {
		code := strings.Repeat("A", 25) + string(rune('A'+i%26))
		rec := getFrom(r, "/api/v1/certificates/verify/"+code, map[string]string{
			"X-Forwarded-For": fmt.Sprintf("198.51.100.%d", i), "X-Real-IP": fmt.Sprintf("192.0.2.%d", i),
			"Forwarded": fmt.Sprintf("for=10.0.0.%d", i), "X-Client-IP": fmt.Sprintf("10.1.0.%d", i), "True-Client-IP": fmt.Sprintf("10.2.0.%d", i),
		})
		if rec.Code == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited < 40 {
		t.Fatalf("only %d of 80 spoofed-header requests were rate limited (budget is 30/min)", limited)
	}
}

// HELD: format abuse on the public route is a clean 400/404, never a 5xx or a
// match; owner-only fields never appear on the public view.
func TestAdversarial_Verify_FormatAbuseAndFieldMinimisation(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env, 1000)
	_, id, code := issued(t, env)
	hostile := []string{
		strings.ToLower(code), code + "A", code[:25], code + "%20", "%20" + code, code + "%00", "%00" + code,
		"0O1I89" + strings.Repeat("A", 20), strings.Repeat("A", 26) + "%0d%0a", "%27%20OR%201%3D1--", "%25", "..%2f" + code,
		code + "/", "a%C3%A9" + strings.Repeat("A", 20), strings.Repeat("A", 100000/50),
	}
	for _, c := range hostile {
		rec := get(r, "/api/v1/certificates/verify/"+c, "")
		if rec.Code != 400 && rec.Code != 404 && (rec.Code != 301 || !strings.HasSuffix(c, "/")) {
			t.Errorf("%.40q: status %d (want 400/404): %.200s", c, rec.Code, rec.Body.String())
		}
	}
	rec := get(r, "/api/v1/certificates/verify/"+code, "")
	if rec.Code != 200 {
		t.Fatalf("control: %d", rec.Code)
	}
	var m map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	if len(m) != 3 {
		t.Errorf("public view exposes %d fields (want exactly course_title, student_name, issued_at): %v", len(m), m)
	}
	for _, leak := range []string{id, code, "student_id", "enrollment_id"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("public view leaks %q", leak)
		}
	}
	// Brute-force shaped probing: unknown well-formed codes all look the same.
	var first string
	for i := 0; i < 40; i++ {
		c := certificateRandomCode(i)
		rec := get(r, "/api/v1/certificates/verify/"+c, "")
		if rec.Code != 404 {
			t.Fatalf("unknown code answered %d", rec.Code)
		}
		if first == "" {
			first = rec.Body.String()
		} else if rec.Body.String() != first {
			t.Fatalf("404 bodies differ between unknown codes (oracle)")
		}
	}
}

func certificateRandomCode(i int) string {
	const alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	b := make([]byte, 26)
	for j := range b {
		b[j] = alpha[(i*7+j*13+j*j)%32]
	}
	return string(b)
}

// HELD: owner routes: another student's id and unknown ids are
// indistinguishable, ids in odd forms are 400/404, and a token for a different
// role does not widen access.
func TestAdversarial_Certificates_OwnerRoutesAbuse(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env, 1000)
	a, aID, _ := issued(t, env)
	b, bID, _ := issued(t, env)
	atok, btok := bearerToken(t, a.studentID), bearerToken(t, b.studentID)
	for _, tc := range []struct{ tok, id string }{{atok, bID}, {btok, aID}} {
		rec := get(r, "/api/v1/certificates/"+tc.id, tc.tok)
		unk := get(r, "/api/v1/certificates/"+uuid.NewString(), tc.tok)
		if rec.Code != 404 || rec.Body.String() != unk.Body.String() {
			t.Errorf("cross-tenant read: %d vs unknown %d", rec.Code, unk.Code)
		}
	}
	for _, id := range []string{"%00", "' OR 1=1", "../" + aID, strings.ToUpper(aID) + "%20", "{" + aID + "}", "urn:uuid:" + aID} {
		rec := get(r, "/api/v1/certificates/"+strings.ReplaceAll(id, " ", "%20"), btok)
		if rec.Code >= 500 || (rec.Code == 200 && strings.Contains(rec.Body.String(), aID)) {
			t.Errorf("id %q: %d %.200s", id, rec.Code, rec.Body.String())
		}
	}
	// B's list never contains A's certificate.
	rec := get(r, "/api/v1/certificates", btok)
	if strings.Contains(rec.Body.String(), aID) {
		t.Error("list leaks another student's certificate")
	}
}
