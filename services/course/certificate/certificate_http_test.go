//go:build integration

// What this file checks, against a real gin.Engine and real Postgres/Redis
// (RFC §18.2): RegisterRoutes boots without a route-shape panic (the static
// /verify/:code segment sits beside /:id); the owner routes reject a request
// with no or a bad token (401); the owner gets 200 on list and get; a
// non-owner's GET answers byte-for-byte like an unknown id (404 not_found,
// DR-022); the public verification route needs no token, reveals exactly
// course title, student name and issue date, answers 404 for an unknown code
// and 400 for a malformed one without touching the database, and is rate
// limited.
package certificate_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

func testJWTConfig() config.JWTConfig {
	return config.JWTConfig{
		SigningKey:     "test-signing-key-at-least-32-bytes-long!!",
		Issuer:         "smartcourse-test",
		AccessTokenTTL: 15 * time.Minute,
	}
}

func newTestEngine(t *testing.T, env *testenv.Env, verifyPerMinute int) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	svc := certificate.NewService(certificate.NewRepository(env.DB))
	limiter := ratelimit.New(env.RDB)
	certificate.RegisterRoutes(api, svc, testJWTConfig(),
		limiter.Middleware("certificate-verify-test-"+uuid.NewString(), ratelimit.PerMinute(verifyPerMinute)))
	return r
}

func bearerToken(t *testing.T, userID uuid.UUID) string {
	t.Helper()
	tok, err := auth.IssueAccessToken(testJWTConfig(), userID, "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	return tok
}

func get(r *gin.Engine, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// issued seeds a completed enrollment, delivers its completion event, and
// returns the fixture with the certificate's id and code.
func issued(t *testing.T, env *testenv.Env) (f fixture, id, code string) {
	t.Helper()
	f = seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if err := env.DB.Raw(`SELECT id::text, verification_code FROM certificates WHERE enrollment_id = ?`, f.enrollmentID).
		Row().Scan(&id, &code); err != nil {
		t.Fatalf("read certificate: %v", err)
	}
	return f, id, code
}

func TestRegisterRoutes_BootsWithoutPanicking(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	_ = newTestEngine(t, env, 30)
}

func TestOwnerRoutes_NoOrBadToken_Unauthorized(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 30)
	_, id, _ := issued(t, env)

	for _, path := range []string{"/api/v1/certificates", "/api/v1/certificates/" + id} {
		if rec := get(r, path, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s with no token: expected 401, got %d: %s", path, rec.Code, rec.Body.String())
		}
		if rec := get(r, path, "not-a-jwt"); rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s with a bad token: expected 401, got %d", path, rec.Code)
		}
	}
}

func TestListMine_ReturnsOnlyCallersCertificates(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 30)
	f, id, code := issued(t, env)
	_, _, _ = issued(t, env) // another student's certificate

	rec := get(r, "/api/v1/certificates", bearerToken(t, f.studentID))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Items []certificate.CertificateResponse `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != id || body.Items[0].VerificationCode != code ||
		body.Items[0].CourseTitle != "Go Concurrency" || body.Items[0].StudentName != "Ada Lovelace" {
		t.Fatalf("unexpected list body: %s", rec.Body.String())
	}

	empty := get(r, "/api/v1/certificates", bearerToken(t, uuid.New()))
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"items":[]`) {
		t.Fatalf("a student with none must get 200 and an empty array, got %d: %s", empty.Code, empty.Body.String())
	}
}

func TestGetByID_Owner200_NonOwnerSameAsUnknown404_Malformed400(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 30)
	f, id, _ := issued(t, env)
	stranger := bearerToken(t, uuid.New())

	if rec := get(r, "/api/v1/certificates/"+id, bearerToken(t, f.studentID)); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), id) {
		t.Fatalf("owner: expected 200 with the certificate, got %d: %s", rec.Code, rec.Body.String())
	}

	nonOwner := get(r, "/api/v1/certificates/"+id, stranger)
	unknown := get(r, "/api/v1/certificates/"+uuid.NewString(), stranger)
	if nonOwner.Code != http.StatusNotFound || unknown.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for both, got %d and %d", nonOwner.Code, unknown.Code)
	}
	if nonOwner.Body.String() != unknown.Body.String() {
		t.Fatalf("non-owner and unknown id must be indistinguishable:\n%s\n%s", nonOwner.Body.String(), unknown.Body.String())
	}
	if !strings.Contains(nonOwner.Body.String(), `"not_found"`) {
		t.Fatalf("expected the not_found code, got %s", nonOwner.Body.String())
	}

	if rec := get(r, "/api/v1/certificates/not-a-uuid", stranger); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed id: expected 400, got %d", rec.Code)
	}
}

func TestVerify_PublicRevealsOnlyWhatAVerifierNeeds(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 30)
	f, id, code := issued(t, env)

	rec := get(r, "/api/v1/certificates/verify/"+code, "") // no Authorization header
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 without a token, got %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	keys := make([]string, 0, len(body))
	for k := range body {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "course_title,issued_at,student_name" {
		t.Fatalf("the public response must carry exactly course_title, issued_at, student_name; got %v", keys)
	}
	if body["course_title"] != "Go Concurrency" || body["student_name"] != "Ada Lovelace" {
		t.Fatalf("wrong values: %v", body)
	}
	for _, secret := range []string{id, f.studentID.String(), f.enrollmentID.String(), f.courseID.String(), code} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("the public response leaks %s: %s", secret, rec.Body.String())
		}
	}
}

func TestVerify_UnknownCode404_MalformedCode400(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 100)
	_, _, code := issued(t, env)

	unknown := strings.Repeat("A", 26)
	if rec := get(r, "/api/v1/certificates/verify/"+unknown, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown well-formed code: expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	for name, bad := range map[string]string{
		"lowercase":   strings.ToLower(code),
		"too short":   code[:25],
		"too long":    code + "A",
		"bad letters": strings.Repeat("1", 26), // base32 has no 0, 1, 8 or 9
		"punctuation": strings.Repeat("A", 25) + "-",
		"sql-ish":     "%27%20OR%201%3D1%20--",
		"uuid":        uuid.NewString(),
	} {
		if rec := get(r, "/api/v1/certificates/verify/"+bad, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d: %s", name, rec.Code, rec.Body.String())
		}
	}
}

func TestVerify_IsRateLimited(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	r := newTestEngine(t, env, 3)
	_, _, code := issued(t, env)

	var last int
	for i := 0; i < 10; i++ {
		last = get(r, "/api/v1/certificates/verify/"+code, "").Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("expected 429 once the per-IP budget is spent, last status was %d", last)
	}
}
