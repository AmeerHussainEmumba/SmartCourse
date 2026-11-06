//go:build integration

// What this file checks, against a real gin.Engine and real Postgres (RFC
// §18.2): that this module's routes actually boot, that request binding
// enforces EnrollRequest's validation tags over real HTTP (not just that
// the struct has binding tags), and that RequireAuth is actually wired
// onto this route group — none of which any Service-level integration
// test in this package can catch, since those call Service methods
// directly and never go through handler.go's BindJSON/route dispatch at
// all. First test in this module to boot a real gin.Engine; see
// docs/guides/testing.md for the new HTTP-level tier this establishes.
package enrollment_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func testJWTCfg() config.JWTConfig {
	return config.JWTConfig{
		SigningKey:      "test-signing-key-at-least-32-bytes-long!!",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		Issuer:          "smartcourse-test",
	}
}

// newTestEngine boots the real router the same way cmd/course-api does
// (bootstrap.RegisterCourseRoutes), just scoped to this one module — this
// call succeeding at all is the regression test for the class of bug that
// bit internal/course live: two routes sharing a path depth with
// conflicting gin wildcard names panics at exactly this point, not later.
func newTestEngine(svc *enrollment.Service) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	enrollment.RegisterRoutes(api, svc, testJWTCfg())
	return r
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func doRequest(t *testing.T, r *gin.Engine, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestEnrollmentRoutes_RegisterRoutesBootsCleanly(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	// newTestEngine itself panics on a route-registration conflict —
	// reaching this line at all is the assertion.
	_ = newTestEngine(svc)
}

func TestEnrollmentRoutes_PostEnroll_NoAuthHeader_Returns401(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	r := newTestEngine(svc)

	rec := doRequest(t, r, http.MethodPost, "/api/v1/enrollments", "", enrollment.EnrollRequest{CourseID: uuid.New().String()})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if body.Code != "unauthorized" {
		t.Fatalf("expected code 'unauthorized', got %q", body.Code)
	}
}

func TestEnrollmentRoutes_PostEnroll_MissingCourseID_Returns400(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	r := newTestEngine(svc)

	studentID := seedStudent(t, env)
	token, err := auth.IssueAccessToken(testJWTCfg(), studentID, "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	// course_id omitted entirely — EnrollRequest's `binding:"required,uuid"` must reject this.
	rec := doRequest(t, r, http.MethodPost, "/api/v1/enrollments", token, map[string]any{})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing course_id, got %d: %s", rec.Code, rec.Body.String())
	}

	// course_id present but not a valid UUID — same binding tag, different violation.
	rec = doRequest(t, r, http.MethodPost, "/api/v1/enrollments", token, map[string]any{"course_id": "not-a-uuid"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed course_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestEnrollmentRoutes_FullAuthenticatedRoundTrip proves routing, binding,
// RequireAuth, the real Service, and response serialization all work
// together over a real HTTP request/response — not just that each layer
// passes its own tests in isolation.
func TestEnrollmentRoutes_FullAuthenticatedRoundTrip(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	r := newTestEngine(svc)

	courseID := seedCourse(t, env, nil)
	studentID := seedStudent(t, env)
	token, err := auth.IssueAccessToken(testJWTCfg(), studentID, "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	rec := doRequest(t, r, http.MethodPost, "/api/v1/enrollments", token, enrollment.EnrollRequest{CourseID: courseID.String()})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 on successful enroll, got %d: %s", rec.Code, rec.Body.String())
	}
	var created enrollment.EnrollmentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created enrollment: %v", err)
	}
	if created.CourseID != courseID.String() || created.Status != "active" {
		t.Fatalf("unexpected created enrollment response: %+v", created)
	}

	// Confirm it actually landed in Postgres, not just in the HTTP response echo.
	var count int64
	if err := env.DB.Raw(`SELECT count(*) FROM enrollments WHERE id = ? AND student_id = ? AND course_id = ?`,
		created.ID, studentID, courseID).Row().Scan(&count); err != nil {
		t.Fatalf("read back enrollment: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected the enrollment created over HTTP to exist in Postgres, found %d matching rows", count)
	}

	// GET /enrollments (listMine) over the same real engine confirms the read path too.
	rec = doRequest(t, r, http.MethodGet, "/api/v1/enrollments", token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from listMine, got %d: %s", rec.Code, rec.Body.String())
	}
	var listResp struct {
		Items []enrollment.EnrollmentResponse `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	found := false
	for _, item := range listResp.Items {
		if item.ID == created.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the just-created enrollment to appear in GET /enrollments, got %+v", listResp.Items)
	}
}
