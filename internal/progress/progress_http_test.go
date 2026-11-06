//go:build integration

// What this file checks, against a real gin.Engine and real Postgres (RFC
// §18.2): that RegisterRoutes actually boots without panicking, that the
// auth middleware it wires in actually rejects an unauthenticated request
// over real HTTP (not just when the service method is called directly),
// and that a full authenticated request — routing, auth, service,
// response body — works end to end, including that ownership is enforced
// at the HTTP layer the same way internal/progress's own service-level
// tests already prove it is at the Go-call layer.
package progress_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func testJWTConfig() config.JWTConfig {
	return config.JWTConfig{
		SigningKey:     "test-signing-key-at-least-32-bytes-long!!",
		Issuer:         "smartcourse-test",
		AccessTokenTTL: 15 * time.Minute,
	}
}

func newTestEngine(t *testing.T, env *testenv.Env) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	svc := progress.NewService(progress.NewRepository(env.DB))
	progress.RegisterRoutes(api, svc, testJWTConfig())
	return r
}

func bearerToken(t *testing.T, userID uuid.UUID, role string) string {
	t.Helper()
	tok, err := auth.IssueAccessToken(testJWTConfig(), userID, role, 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	return tok
}

// TestRegisterRoutes_BootsWithoutPanicking is this tier's baseline: booting
// the real route table against a real gin.Engine must not panic. A router
// panic at boot (two routes colliding on path shape) is exactly the kind
// of bug a Service-level test can never catch, since it never touches
// gin's router at all — see internal/course's handler.go for a real
// instance of this bug class, found live, not by any test.
func TestRegisterRoutes_BootsWithoutPanicking(t *testing.T) {
	env := testenv.Setup(t)
	_ = newTestEngine(t, env)
}

func TestGetProgress_NoAuthHeader_Rejected(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env)

	enrollmentID, _, _ := seedEnrollmentWithTwoLessons(t, env)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/enrollments/"+enrollmentID.String()+"/progress", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMarkComplete_NoAuthHeader_Rejected(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env)

	enrollmentID, lessonKeys, _ := seedEnrollmentWithTwoLessons(t, env)

	req := httptest.NewRequest(http.MethodPut,
		"/api/v1/enrollments/"+enrollmentID.String()+"/lessons/"+lessonKeys[0].String()+"/complete", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestMarkComplete_ThenGetProgress_FullAuthenticatedRoundTrip exercises
// routing, auth middleware, service, and response serialization together
// over real HTTP at least once — proving the whole chain actually works,
// not just that each layer passes in isolation.
func TestMarkComplete_ThenGetProgress_FullAuthenticatedRoundTrip(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env)

	enrollmentID, lessonKeys, studentID := seedEnrollmentWithTwoLessons(t, env)
	token := bearerToken(t, studentID, "student")

	completeReq := httptest.NewRequest(http.MethodPut,
		"/api/v1/enrollments/"+enrollmentID.String()+"/lessons/"+lessonKeys[0].String()+"/complete", nil)
	completeReq.Header.Set("Authorization", "Bearer "+token)
	completeRec := httptest.NewRecorder()
	r.ServeHTTP(completeRec, completeReq)
	if completeRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 marking a lesson complete, got %d: %s", completeRec.Code, completeRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/enrollments/"+enrollmentID.String()+"/progress", nil)
	getReq.Header.Set("Authorization", "Bearer "+token)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 reading progress, got %d: %s", getRec.Code, getRec.Body.String())
	}

	var body struct {
		Items []progress.ProgressItem `json:"items"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	found := false
	for _, item := range body.Items {
		if item.LessonKey == lessonKeys[0].String() {
			found = true
			if item.CompletedAt == nil {
				t.Fatalf("expected lesson %s to show a completed_at after marking it complete, got %+v", lessonKeys[0], item)
			}
		}
	}
	if !found {
		t.Fatalf("expected the completed lesson_key %s to appear in the response, got %+v", lessonKeys[0], body.Items)
	}
}

// TestGetProgress_NonOwner_Rejected proves ownership is enforced at the
// HTTP layer, the same way TestMarkComplete_RejectsNonOwner in
// service_integration_test.go proves it at the Go-call layer.
func TestGetProgress_NonOwner_Rejected(t *testing.T) {
	env := testenv.Setup(t)
	r := newTestEngine(t, env)

	enrollmentID, _, _ := seedEnrollmentWithTwoLessons(t, env)
	someoneElse := uuid.Must(uuid.NewV7())
	token := bearerToken(t, someoneElse, "student")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/enrollments/"+enrollmentID.String()+"/progress", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 reading someone else's progress, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if body.Code != "not_your_enrollment" {
		t.Fatalf("expected error code not_your_enrollment, got %q", body.Code)
	}
}
