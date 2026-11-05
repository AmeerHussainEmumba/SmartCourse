//go:build integration

// What this file checks, against a real gin.Engine and real Postgres (RFC
// §18.2): that RegisterRoutes actually boots without panicking (this
// repo's first HTTP-level test tier — a router-registration panic found
// live elsewhere in this project would never have been caught by any
// Service-level test, only by actually starting the server), that
// GET /analytics/platform enforces RequireAuth, and that a real HTTP
// request returns a real seeded snapshot row's data end to end through
// routing, middleware, and JSON serialization — not just what Service
// returns when called directly in Go.
package analytics_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
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

func newTestEngine(t *testing.T, svc *analytics.Service) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	analytics.RegisterRoutes(api, svc, testJWTCfg())
	return r
}

func TestRegisterRoutes_BootsWithoutPanicking(t *testing.T) {
	env := testenv.Setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	_ = newTestEngine(t, svc) // the test is that this line doesn't panic
}

func TestGetPlatformSnapshot_NoAuthHeader_Rejected(t *testing.T) {
	env := testenv.Setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	r := newTestEngine(t, svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/platform", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetPlatformSnapshot_RealRequest_ReflectsSeededRow(t *testing.T) {
	env := testenv.Setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	r := newTestEngine(t, svc)

	// Seed a real platform_snapshot row directly, same as
	// service_integration_test.go's own convention for this module.
	if err := env.DB.Exec(`
		INSERT INTO platform_snapshot (id, total_students, total_instructors, total_courses_published, avg_courses_per_student, updated_at)
		VALUES (true, 7, 2, 3, 1.5, now())
	`).Error; err != nil {
		t.Fatalf("seed platform_snapshot: %v", err)
	}

	token, err := auth.IssueAccessToken(testJWTCfg(), uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/platform", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		TotalStudents         float64 `json:"total_students"`
		TotalInstructors      float64 `json:"total_instructors"`
		TotalCoursesPublished float64 `json:"total_courses_published"`
		AvgCoursesPerStudent  float64 `json:"avg_courses_per_student"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body.TotalStudents != 7 || body.TotalInstructors != 2 || body.TotalCoursesPublished != 3 || body.AvgCoursesPerStudent != 1.5 {
		t.Fatalf("expected response to reflect the seeded row (7,2,3,1.5), got %+v", body)
	}
}
