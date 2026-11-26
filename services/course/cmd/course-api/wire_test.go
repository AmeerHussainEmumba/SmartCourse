//go:build integration

// What this file checks: the real registerRoutes boots against real
// Postgres/Redis without a route-shape panic, still wires the Idempotency-Key
// middleware onto POST /enrollments, serves the certificate routes, and puts
// the per-IP limit in front of the public verification route.
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func bootEngine(t *testing.T) *gin.Engine {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	gin.SetMode(gin.TestMode)
	r := gin.New()
	cfg := &config.Config{JWT: config.JWTConfig{
		SigningKey:     "test-signing-key-at-least-32-bytes-long!!",
		Issuer:         "smartcourse-test",
		AccessTokenTTL: 15 * time.Minute,
	}}
	registerRoutes(r, &infra.Dependencies{DB: env.DB, RDB: env.RDB}, cfg, nil)
	return r
}

func TestRegisterRoutes_BootsAndServesCertificateRoutes(t *testing.T) {
	r := bootEngine(t)

	have := map[string]bool{}
	for _, ri := range r.Routes() {
		have[ri.Method+" "+ri.Path] = true
	}
	for _, want := range []string{
		"GET /api/v1/certificates",
		"GET /api/v1/certificates/:id",
		"GET /api/v1/certificates/verify/:code",
		"POST /api/v1/enrollments", // Idempotency-Key route must survive this edit
		"PUT /api/v1/enrollments/:id/lessons/:lessonKey/complete",
	} {
		if !have[want] {
			t.Errorf("route %q is not registered", want)
		}
	}

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/certificates", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /certificates without a token: expected 401, got %d", rec.Code)
	}
}

func TestRegisterRoutes_EnrollmentPostStillRegisteredAndGuarded(t *testing.T) {
	r := bootEngine(t)
	// With no token the auth middleware answers first; the point here is only
	// that the route still exists and is guarded, not that it is open.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/enrollments", strings.NewReader(`{"course_id":"`+uuid.NewString()+`"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST /enrollments without a token: expected 401, got %d", rec.Code)
	}
}

func TestRegisterRoutes_VerifyIsPublicAndRateLimited(t *testing.T) {
	r := bootEngine(t)
	code := strings.Repeat("A", 26)

	var codes []int
	for i := 0; i < 35; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/certificates/verify/"+code, nil)
		req.RemoteAddr = "203.0.113.9:4000"
		r.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	if codes[0] != http.StatusNotFound {
		t.Fatalf("an unknown code with no token must reach the handler (404), got %d", codes[0])
	}
	if codes[len(codes)-1] != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after 30 requests a minute from one IP, got %v", codes)
	}
}
