//go:build integration

// What this file checks, over real HTTP (RFC §18 — a gap closed here for
// the first time in this repo): that RegisterRoutes actually boots against
// a real gin.Engine without panicking (the regression test for a real bug
// — two GET routes with conflicting wildcard names crashed course-api at
// startup, caught only by actually running the server, not by any test —
// see handler.go's :slug/publish-status comment and ADR-0025), that
// request binding rejects a malformed body with 400 before ever reaching
// the service layer, and that RequireAuth/RequireRole actually reject an
// unauthenticated or wrong-role request when wired into this module's real
// routes (not just the middleware in isolation — that's covered
// separately in internal/platform/auth). Every other test in this package
// calls Service methods directly; this file is the one place the full
// chain — routing, binding, middleware, service, JSON response — is
// exercised together.
package course_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	identityauth "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
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

// newTestEngine boots RegisterRoutes for real — the act of calling this is
// itself the regression test for the router-panic bug class: a conflicting
// route registration fails here with a panic, not a confusing assertion
// failure somewhere else.
func newTestEngine(svc *course.Service) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	course.RegisterRoutes(api, svc, testJWTCfg())
	return r
}

func bearerToken(t *testing.T, userID uuid.UUID, role string) string {
	t.Helper()
	tok, err := identityauth.IssueAccessToken(testJWTCfg(), userID, role, 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	return tok
}

func doJSON(t *testing.T, r *gin.Engine, method, path, token string, body any) *httptest.ResponseRecorder {
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

func TestRegisterRoutes_BootsWithoutPanic(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	_ = newTestEngine(svc) // panics here, not in a later test, if any route registration conflicts (e.g. :slug vs :id at the same depth)
}

func TestCreateCourse_HTTP_RequiresAuth(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	r := newTestEngine(svc)

	rec := doJSON(t, r, http.MethodPost, "/api/v1/courses", "", course.CreateCourseRequest{Title: "X", Slug: "x"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateCourse_HTTP_RequiresInstructorRole(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	r := newTestEngine(svc)

	studentToken := bearerToken(t, uuid.Must(uuid.NewV7()), "student")
	rec := doJSON(t, r, http.MethodPost, "/api/v1/courses", studentToken, course.CreateCourseRequest{Title: "X", Slug: "x"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for a student token, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateCourse_HTTP_RejectsMalformedBody(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	r := newTestEngine(svc)

	instructorID := seedInstructor(t, env)
	token := bearerToken(t, instructorID, "instructor")

	// Missing required "title" and "slug" (CreateCourseRequest's binding tags).
	rec := doJSON(t, r, http.MethodPost, "/api/v1/courses", token, map[string]any{"description": "no title or slug"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a body missing required fields, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != "validation_failed" {
		t.Fatalf("expected error code 'validation_failed', got %q", body.Code)
	}
}

func TestCreateCourse_HTTP_SuccessRoundTrip(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	r := newTestEngine(svc)

	instructorID := seedInstructor(t, env)
	token := bearerToken(t, instructorID, "instructor")

	createRec := doJSON(t, r, http.MethodPost, "/api/v1/courses", token, course.CreateCourseRequest{
		Title: "HTTP Round Trip", Slug: "http-round-trip", Description: "created over real HTTP",
	})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating a course, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created struct {
		Course struct {
			Slug string `json:"slug"`
		} `json:"course"`
	}
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Course.Slug != "http-round-trip" {
		t.Fatalf("expected slug 'http-round-trip' in create response, got %q", created.Course.Slug)
	}

	// Created courses have no published version yet, so the public detail
	// route correctly 404s — a real round trip through routing + service +
	// response serialization, not a 200 that would mean nothing here.
	detailRec := doJSON(t, r, http.MethodGet, "/api/v1/courses/http-round-trip", "", nil)
	if detailRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unpublished course's detail, got %d: %s", detailRec.Code, detailRec.Body.String())
	}
}
