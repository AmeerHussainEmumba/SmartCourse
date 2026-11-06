//go:build integration

// What this file checks, against a real gin.Engine and real Postgres/Redis
// (RFC §18.2): that this module's routes actually boot without panicking,
// that request binding/validation runs over real HTTP (not just that the
// DTO has binding tags), and that RequireAuth/RequireRole reject requests
// at the HTTP layer the way the route table says they should. This is the
// first HTTP-level test tier in this repo — every other "integration test"
// calls Service methods directly, never through handler.go's BindJSON/
// c.Param/route dispatch/route registration at all, which is exactly the
// blind spot that let a real bug through: two GET routes panicking gin's
// router at boot over conflicting wildcard names (internal/course,
// ADR-0025) was invisible to every test in this repo until the server was
// actually started. Scope is deliberately boot + binding + auth, not
// exhaustive per-route coverage — the service layer already covers
// business logic; this tier covers the HTTP surface wrapping it.
package identity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/identity"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// newTestRouter boots internal/identity's real routes against a real
// gin.Engine — the thing no test in this repo did before this file.
// Booting successfully is itself the regression test for the router-panic
// bug class (ADR-0025).
func newTestRouter(t *testing.T) (*gin.Engine, *identity.Service, config.JWTConfig, *testenv.Env) {
	t.Helper()
	env := testenv.Setup(t)
	jwtCfg := config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 7 * 24 * time.Hour, Issuer: "smartcourse-test"}
	argonCfg := config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	repo := identity.NewRepository(env.DB)
	svc := identity.NewService(repo, env.RDB, jwtCfg, argonCfg, identity.LoggingEmailSender{})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	identity.RegisterRoutes(api, svc, jwtCfg, env.RDB)
	return r, svc, jwtCfg, env
}

func doJSON(r *gin.Engine, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func decodeErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error response body %q: %v", rec.Body.String(), err)
	}
	return body.Code
}

// TestRegisterRoutes_BootsWithoutPanicking is the router-panic regression
// test: newTestRouter already calls RegisterRoutes; if that panicked (the
// exact bug class found live in internal/course), this test would never
// even reach the assertion below — t.Fatal here is almost decorative, the
// real assertion is "this function returned at all."
func TestRegisterRoutes_BootsWithoutPanicking(t *testing.T) {
	r, _, _, _ := newTestRouter(t)
	if r == nil {
		t.Fatal("expected a non-nil engine after RegisterRoutes")
	}
}

func TestRegisterHTTP_MalformedBody_Returns400NotPanic(t *testing.T) {
	r, _, _, _ := newTestRouter(t)

	// Missing every required field (email, password, full_name all have
	// `binding:"required"` tags in dto.go) — proves BindJSON's validation
	// actually runs over a real request body, not just that the struct
	// declares the tags.
	rec := doJSON(r, http.MethodPost, "/api/v1/auth/register", map[string]any{}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a body missing every required field, got %d: %s", rec.Code, rec.Body.String())
	}
	if code := decodeErrorCode(t, rec); code != "validation_failed" {
		t.Fatalf("expected validation_failed code, got %q", code)
	}
}

func TestRegisterHTTP_InvalidEmailFormat_Returns400(t *testing.T) {
	r, _, _, _ := newTestRouter(t)

	rec := doJSON(r, http.MethodPost, "/api/v1/auth/register", map[string]any{
		"email": "not-an-email", "password": "correct-horse-battery", "full_name": "A Name",
	}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an invalid email format, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRegisterHTTP_ValidBody_201AndRealTokens(t *testing.T) {
	r, _, _, _ := newTestRouter(t)

	rec := doJSON(r, http.MethodPost, "/api/v1/auth/register", map[string]any{
		"email": "http-register@example.com", "password": "correct-horse-battery", "full_name": "HTTP User",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp identity.AuthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.AccessToken == "" || resp.User.Email != "http-register@example.com" {
		t.Fatalf("unexpected register response: %+v", resp)
	}
}

func TestMeHTTP_NoAuthorizationHeader_Returns401(t *testing.T) {
	r, _, _, _ := newTestRouter(t)

	rec := doJSON(r, http.MethodGet, "/api/v1/me", nil, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no Authorization header, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMeHTTP_ValidToken_200WithOwnProfile(t *testing.T) {
	r, svc, jwtCfg, _ := newTestRouter(t)

	authResp, err := svc.Register(context.Background(), identity.RegisterRequest{
		Email: "http-me@example.com", Password: "correct-horse-battery", FullName: "Me User",
	})
	if err != nil {
		t.Fatalf("register (service-layer setup): %v", err)
	}
	userID, err := uuid.Parse(authResp.User.ID)
	if err != nil {
		t.Fatalf("parse user id: %v", err)
	}
	token, err := auth.IssueAccessToken(jwtCfg, userID, authResp.User.Role, 1)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	rec := doJSON(r, http.MethodGet, "/api/v1/me", nil, map[string]string{"Authorization": "Bearer " + token})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with a valid token, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp identity.UserResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Email != "http-me@example.com" {
		t.Fatalf("expected the caller's own profile, got %+v", resp)
	}
}

func TestAdminUpdateRoleHTTP_NoAuth401_WrongRole403(t *testing.T) {
	r, svc, jwtCfg, _ := newTestRouter(t)

	target, err := svc.Register(context.Background(), identity.RegisterRequest{
		Email: "http-target@example.com", Password: "correct-horse-battery", FullName: "Target User",
	})
	if err != nil {
		t.Fatalf("register target: %v", err)
	}
	targetID := target.User.ID

	// No Authorization header at all.
	rec := doJSON(r, http.MethodPatch, "/api/v1/admin/users/"+targetID+"/role",
		map[string]any{"role": "instructor", "status": "active"}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no auth header, got %d: %s", rec.Code, rec.Body.String())
	}

	// A valid token, but for a student — RequireRole(admin) must reject it.
	nonAdmin, err := svc.Register(context.Background(), identity.RegisterRequest{
		Email: "http-nonadmin@example.com", Password: "correct-horse-battery", FullName: "Non Admin",
	})
	if err != nil {
		t.Fatalf("register non-admin: %v", err)
	}
	nonAdminID, err := uuid.Parse(nonAdmin.User.ID)
	if err != nil {
		t.Fatalf("parse non-admin id: %v", err)
	}
	studentToken, err := auth.IssueAccessToken(jwtCfg, nonAdminID, "student", 1)
	if err != nil {
		t.Fatalf("issue student token: %v", err)
	}

	rec = doJSON(r, http.MethodPatch, "/api/v1/admin/users/"+targetID+"/role",
		map[string]any{"role": "instructor", "status": "active"},
		map[string]string{"Authorization": "Bearer " + studentToken})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for a non-admin role, got %d: %s", rec.Code, rec.Body.String())
	}
}
