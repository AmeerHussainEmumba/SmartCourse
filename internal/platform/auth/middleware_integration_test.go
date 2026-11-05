//go:build integration

// What this checks, against real Redis (RFC §18.2 — mocks are not used for
// infrastructure): RequireFreshPrivilege (ADR-0015 addendum), previously
// completely unverified at any level. A cache miss defaults to "version 1,
// never changed" per its own doc comment; a cached token_version that
// matches the token's own claim passes; a mismatch — the actual revocation
// case this middleware exists to catch — is rejected with the specific
// token_stale error code, not just a non-2xx status.
package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestRequireFreshPrivilege_CacheMiss_DefaultsToVersion1(t *testing.T) {
	env := testenv.Setup(t)
	cfg := testJWTCfg()
	userID := uuid.New()
	// TokenVersion 1, nothing written to Redis for this user — the
	// documented "never changed" default.
	signed, err := IssueAccessToken(cfg, userID, "admin", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilege(env.RDB), echoIdentityHandler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on a cache miss with token_version 1, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireFreshPrivilege_CacheMiss_RejectsNonDefaultVersion(t *testing.T) {
	env := testenv.Setup(t)
	cfg := testJWTCfg()
	userID := uuid.New()
	// TokenVersion 2 with nothing cached in Redis — the cache miss default
	// of "1" must NOT match this token, since that would mean a revoked
	// token silently passing whenever Redis happens to be empty for that user.
	signed, err := IssueAccessToken(cfg, userID, "admin", 2)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilege(env.RDB), echoIdentityHandler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 (token_stale) for token_version 2 against an empty cache, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"token_stale"`) {
		t.Errorf("expected error body to carry code=token_stale, got %s", rec.Body.String())
	}
}

func TestRequireFreshPrivilege_CachedVersionMatches_Passes(t *testing.T) {
	env := testenv.Setup(t)
	cfg := testJWTCfg()
	userID := uuid.New()
	signed, err := IssueAccessToken(cfg, userID, "admin", 3)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	// Same Set call shape internal/identity.Service.UpdateUserRole uses —
	// TokenVersionRedisKey, the raw int, no TTL.
	if err := env.RDB.Set(t.Context(), TokenVersionRedisKey(userID.String()), 3, 0).Err(); err != nil {
		t.Fatalf("seed redis: %v", err)
	}

	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilege(env.RDB), echoIdentityHandler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 when the cached version matches the token's, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestRequireFreshPrivilege_CachedVersionMismatch_RejectsStaleToken is the
// actual revocation case: a token issued before a role change (token_version
// still 1) presented after Redis has already been bumped to 2 by
// UpdateUserRole must be rejected, not honored.
func TestRequireFreshPrivilege_CachedVersionMismatch_RejectsStaleToken(t *testing.T) {
	env := testenv.Setup(t)
	cfg := testJWTCfg()
	userID := uuid.New()
	signed, err := IssueAccessToken(cfg, userID, "admin", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	if err := env.RDB.Set(t.Context(), TokenVersionRedisKey(userID.String()), 2, 0).Err(); err != nil {
		t.Fatalf("seed redis: %v", err)
	}

	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilege(env.RDB), echoIdentityHandler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 (token_stale) for a token whose version is behind Redis's, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"token_stale"`) {
		t.Errorf("expected error body to carry code=token_stale, got %s", rec.Body.String())
	}
}
