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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
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
	// Same Set call shape services/identity/identity.Service.UpdateUserRole uses —
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

type fakeVersionSource struct {
	version int16
	err     error
	calls   int
}

func (f *fakeVersionSource) CurrentTokenVersion(_ context.Context, _ uuid.UUID) (int16, error) {
	f.calls++
	return f.version, f.err
}

func serveWithSource(t *testing.T, env *testenv.Env, src TokenVersionSource, userID uuid.UUID, tokenVersion int16) *httptest.ResponseRecorder {
	t.Helper()
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, userID, "admin", tokenVersion)
	if err != nil {
		t.Fatal(err)
	}
	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilegeWithSource(env.RDB, src), echoIdentityHandler)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)
	return rec
}

// F-03: a Redis miss is resolved from the durable source, not assumed to be
// version 1, and the answer is written back.
func TestRequireFreshPrivilegeWithSource_CacheMiss_UsesSourceAndRepopulates(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.New()
	src := &fakeVersionSource{version: 3}

	if rec := serveWithSource(t, env, src, userID, 1); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "token_stale") {
		t.Fatalf("version-1 token with source version 3 must be stale, got %d %s", rec.Code, rec.Body.String())
	}
	if v, err := env.RDB.Get(t.Context(), TokenVersionRedisKey(userID.String())).Int(); err != nil || v != 3 {
		t.Fatalf("redis not repopulated: v=%d err=%v", v, err)
	}
	// Now cached: a matching token passes without another source call.
	if rec := serveWithSource(t, env, src, userID, 3); rec.Code != http.StatusOK {
		t.Fatalf("matching token: %d %s", rec.Code, rec.Body.String())
	}
	if src.calls != 1 {
		t.Fatalf("source consulted %d times, want 1 (second request served from cache)", src.calls)
	}
}

func TestRequireFreshPrivilegeWithSource_CacheHitWins_SourceNotConsulted(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.New()
	if err := env.RDB.Set(t.Context(), TokenVersionRedisKey(userID.String()), 5, 0).Err(); err != nil {
		t.Fatal(err)
	}
	src := &fakeVersionSource{version: 2}
	if rec := serveWithSource(t, env, src, userID, 5); rec.Code != http.StatusOK {
		t.Fatalf("cached value must win: %d", rec.Code)
	}
	if src.calls != 0 {
		t.Fatal("source consulted despite a cache hit")
	}
}

func TestRequireFreshPrivilegeWithSource_UnknownUser401_SourceError503(t *testing.T) {
	env := testenv.Setup(t)
	if rec := serveWithSource(t, env, &fakeVersionSource{err: ErrUserNotFound}, uuid.New(), 1); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown user: %d", rec.Code)
	}
	if rec := serveWithSource(t, env, &fakeVersionSource{err: errors.New("db down")}, uuid.New(), 1); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("source failure must fail closed: %d", rec.Code)
	}
}
