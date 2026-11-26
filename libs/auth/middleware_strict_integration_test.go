//go:build integration

// RequireFreshPrivilegeStrict, against real Redis: the admin-surface rule that a
// token-version cache miss fails CLOSED (401 token_stale, never "version 1"),
// because after a Redis flush the cache has forgotten every demotion. The
// instructor-route behaviour (miss means version 1) is covered by the
// RequireFreshPrivilege tests in middleware_integration_test.go and is unchanged.
package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// strictGet runs one request through RequireAuth + RequireFreshPrivilegeStrict.
func strictGet(t *testing.T, env *testenv.Env, version int16, userID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, userID, "admin", version)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	e := gin.New()
	e.GET("/test", RequireAuth(cfg), RequireFreshPrivilegeStrict(env.RDB), echoIdentityHandler)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	e.ServeHTTP(rec, req)
	return rec
}

func TestRequireFreshPrivilegeStrict_CacheMiss_FailsClosedEvenForVersion1(t *testing.T) {
	env := testenv.Setup(t)
	for _, v := range []int16{1, 2} {
		rec := strictGet(t, env, v, uuid.New())
		if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"code":"token_stale"`) {
			t.Errorf("token_version %d against an empty cache: got %d %s, want 401 token_stale", v, rec.Code, rec.Body.String())
		}
	}
}

func TestRequireFreshPrivilegeStrict_CachedVersion_MatchPassesMismatchRejected(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.New()
	if err := env.RDB.Set(context.Background(), TokenVersionRedisKey(userID.String()), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := strictGet(t, env, 2, userID); rec.Code != http.StatusOK {
		t.Errorf("matching cached version: got %d %s, want 200", rec.Code, rec.Body.String())
	}
	if rec := strictGet(t, env, 1, userID); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "token_stale") {
		t.Errorf("older token: got %d %s, want 401 token_stale", rec.Code, rec.Body.String())
	}
}

func TestRequireFreshPrivilegeStrict_RedisDown_503(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.New()
	_ = env.RDB.Close() // every command now errors, and the error is not redis.Nil
	if rec := strictGet(t, env, 1, userID); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Redis unreachable: got %d, want 503", rec.Code)
	}
}
