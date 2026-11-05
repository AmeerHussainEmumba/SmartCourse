// What this checks (RFC §11.4's three authorization layers): RequireAuth
// rejects a missing/malformed/invalid Authorization header and, on success,
// makes the identity available to downstream handlers via FromContext;
// RequireRole rejects a role outside its allow-list and accepts one inside
// it. No database — RequireFreshPrivilege (the third layer, Redis-backed)
// is tested separately in middleware_integration_test.go, since it needs a
// real Redis to be meaningful (RFC §18.2). No test anywhere previously sent
// an HTTP request through this middleware at all — these are the first.
package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// echoIdentityHandler is the terminal handler every RequireAuth test uses —
// it proves c.Next() actually ran with the identity RequireAuth attached to
// the request context, not just that the middleware returned 200 on its own.
func echoIdentityHandler(c *gin.Context) {
	id, ok := FromContext(c.Request.Context())
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "no identity in context"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": id.UserID.String(), "role": id.Role, "token_version": id.TokenVersion})
}

func newEngine(handlers ...gin.HandlerFunc) *gin.Engine {
	e := gin.New()
	e.GET("/test", append(handlers, echoIdentityHandler)...)
	return e
}

func doRequest(t *testing.T, e *gin.Engine, authHeader string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestRequireAuth_RejectsMissingHeader(t *testing.T) {
	e := newEngine(RequireAuth(testJWTCfg()))
	rec := doRequest(t, e, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"unauthorized"`) {
		t.Errorf("expected error body to carry code=unauthorized, got %s", rec.Body.String())
	}
}

func TestRequireAuth_RejectsMalformedHeader(t *testing.T) {
	e := newEngine(RequireAuth(testJWTCfg()))
	// No "Bearer " prefix — a raw token value isn't a valid Authorization header.
	signed, err := IssueAccessToken(testJWTCfg(), uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	rec := doRequest(t, e, signed)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_RejectsExpiredToken(t *testing.T) {
	cfg := testJWTCfg()
	cfg.AccessTokenTTL = -time.Minute // already expired the moment it's issued
	signed, err := IssueAccessToken(cfg, uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := newEngine(RequireAuth(testJWTCfg()))
	rec := doRequest(t, e, "Bearer "+signed)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for an expired token, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_RejectsWrongSigningKey(t *testing.T) {
	issuingCfg := testJWTCfg()
	issuingCfg.SigningKey = "a-completely-different-signing-key-32bytes"
	signed, err := IssueAccessToken(issuingCfg, uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := newEngine(RequireAuth(testJWTCfg()))
	rec := doRequest(t, e, "Bearer "+signed)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a token signed with a different key, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_Success_PopulatesContext(t *testing.T) {
	cfg := testJWTCfg()
	userID := uuid.New()
	signed, err := IssueAccessToken(cfg, userID, "instructor", 4)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := newEngine(RequireAuth(cfg))
	rec := doRequest(t, e, "Bearer "+signed)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, userID.String()) {
		t.Errorf("expected response to echo user_id %s, got %s", userID, body)
	}
	if !strings.Contains(body, `"role":"instructor"`) {
		t.Errorf("expected response to echo role=instructor, got %s", body)
	}
	if !strings.Contains(body, `"token_version":4`) {
		t.Errorf("expected response to echo token_version=4, got %s", body)
	}
}

func TestRequireRole_RejectsWhenNoIdentityInContext(t *testing.T) {
	// RequireRole alone, with no RequireAuth ahead of it — exercises its own
	// "no identity in context" branch directly, independent of RequireAuth's
	// behavior (which always populates one on success).
	e := newEngine(RequireRole("instructor"))
	rec := doRequest(t, e, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when no identity is in context, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireRole_RejectsDisallowedRole(t *testing.T) {
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := newEngine(RequireAuth(cfg), RequireRole("instructor", "admin"))
	rec := doRequest(t, e, "Bearer "+signed)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for a student hitting an instructor-only route, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"forbidden"`) {
		t.Errorf("expected error body to carry code=forbidden, got %s", rec.Body.String())
	}
}

func TestRequireRole_AllowsAllowedRole(t *testing.T) {
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, uuid.New(), "instructor", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	e := newEngine(RequireAuth(cfg), RequireRole("instructor", "admin"))
	rec := doRequest(t, e, "Bearer "+signed)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for an instructor hitting an instructor-only route, got %d: %s", rec.Code, rec.Body.String())
	}
}
