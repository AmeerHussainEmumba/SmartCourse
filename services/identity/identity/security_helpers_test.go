//go:build integration

// Shared harness for the adversarial identity tests (security_*_test.go).
package identity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

// captureMailer records the raw tokens the service "emails", so tests can
// play the role of the mailbox owner (or of an attacker who stole one).
type captureMailer struct {
	mu     sync.Mutex
	reset  map[string]string
	verify map[string]string
}

func (m *captureMailer) SendPasswordReset(_ context.Context, to, raw string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reset[strings.ToLower(to)] = raw
	return nil
}

func (m *captureMailer) SendEmailVerification(_ context.Context, to, raw string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.verify[strings.ToLower(to)] = raw
	return nil
}

func (m *captureMailer) resetToken(email string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reset[strings.ToLower(email)]
}

func (m *captureMailer) verifyToken(email string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.verify[strings.ToLower(email)]
}

type harness struct {
	t      *testing.T
	r      *gin.Engine
	svc    *identity.Service
	env    *testenv.Env
	jwtCfg config.JWTConfig
	mail   *captureMailer
}

func secJWTCfg() config.JWTConfig {
	return config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 7 * 24 * time.Hour, Issuer: "smartcourse-test"}
}

var lightArgon = config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}

// newHarness boots the real identity routes on a plain gin.New(), the same
// engine shape router.New builds (no trusted-proxy configuration).
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWith(t, testenv.Setup(t), lightArgon)
}

func newHarnessWith(t *testing.T, env *testenv.Env, argon config.Argon2Config) *harness {
	t.Helper()
	return newHarnessRDB(t, env, env.RDB, argon)
}

func newHarnessRDB(t *testing.T, env *testenv.Env, rdb *redis.Client, argon config.Argon2Config) *harness {
	t.Helper()
	jwtCfg := secJWTCfg()
	mail := &captureMailer{reset: map[string]string{}, verify: map[string]string{}}
	svc := identity.NewService(identity.NewRepository(env.DB), rdb, jwtCfg, argon, mail)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	identity.RegisterRoutes(r.Group("/api/v1"), svc, jwtCfg, rdb)
	return &harness{t: t, r: r, svc: svc, env: env, jwtCfg: jwtCfg, mail: mail}
}

type reqOpt func(*http.Request)

func withToken(tok string) reqOpt {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+tok) }
}
func withIP(ip string) reqOpt { return func(r *http.Request) { r.RemoteAddr = ip + ":40000" } }
func withHeader(k, v string) reqOpt {
	return func(r *http.Request) { r.Header.Set(k, v) }
}

func (h *harness) rawCall(method, path string, body []byte, opts ...reqOpt) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "192.0.2.1:40000"
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	h.r.ServeHTTP(rec, req)
	return rec
}

func (h *harness) call(method, path string, body any, opts ...reqOpt) *httptest.ResponseRecorder {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	return h.rawCall(method, path, b, opts...)
}

func (h *harness) mustRegister(email, password string) identity.AuthResponse {
	h.t.Helper()
	// Unique IP per registration so the per-IP register limit (5/min) is not the subject here.
	ip := fmt.Sprintf("10.%d.%d.%d", uuid.New()[0], uuid.New()[1], uuid.New()[2])
	rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": email, "password": password, "full_name": "Sec Tester"}, withIP(ip))
	if rec.Code != http.StatusCreated {
		h.t.Fatalf("register %s: %d %s", email, rec.Code, rec.Body.String())
	}
	var out identity.AuthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *harness) login(email, password string, opts ...reqOpt) *httptest.ResponseRecorder {
	return h.call("POST", "/api/v1/auth/login", map[string]any{"email": email, "password": password}, opts...)
}

func (h *harness) refresh(rt string) *httptest.ResponseRecorder {
	return h.call("POST", "/api/v1/auth/refresh", map[string]any{"refresh_token": rt})
}

func (h *harness) setRoleSQL(userID, role string) {
	h.t.Helper()
	if err := h.env.DB.Exec(`UPDATE users SET role = ?::user_role WHERE id = ?`, role, userID).Error; err != nil {
		h.t.Fatal(err)
	}
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var b struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &b)
	return b.Code
}

func uniq(prefix string) string { return prefix + "-" + uuid.NewString()[:8] + "@example.com" }
