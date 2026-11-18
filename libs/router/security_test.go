// Security checks for the shared HTTP shell (areas B/E): CORS allow-list
// behaviour, panic recovery, request-id echo, health endpoint output. No
// infra: pure gin + httptest.
package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func newEngineWithProbe(origins []string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := New("test", origins)
	r.GET("/ok", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })
	r.GET("/boom", func(c *gin.Context) {
		panic("sql: connection to postgres://u:hunter2@db:5432/x failed at /srv/app/repo.go:42")
	})
	return r
}

func serve(r *gin.Engine, method, path string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestCORS_OnlyExactAllowListedOriginsAreReflected(t *testing.T) {
	r := newEngineWithProbe([]string{"https://app.example.com"})

	reflected := []string{"https://app.example.com"}
	denied := []string{
		"https://evil.example.com",
		"https://app.example.com.evil.com",
		"http://app.example.com",       // scheme downgrade
		"https://app.example.com:8443", // port
		"https://APP.example.com",      // case
		"https://app.example.com/",     // trailing slash
		"null",                         // sandboxed iframe / file://
		"*",
		"",
	}
	for _, o := range reflected {
		rec := serve(r, "GET", "/ok", map[string]string{"Origin": o})
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != o {
			t.Errorf("origin %q: ACAO=%q", o, got)
		}
	}
	for _, o := range denied {
		for _, m := range []string{"GET", "OPTIONS"} {
			rec := serve(r, m, "/ok", map[string]string{"Origin": o, "Access-Control-Request-Method": "GET"})
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("%s origin %q must not be granted CORS, got ACAO=%q", m, o, got)
			}
		}
	}
}

func TestCORS_EmptyAllowList_DeniesEverything_NoWildcardWithCredentials(t *testing.T) {
	for _, list := range [][]string{nil, {}} {
		r := newEngineWithProbe(list)
		rec := serve(r, "GET", "/ok", map[string]string{"Origin": "https://anything.example"})
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("empty allow list granted CORS: %v", rec.Header())
		}
	}
	// And in no configuration is the credentials header combined with '*'.
	r := newEngineWithProbe([]string{"*", "https://a.example"})
	rec := serve(r, "GET", "/ok", map[string]string{"Origin": "https://a.example"})
	if rec.Header().Get("Access-Control-Allow-Origin") == "*" && rec.Header().Get("Access-Control-Allow-Credentials") == "true" {
		t.Fatal("wildcard origin together with Allow-Credentials")
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://a.example" {
		t.Fatalf("expected the concrete origin to be reflected, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
	if rec.Header().Get("Vary") != "Origin" {
		t.Fatal("reflected CORS response must carry Vary: Origin (cache poisoning)")
	}
}

func TestRecovery_PanicReturnsCleanBody_NoStackNoSecrets(t *testing.T) {
	r := newEngineWithProbe(nil)
	rec := serve(r, "GET", "/boom", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, leak := range []string{"hunter2", "postgres://", "repo.go", "goroutine", "panic", "runtime", ".go:", "sql:"} {
		if strings.Contains(body, leak) {
			t.Fatalf("500 body leaks %q: %s", leak, body)
		}
	}
	for k, v := range rec.Header() {
		if strings.Contains(strings.Join(v, ","), "hunter2") {
			t.Fatalf("header %s leaks panic value", k)
		}
	}
	// The server must survive and keep serving.
	if rec := serve(r, "GET", "/ok", nil); rec.Code != 200 {
		t.Fatalf("engine unusable after panic: %d", rec.Code)
	}
}

func TestUnknownRoutesAndMethods_AreCleanAndNotLeaky(t *testing.T) {
	r := newEngineWithProbe(nil)
	for _, c := range []struct{ m, p string }{
		{"GET", "/nope"}, {"POST", "/ok"}, {"DELETE", "/"}, {"GET", "/../../etc/passwd"},
		{"GET", "/%2e%2e/%2e%2e/etc/passwd"}, {"GET", "/ok/%00"}, {"TRACE", "/ok"},
	} {
		rec := serve(r, c.m, c.p, nil)
		if rec.Code >= 500 {
			t.Errorf("%s %s -> %d", c.m, c.p, rec.Code)
		}
		if strings.Contains(strings.ToLower(rec.Body.String()), "gin") {
			t.Errorf("%s %s body names the framework: %q", c.m, c.p, rec.Body.String())
		}
		if s := rec.Header().Get("Server"); s != "" {
			t.Errorf("Server header set: %q", s)
		}
	}
}

func TestHealth_LiveIsMinimal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterHealth(r, map[string]Checker{"db": func(context.Context) error { return nil }})
	rec := serve(r, "GET", "/health/live", nil)
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"status":"live"}` {
		t.Fatalf("unexpected live body: %d %s", rec.Code, rec.Body.String())
	}
	rec = serve(r, "GET", "/health/ready", nil)
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("unexpected ready body: %d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, leak := range []string{"version", "go1", "postgres", "redis", "host", "dsn"} {
		if strings.Contains(strings.ToLower(body), leak) {
			t.Errorf("healthy /health/ready leaks %q", leak)
		}
	}
}

func TestHealth_ReadyFailureReturns503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterHealth(r, map[string]Checker{"db": func(context.Context) error { return errors.New("down") }})
	if rec := serve(r, "GET", "/health/ready", nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestRequestID_GeneratedWhenAbsent_UniquePerRequest(t *testing.T) {
	r := newEngineWithProbe(nil)
	a := serve(r, "GET", "/ok", nil).Header().Get("X-Request-Id")
	b := serve(r, "GET", "/ok", nil).Header().Get("X-Request-Id")
	if a == "" || b == "" || a == b {
		t.Fatalf("request ids not generated/unique: %q %q", a, b)
	}
}
