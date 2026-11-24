package router

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHSTS_OnlyWhenEnabledOrTLS(t *testing.T) {
	gin.SetMode(gin.TestMode)
	plain := New("test", nil)
	plain.GET("/ok", func(c *gin.Context) { c.Status(204) })
	rec := httptest.NewRecorder()
	plain.ServeHTTP(rec, httptest.NewRequest("GET", "/ok", nil))
	if rec.Header().Get("Strict-Transport-Security") != "" {
		t.Error("HSTS sent over plain HTTP without being enabled")
	}

	on, err := NewWithOptions(Options{Env: "test", HSTS: true})
	if err != nil {
		t.Fatal(err)
	}
	on.GET("/ok", func(c *gin.Context) { c.Status(204) })
	rec = httptest.NewRecorder()
	on.ServeHTTP(rec, httptest.NewRequest("GET", "/ok", nil))
	if rec.Header().Get("Strict-Transport-Security") == "" {
		t.Error("HSTS missing when enabled")
	}
}

func TestTrustedProxies_HonouredOnlyForConfiguredPeers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r, err := NewWithOptions(Options{Env: "test", TrustedProxies: []string{"10.0.0.0/8"}})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/ip", func(c *gin.Context) { c.String(200, c.ClientIP()) })
	for peer, want := range map[string]string{"10.1.2.3:99": "198.51.100.7", "203.0.113.9:99": "203.0.113.9"} {
		req := httptest.NewRequest("GET", "/ip", nil)
		req.RemoteAddr = peer
		req.Header.Set("X-Forwarded-For", "198.51.100.7")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if got := rec.Body.String(); got != want {
			t.Errorf("peer %s: ClientIP=%q want %q", peer, got, want)
		}
	}
	if _, err := NewWithOptions(Options{TrustedProxies: []string{"not-a-cidr"}}); err == nil {
		t.Error("invalid trusted proxy accepted")
	}
}

func TestBodyCap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r, _ := NewWithOptions(Options{Env: "test", MaxBodyBytes: 100})
	r.POST("/echo", func(c *gin.Context) {
		b, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.Status(413)
			return
		}
		c.String(200, "%d", len(b))
	})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", 50))))
	if rec.Code != 200 {
		t.Errorf("small body: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", 5000))))
	if rec.Code != 413 {
		t.Errorf("oversized body: %d want 413", rec.Code)
	}
	// Chunked/undeclared length is cut off while reading.
	req := httptest.NewRequest("POST", "/echo", io.NopCloser(strings.NewReader(strings.Repeat("a", 5000))))
	req.ContentLength = -1
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Errorf("undeclared oversized body: %d want 413", rec.Code)
	}
}

func TestRequestID_ValidInboundKept(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := New("test", nil)
	r.GET("/ok", func(c *gin.Context) { c.Status(204) })
	req := httptest.NewRequest("GET", "/ok", nil)
	req.Header.Set("X-Request-Id", "abc-123_X.y")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got != "abc-123_X.y" {
		t.Errorf("valid id replaced: %q", got)
	}
}

func TestHealthReady_ReportsPerDependencyStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterHealth(r, map[string]Checker{
		"postgres": func(context.Context) error { return errors.New("secret detail") },
		"redis":    func(context.Context) error { return nil },
	})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/health/ready", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), `"postgres":"failed"`) || !strings.Contains(rec.Body.String(), `"redis":"ok"`) {
		t.Errorf("unexpected: %d %s", rec.Code, rec.Body.String())
	}
}
