//go:build integration

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/auditconfig"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func get(t *testing.T, url, token string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// audit-api with an unreachable MongoDB (and Kafka): it starts, stays up, the
// failed-events routes answer 503 (not 500, no cause in the body), readiness is
// degraded (200) and names mongodb as failed, /metrics is only on the internal
// listener, and it shuts down cleanly on cancel.
func TestAuditAPI_MongoUnreachable_Degraded503NoCrash(t *testing.T) {
	pg := testenv.Setup(t) // Redis for the token-version check
	api, internal := freeAddr(t), freeAddr(t)
	_, port, _ := net.SplitHostPort(api)
	const key = "test-signing-key-0123456789-abcdefghij"
	t.Setenv("HTTP_PORT", port)
	t.Setenv("METRICS_HTTP_ADDR", internal)
	t.Setenv("REDIS_ADDR", pg.RDB.Options().Addr)
	t.Setenv("JWT_SIGNING_KEY", key)
	t.Setenv("MONGO_URI", "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500")
	t.Setenv("MONGO_SERVER_SELECTION_TIMEOUT", "500ms")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- rt.Run(ctx, "audit-api", wire, rt.WithConfig(auditconfig.LoadAPI))
	}()
	t.Cleanup(cancel)

	base := "http://" + api
	deadline := time.Now().Add(30 * time.Second)
	var body string
	var code int
	for {
		code, body = get(t, base+"/health/ready", "")
		if code == 200 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("audit-api exited: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("never became ready: %d %s", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(body, `"status":"degraded"`) || !strings.Contains(body, `"mongodb":"failed"`) || !strings.Contains(body, `"redis":"ok"`) {
		t.Errorf("ready = %s, want degraded with mongodb failed and redis ok", body)
	}
	if strings.Contains(body, "127.0.0.1:1") {
		t.Errorf("readiness leaks an address: %s", body)
	}

	cfg := config.JWTConfig{SigningKey: key, AccessTokenTTL: time.Hour, Issuer: "smartcourse"}
	adminID := uuid.New()
	tok, err := auth.IssueAccessToken(cfg, adminID, "admin", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 1, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/admin/failed-events", "/api/v1/admin/failed-events/" + uuid.NewString()} {
		if code, body := get(t, base+path, tok); code != 503 || !strings.Contains(body, "temporarily_unavailable") {
			t.Errorf("GET %s with MongoDB down = %d %s, want 503", path, code, body)
		}
	}
	if code, _ := get(t, base+"/health/live", ""); code != 200 {
		t.Errorf("live = %d", code)
	}
	if code, _ := get(t, base+"/api/v1/admin/failed-events", ""); code != 401 {
		t.Errorf("no token = %d, want 401", code)
	}
	if code, _ := get(t, base+"/metrics", tok); code != 404 {
		t.Errorf("/metrics on the API port = %d, want 404 (internal listener only)", code)
	}
	if code, body := get(t, "http://"+internal+"/metrics", ""); code != 200 || !strings.Contains(body, "# HELP") {
		t.Errorf("/metrics on the internal listener = %d (%.60s)", code, body)
	}
	if code, _ := get(t, "http://"+internal+"/health/live", ""); code != 200 {
		t.Errorf("internal live = %d", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("shutdown error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("audit-api did not shut down")
	}
}

// Configuration errors fail fast with a message that does not echo secrets.
func TestAuditAPI_ConfigurationErrors(t *testing.T) {
	t.Setenv("JWT_SIGNING_KEY", "test-signing-key-0123456789-abcdefghij")
	t.Setenv("REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("MONGO_URI", "")
	if _, err := auditconfig.LoadAPI(); err == nil || !strings.Contains(err.Error(), "MONGO_URI") {
		t.Fatalf("missing MONGO_URI: %v", err)
	}
	t.Setenv("MONGO_URI", "mongodb://u:topsecret@127.0.0.1:27017")
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := auditconfig.LoadAPI(); err == nil {
		t.Fatal("missing KAFKA_BROKERS accepted")
	}
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("JWT_SIGNING_KEY", "short")
	if _, err := auditconfig.LoadAPI(); err == nil || strings.Contains(err.Error(), "topsecret") {
		t.Fatalf("short JWT key: %v", err)
	}
}
