package router

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
)

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func lastLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var m map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &m); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, buf.String())
	}
	return m
}

// fakeSpan puts a valid span context in the request context, standing in for
// telemetry.HTTPTracing without importing the SDK.
func fakeSpan(c *gin.Context) {
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(c.Request.Context(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	c.Request = c.Request.WithContext(ctx)
	c.Next()
}

func TestAccessLog_CarriesTraceIDWhenSpanActive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	buf := captureLogs(t)
	r, err := NewWithOptions(Options{Env: "test", Observe: []gin.HandlerFunc{fakeSpan}})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/things/:id", func(c *gin.Context) { c.Status(204) })
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/things/42", nil))

	line := lastLogLine(t, buf)
	if line["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || line["span_id"] != "00f067aa0ba902b7" {
		t.Errorf("trace ids missing from the access log: %v", line)
	}
	if line["path"] != "/things/:id" || line["request_id"] == "" {
		t.Errorf("path must be the template and request_id present: %v", line)
	}
}

func TestAccessLog_NoTraceFieldsWithoutSpan(t *testing.T) {
	gin.SetMode(gin.TestMode)
	buf := captureLogs(t)
	r := New("test", nil)
	r.GET("/ok", func(c *gin.Context) { c.Status(204) })
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/ok", nil))

	line := lastLogLine(t, buf)
	if _, ok := line["trace_id"]; ok {
		t.Errorf("trace_id logged with no span: %v", line)
	}
}

// Observe middleware must run before Recovery so a panic is seen as a 500.
func TestObserve_SeesRecoveredPanicAs500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	captureLogs(t)
	status := 0
	r, err := NewWithOptions(Options{Env: "test", Observe: []gin.HandlerFunc{func(c *gin.Context) {
		c.Next()
		status = c.Writer.Status()
	}}})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/boom", func(c *gin.Context) { panic("boom") })
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/boom", nil))
	if status != 500 {
		t.Errorf("observer saw status %d, want 500", status)
	}
}
