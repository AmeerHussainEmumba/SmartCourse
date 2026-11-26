package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func jsonLogger(buf *bytes.Buffer) *slog.Logger { return slog.New(slog.NewJSONHandler(buf, nil)) }

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("not JSON: %v (%s)", err, buf.String())
	}
	return m
}

func TestCorrelated_AddsTraceAndSpanIDs(t *testing.T) {
	var buf bytes.Buffer
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(WithContext(context.Background(), jsonLogger(&buf)),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))

	Correlated(ctx).Info("hello")
	m := decode(t, &buf)
	if m[KeyTraceID] != tid.String() || m[KeySpanID] != sid.String() {
		t.Errorf("got %v", m)
	}
}

func TestCorrelated_NoSpanNoFields(t *testing.T) {
	var buf bytes.Buffer
	Correlated(WithContext(context.Background(), jsonLogger(&buf))).Info("hello")
	m := decode(t, &buf)
	if _, ok := m[KeyTraceID]; ok {
		t.Errorf("trace_id present without a span: %v", m)
	}
}

func TestWithEventIDAndJobID(t *testing.T) {
	var buf bytes.Buffer
	l := WithJobID(WithEventID(jsonLogger(&buf), "evt-1"), "job-9")
	l.Info("x")
	m := decode(t, &buf)
	if m[KeyEventID] != "evt-1" || m[KeyJobID] != "job-9" {
		t.Errorf("got %v", m)
	}

	buf.Reset()
	WithJobID(WithEventID(jsonLogger(&buf), ""), "").Info("y")
	m = decode(t, &buf)
	if _, ok := m[KeyEventID]; ok {
		t.Errorf("empty event id was logged: %v", m)
	}
	if _, ok := m[KeyJobID]; ok {
		t.Errorf("empty job id was logged: %v", m)
	}
}
