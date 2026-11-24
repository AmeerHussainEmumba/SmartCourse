//go:build integration

// The trace of a real course-api request reaches the outbox row it writes, and
// the request shows HTTP, DB and Redis spans in that same trace (ADR-0032).
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestArchiveCourse_OutboxRowCarriesTheRequestTrace(t *testing.T) {
	env := testenv.Setup(t)
	deps := &infra.Dependencies{DB: env.DB, RDB: env.RDB}
	cfg := &config.Config{
		Env: "test",
		JWT: config.JWTConfig{SigningKey: "test-signing-key-at-least-32-bytes-long!!", Issuer: "smartcourse-test", AccessTokenTTL: 15 * time.Minute},
		Telemetry: config.TelemetryConfig{
			OTLPEndpoint: "jaeger:4318", SampleRatio: 1, MetricsEnabled: true,
		},
	}
	exp := tracetest.NewInMemoryExporter()
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "course-api", Outbox: true, SpanExporter: exp})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })

	r, err := router.NewWithOptions(router.Options{Env: "test", Observe: obs.Middleware()})
	if err != nil {
		t.Fatal(err)
	}
	obs.RegisterMetricsRoute(r)
	registerRoutes(r, deps, cfg, nil)

	instructor, courseID := uuid.New(), uuid.New()
	if err := env.DB.Exec(`INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, 'trace-test')`, courseID, instructor).Error; err != nil {
		t.Fatal(err)
	}
	token, err := auth.IssueAccessToken(cfg.JWT, instructor, "instructor", 1)
	if err != nil {
		t.Fatal(err)
	}

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/courses/"+courseID.String()+"/archive", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("archive returned %d: %s", rec.Code, rec.Body)
	}
	if err := obs.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The outbox row names the request's trace, its server span and the actor.
	var row outbox.Row
	if err := env.DB.Where("event_type = ?", "course.course_archived").First(&row).Error; err != nil {
		t.Fatalf("no outbox row: %v", err)
	}
	if row.TraceID == nil || *row.TraceID != traceID {
		t.Fatalf("outbox trace_id = %v, want the request's %s", row.TraceID, traceID)
	}
	if row.ActorID == nil || *row.ActorID != instructor {
		t.Errorf("outbox actor_id = %v, want %s", row.ActorID, instructor)
	}

	var serverSpanID string
	var db, redisSpan bool
	for _, s := range exp.GetSpans() {
		if s.SpanContext.TraceID().String() != traceID {
			t.Errorf("span %q left the request's trace: %s", s.Name, s.SpanContext.TraceID())
		}
		if s.Name == "POST /api/v1/courses/:id/archive" {
			serverSpanID = s.SpanContext.SpanID().String()
		}
		for _, a := range s.Attributes {
			if a.Key == "db.system.name" {
				db = db || a.Value.AsString() == "postgresql"
				redisSpan = redisSpan || a.Value.AsString() == "redis"
			}
		}
	}
	if serverSpanID == "" {
		t.Fatalf("no server span named after the route template among %d spans", len(exp.GetSpans()))
	}
	if row.SpanID == nil || *row.SpanID != serverSpanID {
		t.Errorf("outbox span_id = %v, want the server span %s", row.SpanID, serverSpanID)
	}
	if !db || !redisSpan {
		t.Errorf("want Postgres and Redis child spans, got db=%v redis=%v", db, redisSpan)
	}
}
