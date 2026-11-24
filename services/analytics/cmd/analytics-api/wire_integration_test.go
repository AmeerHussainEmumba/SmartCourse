//go:build integration

// analytics-api as deployed: registerRoutes with a real config over real
// Postgres, Redis and MongoDB. The Failed Events metric must come from the
// real failed_events collection, move when that collection moves, and answer
// 503 (never 5xx noise elsewhere) when MongoDB is unset, unreachable or gone.
package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

const issuesPath = "/api/v1/analytics/issues"

func cfgFor(mongoURI string) *config.Config {
	return &config.Config{
		JWT: config.JWTConfig{SigningKey: "test-signing-key-0123456789-abcdefghij", AccessTokenTTL: time.Hour, Issuer: "smartcourse"},
		Mongo: config.MongoConfig{
			URI: mongoURI, Database: "analytics_wire_test", ConnectTimeout: time.Second,
			ServerSelectionTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
		},
	}
}

type harness struct {
	t   *testing.T
	r   *gin.Engine
	cfg *config.Config
}

func newHarness(t *testing.T, pg *testenv.Env, mongoURI string) *harness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := cfgFor(mongoURI)
	r := gin.New()
	closeFn := registerRoutes(r, &infra.Dependencies{DB: pg.DB, RDB: pg.RDB}, cfg)
	t.Cleanup(func() { closeFn(context.Background()) })
	return &harness{t: t, r: r, cfg: cfg}
}

func (h *harness) get(path, role string) *httptest.ResponseRecorder {
	tok, err := auth.IssueAccessToken(h.cfg.JWT, uuid.New(), role, 1)
	if err != nil {
		h.t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.r.ServeHTTP(rec, req)
	return rec
}

type issuesBody struct {
	FailedEvents struct {
		Total       int64            `json:"total"`
		Unresolved  int64            `json:"unresolved"`
		Replayed    int64            `json:"replayed"`
		ByConsumer  map[string]int64 `json:"by_consumer"`
		ByEventType map[string]int64 `json:"by_event_type"`
		OldestAge   *int64           `json:"oldest_unresolved_age_seconds"`
	} `json:"failed_events"`
	WorkflowIssues json.RawMessage `json:"workflow_issues"`
}

func TestAnalyticsAPI_Issues_ServedFromRealFailedEvents(t *testing.T) {
	pg, mongoEnv := testenv.Setup(t), testenv.SetupMongo(t)
	h := newHarness(t, pg, mongoEnv.URI)
	store, err := mongostore.Open(context.Background(), mongostore.Config{URI: mongoEnv.URI, Database: "analytics_wire_test"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	park := func(id, consumer, typ string, at time.Time) {
		t.Helper()
		if _, err := store.ParkFailedEvent(context.Background(), mongostore.ParkInput{EventID: id, Consumer: consumer, EventType: typ, Err: "boom", FailedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	read := func() issuesBody {
		t.Helper()
		rec := h.get(issuesPath, "admin")
		if rec.Code != 200 {
			t.Fatalf("issues = %d %s", rec.Code, rec.Body.String())
		}
		var b issuesBody
		if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
			t.Fatal(err)
		}
		return b
	}

	b := read()
	if b.FailedEvents.Total != 0 || b.FailedEvents.OldestAge != nil || string(b.WorkflowIssues) != `{"by_workflow":{},"failed":0}` {
		t.Errorf("empty store: %+v workflow=%s", b.FailedEvents, b.WorkflowIssues)
	}

	park("e1", "analytics", "enrollment.student_enrolled", time.Now().Add(-2*time.Hour))
	park("e2", "certificates", "progress.course_completed", time.Now())
	park("e3", "analytics", "enrollment.student_enrolled", time.Now())
	b = read()
	if b.FailedEvents.Total != 3 || b.FailedEvents.Unresolved != 3 || b.FailedEvents.ByConsumer["analytics"] != 2 ||
		b.FailedEvents.ByEventType["progress.course_completed"] != 1 {
		t.Errorf("after parking three: %+v", b.FailedEvents)
	}
	if b.FailedEvents.OldestAge == nil || *b.FailedEvents.OldestAge < 7100 || *b.FailedEvents.OldestAge > 7400 {
		t.Errorf("oldest unresolved age = %v, want about 2 hours", b.FailedEvents.OldestAge)
	}

	// The metric moves when the source moves: a replay turns unresolved into replayed.
	if _, err := store.MarkReplayed(context.Background(), "e1", "analytics", time.Now()); err != nil {
		t.Fatal(err)
	}
	b = read()
	if b.FailedEvents.Unresolved != 2 || b.FailedEvents.Replayed != 1 || b.FailedEvents.ByConsumer["analytics"] != 1 {
		t.Errorf("after one replay: %+v", b.FailedEvents)
	}
	if b.FailedEvents.OldestAge == nil || *b.FailedEvents.OldestAge > 60 {
		t.Errorf("oldest unresolved age after replaying the old one = %v, want seconds", b.FailedEvents.OldestAge)
	}

	if rec := h.get(issuesPath, "student"); rec.Code != 403 {
		t.Errorf("student: want 403, got %d", rec.Code)
	}

	// MongoDB goes away: only this route degrades, to 503.
	mongoEnv.Stop(t)
	if rec := h.get(issuesPath, "admin"); rec.Code != 503 {
		t.Errorf("issues with MongoDB down: want 503, got %d %s", rec.Code, rec.Body.String())
	}
	for _, p := range []string{"/api/v1/analytics/platform", "/api/v1/analytics/popular-courses"} {
		if rec := h.get(p, "student"); rec.Code != 200 {
			t.Errorf("%s with MongoDB down: want 200, got %d", p, rec.Code)
		}
	}
}

func TestAnalyticsAPI_Issues_MongoNotConfigured_Is503AndOtherRoutesWork(t *testing.T) {
	pg := testenv.Setup(t)
	h := newHarness(t, pg, "")
	if rec := h.get(issuesPath, "admin"); rec.Code != 503 {
		t.Errorf("issues without MONGO_URI: want 503, got %d", rec.Code)
	}
	if rec := h.get("/api/v1/analytics/platform", "student"); rec.Code != 200 {
		t.Errorf("platform: want 200, got %d", rec.Code)
	}
}

// MongoDB configured but unreachable at boot: routes register at once (no dial
// at startup), the issues route says 503, and nothing else notices.
func TestAnalyticsAPI_Issues_MongoUnreachableAtBoot(t *testing.T) {
	pg := testenv.Setup(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close() // nothing listens there now

	start := time.Now()
	h := newHarness(t, pg, "mongodb://"+addr)
	if d := time.Since(start); d > time.Second {
		t.Errorf("registerRoutes took %s with MongoDB unreachable; it must not dial at boot", d)
	}
	if rec := h.get(issuesPath, "admin"); rec.Code != 503 {
		t.Errorf("issues with MongoDB unreachable: want 503, got %d %s", rec.Code, rec.Body.String())
	}
	if rec := h.get("/api/v1/analytics/platform", "student"); rec.Code != 200 {
		t.Errorf("platform: want 200, got %d", rec.Code)
	}
}
