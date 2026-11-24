// Package admin is the worker's small HTTP surface (ADR-0031, RFC §6.9, §15.8):
// health probes and the admin-only failed-events routes (list, inspect,
// replay). It lives inside the worker because a replay must re-run the
// worker's own handlers, which only the worker links.
//
// It is built like the other services' HTTP layer: libs/router's hardened
// engine (body cap, security headers, request id, recovery), libs/auth for the
// JWT (the same JWT_SIGNING_KEY as every service), errors as
// {code, message, details}. The failed-events routes demand an admin role and
// a fresh token (RequireFreshPrivilege). MongoDB being unreachable answers
// 503 on those routes and shows as degraded, not failed, on /health/ready.
package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

// ErrNotConfigured is what a Checker returns for a dependency that is not
// configured at all; /health/ready reports it as "not_configured".
var ErrNotConfigured = errors.New("admin: dependency not configured")

// FailedStore is the read side of failed_events.
type FailedStore interface {
	ListFailedEvents(ctx context.Context, f mongostore.FailedFilter, cursor string, limit int) (*mongostore.FailedPage, error)
	GetFailedEvent(ctx context.Context, eventID, consumer string) (*mongostore.FailedEvent, error)
}

// Replayer replays failed events (deadletter.Replayer).
type Replayer interface {
	Replay(ctx context.Context, eventID string) (*deadletter.Result, error)
}

// Deps is what the admin surface needs.
type Deps struct {
	Config *config.Config
	// RDB backs RequireFreshPrivilege (the token-version cache).
	RDB *redis.Client
	// Failed and Replayer may be nil when MongoDB is not configured: the
	// failed-events routes then answer 503.
	Failed   FailedStore
	Replayer Replayer
	// Required are the dependencies whose failure makes /health/ready 503
	// (Postgres, Redis); Optional ones only mark it degraded (MongoDB).
	Required map[string]router.Checker
	Optional map[string]router.Checker
}

// Handler builds the admin engine.
func Handler(d Deps) (*gin.Engine, error) {
	cfg := d.Config
	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
		// No AllowedOrigins: an operator tool is never called cross-origin from a browser.
	})
	if err != nil {
		return nil, err
	}
	h := &handler{failed: d.Failed, replayer: d.Replayer}
	registerHealth(r, d.Required, d.Optional)

	g := r.Group("/api/v1/admin/failed-events")
	g.Use(auth.RequireAuth(cfg.JWT), auth.RequireRole("admin"), auth.RequireFreshPrivilege(d.RDB))
	g.GET("", h.list)
	g.GET("/:event_id", h.get)
	g.POST("/:event_id/replay", h.replay)
	return r, nil
}

// NewServer wraps the engine in an http.Server with every timeout set,
// listening on cfg.Admin.Addr.
func NewServer(cfg *config.Config, h http.Handler) *http.Server {
	return router.NewServer(cfg.Admin.Addr, h, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})
}

// registerHealth wires /health/live (never checks dependencies) and
// /health/ready: 503 when a required dependency fails, 200 with status
// "degraded" when only an optional one does. Per-dependency results are bare
// words: driver errors carry hosts and users and the route is unauthenticated.
func registerHealth(r *gin.Engine, required, optional map[string]router.Checker) {
	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "live"})
	})
	r.GET("/health/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		checks := map[string]string{}
		fatal, degraded := false, false
		for name, check := range required {
			if err := check(ctx); err != nil {
				fatal, checks[name] = true, "failed"
				slog.Error("readiness check failed", slog.String("dependency", name), slog.Any("error", err))
				continue
			}
			checks[name] = "ok"
		}
		for name, check := range optional {
			err := check(ctx)
			switch {
			case err == nil:
				checks[name] = "ok"
			case errors.Is(err, ErrNotConfigured):
				checks[name] = "not_configured"
			default:
				degraded, checks[name] = true, "failed"
				slog.Warn("optional dependency unavailable", slog.String("dependency", name), slog.Any("error", err))
			}
		}
		switch {
		case fatal:
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "checks": checks})
		case degraded:
			c.JSON(http.StatusOK, gin.H{"status": "degraded", "checks": checks})
		default:
			c.JSON(http.StatusOK, gin.H{"status": "ready", "checks": checks})
		}
	})
}

type handler struct {
	failed   FailedStore
	replayer Replayer
}

const maxConsumerLen = 128

// failedItem is the list/detail shape of one failed event. Payload is only on
// the detail view.
type failedItem struct {
	ID            string       `json:"id"`
	EventID       string       `json:"event_id"`
	Consumer      string       `json:"consumer"`
	EventType     string       `json:"event_type"`
	Topic         string       `json:"topic,omitempty"`
	AggregateType string       `json:"aggregate_type,omitempty"`
	AggregateID   string       `json:"aggregate_id,omitempty"`
	Status        string       `json:"status"`
	Attempts      int          `json:"attempts"`
	SchemaID      int32        `json:"schema_id,omitempty"`
	SchemaVersion int32        `json:"schema_version,omitempty"`
	OccurredAt    *time.Time   `json:"occurred_at,omitempty"`
	FirstFailedAt time.Time    `json:"first_failed_at"`
	LastFailedAt  time.Time    `json:"last_failed_at"`
	ReplayedAt    *time.Time   `json:"replayed_at,omitempty"`
	LastError     string       `json:"last_error,omitempty"`
	Errors        []errorEntry `json:"errors,omitempty"`
	Payload       any          `json:"payload,omitempty"`
	PayloadBase64 string       `json:"payload_base64,omitempty"`
}

type errorEntry struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Message string    `json:"message"`
}

func toItem(f mongostore.FailedEvent, detail bool) failedItem {
	it := failedItem{
		ID: f.ID, EventID: f.EventID, Consumer: f.Consumer, EventType: f.EventType, Topic: f.Topic,
		AggregateType: f.AggregateType, AggregateID: f.AggregateID, Status: string(f.Status), Attempts: f.Attempts,
		SchemaID: f.SchemaID, SchemaVersion: f.SchemaVersion,
		FirstFailedAt: f.FirstFailedAt, LastFailedAt: f.LastFailedAt, ReplayedAt: f.ReplayedAt,
	}
	if !f.OccurredAt.IsZero() {
		t := f.OccurredAt
		it.OccurredAt = &t
	}
	if n := len(f.Errors); n > 0 {
		it.LastError = f.Errors[n-1].Message
	}
	if detail {
		for _, e := range f.Errors {
			it.Errors = append(it.Errors, errorEntry{At: e.At, Attempt: e.Attempt, Message: e.Message})
		}
		switch {
		case len(f.Payload) == 0:
		case json.Valid(f.Payload):
			it.Payload = json.RawMessage(f.Payload)
		default:
			it.PayloadBase64 = base64.StdEncoding.EncodeToString(f.Payload)
		}
	}
	return it
}

// list backs GET /api/v1/admin/failed-events: newest first, keyset-paginated.
func (h *handler) list(c *gin.Context) {
	if h.failed == nil {
		unavailable(c, errNoStore)
		return
	}
	var filter mongostore.FailedFilter
	switch s := c.Query("status"); s {
	case "":
	case string(mongostore.StatusParked), string(mongostore.StatusReplayed):
		filter.Status = mongostore.Status(s)
	default:
		invalid(c, "status must be parked or replayed")
		return
	}
	if v := c.Query("consumer"); v != "" {
		if len(v) > maxConsumerLen || !utf8.ValidString(v) {
			invalid(c, "consumer is too long or not valid text")
			return
		}
		filter.Consumer = v
	}
	limit := 0 // httpx.ClampLimit turns "not given" into the default
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			invalid(c, "limit must be a positive integer")
			return
		}
		limit = n
	}
	cursor, ok := decodeCursor(c.Query("cursor"))
	if !ok {
		invalid(c, "cursor is not valid")
		return
	}
	page, err := h.failed.ListFailedEvents(c.Request.Context(), filter, cursor, httpx.ClampLimit(limit))
	if errors.Is(err, mongostore.ErrInvalid) {
		invalid(c, "cursor is not valid")
		return
	}
	if err != nil {
		unavailable(c, err)
		return
	}
	items := make([]failedItem, 0, len(page.Items))
	for _, f := range page.Items {
		items = append(items, toItem(f, false))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "next_cursor": encodeCursor(page.NextCursor)})
}

// get backs GET /api/v1/admin/failed-events/{event_id}: every record parked
// for the event (one per consumer), with payload and error history.
func (h *handler) get(c *gin.Context) {
	eventID, ok := eventIDParam(c)
	if !ok {
		return
	}
	if h.failed == nil {
		unavailable(c, errNoStore)
		return
	}
	ctx := c.Request.Context()
	var records []failedItem
	cursor := ""
	for {
		page, err := h.failed.ListFailedEvents(ctx, mongostore.FailedFilter{EventID: eventID}, cursor, mongostore.MaxPageSize)
		if err != nil {
			unavailable(c, err)
			return
		}
		for _, f := range page.Items {
			full, err := h.failed.GetFailedEvent(ctx, f.EventID, f.Consumer)
			if errors.Is(err, mongostore.ErrNotFound) {
				continue
			}
			if err != nil {
				unavailable(c, err)
				return
			}
			records = append(records, toItem(*full, true))
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(records) == 0 {
		httpx.Fail(c, apperrors.ErrNotFound)
		return
	}
	c.JSON(http.StatusOK, gin.H{"event_id": eventID, "records": records})
}

// replay backs POST /api/v1/admin/failed-events/{event_id}/replay. Replaying
// an already replayed event is a 200 that says so (replayed=false).
func (h *handler) replay(c *gin.Context) {
	eventID, ok := eventIDParam(c)
	if !ok {
		return
	}
	if h.replayer == nil {
		unavailable(c, errNoStore)
		return
	}
	res, err := h.replayer.Replay(c.Request.Context(), eventID)
	switch {
	case errors.Is(err, deadletter.ErrNotFound):
		httpx.Fail(c, apperrors.ErrNotFound)
		return
	case errors.Is(err, deadletter.ErrInvalidID):
		invalid(c, "event_id must be a UUID")
		return
	case err != nil:
		unavailable(c, err)
		return
	}
	outcomes := make([]gin.H, 0, len(res.Outcomes))
	for _, o := range res.Outcomes {
		m := gin.H{"consumer": o.Consumer, "status": o.Status, "applied": o.Applied}
		if o.Error != "" {
			m["error"] = o.Error
		}
		outcomes = append(outcomes, m)
	}
	if res.Failed() {
		e := *apperrors.New(http.StatusUnprocessableEntity, "replay_failed",
			"The event was re-applied but a consumer failed again; it stays parked with the new error.")
		e.Details = gin.H{"event_id": res.EventID, "outcomes": outcomes}
		slog.Warn("replay failed", slog.String("event_id", res.EventID))
		httpx.Fail(c, &e)
		return
	}
	c.JSON(http.StatusOK, gin.H{"event_id": res.EventID, "replayed": res.Replayed(), "outcomes": outcomes})
}

var errNoStore = errors.New("MongoDB is not available")

func eventIDParam(c *gin.Context) (string, bool) {
	id, err := uuid.Parse(c.Param("event_id"))
	if err != nil {
		invalid(c, "event_id must be a UUID")
		return "", false
	}
	return id.String(), true
}

func invalid(c *gin.Context, msg string) {
	e := *apperrors.ErrValidation
	e.Message = msg
	httpx.Fail(c, &e)
}

// unavailable answers 503 and logs the cause; the raw error (hosts, users) is
// never sent.
func unavailable(c *gin.Context, cause error) {
	slog.Error("admin: failed-events store unavailable", slog.String("path", c.FullPath()), slog.Any("error", cause))
	httpx.Fail(c, apperrors.ErrUnavailable.Wrap(fmt.Errorf("failed-events: %w", cause)))
}

// The page cursor is mongostore's keyset position (a document id), wrapped so
// the wire format stays opaque and can change.
func encodeCursor(next string) string {
	if next == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(next))
}

func decodeCursor(s string) (string, bool) {
	if s == "" {
		return "", true
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) == 0 || len(b) > 64 {
		return "", false
	}
	return string(b), true
}
