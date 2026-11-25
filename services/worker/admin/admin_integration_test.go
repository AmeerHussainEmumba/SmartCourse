//go:build integration

// The admin HTTP surface end to end: real MongoDB for failed_events, real
// Postgres for the replay's transaction and the course instructor-mirror
// handler it re-runs, real Redis for the token-version check. The matrix is
// who may call (401/403/stale), what each route answers (200/400/404/422/503)
// and that a replay changes a real table exactly once.
package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/admin"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

const base = "/api/v1/admin/failed-events"

type env struct {
	t     *testing.T
	pg    *testenv.Env
	mongo *testenv.MongoEnv
	store *mongostore.Store
	sink  *deadletter.Sink
	cfg   *config.Config
	r     *gin.Engine
}

func testConfig() *config.Config {
	return &config.Config{
		Env: "test", MaxBodyBytes: 1 << 20,
		JWT: config.JWTConfig{SigningKey: "test-signing-key-0123456789-abcdefghij", AccessTokenTTL: time.Hour, RefreshTokenTTL: time.Hour, Issuer: "smartcourse"},
	}
}

func setup(t *testing.T, handlers ...relay.Handler) *env { return setupWith(t, nil, handlers...) }

// setupWith is setup with a replacement for the audit store (nil keeps the real one).
func setupWith(t *testing.T, aud admin.Auditor, handlers ...relay.Handler) *env {
	t.Helper()
	e := &env{t: t, pg: testenv.Setup(t), mongo: testenv.SetupMongo(t), cfg: testConfig()}
	s, err := mongostore.Open(context.Background(), mongostore.Config{URI: e.mongo.URI, ServerSelectionTimeout: time.Second, OpTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	e.store, e.sink = s, deadletter.NewSink(s)
	if aud == nil {
		aud = s
	}
	if len(handlers) == 0 {
		handlers = course.Handlers()
	}
	e.r, err = admin.Handler(admin.Deps{
		Config: e.cfg, RDB: e.pg.RDB, Failed: s, Replayer: deadletter.NewReplayer(e.pg.DB, s, handlers), Auditor: aud,
		Required: map[string]router.Checker{"postgres": func(context.Context) error { return nil }, "redis": func(ctx context.Context) error { return e.pg.RDB.Ping(ctx).Err() }},
		Optional: map[string]router.Checker{"mongodb": s.Ping},
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// token issues a version-1 token. An admin's version is also written to Redis,
// as identity does at login: the admin routes fail closed on a cache miss.
func (e *env) token(role string) string {
	e.t.Helper()
	tok, _ := e.tokenFor(role)
	return tok
}

func (e *env) tokenFor(role string) (string, uuid.UUID) {
	e.t.Helper()
	id := uuid.New()
	tok, err := auth.IssueAccessToken(e.cfg.JWT, id, role, 1)
	if err != nil {
		e.t.Fatal(err)
	}
	if role == "admin" {
		if err := e.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(id.String()), 1, 0).Err(); err != nil {
			e.t.Fatal(err)
		}
	}
	return tok, id
}

func (e *env) do(method, path, token string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.r.ServeHTTP(rec, req)
	return rec
}

func (e *env) admin() string { return e.token("admin") }

func (e *env) parkProfile(userID uuid.UUID, name string, outboxID int64) uuid.UUID {
	e.t.Helper()
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "full_name": name})
	evt := relay.DecodedEvent{
		OutboxID: outboxID, EventID: uuid.New(), AggregateType: "user", AggregateID: userID,
		EventType: "identity.user_profile_updated", SchemaVersion: 1, Payload: payload, OccurredAt: time.Now().UTC(),
	}
	if err := e.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: "course-instructor-mirror", Err: errors.New("db down"), Attempts: 5}); err != nil {
		e.t.Fatal(err)
	}
	return evt.EventID
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details"`
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body.String())
	}
	if code != "" {
		if e := decode[apiError](t, rec); e.Code != code || e.Message == "" {
			t.Fatalf("error body = %+v, want code %q and a message", e, code)
		}
	}
}

func TestAdmin_Authorization(t *testing.T) {
	e := setup(t)
	id := e.parkProfile(uuid.New(), "N", 1)
	paths := []struct{ method, path string }{
		{"GET", base}, {"GET", base + "/" + id.String()}, {"POST", base + "/" + id.String() + "/replay"},
	}
	for _, p := range paths {
		wantStatus(t, e.do(p.method, p.path, "", nil), 401, "unauthorized")
		wantStatus(t, e.do(p.method, p.path, "garbage", nil), 401, "unauthorized")
		for _, role := range []string{"student", "instructor"} {
			wantStatus(t, e.do(p.method, p.path, e.token(role), nil), 403, "forbidden")
		}
	}
	// A student's replay attempt must not have replayed anything.
	if rec, _ := e.store.GetFailedEvent(context.Background(), id.String(), "course-instructor-mirror"); rec.Status != mongostore.StatusParked {
		t.Error("an unauthorised call changed the record")
	}
	wantStatus(t, e.do("GET", base, e.admin(), nil), 200, "")
}

// A demoted admin's old token carries a stale token version: RequireFreshPrivilege.
func TestAdmin_StaleAdminTokenIsRefused(t *testing.T) {
	e := setup(t)
	adminID := uuid.New()
	tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1)
	if err := e.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct{ m, path string }{{"GET", base}, {"POST", base + "/" + uuid.NewString() + "/replay"}} {
		wantStatus(t, e.do(p.m, p.path, tok, nil), 401, "token_stale")
	}
}

type item struct {
	ID        string `json:"id"`
	EventID   string `json:"event_id"`
	Consumer  string `json:"consumer"`
	EventType string `json:"event_type"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
	Payload   any    `json:"payload"`
}

type page struct {
	Items      []item `json:"items"`
	NextCursor string `json:"next_cursor"`
}

func TestAdmin_List_FiltersPaginationAndValidation(t *testing.T) {
	e := setup(t)
	var ids []uuid.UUID
	for range 5 {
		ids = append(ids, e.parkProfile(uuid.New(), "N", 1))
	}
	other := relay.DecodedEvent{EventID: uuid.New(), EventType: "enrollment.student_enrolled", Payload: []byte(`{}`), AggregateID: uuid.New()}
	if err := e.sink.Park(context.Background(), deadletter.Parked{Event: other, Consumer: "analytics", Err: errors.New("x")}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.MarkReplayed(context.Background(), ids[0].String(), "course-instructor-mirror", time.Now()); err != nil {
		t.Fatal(err)
	}

	all := decode[page](t, e.do("GET", base, e.admin(), nil))
	if len(all.Items) != 6 || all.NextCursor != "" {
		t.Fatalf("list = %d items, cursor %q; want all 6 and no cursor", len(all.Items), all.NextCursor)
	}
	if it := all.Items[0]; it.Payload != nil {
		t.Error("the list view must not carry payloads")
	}
	if n := len(decode[page](t, e.do("GET", base+"?status=parked", e.admin(), nil)).Items); n != 5 {
		t.Errorf("status=parked: %d items, want 5", n)
	}
	if n := len(decode[page](t, e.do("GET", base+"?status=replayed", e.admin(), nil)).Items); n != 1 {
		t.Errorf("status=replayed: %d items, want 1", n)
	}
	if n := len(decode[page](t, e.do("GET", base+"?consumer=analytics", e.admin(), nil)).Items); n != 1 {
		t.Errorf("consumer=analytics: %d items, want 1", n)
	}

	// Keyset walk, 2 per page: every item once, newest first, no cursor at the end.
	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		path := base + "?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		p := decode[page](t, e.do("GET", path, e.admin(), nil))
		for _, it := range p.Items {
			if seen[it.ID] {
				t.Fatalf("item %s repeated across pages", it.ID)
			}
			seen[it.ID] = true
		}
		pages++
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(seen) != 6 || pages != 3 {
		t.Errorf("walk saw %d items in %d pages, want 6 in 3", len(seen), pages)
	}

	for _, q := range []string{"?status=bogus", "?limit=abc", "?limit=0", "?limit=-3", "?cursor=!!!notbase64", "?cursor=bm90aGV4", "?consumer=" + strings.Repeat("x", 200)} {
		wantStatus(t, e.do("GET", base+q, e.admin(), nil), 400, "validation_failed")
	}
	if n := len(decode[page](t, e.do("GET", base+"?limit=100000", e.admin(), nil)).Items); n != 6 {
		t.Errorf("an oversized limit is clamped, not an error: got %d items", n)
	}
}

func TestAdmin_Detail(t *testing.T) {
	e := setup(t)
	id := e.parkProfile(uuid.New(), "Detail Name", 1)
	rec := e.do("GET", base+"/"+id.String(), e.admin(), nil)
	wantStatus(t, rec, 200, "")
	body := decode[struct {
		EventID string `json:"event_id"`
		Records []struct {
			Consumer string `json:"consumer"`
			Payload  struct {
				FullName string `json:"full_name"`
			} `json:"payload"`
			Errors []struct{ Message string } `json:"errors"`
		} `json:"records"`
	}](t, rec)
	if body.EventID != id.String() || len(body.Records) != 1 || body.Records[0].Payload.FullName != "Detail Name" ||
		len(body.Records[0].Errors) != 1 || body.Records[0].Errors[0].Message != "db down" {
		t.Errorf("detail = %+v", body)
	}
	wantStatus(t, e.do("GET", base+"/"+uuid.NewString(), e.admin(), nil), 404, "not_found")
	wantStatus(t, e.do("GET", base+"/not-a-uuid", e.admin(), nil), 400, "validation_failed")
}

type replayBody struct {
	EventID  string `json:"event_id"`
	Replayed bool   `json:"replayed"`
	Outcomes []struct {
		Consumer string `json:"consumer"`
		Status   string `json:"status"`
		Applied  bool   `json:"applied"`
	} `json:"outcomes"`
}

func TestAdmin_Replay_AppliesOnce_SecondCallSaysNoOp(t *testing.T) {
	e := setup(t)
	user := uuid.New()
	e.pg.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'Old Name', 0)`, user)
	id := e.parkProfile(user, "Replayed Via HTTP", 5)
	path := base + "/" + id.String() + "/replay"
	mirror := func() string {
		var n string
		e.pg.DB.Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, user).Row().Scan(&n)
		return n
	}

	rec := e.do("POST", path, e.admin(), nil)
	wantStatus(t, rec, 200, "")
	first := decode[replayBody](t, rec)
	if !first.Replayed || len(first.Outcomes) != 1 || first.Outcomes[0].Status != "replayed" || !first.Outcomes[0].Applied {
		t.Errorf("first replay = %+v", first)
	}
	if mirror() != "Replayed Via HTTP" {
		t.Fatalf("mirror = %q after replay", mirror())
	}

	e.pg.DB.Exec(`UPDATE instructor_mirror SET full_name = 'Edited Since', last_event_id = 0 WHERE user_id = ?`, user)
	rec = e.do("POST", path, e.admin(), nil)
	wantStatus(t, rec, 200, "")
	second := decode[replayBody](t, rec)
	if second.Replayed || second.Outcomes[0].Status != "already_replayed" || second.Outcomes[0].Applied {
		t.Errorf("second replay = %+v, want replayed=false, already_replayed", second)
	}
	if mirror() != "Edited Since" {
		t.Errorf("second replay re-applied the event: mirror = %q", mirror())
	}
	// The listing reflects it.
	if p := decode[page](t, e.do("GET", base+"?status=replayed", e.admin(), nil)); len(p.Items) != 1 || p.Items[0].EventID != id.String() {
		t.Errorf("replayed listing = %+v", p)
	}
}

func TestAdmin_Replay_NotFoundBadIDAndFailure(t *testing.T) {
	failing := relay.NewHandler("course-instructor-mirror", []string{"identity.user_profile_updated"},
		func(context.Context, *gorm.DB, relay.DecodedEvent) error { return errors.New("still broken") })
	e := setup(t, failing)
	wantStatus(t, e.do("POST", base+"/"+uuid.NewString()+"/replay", e.admin(), nil), 404, "not_found")
	wantStatus(t, e.do("POST", base+"/nope/replay", e.admin(), nil), 400, "validation_failed")

	id := e.parkProfile(uuid.New(), "N", 1)
	rec := e.do("POST", base+"/"+id.String()+"/replay", e.admin(), nil)
	wantStatus(t, rec, 422, "replay_failed")
	if d := decode[apiError](t, rec).Details; d == nil || !strings.Contains(rec.Body.String(), "still broken") {
		t.Errorf("failure details = %v, want the consumer outcome", d)
	}
	got := decode[struct {
		Records []struct {
			Status string `json:"status"`
			Errors []any  `json:"errors"`
		} `json:"records"`
	}](t, e.do("GET", base+"/"+id.String(), e.admin(), nil))
	if got.Records[0].Status != "parked" || len(got.Records[0].Errors) != 2 {
		t.Errorf("after a failed replay: %+v, want parked with 2 errors", got.Records[0])
	}
}

func TestAdmin_Hardening_BodyCapAndHeaders(t *testing.T) {
	e := setup(t)
	id := e.parkProfile(uuid.New(), "N", 1)
	big := bytes.Repeat([]byte("a"), 2<<20)
	wantStatus(t, e.do("POST", base+"/"+id.String()+"/replay", e.admin(), big), 413, "")
	rec := e.do("GET", base, e.admin(), nil)
	for h, want := range map[string]string{"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Cache-Control": "no-store"} {
		if rec.Header().Get(h) != want {
			t.Errorf("header %s = %q, want %q", h, rec.Header().Get(h), want)
		}
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("no request id on the response")
	}
	if rec := e.do("GET", base, "", nil); rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("admin must not allow any cross-origin caller")
	}
}

func TestAdmin_Health(t *testing.T) {
	e := setup(t)
	type health struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	wantStatus(t, e.do("GET", "/health/live", "", nil), 200, "")
	rec := e.do("GET", "/health/ready", "", nil)
	wantStatus(t, rec, 200, "")
	if h := decode[health](t, rec); h.Status != "ready" || h.Checks["mongodb"] != "ok" || h.Checks["postgres"] != "ok" {
		t.Errorf("ready = %+v", h)
	}

	// MongoDB down: degraded and reported, not a failure; live is unaffected.
	e.mongo.Stop(t)
	rec = e.do("GET", "/health/ready", "", nil)
	wantStatus(t, rec, 200, "")
	if h := decode[health](t, rec); h.Status != "degraded" || h.Checks["mongodb"] != "failed" || h.Checks["redis"] != "ok" {
		t.Errorf("ready with MongoDB down = %+v, want degraded", h)
	}
	if strings.Contains(rec.Body.String(), e.mongo.URI) {
		t.Error("the health response leaks the MongoDB URI")
	}

	// The failed-events routes answer 503, not 500, while MongoDB is down.
	wantStatus(t, e.do("GET", base, e.admin(), nil), 503, "temporarily_unavailable")
	wantStatus(t, e.do("GET", base+"/"+uuid.NewString(), e.admin(), nil), 503, "temporarily_unavailable")
	wantStatus(t, e.do("POST", base+"/"+uuid.NewString()+"/replay", e.admin(), nil), 503, "temporarily_unavailable")
}

// A required dependency failing is a 503 on readiness.
func TestAdmin_Health_RequiredDependencyDown(t *testing.T) {
	cfg := testConfig()
	r, err := admin.Handler(admin.Deps{
		Config:   cfg,
		Required: map[string]router.Checker{"postgres": func(context.Context) error { return errors.New("down: host=secret") }},
		Optional: map[string]router.Checker{"mongodb": func(context.Context) error { return admin.ErrNotConfigured }},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "secret") ||
		!strings.Contains(rec.Body.String(), `"mongodb":"not_configured"`) {
		t.Errorf("ready = %d %s", rec.Code, rec.Body.String())
	}
}
