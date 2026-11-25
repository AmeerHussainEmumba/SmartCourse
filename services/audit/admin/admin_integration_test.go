//go:build integration

// The admin HTTP surface end to end: real MongoDB for failed_events and
// audit_log, real Redis for the token-version check. The producer is a
// recording stand-in here (the real Kafka path is in replay_kafka_integration_test.go
// at the module root). The matrix is who may call (401/403/stale), what each
// route answers (200/400/404/422/503) and that a replay produces, marks and
// audits exactly as designed.
//
// One Postgres+Redis and one MongoDB container serve all subtests (each subtest
// gets its own Mongo database); the outage test starts its own MongoDB.
package admin_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/admin"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/deadletter"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/replay"
)

const (
	base     = "/api/v1/admin/failed-events"
	consumer = topics.ConsumerCourseInstructorMirror
)

type shared struct {
	pg    *testenv.Env
	mongo *testenv.MongoEnv
}

type recProducer struct {
	mu   sync.Mutex
	err  error
	sent []string // "consumer/event id"
}

func (p *recProducer) PublishRetry(_ context.Context, c string, _ uint32, evt relay.DecodedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.sent = append(p.sent, c+"/"+evt.EventID.String())
	return nil
}

func (p *recProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sent)
}

type env struct {
	t     *testing.T
	sh    *shared
	uri   string
	db    string
	store *mongostore.Store
	sink  *deadletter.Sink
	cfg   *config.Config
	prod  *recProducer
	r     *gin.Engine
}

func testConfig() *config.Config {
	return &config.Config{
		Env: "test", MaxBodyBytes: 1 << 20,
		JWT: config.JWTConfig{SigningKey: "test-signing-key-0123456789-abcdefghij", AccessTokenTTL: time.Hour, RefreshTokenTTL: time.Hour, Issuer: "smartcourse"},
	}
}

func newEnv(t *testing.T, sh *shared, aud admin.Auditor) *env {
	t.Helper()
	return newEnvOn(t, sh, sh.mongo.URI, aud)
}

func newEnvOn(t *testing.T, sh *shared, uri string, aud admin.Auditor) *env {
	t.Helper()
	e := &env{t: t, sh: sh, uri: uri, db: "t" + strings.ReplaceAll(uuid.NewString(), "-", ""), cfg: testConfig(), prod: &recProducer{}}
	s, err := mongostore.Open(context.Background(), mongostore.Config{URI: uri, Database: e.db, ServerSelectionTimeout: time.Second, OpTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	e.store, e.sink = s, deadletter.NewSink(s)
	if aud == nil {
		aud = s
	}
	e.r, err = admin.Handler(admin.Deps{
		Config: e.cfg, RDB: sh.pg.RDB, Failed: s, Replayer: replay.New(s, e.prod), Auditor: aud,
		Required: map[string]router.Checker{"redis": func(ctx context.Context) error { return sh.pg.RDB.Ping(ctx).Err() }},
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
		if err := e.sh.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(id.String()), 1, 0).Err(); err != nil {
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

// parkProfile parks a valid, replayable record (schema id and version stored).
func (e *env) parkProfile(userID uuid.UUID, name string, outboxID int64) uuid.UUID {
	e.t.Helper()
	return e.parkFor(consumer, userID, name, outboxID, 7)
}

func (e *env) parkFor(c string, userID uuid.UUID, name string, outboxID int64, schemaID int32) uuid.UUID {
	e.t.Helper()
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "full_name": name})
	evt := relay.DecodedEvent{
		OutboxID: outboxID, EventID: uuid.New(), AggregateType: "user", AggregateID: userID,
		EventType: "identity.user_profile_updated", SchemaVersion: 1, Payload: payload, OccurredAt: time.Now().UTC(),
	}
	if err := e.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: c, SchemaID: schemaID, Topic: topics.Identity, Err: errors.New("db down"), Attempts: 5}); err != nil {
		e.t.Fatal(err)
	}
	return evt.EventID
}

func (e *env) status(id uuid.UUID, c string) mongostore.Status {
	e.t.Helper()
	rec, err := e.store.GetFailedEvent(context.Background(), id.String(), c)
	if err != nil {
		e.t.Fatal(err)
	}
	return rec.Status
}

func (e *env) auditDocs(filter bson.D) []bson.M {
	e.t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(e.uri))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	cur, err := c.Database(e.db).Collection(mongostore.CollAuditLog).Find(context.Background(), filter)
	if err != nil {
		e.t.Fatal(err)
	}
	var docs []bson.M
	if err := cur.All(context.Background(), &docs); err != nil {
		e.t.Fatal(err)
	}
	return docs
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

func TestAdmin(t *testing.T) {
	sh := &shared{pg: testenv.Setup(t), mongo: testenv.SetupMongo(t)}
	run := func(name string, f func(t *testing.T, e *env)) {
		t.Run(name, func(t *testing.T) { f(t, newEnv(t, sh, nil)) })
	}

	run("authorization matrix", func(t *testing.T, e *env) {
		id := e.parkProfile(uuid.New(), "N", 1)
		for _, p := range []struct{ method, path string }{
			{"GET", base}, {"GET", base + "/" + id.String()}, {"POST", base + "/" + id.String() + "/replay"},
		} {
			wantStatus(t, e.do(p.method, p.path, "", nil), 401, "unauthorized")
			wantStatus(t, e.do(p.method, p.path, "garbage", nil), 401, "unauthorized")
			for _, role := range []string{"student", "instructor"} {
				wantStatus(t, e.do(p.method, p.path, e.token(role), nil), 403, "forbidden")
			}
		}
		if e.status(id, consumer) != mongostore.StatusParked || e.prod.count() != 0 {
			t.Error("an unauthorised call replayed or produced something")
		}
		wantStatus(t, e.do("GET", base, e.admin(), nil), 200, "")
	})

	run("stale admin token is refused", func(t *testing.T, e *env) {
		adminID := uuid.New()
		tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1)
		if err := sh.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 2, 0).Err(); err != nil {
			t.Fatal(err)
		}
		for _, p := range []struct{ m, path string }{{"GET", base}, {"POST", base + "/" + uuid.NewString() + "/replay"}} {
			wantStatus(t, e.do(p.m, p.path, tok, nil), 401, "token_stale")
		}
	})

	// The E10 defect: after a Redis flush a version-1 token of an admin who was
	// demoted (database version 2) has no cached version to compare with. A miss
	// must refuse, not read as version 1.
	run("token-version cache miss fails closed", func(t *testing.T, e *env) {
		adminID := uuid.New()
		tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1) // nothing cached for this admin
		for _, p := range []struct{ m, path string }{{"GET", base}, {"GET", base + "/" + uuid.NewString()}, {"POST", base + "/" + uuid.NewString() + "/replay"}} {
			wantStatus(t, e.do(p.m, p.path, tok, nil), 401, "token_stale")
		}
		if err := sh.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 1, 0).Err(); err != nil {
			t.Fatal(err)
		}
		wantStatus(t, e.do("GET", base, tok, nil), 200, "")
	})

	// S-01 (adversarial pass): a Redis flush must not resurrect a demoted admin.
	run("redis flush does not resurrect a demoted admin", func(t *testing.T, e *env) {
		adminID := uuid.New()
		tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1)
		key := auth.TokenVersionRedisKey(adminID.String())
		if err := sh.pg.RDB.Set(context.Background(), key, 2, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if rec := e.do("GET", base, tok, nil); rec.Code != 401 {
			t.Fatalf("precondition: stale token must be refused, got %d", rec.Code)
		}
		if err := sh.pg.RDB.FlushAll(context.Background()).Err(); err != nil {
			t.Fatal(err)
		}
		if rec := e.do("GET", base, tok, nil); rec.Code == http.StatusOK {
			t.Fatal("after a Redis flush the demoted admin's stale token was ACCEPTED on the list")
		}
		id := e.parkProfile(uuid.New(), "N", 77)
		if rec := e.do("POST", base+"/"+id.String()+"/replay", tok, nil); rec.Code == http.StatusOK {
			t.Fatal("stale admin token replayed an event after a Redis flush")
		}
		if e.prod.count() != 0 {
			t.Fatal("a refused replay produced a record")
		}
	})

	run("list: filters, pagination, validation", func(t *testing.T, e *env) {
		var ids []uuid.UUID
		for range 5 {
			ids = append(ids, e.parkProfile(uuid.New(), "N", 1))
		}
		e.parkFor(topics.ConsumerAnalyticsAggregator, uuid.New(), "A", 1, 7)
		if _, err := e.store.MarkReplayed(context.Background(), ids[0].String(), consumer, time.Now()); err != nil {
			t.Fatal(err)
		}
		type item struct {
			ID       string `json:"id"`
			EventID  string `json:"event_id"`
			Payload  any    `json:"payload"`
			Consumer string `json:"consumer"`
		}
		type page struct {
			Items      []item `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		get := func(q string) page { return decode[page](t, e.do("GET", base+q, e.admin(), nil)) }
		all := get("")
		if len(all.Items) != 6 || all.NextCursor != "" {
			t.Fatalf("list = %d items, cursor %q; want all 6 and no cursor", len(all.Items), all.NextCursor)
		}
		if all.Items[0].Payload != nil {
			t.Error("the list view must not carry payloads")
		}
		for q, want := range map[string]int{"?status=parked": 5, "?status=replayed": 1, "?consumer=" + topics.ConsumerAnalyticsAggregator: 1} {
			if n := len(get(q).Items); n != want {
				t.Errorf("%s: %d items, want %d", q, n, want)
			}
		}
		seen := map[string]bool{}
		cursor, pages := "", 0
		for {
			path := "?limit=2"
			if cursor != "" {
				path += "&cursor=" + cursor
			}
			p := get(path)
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
		if n := len(get("?limit=100000").Items); n != 6 {
			t.Errorf("an oversized limit is clamped, not an error: got %d items", n)
		}
	})

	run("detail, with the consumer filter", func(t *testing.T, e *env) {
		user := uuid.New()
		id := e.parkProfile(user, "Detail Name", 1)
		// A second consumer parked the same event.
		var evt relay.DecodedEvent
		rec0, _ := e.store.GetFailedEvent(context.Background(), id.String(), consumer)
		evt = relay.DecodedEvent{EventID: id, AggregateType: "user", AggregateID: user, EventType: rec0.EventType, SchemaVersion: 1, Payload: rec0.Payload, OccurredAt: time.Now().UTC()}
		if err := e.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: topics.ConsumerAnalyticsAggregator, SchemaID: 7, Err: errors.New("other")}); err != nil {
			t.Fatal(err)
		}
		type detail struct {
			EventID string `json:"event_id"`
			Records []struct {
				Consumer string `json:"consumer"`
				Payload  struct {
					FullName string `json:"full_name"`
				} `json:"payload"`
				Errors []struct{ Message string } `json:"errors"`
			} `json:"records"`
		}
		body := decode[detail](t, e.do("GET", base+"/"+id.String(), e.admin(), nil))
		if body.EventID != id.String() || len(body.Records) != 2 {
			t.Fatalf("detail = %+v, want both consumers' records", body)
		}
		one := decode[detail](t, e.do("GET", base+"/"+id.String()+"?consumer="+consumer, e.admin(), nil))
		if len(one.Records) != 1 || one.Records[0].Consumer != consumer || one.Records[0].Payload.FullName != "Detail Name" ||
			len(one.Records[0].Errors) != 1 || one.Records[0].Errors[0].Message != "db down" {
			t.Errorf("filtered detail = %+v", one)
		}
		wantStatus(t, e.do("GET", base+"/"+id.String()+"?consumer=nobody", e.admin(), nil), 404, "not_found")
		wantStatus(t, e.do("GET", base+"/"+uuid.NewString(), e.admin(), nil), 404, "not_found")
		wantStatus(t, e.do("GET", base+"/not-a-uuid", e.admin(), nil), 400, "validation_failed")
	})

	type replayBody struct {
		EventID  string `json:"event_id"`
		Replayed bool   `json:"replayed"`
		Outcomes []struct {
			Consumer   string `json:"consumer"`
			Status     string `json:"status"`
			RetryTopic string `json:"retry_topic"`
			Error      string `json:"error"`
		} `json:"outcomes"`
	}

	run("replay produces once, second call says no-op, is audited per attempt", func(t *testing.T, e *env) {
		tok, adminID := e.tokenFor("admin")
		id := e.parkProfile(uuid.New(), "Replayed", 5)
		path := base + "/" + id.String() + "/replay"

		rec := e.do("POST", path, tok, nil)
		wantStatus(t, rec, 200, "")
		first := decode[replayBody](t, rec)
		if !first.Replayed || len(first.Outcomes) != 1 || first.Outcomes[0].Status != "replayed" || first.Outcomes[0].RetryTopic != topics.Retry(consumer) {
			t.Errorf("first replay = %+v", first)
		}
		if e.prod.count() != 1 || e.status(id, consumer) != mongostore.StatusReplayed {
			t.Fatalf("produced %d, status %s: want 1 and replayed", e.prod.count(), e.status(id, consumer))
		}
		rec = e.do("POST", path, tok, nil)
		wantStatus(t, rec, 200, "")
		second := decode[replayBody](t, rec)
		if second.Replayed || second.Outcomes[0].Status != "already_replayed" {
			t.Errorf("second replay = %+v, want replayed=false, already_replayed", second)
		}
		if e.prod.count() != 1 {
			t.Errorf("the second replay produced again (%d)", e.prod.count())
		}

		docs := e.auditDocs(bson.D{{Key: "action", Value: admin.ActionReplayed}, {Key: "resource_id", Value: id.String()}})
		if len(docs) != 2 {
			t.Fatalf("%d audit entries for two replay attempts, want 2: %v", len(docs), docs)
		}
		outcomes := map[string]int{}
		for _, d := range docs {
			if d["actor_id"] != adminID.String() || d["actor_role"] != "admin" || d["resource_type"] != "failed_event" || d["at"] == nil {
				t.Errorf("entry %v: want actor %s (admin), resource_type failed_event and a timestamp", d, adminID)
			}
			after := asMap(d["after"])
			if after["consumer"] != consumer {
				t.Errorf("entry %v: consumer = %v", d, after["consumer"])
			}
			outcomes[after["outcome"].(string)]++
		}
		if outcomes["replayed"] != 1 || outcomes["already_replayed"] != 1 {
			t.Errorf("outcomes = %v, want one replayed and one already_replayed", outcomes)
		}
		// The listing reflects it.
		type page struct {
			Items []struct {
				EventID string `json:"event_id"`
			} `json:"items"`
		}
		if p := decode[page](t, e.do("GET", base+"?status=replayed", e.admin(), nil)); len(p.Items) != 1 || p.Items[0].EventID != id.String() {
			t.Errorf("replayed listing = %+v", p)
		}
	})

	run("replay: not found, bad id and not-found writes no audit entry", func(t *testing.T, e *env) {
		tok := e.admin()
		wantStatus(t, e.do("POST", base+"/"+uuid.NewString()+"/replay", tok, nil), 404, "not_found")
		wantStatus(t, e.do("POST", base+"/nope/replay", tok, nil), 400, "validation_failed")
		if n := len(e.auditDocs(bson.D{{Key: "action", Value: admin.ActionReplayed}})); n != 0 {
			t.Errorf("%d replay audit entries for not-found and bad-id calls, want 0", n)
		}
	})

	// B-01 over HTTP: an undecodable record is refused with a clear 422, stays
	// parked, produces nothing, and the refusal is audited.
	run("replay refuses an undecodable record", func(t *testing.T, e *env) {
		tok, adminID := e.tokenFor("admin")
		garbage := uuid.New()
		if _, err := e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
			EventID: garbage.String(), Consumer: consumer, EventType: "identity.user_profile_updated", SchemaID: 7, SchemaVersion: 1,
			AggregateID: uuid.NewString(), Payload: []byte("\x00\xff{not json"), Attempts: 1, Err: "x", FailedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		rec := e.do("POST", base+"/"+garbage.String()+"/replay", tok, nil)
		wantStatus(t, rec, 422, "replay_refused")
		if !strings.Contains(rec.Body.String(), "refused") {
			t.Errorf("body does not say refused: %s", rec.Body.String())
		}
		if e.prod.count() != 0 || e.status(garbage, consumer) != mongostore.StatusParked {
			t.Fatal("an undecodable record was produced or marked")
		}
		docs := e.auditDocs(bson.D{{Key: "resource_id", Value: garbage.String()}})
		if len(docs) != 1 || docs[0]["actor_id"] != adminID.String() || asMap(docs[0]["after"])["outcome"] != "refused" {
			t.Errorf("audit = %v, want one refused entry by %s", docs, adminID)
		}
		// A record parked without a schema id (the old format) is refused too.
		old := e.parkFor(consumer, uuid.New(), "Old", 1, 0)
		wantStatus(t, e.do("POST", base+"/"+old.String()+"/replay", tok, nil), 422, "replay_refused")
	})

	run("replay: produce failure is 503 without detail and leaves the record parked", func(t *testing.T, e *env) {
		e.prod.err = errors.New("kafka: dial tcp 10.9.8.7:9092: connection refused (password=S3cret)")
		tok := e.admin()
		id := e.parkProfile(uuid.New(), "N", 1)
		rec := e.do("POST", base+"/"+id.String()+"/replay", tok, nil)
		wantStatus(t, rec, 503, "temporarily_unavailable")
		if s := rec.Body.String(); strings.Contains(s, "10.9.8.7") || strings.Contains(s, "S3cret") || strings.Contains(s, "kafka") {
			t.Errorf("503 leaks the cause: %s", s)
		}
		if e.status(id, consumer) != mongostore.StatusParked {
			t.Fatal("a failed produce marked the record replayed")
		}
		// Broker back: the same record replays.
		e.prod.err = nil
		wantStatus(t, e.do("POST", base+"/"+id.String()+"/replay", tok, nil), 200, "")
		if e.status(id, consumer) != mongostore.StatusReplayed || e.prod.count() != 1 {
			t.Fatal("the retry after the outage did not replay")
		}
		outs := []string{}
		for _, d := range e.auditDocs(bson.D{{Key: "resource_id", Value: id.String()}}) {
			outs = append(outs, asMap(d["after"])["outcome"].(string))
		}
		if len(outs) != 2 {
			t.Errorf("audit outcomes %v, want produce_failed then replayed", outs)
		}
	})

	// The replay already happened: a failure to write its audit entry must not
	// turn the response into an error, but it is counted (and logged at error level).
	t.Run("audit failure does not undo the replay", func(t *testing.T) {
		aud := &failingAuditor{}
		e := newEnv(t, sh, aud)
		id := e.parkProfile(uuid.New(), "Replayed", 5)
		before := admin.AuditFailures()
		rec := e.do("POST", base+"/"+id.String()+"/replay", e.admin(), nil)
		wantStatus(t, rec, 200, "")
		if body := decode[replayBody](t, rec); !body.Replayed {
			t.Errorf("replay = %+v, want replayed", body)
		}
		if e.status(id, consumer) != mongostore.StatusReplayed || e.prod.count() != 1 {
			t.Error("the replay was undone by the audit failure")
		}
		if aud.calls != 1 || admin.AuditFailures() != before+1 {
			t.Errorf("auditor calls %d, audit failures %d (was %d): want 1 call and the failure counted", aud.calls, admin.AuditFailures(), before)
		}
	})

	run("concurrent HTTP replays mark once", func(t *testing.T, e *env) {
		tok := e.admin()
		id := e.parkProfile(uuid.New(), "N", 1)
		const n = 6
		var wg sync.WaitGroup
		codes := make(chan replayBody, n)
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rec := e.do("POST", base+"/"+id.String()+"/replay", tok, nil)
				if rec.Code != 200 {
					t.Errorf("status %d: %s", rec.Code, rec.Body.String())
					return
				}
				codes <- decode[replayBody](t, rec)
			}()
		}
		wg.Wait()
		close(codes)
		replayed := 0
		for b := range codes {
			if b.Replayed {
				replayed++
			}
		}
		if replayed != 1 {
			t.Fatalf("%d callers reported replayed, want exactly 1", replayed)
		}
		if c := e.prod.count(); c < 1 || c > n {
			t.Fatalf("produced %d, want between 1 and %d", c, n)
		}
	})

	run("hardening: body cap and headers", func(t *testing.T, e *env) {
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
			t.Error("the admin API must not allow any cross-origin caller")
		}
		// /metrics is on the internal listener only.
		if rec := e.do("GET", "/metrics", e.admin(), nil); rec.Code != 404 {
			t.Errorf("/metrics on the API port answered %d, want 404", rec.Code)
		}
	})

	run("health: live, ready", func(t *testing.T, e *env) {
		type health struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		wantStatus(t, e.do("GET", "/health/live", "", nil), 200, "")
		rec := e.do("GET", "/health/ready", "", nil)
		wantStatus(t, rec, 200, "")
		if h := decode[health](t, rec); h.Status != "ready" || h.Checks["mongodb"] != "ok" || h.Checks["redis"] != "ok" {
			t.Errorf("ready = %+v", h)
		}
	})

	t.Run("MongoDB down: 503 on the routes, degraded readiness, no crash", func(t *testing.T) {
		own := testenv.SetupMongo(t)
		e := newEnvOn(t, sh, own.URI, nil)
		id := e.parkProfile(uuid.New(), "N", 1)
		own.Stop(t)
		type health struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		rec := e.do("GET", "/health/ready", "", nil)
		wantStatus(t, rec, 200, "")
		if h := decode[health](t, rec); h.Status != "degraded" || h.Checks["mongodb"] != "failed" || h.Checks["redis"] != "ok" {
			t.Errorf("ready with MongoDB down = %+v, want degraded", h)
		}
		if strings.Contains(rec.Body.String(), own.URI) {
			t.Error("the health response leaks the MongoDB URI")
		}
		wantStatus(t, e.do("GET", "/health/live", "", nil), 200, "")
		wantStatus(t, e.do("GET", base, e.admin(), nil), 503, "temporarily_unavailable")
		wantStatus(t, e.do("GET", base+"/"+id.String(), e.admin(), nil), 503, "temporarily_unavailable")
		wantStatus(t, e.do("POST", base+"/"+id.String()+"/replay", e.admin(), nil), 503, "temporarily_unavailable")
		if e.prod.count() != 0 {
			t.Error("a replay produced while MongoDB was down")
		}
	})

	t.Run("required dependency down is 503 on readiness", func(t *testing.T) {
		r, err := admin.Handler(admin.Deps{
			Config:   testConfig(),
			Required: map[string]router.Checker{"redis": func(context.Context) error { return errors.New("down: host=secret") }},
			Optional: map[string]router.Checker{"mongodb": func(context.Context) error { return errors.New("nope") }},
		})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/health/ready", nil))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "secret") {
			t.Errorf("ready = %d %s", rec.Code, rec.Body.String())
		}
	})

	adversarial(t, sh, run)
}

type failingAuditor struct {
	mu    sync.Mutex
	calls int
}

func (f *failingAuditor) AppendAudit(context.Context, mongostore.AuditEntry) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return "", errors.New("mongo down")
}

// asMap reads a nested document, which the driver returns as bson.D or bson.M.
func asMap(v any) bson.M {
	switch m := v.(type) {
	case bson.M:
		return m
	case bson.D:
		out := bson.M{}
		for _, e := range m {
			out[e.Key] = e.Value
		}
		return out
	}
	return bson.M{}
}

// ---- the adversarial reproducers that still make sense on this service ----

func forge(t *testing.T, method jwt.SigningMethod, key any, mut func(*auth.Claims)) string {
	t.Helper()
	c := auth.Claims{Role: "admin", TokenVersion: 1, RegisteredClaims: jwt.RegisteredClaims{
		Subject: uuid.NewString(), Issuer: "smartcourse", IssuedAt: jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	if mut != nil {
		mut(&c)
	}
	s, err := jwt.NewWithClaims(method, c).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type failingStore struct{ err error }

func (f failingStore) ListFailedEvents(context.Context, mongostore.FailedFilter, string, int) (*mongostore.FailedPage, error) {
	return nil, f.err
}
func (f failingStore) GetFailedEvent(context.Context, string, string) (*mongostore.FailedEvent, error) {
	return nil, f.err
}

type failingReplayer struct{ err error }

func (f failingReplayer) Replay(context.Context, string) (*replay.Result, error) { return nil, f.err }

func adversarial(t *testing.T, sh *shared, run func(string, func(*testing.T, *env))) {
	// HELD: forged, mangled and wrong-type tokens never reach the failed-events routes.
	run("adversarial: JWT forgery matrix", func(t *testing.T, e *env) {
		good := []byte(e.cfg.JWT.SigningKey)
		otherKey := []byte("a-completely-different-signing-key-0123456789")
		cases := map[string]string{
			"alg none":       forge(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, nil),
			"wrong key":      forge(t, jwt.SigningMethodHS256, otherKey, nil),
			"wrong issuer":   forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Issuer = "evil" }),
			"empty issuer":   forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Issuer = "" }),
			"expired":        forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) }),
			"no expiry":      forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.ExpiresAt = nil }),
			"non-uuid sub":   forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Subject = "admin" }),
			"empty sub":      forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Subject = "" }),
			"role Admin":     forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Role = "Admin" }),
			"role admin+sp":  forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Role = "admin " }),
			"role empty":     forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Role = "" }),
			"role superuser": forge(t, jwt.SigningMethodHS256, good, func(c *auth.Claims) { c.Role = "superuser" }),
		}
		refresh, _, err := auth.GenerateOpaqueToken()
		if err != nil {
			t.Fatal(err)
		}
		cases["opaque refresh token as access"] = refresh
		cases["refresh hash"] = base64.RawURLEncoding.EncodeToString(auth.HashOpaqueToken(refresh))
		cases["empty"] = ""
		cases["truncated"] = forge(t, jwt.SigningMethodHS256, good, nil)[:40]
		for name, tok := range cases {
			if rec := e.do("GET", base, tok, nil); rec.Code != 401 && rec.Code != 403 {
				t.Errorf("%s: status %d, want 401/403 (body %s)", name, rec.Code, rec.Body.String())
			}
		}
		for name, h := range map[string]string{"lowercase bearer": "bearer " + e.admin(), "basic": "Basic " + e.admin(), "raw": e.admin(), "double": "Bearer Bearer " + e.admin()} {
			req := httptest.NewRequest("GET", base, nil)
			req.Header.Set("Authorization", h)
			rec := httptest.NewRecorder()
			e.r.ServeHTTP(rec, req)
			if rec.Code == 200 {
				t.Errorf("authorization header variant %q accepted", name)
			}
		}
	})

	// HELD: NoSQL operator injection, parameter pollution and bad numbers through
	// the list query never become a Mongo operator, never 500 and never leak a
	// driver error.
	run("adversarial: list query injection", func(t *testing.T, e *env) {
		e.parkProfile(uuid.New(), "Victim Name", 5)
		adm := e.admin()
		queries := []string{
			`consumer={"$ne":null}`, `consumer[$ne]=x`, `consumer[$regex]=.*`, `consumer[$gt]=`, `consumer=%24ne`,
			`status[$ne]=replayed`, `status=parked&status=replayed`, `status=parked%00`, `status=PARKED`, `status={"$ne":"x"}`,
			`limit=-1`, `limit=0`, `limit=99999999999999999999`, `limit=1e3`, `limit=0x10`, `limit=%20%205`, `limit=5&limit=500`, `limit[$gt]=1`,
			`limit=2147483648`, `limit=9223372036854775807`,
			`cursor=` + base64.RawURLEncoding.EncodeToString([]byte(`{"$gt":""}`)),
			`cursor=` + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("0", 24))),
			`cursor=` + base64.RawURLEncoding.EncodeToString([]byte("ffffffffffffffffffffffff")),
			`cursor=` + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("a", 200))),
			`cursor=%00`, `cursor=....`, `cursor[$ne]=`, `consumer=` + strings.Repeat("a", 129), `consumer=%ff%fe`,
			`consumer=%00`, `unknown=1&$where=1`,
		}
		for _, q := range queries {
			rec := e.do("GET", base+"?"+q, adm, nil)
			body := rec.Body.String()
			if rec.Code >= 500 {
				t.Errorf("%s: status %d (body %s)", q, rec.Code, body)
			}
			for _, leak := range []string{"mongo", "bson", "ObjectID", "BSON", "mongodb://", "goroutine", ".go:"} {
				if strings.Contains(body, leak) {
					t.Errorf("%s: body leaks %q: %s", q, leak, body)
				}
			}
			if rec.Code == 200 && strings.HasPrefix(q, "consumer=") && strings.Contains(q, "ne") {
				var p struct {
					Items []any `json:"items"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &p)
				if len(p.Items) > 0 {
					t.Errorf("%s: operator took effect, %d items returned", q, len(p.Items))
				}
			}
		}
	})

	// HELD: path parameter oddities on get/replay, and no existence oracle for non-admins.
	run("adversarial: event id oddities", func(t *testing.T, e *env) {
		adm := e.admin()
		for _, p := range []string{"%00", "../../etc/passwd", "%2e%2e%2f", "%27%20OR%201=1", "%7B$ne:1%7D", strings.Repeat("a", 5000), "%F0%9F%92%A9", "00000000-0000-0000-0000-000000000000"} {
			for _, m := range []string{"GET", "POST"} {
				path := base + "/" + p
				if m == "POST" {
					path += "/replay"
				}
				if rec := e.do(m, path, adm, nil); rec.Code >= 500 {
					t.Errorf("%s %s: %d %s", m, p, rec.Code, rec.Body.String())
				}
			}
		}
		existing := e.parkProfile(uuid.New(), "N", 9)
		for _, role := range []string{"student", "instructor"} {
			a := e.do("GET", base+"/"+existing.String(), e.token(role), nil)
			b := e.do("GET", base+"/"+uuid.NewString(), e.token(role), nil)
			if a.Code != 403 || b.Code != 403 || a.Body.String() != b.Body.String() {
				t.Errorf("%s: existence oracle: %d/%d", role, a.Code, b.Code)
			}
		}
	})

	// HELD: a Mongo/driver error carrying a DSN and credentials never reaches the
	// response body or the headers.
	run("adversarial: store errors do not leak", func(t *testing.T, e *env) {
		secret := errors.New(`server selection error: mongodb://admin:S3cretPw@10.9.8.7:27017/?authSource=admin: connection refused`)
		r, err := admin.Handler(admin.Deps{Config: e.cfg, RDB: sh.pg.RDB, Failed: failingStore{secret}, Replayer: failingReplayer{secret},
			Required: map[string]router.Checker{"redis": func(context.Context) error { return secret }},
			Optional: map[string]router.Checker{"mongodb": func(context.Context) error { return secret }}})
		if err != nil {
			t.Fatal(err)
		}
		do := func(method, path string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, nil)
			req.Header.Set("Authorization", "Bearer "+e.admin())
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			return rec
		}
		id := uuid.NewString()
		for _, rec := range []*httptest.ResponseRecorder{do("GET", base), do("GET", base+"/"+id), do("POST", base+"/"+id+"/replay"), do("GET", "/health/ready")} {
			all := rec.Body.String()
			for k, v := range rec.Header() {
				all += k + strings.Join(v, ",")
			}
			if strings.Contains(all, "S3cretPw") || strings.Contains(all, "10.9.8.7") || strings.Contains(all, "mongodb://") {
				t.Errorf("status %d leaks the driver error: %s", rec.Code, rec.Body.String())
			}
		}
	})

	// HELD: a header-borne request id with control characters is replaced, not echoed.
	run("adversarial: request id injection", func(t *testing.T, e *env) {
		for _, v := range []string{"abc\r\nSet-Cookie: x=1", "a b", "id;rm -rf", "<script>", strings.Repeat("a", 500), "id\x00", "%0d%0aX: y", "ünï"} {
			req := httptest.NewRequest("GET", "/health/live", nil)
			req.Header["X-Request-Id"] = []string{v}
			rec := httptest.NewRecorder()
			e.r.ServeHTTP(rec, req)
			if got := rec.Header().Get("X-Request-Id"); got == v && v != "" {
				t.Errorf("hostile request id %q echoed back", v)
			}
			if rec.Header().Get("Set-Cookie") != "" {
				t.Errorf("response splitting through %q", v)
			}
		}
	})

	// A parked event with a payload at the 15 MiB cap is returned whole by the
	// detail route; over the cap is refused at park time; a garbage payload
	// replays cleanly as a refusal (no 5xx, no panic).
	run("adversarial: payload cap", func(t *testing.T, e *env) {
		pad := strings.Repeat("A", mongostore.MaxPayloadBytes-64)
		payload, _ := json.Marshal(map[string]string{"x": pad})
		eid := uuid.New()
		if _, err := e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
			EventID: eid.String(), Consumer: consumer, EventType: "identity.user_profile_updated",
			Payload: payload, Attempts: 1, Err: "boom", FailedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		if rec := e.do("GET", base+"/"+eid.String(), e.admin(), nil); rec.Code != 200 {
			t.Fatalf("detail of a cap-sized payload: %d", rec.Code)
		}
		if _, err := e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
			EventID: uuid.NewString(), Consumer: "c", EventType: "t", Payload: make([]byte, mongostore.MaxPayloadBytes+1), Err: "x", FailedAt: time.Now().UTC(),
		}); err == nil {
			t.Error("payload over the cap was accepted")
		}
		if rec := e.do("POST", base+"/"+eid.String()+"/replay", e.admin(), nil); rec.Code >= 500 {
			t.Errorf("cap-sized payload replay: %d", rec.Code)
		}
	})

	// HELD (re-shaped for republish): a record parked for a consumer nobody reads
	// (forged, cross-consumer or path-like) is refused; nothing is produced to a
	// topic derived from the hostile name, and the record stays parked.
	run("adversarial: replay does not follow forged consumer names", func(t *testing.T, e *env) {
		eid := uuid.New()
		payload, _ := json.Marshal(map[string]any{"user_id": uuid.New(), "full_name": "Forged"})
		for _, c := range []string{"not-a-consumer", "../course-instructor-mirror", "course-instructor-mirror\x00", "retry.x"} {
			_, _ = e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
				EventID: eid.String(), Consumer: c, EventType: "identity.user_profile_updated", AggregateID: uuid.NewString(),
				SchemaID: 7, SchemaVersion: 1, Payload: payload, OutboxID: 1, Attempts: 1, Err: "x", FailedAt: time.Now().UTC(),
			})
		}
		rec := e.do("POST", base+"/"+eid.String()+"/replay", e.admin(), nil)
		if rec.Code != 422 {
			t.Fatalf("replay of unknown-consumer records: %d %s", rec.Code, rec.Body.String())
		}
		if e.prod.count() != 0 {
			t.Errorf("%d records produced for forged consumer names", e.prod.count())
		}
	})

	// A huge body on POST is not read beyond the cap and does not change the outcome.
	run("adversarial: replay huge body is capped", func(t *testing.T, e *env) {
		id := e.parkProfile(uuid.New(), "N", 3)
		req := httptest.NewRequest("POST", base+"/"+id.String()+"/replay", strings.NewReader(strings.Repeat("x", 3<<20)))
		req.Header.Set("Authorization", "Bearer "+e.admin())
		rec := httptest.NewRecorder()
		e.r.ServeHTTP(rec, req)
		if rec.Code >= 500 {
			t.Fatalf("huge replay body: %d", rec.Code)
		}
		_ = url.QueryEscape
		_ = fmt.Sprint
	})
}
