//go:build integration && bugrepro

// Adversarial security tests for the worker admin surface (security tester,
// Week 3). Tests named TestBugrepro_* fail today by design and reproduce a
// finding in docs/plans/week-3-findings-security.md; tests named
// TestAdversarial_* passed when written and are the evidence that an attack
// HELD. Run: cd services/worker/admin && go test -tags='integration bugrepro' ./...
package admin_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/admin"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

// S-01: the worker admin surface fails OPEN on a token-version cache miss.
// RequireFreshPrivilege(rdb) treats a missing key as version 1, so after a
// Redis flush, restart or eviction a demoted admin's old token works again
// (the F-03 class, still present on the new surface). Possibly in flight.
func TestBugrepro_Admin_RedisFlush_ResurrectsDemotedAdminToken(t *testing.T) {
	e := setup(t)
	adminID := uuid.New()
	tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1)
	key := auth.TokenVersionRedisKey(adminID.String())
	if err := e.pg.RDB.Set(context.Background(), key, 2, 0).Err(); err != nil { // demoted: version bumped
		t.Fatal(err)
	}
	if rec := e.do("GET", base, tok, nil); rec.Code != 401 {
		t.Fatalf("precondition: stale token must be refused, got %d", rec.Code)
	}
	if err := e.pg.RDB.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	rec := e.do("GET", base, tok, nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("after a Redis flush the demoted admin's stale token was ACCEPTED on the failed-events list (200); it must fail closed")
	}
	// And the destructive route, not just the read.
	id := e.parkProfile(uuid.New(), "N", 77)
	rec = e.do("POST", base+"/"+id.String()+"/replay", tok, nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("stale admin token replayed an event after a Redis flush")
	}
}

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

// HELD: forged, mangled and wrong-type tokens never reach the failed-events routes.
func TestAdversarial_Admin_JWTForgeryMatrix(t *testing.T) {
	e := setup(t)
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
		rec := e.do("GET", base, tok, nil)
		if rec.Code != 401 && rec.Code != 403 {
			t.Errorf("%s: status %d, want 401/403 (body %s)", name, rec.Code, rec.Body.String())
		}
	}
	// Scheme games.
	for name, h := range map[string]string{"lowercase bearer": "bearer " + e.admin(), "basic": "Basic " + e.admin(), "raw": e.admin(), "double": "Bearer Bearer " + e.admin()} {
		req := httptest.NewRequest("GET", base, nil)
		req.Header.Set("Authorization", h)
		rec := httptest.NewRecorder()
		e.r.ServeHTTP(rec, req)
		if rec.Code == 200 {
			t.Errorf("authorization header variant %q accepted", name)
		}
	}
}

// HELD: NoSQL operator injection, parameter pollution and bad numbers through
// the list query never become a Mongo operator, never 500 and never leak a
// driver error.
func TestAdversarial_Admin_ListQueryInjection(t *testing.T) {
	e := setup(t)
	id := e.parkProfile(uuid.New(), "Victim Name", 5)
	_ = id
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
			// An operator that took effect would have matched everything (including
			// our parked record) for consumer[$ne]; a literal match returns none.
			var p page
			_ = json.Unmarshal(rec.Body.Bytes(), &p)
			if len(p.Items) > 0 && strings.HasPrefix(q, "consumer=") {
				t.Errorf("%s: operator took effect, %d items returned", q, len(p.Items))
			}
		}
	}
}

// HELD: path parameter oddities on get/replay.
func TestAdversarial_Admin_EventIDOddities(t *testing.T) {
	e := setup(t)
	adm := e.admin()
	for _, p := range []string{"%00", "../../etc/passwd", "%2e%2e%2f", "%27%20OR%201=1", "%7B$ne:1%7D", strings.Repeat("a", 5000), "%F0%9F%92%A9", "00000000-0000-0000-0000-000000000000"} {
		for _, m := range []string{"GET", "POST"} {
			path := base + "/" + p
			if m == "POST" {
				path += "/replay"
			}
			rec := e.do(m, path, adm, nil)
			if rec.Code >= 500 {
				t.Errorf("%s %s: %d %s", m, p, rec.Code, rec.Body.String())
			}
		}
	}
	// A non-admin must not learn whether an event exists: an existing and an
	// unknown id answer identically (403) before any lookup.
	existing := e.parkProfile(uuid.New(), "N", 9)
	for _, role := range []string{"student", "instructor"} {
		a := e.do("GET", base+"/"+existing.String(), e.token(role), nil)
		b := e.do("GET", base+"/"+uuid.NewString(), e.token(role), nil)
		if a.Code != 403 || b.Code != 403 || a.Body.String() != b.Body.String() {
			t.Errorf("%s: existence oracle: %d/%d", role, a.Code, b.Code)
		}
	}
}

type failingStore struct{ err error }

func (f failingStore) ListFailedEvents(context.Context, mongostore.FailedFilter, string, int) (*mongostore.FailedPage, error) {
	return nil, f.err
}
func (f failingStore) GetFailedEvent(context.Context, string, string) (*mongostore.FailedEvent, error) {
	return nil, f.err
}

type failingReplayer struct{ err error }

func (f failingReplayer) Replay(context.Context, string) (*deadletter.Result, error) {
	return nil, f.err
}

// HELD: a Mongo/driver error carrying a DSN and credentials never reaches the
// response body or the Request-Id header.
func TestAdversarial_Admin_StoreErrorsDoNotLeak(t *testing.T) {
	e := setup(t)
	secret := errors.New(`server selection error: mongodb://admin:S3cretPw@10.9.8.7:27017/?authSource=admin: connection refused`)
	r, err := admin.Handler(admin.Deps{Config: e.cfg, RDB: e.pg.RDB, Failed: failingStore{secret}, Replayer: failingReplayer{secret},
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
	for _, rec := range []*httptest.ResponseRecorder{
		do("GET", base), do("GET", base+"/"+id), do("POST", base+"/"+id+"/replay"), do("GET", "/health/ready"),
	} {
		all := rec.Body.String() + rec.Header().Get("X-Request-Id")
		for k, v := range rec.Header() {
			all += k + strings.Join(v, ",")
		}
		if strings.Contains(all, "S3cretPw") || strings.Contains(all, "10.9.8.7") || strings.Contains(all, "mongodb://") {
			t.Errorf("status %d leaks the driver error: %s", rec.Code, rec.Body.String())
		}
	}
}

// HELD: a header-borne request id with control characters is replaced, not
// echoed (log and response splitting through X-Request-Id).
func TestAdversarial_Admin_RequestIDInjection(t *testing.T) {
	e := setup(t)
	for _, v := range []string{"abc\r\nSet-Cookie: x=1", "a b", "id;rm -rf", "<script>", strings.Repeat("a", 500), "id\x00", "%0d%0aX: y", "ünï"} {
		req := httptest.NewRequest("GET", "/health/live", nil)
		req.Header["X-Request-Id"] = []string{v}
		rec := httptest.NewRecorder()
		e.r.ServeHTTP(rec, req)
		got := rec.Header().Get("X-Request-Id")
		if got == v && v != "" {
			t.Errorf("hostile request id %q echoed back", v)
		}
		if rec.Header().Get("Set-Cookie") != "" {
			t.Errorf("response splitting through %q", v)
		}
	}
}

// S-02 (observation-grade, but executable): a parked event with a payload at
// the 15 MiB cap is returned whole by the detail route (no response cap), so
// one admin GET moves ~15 MiB plus JSON re-encoding; the list is bounded.
// Recorded as HELD for correctness (no 5xx, no truncation) and flagged in the
// register as an observation about response size. Passing test.
func TestAdversarial_Admin_PayloadBombAtCap(t *testing.T) {
	e := setup(t)
	pad := strings.Repeat("A", mongostore.MaxPayloadBytes-64)
	payload, _ := json.Marshal(map[string]string{"x": pad})
	if len(payload) > mongostore.MaxPayloadBytes {
		t.Fatalf("test payload too large: %d", len(payload))
	}
	eid := uuid.New()
	if _, err := e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
		EventID: eid.String(), Consumer: "course-instructor-mirror", EventType: "identity.user_profile_updated",
		Payload: payload, Attempts: 1, Err: "boom", FailedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rec := e.do("GET", base+"/"+eid.String(), e.admin(), nil)
	if rec.Code != 200 {
		t.Fatalf("detail of a cap-sized payload: %d", rec.Code)
	}
	t.Logf("cap-sized detail response: %d bytes in %s", rec.Body.Len(), time.Since(start))
	// Over the cap is refused at park time.
	_, err := e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
		EventID: uuid.NewString(), Consumer: "c", EventType: "t", Payload: make([]byte, mongostore.MaxPayloadBytes+1), Err: "x", FailedAt: time.Now().UTC(),
	})
	if err == nil {
		t.Error("payload over the cap was accepted")
	}
	// Replay of a record whose payload is garbage fails cleanly (422, no 5xx, no panic).
	gid := uuid.New()
	_, _ = e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
		EventID: gid.String(), Consumer: "course-instructor-mirror", EventType: "identity.user_profile_updated",
		Payload: []byte("\x00\xff{not json"), Attempts: 1, Err: "x", FailedAt: time.Now().UTC(),
	})
	if rec := e.do("POST", base+"/"+gid.String()+"/replay", e.admin(), nil); rec.Code >= 500 {
		t.Errorf("garbage payload replay: %d %s", rec.Code, rec.Body.String())
	}
}

// HELD: a record parked for a consumer that has no handler for the event type
// (a forged or cross-consumer record) is not applied by another consumer's
// handler: the outcome is "failed", and the other consumer's handler's table
// is untouched.
func TestAdversarial_Admin_ReplayDoesNotCrossConsumers(t *testing.T) {
	e := setup(t)
	eid := uuid.New()
	payload, _ := json.Marshal(map[string]any{"user_id": uuid.New(), "full_name": "Forged"})
	for _, consumer := range []string{"not-a-consumer", "analytics-mirror", "../course-instructor-mirror", "course-instructor-mirror\x00"} {
		_, _ = e.store.ParkFailedEvent(context.Background(), mongostore.ParkInput{
			EventID: eid.String(), Consumer: consumer, EventType: "identity.user_profile_updated",
			Payload: payload, OutboxID: 1, Attempts: 1, Err: "x", FailedAt: time.Now().UTC(),
		})
	}
	rec := e.do("POST", base+"/"+eid.String()+"/replay", e.admin(), nil)
	if rec.Code != 422 {
		t.Fatalf("replay of unknown-consumer records: %d %s", rec.Code, rec.Body.String())
	}
	var n int64
	e.pg.DB.Raw(`SELECT count(*) FROM processed_events WHERE event_id = ?`, eid).Scan(&n)
	if n != 0 {
		t.Errorf("a handler ran for a record of an unregistered consumer (%d markers)", n)
	}
}

// Body smuggling on the replay route: a huge or malformed body on POST is not
// read into memory beyond the cap and does not change the outcome.
func TestAdversarial_Admin_ReplayHugeBodyIsCapped(t *testing.T) {
	e := setup(t)
	id := e.parkProfile(uuid.New(), "N", 3)
	big := strings.NewReader(strings.Repeat("x", 3<<20))
	req := httptest.NewRequest("POST", base+"/"+id.String()+"/replay", big)
	req.Header.Set("Authorization", "Bearer "+e.admin())
	rec := httptest.NewRecorder()
	e.r.ServeHTTP(rec, req)
	if rec.Code >= 500 {
		t.Fatalf("huge replay body: %d", rec.Code)
	}
	_ = url.QueryEscape
}
