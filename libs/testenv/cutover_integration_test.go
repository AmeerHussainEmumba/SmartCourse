//go:build integration

package testenv

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
)

// These tests live in package testenv (not testenv_test) because they need
// the unexported container and migrator helpers to stop BEFORE the cutover
// migration, seed the old platform tables, and then run it.

// migrateSteps applies n migrations of one set (negative n rolls back).
func migrateSteps(dsn, set string, n int) error {
	m, err := newMigrator(dsn, set)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Steps(n); err != nil {
		return fmt.Errorf("%s steps %d: %w", set, n, err)
	}
	return nil
}

func ownerDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	g, err := db.Open(dbConfigFor(dsn))
	if err != nil {
		t.Fatalf("open owner connection: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := g.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return g
}

func tableExists(t *testing.T, g *gorm.DB, qualified string) bool {
	t.Helper()
	var ok bool
	if err := g.Raw(`SELECT to_regclass(?) IS NOT NULL`, qualified).Row().Scan(&ok); err != nil {
		t.Fatalf("to_regclass(%s): %v", qualified, err)
	}
	return ok
}

func count(t *testing.T, g *gorm.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := g.Raw(q, args...).Row().Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

type seededEvent struct {
	ID        int64
	EventID   uuid.UUID
	Type      string
	Aggregate uuid.UUID
	Published bool
}

type outboxView struct {
	ID        int64
	EventID   uuid.UUID
	EventType string
	Published bool
	Attempts  int
	LastError *string
	TraceID   *string
	ActorID   *uuid.UUID
	Payload   []byte
}

func readOutbox(t *testing.T, g *gorm.DB, schema string) []outboxView {
	t.Helper()
	rows, err := g.Raw(`SELECT id, event_id, event_type, published_at IS NOT NULL, attempts, last_error, trace_id, actor_id, payload
		FROM ` + schema + `.outbox ORDER BY id`).Rows()
	if err != nil {
		t.Fatalf("read %s.outbox: %v", schema, err)
	}
	defer func() { _ = rows.Close() }()
	var out []outboxView
	for rows.Next() {
		var v outboxView
		if err := rows.Scan(&v.ID, &v.EventID, &v.EventType, &v.Published, &v.Attempts, &v.LastError, &v.TraceID, &v.ActorID, &v.Payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// TestCutoverMovesPlatformRowsToTheOwningService seeds the old shared platform
// tables, runs migrations/cutover, and proves every row landed in the right
// service with its id, order and event id intact and that platform is gone.
func TestCutoverMovesPlatformRowsToTheOwningService(t *testing.T) {
	pg := startPostgres(t)
	pg.provisionRoles(t)
	pre := migrationServices[:len(migrationServices)-1] // everything but cutover
	if got := migrationServices[len(migrationServices)-1]; got != "cutover" {
		t.Fatalf("cutover must be the last migration set, last is %q", got)
	}
	if err := applyMigrations(pg.dsn, pre...); err != nil {
		t.Fatalf("apply pre-cutover migrations: %v", err)
	}
	g := ownerDB(t, pg.dsn)

	// --- seed platform.outbox: interleaved identity / course / enrollment /
	// progress events, published and unpublished, with trace/actor/error data.
	types := []struct {
		typ       string
		published bool
	}{
		{"identity.user_registered", true},
		{"course.course_created", false},
		{"identity.role_changed", false},
		{"enrollment.enrolled", true},
		{"course.course_published", true},
		{"progress.lesson_completed", false},
		{"identity.user_registered", false},
		{"progress.course_completed", false},
		{"enrollment.enrolled", false},
		{"identity.status_changed", true},
	}
	var seeded []seededEvent
	actor := uuid.New()
	for i, e := range types {
		ev := seededEvent{EventID: uuid.New(), Type: e.typ, Aggregate: uuid.New(), Published: e.published}
		var published any
		if e.published {
			published = time.Now().UTC().Add(-time.Duration(i) * time.Minute)
		}
		var id int64
		err := g.Raw(`INSERT INTO platform.outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, headers,
				published_at, attempts, last_error, trace_id, span_id, actor_id)
			VALUES (?, 'agg', ?, ?, 1, ?, '{"k":"v"}', ?, ?, ?, ?, ?, ?) RETURNING id`,
			ev.EventID, ev.Aggregate, ev.Type, []byte{byte(i), 0xff}, published, i, fmt.Sprintf("err-%d", i),
			strings.Repeat("a", 32), strings.Repeat("b", 16), actor).Row().Scan(&id)
		if err != nil {
			t.Fatalf("seed outbox: %v", err)
		}
		ev.ID = id
		seeded = append(seeded, ev)
	}
	// Burn some ids so the old table has a gap: ids must be preserved, not renumbered.
	if err := g.Exec(`SELECT setval('platform.outbox_id_seq', 100)`).Error; err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"identity.user_registered", "course.course_updated"} {
		ev := seededEvent{EventID: uuid.New(), Type: typ, Aggregate: uuid.New()}
		if err := g.Raw(`INSERT INTO platform.outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload)
			VALUES (?, 'agg', ?, ?, 1, '\x00') RETURNING id`, ev.EventID, ev.Aggregate, ev.Type).Row().Scan(&ev.ID); err != nil {
			t.Fatal(err)
		}
		seeded = append(seeded, ev)
	}

	// --- seed platform.processed_events: several consumers.
	consumers := map[string]string{ // consumer -> expected service ("" = dropped)
		"analytics-aggregator":       "analytics",
		"notification-dispatcher":    "notification",
		"course-instructor-mirror":   "course",
		"certificate-issuer":         "course",
		"enrollment-course-archival": "course",
		"audit-archiver":             "",
		"some-future-consumer":       "course",
	}
	type pe struct {
		consumer string
		event    uuid.UUID
		at       time.Time
	}
	var processed []pe
	for c := range consumers {
		for i := 0; i < 3; i++ {
			p := pe{c, uuid.New(), time.Now().UTC().Add(-time.Duration(i+1) * time.Hour).Truncate(time.Microsecond)}
			if err := g.Exec(`INSERT INTO platform.processed_events (consumer_name, event_id, processed_at) VALUES (?, ?, ?)`, p.consumer, p.event, p.at).Error; err != nil {
				t.Fatal(err)
			}
			processed = append(processed, p)
		}
	}

	// --- seed platform.idempotency_keys.
	user := uuid.New()
	for i, state := range []string{"completed", "in_progress"} {
		if err := g.Exec(`INSERT INTO platform.idempotency_keys (key, user_id, endpoint, request_hash, response_status, response_body, state, expires_at)
			VALUES (?, ?, 'POST /v1/enrollments', ?, ?, '{"ok":true}', ?, now() + interval '1 day')`,
			fmt.Sprintf("key-%d", i), user, []byte{byte(i)}, 201, state).Error; err != nil {
			t.Fatal(err)
		}
	}

	// --- run the cutover.
	if err := applyMigrations(pg.dsn, "cutover"); err != nil {
		t.Fatalf("cutover: %v", err)
	}

	// platform is gone; the extensions stay in public.
	if n := count(t, g, `SELECT count(*) FROM pg_namespace WHERE nspname = 'platform'`); n != 0 {
		t.Fatal("platform schema still exists after the cutover")
	}
	for _, q := range []string{"platform.outbox", "platform.processed_events", "platform.idempotency_keys"} {
		if tableExists(t, g, q) {
			t.Fatalf("%s still exists", q)
		}
	}
	if n := count(t, g, `SELECT count(*) FROM pg_extension WHERE extname IN ('citext','pg_trgm')`); n != 2 {
		t.Fatalf("citext and pg_trgm must stay installed, found %d", n)
	}

	// outbox: routed by prefix, same ids, same order, same event ids, payload and metadata intact.
	wantIdentity, wantCourse := []outboxView{}, []outboxView{}
	for _, e := range seeded {
		if strings.HasPrefix(e.Type, "identity.") {
			wantIdentity = append(wantIdentity, outboxView{ID: e.ID, EventID: e.EventID, EventType: e.Type})
		} else {
			wantCourse = append(wantCourse, outboxView{ID: e.ID, EventID: e.EventID, EventType: e.Type})
		}
	}
	for schema, want := range map[string][]outboxView{"identity": wantIdentity, "course": wantCourse} {
		got := readOutbox(t, g, schema)
		if len(got) != len(want) {
			t.Fatalf("%s.outbox has %d rows, want %d", schema, len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID || got[i].EventID != want[i].EventID || got[i].EventType != want[i].EventType {
				t.Fatalf("%s.outbox[%d] = (%d %s %s), want (%d %s %s): id order or event id changed",
					schema, i, got[i].ID, got[i].EventID, got[i].EventType, want[i].ID, want[i].EventID, want[i].EventType)
			}
		}
		for i := 1; i < len(got); i++ {
			if got[i].ID <= got[i-1].ID {
				t.Fatalf("%s.outbox ids not increasing", schema)
			}
		}
	}
	// A moved row keeps its published flag, attempts, last_error, trace and actor, and payload.
	for _, e := range seeded[:len(types)] {
		schema := "course"
		if strings.HasPrefix(e.Type, "identity.") {
			schema = "identity"
		}
		var v outboxView
		var i int
		for i = range types {
			if seeded[i].ID == e.ID {
				break
			}
		}
		rows := readOutbox(t, g, schema)
		for _, r := range rows {
			if r.ID == e.ID {
				v = r
			}
		}
		if v.Published != e.Published || v.Attempts != i || v.LastError == nil || *v.LastError != fmt.Sprintf("err-%d", i) ||
			v.TraceID == nil || v.ActorID == nil || *v.ActorID != actor || !reflect.DeepEqual(v.Payload, []byte{byte(i), 0xff}) {
			t.Fatalf("row %d changed in the move: %+v", e.ID, v)
		}
	}
	unpublishedBefore := 0
	for _, e := range seeded[:len(types)] {
		if !e.Published {
			unpublishedBefore++
		}
	}
	unpublishedAfter := count(t, g, `SELECT count(*) FROM identity.outbox WHERE published_at IS NULL`) +
		count(t, g, `SELECT count(*) FROM course.outbox WHERE published_at IS NULL`)
	if unpublishedAfter != unpublishedBefore+2 { // the two extra rows seeded unpublished
		t.Fatalf("unpublished rows after cutover = %d, want %d", unpublishedAfter, unpublishedBefore+2)
	}

	// The id sequences continue after the highest moved id, never collide.
	for _, schema := range []string{"identity", "course"} {
		max := count(t, g, `SELECT max(id) FROM `+schema+`.outbox`)
		var next int
		if err := g.Raw(`INSERT INTO ` + schema + `.outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload)
			VALUES (gen_random_uuid(), 'agg', gen_random_uuid(), 'x.y', 1, '\x00') RETURNING id`).Row().Scan(&next); err != nil {
			t.Fatalf("insert after cutover into %s.outbox: %v", schema, err)
		}
		if next <= max {
			t.Fatalf("%s.outbox next id %d <= max moved id %d", schema, next, max)
		}
	}

	// processed_events: each consumer's rows are in exactly the right service.
	for c, svc := range consumers {
		for _, schema := range Services {
			if schema == "identity" {
				continue // identity has no processed_events table
			}
			got := count(t, g, `SELECT count(*) FROM `+schema+`.processed_events WHERE consumer_name = ?`, c)
			want := 0
			if schema == svc {
				want = 3
			}
			if got != want {
				t.Fatalf("consumer %s: %d rows in %s.processed_events, want %d", c, got, schema, want)
			}
		}
	}
	for _, p := range processed {
		svc := consumers[p.consumer]
		if svc == "" {
			continue
		}
		var at time.Time
		if err := g.Raw(`SELECT processed_at FROM `+svc+`.processed_events WHERE consumer_name = ? AND event_id = ?`, p.consumer, p.event).Row().Scan(&at); err != nil {
			t.Fatalf("processed event %s/%s missing in %s: %v", p.consumer, p.event, svc, err)
		}
		if !at.Equal(p.at) {
			t.Fatalf("processed_at changed for %s: %v != %v", p.event, at, p.at)
		}
	}

	// idempotency keys land in course, intact.
	if n := count(t, g, `SELECT count(*) FROM course.idempotency_keys WHERE user_id = ? AND endpoint = 'POST /v1/enrollments'`, user); n != 2 {
		t.Fatalf("course.idempotency_keys has %d seeded rows, want 2", n)
	}
	if n := count(t, g, `SELECT count(*) FROM course.idempotency_keys WHERE key = 'key-0' AND state = 'completed' AND response_status = 201 AND response_body->>'ok' = 'true'`); n != 1 {
		t.Fatal("idempotency key content changed in the move")
	}
}

// TestCutoverOnFreshDatabase: nothing seeded, nothing moved, platform gone.
func TestCutoverOnFreshDatabase(t *testing.T) {
	pg := startPostgres(t)
	pg.provisionRoles(t)
	if err := applyMigrations(pg.dsn, migrationServices...); err != nil {
		t.Fatalf("migrate fresh: %v", err)
	}
	g := ownerDB(t, pg.dsn)
	if n := count(t, g, `SELECT count(*) FROM pg_namespace WHERE nspname = 'platform'`); n != 0 {
		t.Fatal("platform schema exists on a fresh database after all migrations")
	}
	for _, q := range []string{"identity.outbox", "course.outbox", "course.processed_events", "course.idempotency_keys", "analytics.processed_events", "notification.processed_events"} {
		if !tableExists(t, g, q) {
			t.Fatalf("%s missing", q)
		}
		if n := count(t, g, `SELECT count(*) FROM `+q); n != 0 {
			t.Fatalf("%s has %d rows on a fresh database", q, n)
		}
	}
	// Re-running every set is a no-op.
	if err := applyMigrations(pg.dsn, migrationServices...); err != nil {
		t.Fatalf("second up: %v", err)
	}
}

// TestCutoverIsIdempotentAndHasADownPath: the cutover's down restores an EMPTY
// platform schema in the shape platform/000002+000003 leave (so platform's own
// down migrations still run), the per-service tables keep their rows, and
// running it again moves nothing; then every set rolls all the way down.
func TestCutoverIsIdempotentAndHasADownPath(t *testing.T) {
	pg := startPostgres(t)
	pg.provisionRoles(t)
	if err := applyMigrations(pg.dsn, migrationServices...); err != nil {
		t.Fatal(err)
	}
	g := ownerDB(t, pg.dsn)
	ev := uuid.New()
	if err := g.Exec(`INSERT INTO course.outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload)
		VALUES (?, 'agg', gen_random_uuid(), 'course.x', 1, '\x00')`, ev).Error; err != nil {
		t.Fatal(err)
	}

	if err := migrateSteps(pg.dsn, "cutover", -1); err != nil {
		t.Fatalf("cutover down: %v", err)
	}
	for _, q := range []string{"platform.outbox", "platform.processed_events", "platform.idempotency_keys"} {
		if !tableExists(t, g, q) {
			t.Fatalf("%s not restored by the cutover down", q)
		}
		if n := count(t, g, `SELECT count(*) FROM `+q); n != 0 {
			t.Fatalf("%s restored with %d rows; the down restores empty tables", q, n)
		}
	}
	if n := count(t, g, `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'platform' AND table_name = 'outbox'
		AND column_name IN ('trace_id','span_id','actor_id')`); n != 3 {
		t.Fatalf("restored platform.outbox lacks trace/actor columns (%d of 3)", n)
	}
	if n := count(t, g, `SELECT count(*) FROM course.outbox WHERE event_id = ?`, ev); n != 1 {
		t.Fatal("rows are not supposed to move back, but the course row vanished")
	}

	// Up again: moves nothing new (platform empty), drops platform again.
	if err := applyMigrations(pg.dsn, "cutover"); err != nil {
		t.Fatalf("cutover up again: %v", err)
	}
	if n := count(t, g, `SELECT count(*) FROM course.outbox`); n != 1 {
		t.Fatalf("course.outbox has %d rows after a second cutover, want 1", n)
	}

	// Full teardown in the reverse of the apply order (make migrate-down-all).
	for i := len(migrationServices) - 1; i >= 0; i-- {
		m, err := newMigrator(pg.dsn, migrationServices[i])
		if err != nil {
			t.Fatal(err)
		}
		err = m.Down()
		_, _ = m.Close()
		if err != nil {
			t.Fatalf("down %s: %v", migrationServices[i], err)
		}
	}
	if n := count(t, g, `SELECT count(*) FROM pg_namespace WHERE nspname = 'platform'`); n != 0 {
		t.Fatal("platform schema left behind")
	}
	if n := count(t, g, `SELECT count(*) FROM pg_tables WHERE schemaname IN ('platform','identity','course','analytics','notification')`); n != 0 {
		t.Fatalf("%d tables left after a full teardown", n)
	}
}

// TestNewTableMigrationsHaveWorkingDownPaths rolls each new per-service
// migration down and up again.
func TestNewTableMigrationsHaveWorkingDownPaths(t *testing.T) {
	pg := startPostgres(t)
	pg.provisionRoles(t)
	if err := applyMigrations(pg.dsn, migrationServices...); err != nil {
		t.Fatal(err)
	}
	g := ownerDB(t, pg.dsn)
	cases := []struct {
		set    string
		tables []string
		steps  int // migrations from the one that creates the tables to the set's latest
	}{
		{"identity", []string{"identity.outbox"}, 1},
		// course/000009 (idempotency_keys.response_raw and response_headers) sits on top of 000008.
		{"course", []string{"course.outbox", "course.processed_events", "course.idempotency_keys"}, 2},
		{"analytics", []string{"analytics.processed_events"}, 1},
		{"notification", []string{"notification.processed_events"}, 1},
	}
	for _, c := range cases {
		t.Run(c.set, func(t *testing.T) {
			if err := migrateSteps(pg.dsn, c.set, -c.steps); err != nil {
				t.Fatalf("down: %v", err)
			}
			for _, q := range c.tables {
				if tableExists(t, g, q) {
					t.Fatalf("%s still exists after down", q)
				}
			}
			if err := migrateSteps(pg.dsn, c.set, c.steps); err != nil {
				t.Fatalf("up again: %v", err)
			}
			for _, q := range c.tables {
				if !tableExists(t, g, q) {
					t.Fatalf("%s missing after up", q)
				}
			}
		})
	}
}
