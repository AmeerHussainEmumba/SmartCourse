//go:build integration

// Adversarial analytics tests (ADR-0017 / ADR-0021 / ADR-0024): replaying the
// same events must not change any counter; reordered or duplicated delivery
// of the events that are meant to commute must converge to the same state;
// counters must never go negative or inconsistent.
package analytics_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// advEvent is one logical event with a fixed identity, so redelivery of the
// same advEvent reuses the same EventID and OutboxID (what at-least-once
// delivery really does).
type advEvent struct {
	outboxID int64
	eventID  uuid.UUID
	typ      string
	payload  map[string]any
}

func ev(id int64, typ string, payload map[string]any) advEvent {
	return advEvent{outboxID: id, eventID: uuid.New(), typ: typ, payload: payload}
}

// deliver runs one delivery of e through relay.Deliver, the path the
// analytics-worker's Kafka consumers use: the handler and the
// analytics.processed_events marker commit in one transaction, and an event id
// already marked is skipped.
func deliver(env *testenv.Env, e advEvent) error {
	raw, _ := json.Marshal(e.payload)
	for _, h := range analytics.Handlers() {
		if !h.EventTypes[e.typ] {
			continue
		}
		evt := relay.DecodedEvent{OutboxID: e.outboxID, EventID: e.eventID, EventType: e.typ, Payload: raw, OccurredAt: time.Now()}
		if err := relay.Deliver(context.Background(), env.DB, h, evt); err != nil {
			return err
		}
	}
	return nil
}

// deliverWithRetry mimics the relay's retry loop for events that may fail
// until a prerequisite event has been applied.
func deliverWithRetry(t *testing.T, env *testenv.Env, pending []advEvent) {
	t.Helper()
	for round := 0; round < 10 && len(pending) > 0; round++ {
		var next []advEvent
		for _, e := range pending {
			if err := deliver(env, e); err != nil {
				next = append(next, e)
			}
		}
		pending = next
	}
	if len(pending) != 0 {
		t.Fatalf("%d events never became deliverable", len(pending))
	}
}

// stateDump returns a deterministic textual dump of every analytics table,
// without timestamps that legitimately change on re-application.
func stateDump(t *testing.T, env *testenv.Env, includeDaily bool) string {
	t.Helper()
	queries := []string{
		`SELECT 'role', user_id, role, last_event_id FROM user_role_mirror ORDER BY user_id`,
		`SELECT 'cm', course_id, published FROM course_mirror ORDER BY course_id`,
		`SELECT 'em', enrollment_id, course_id, student_id, status, enrolled_at, completed_at FROM enrollment_mirror ORDER BY enrollment_id`,
		`SELECT 'cs', course_id, total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s FROM course_stats ORDER BY course_id`,
		`SELECT 'ps', total_students, total_instructors, total_courses_published, avg_courses_per_student FROM platform_snapshot`,
	}
	if includeDaily {
		queries = append(queries, `SELECT 'de', day, course_id, count FROM daily_enrollments ORDER BY day, course_id`)
	}
	var b strings.Builder
	for _, q := range queries {
		rows, err := env.DB.Raw(q).Rows()
		if err != nil {
			t.Fatalf("dump %q: %v", q, err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			b.WriteString(fmt.Sprint(vals...) + "\n")
		}
		rows.Close()
	}
	return b.String()
}

func assertCountersSane(t *testing.T, env *testenv.Env) {
	t.Helper()
	var bad int64
	if err := env.DB.Raw(`SELECT
		(SELECT count(*) FROM platform_snapshot WHERE total_students < 0 OR total_instructors < 0 OR total_courses_published < 0 OR avg_courses_per_student < 0)
	  + (SELECT count(*) FROM course_stats WHERE total_enrollments < 0 OR active_enrollments < 0 OR completions < 0
	        OR active_enrollments + completions <> total_enrollments OR completion_rate < 0 OR completion_rate > 1)
	  + (SELECT count(*) FROM daily_enrollments WHERE count < 0)`).Row().Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d rows violate counter sanity (negative, or active+completions != total, or rate outside [0,1])", bad)
	}
}

func scenario() []advEvent {
	course := uuid.New()
	u1, u2, u3 := uuid.New(), uuid.New(), uuid.New()
	e1, e2, e3 := uuid.New(), uuid.New(), uuid.New()
	s1, s2, s3 := uuid.New(), uuid.New(), uuid.New()
	enrolledAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	return []advEvent{
		ev(1, "identity.user_registered", map[string]any{"user_id": u1, "role": "student"}),
		ev(2, "identity.user_registered", map[string]any{"user_id": u2, "role": "student"}),
		ev(3, "identity.user_registered", map[string]any{"user_id": u3, "role": "student"}),
		ev(4, "identity.user_role_changed", map[string]any{"user_id": u3, "role": "instructor"}),
		ev(5, "course.course_published", map[string]any{"course_id": course}),
		ev(6, "enrollment.student_enrolled", map[string]any{"enrollment_id": e1, "student_id": s1, "course_id": course, "enrolled_at": enrolledAt}),
		ev(7, "enrollment.student_enrolled", map[string]any{"enrollment_id": e2, "student_id": s2, "course_id": course, "enrolled_at": enrolledAt}),
		ev(8, "enrollment.student_enrolled", map[string]any{"enrollment_id": e3, "student_id": s3, "course_id": course, "enrolled_at": enrolledAt}),
		ev(9, "progress.course_completed", map[string]any{"enrollment_id": e1, "student_id": s1, "completed_at": enrolledAt.Add(time.Hour)}),
	}
}

func truncateAnalytics(t *testing.T, env *testenv.Env) {
	t.Helper()
	// TRUNCATE is DDL-class: the service role cannot, the owner can (qualified names).
	if err := env.Owner.Exec(`TRUNCATE analytics.user_role_mirror, analytics.course_mirror, analytics.enrollment_mirror, analytics.course_stats, analytics.daily_enrollments, analytics.platform_snapshot, analytics.processed_events`).Error; err != nil {
		t.Fatal(err)
	}
}

// Replaying the whole event history through the consumer path (what a retry
// topic replay or a Kafka redelivery does) N times must change nothing, both
// while processed_events remembers the events (exact), and after PruneProcessed
// has forgotten them: then the mirrors still converge but the incremental
// daily_enrollments counter is applied again (a known gap: the marker is what
// makes that handler idempotent, hence the PROCESSED_EVENTS_RETENTION window),
// and the reconciler, which recomputes every aggregate from the mirrors, must
// restore the exact state.
func TestAdvAnalytics_ReplayThroughConsumerPathIsIdempotent(t *testing.T) {
	env := setup(t)
	events := scenario() // one set of event ids: a replay redelivers the same events
	feedAll := func() {
		t.Helper()
		for _, e := range events {
			if err := deliver(env, e); err != nil {
				t.Fatalf("deliver %s: %v", e.typ, err)
			}
		}
	}
	feedAll()
	want := stateDump(t, env, true)
	if !strings.Contains(want, "ps") || !strings.Contains(want, "em") {
		t.Fatalf("scenario produced no state:\n%s", want)
	}
	assertCountersSane(t, env)

	for i := 0; i < 3; i++ {
		feedAll() // markers present: every delivery is a duplicate
		if got := stateDump(t, env, true); got != want {
			t.Fatalf("replay #%d changed analytics state\n--- before\n%s\n--- after\n%s", i+1, want, got)
		}
	}
	for i := 0; i < 2; i++ {
		if err := env.Owner.Exec(`DELETE FROM analytics.processed_events`).Error; err != nil {
			t.Fatal(err)
		}
		feedAll() // markers gone (pruned): the handlers run again
		if err := analytics.NewReconciler(env.DB, nil).Run(context.Background()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		assertCountersSane(t, env)
		if got := stateDump(t, env, true); got != want {
			t.Fatalf("replay #%d after the markers were pruned and a reconcile changed analytics state\n--- before\n%s\n--- after\n%s", i+1, want, got)
		}
	}
}

// The events that are supposed to commute (registered/role_changed with the
// last_event_id guard, published, enrolled, completed-after-enrolled) must
// converge to the same state under shuffling and N-fold duplication.
func TestAdvAnalytics_ShuffledAndDuplicatedDeliveryConverges(t *testing.T) {
	env := setup(t)
	events := scenario()

	// Canonical: in-order, once.
	for _, e := range events {
		if err := deliver(env, e); err != nil {
			t.Fatalf("canonical delivery of %s: %v", e.typ, err)
		}
	}
	want := stateDump(t, env, true)
	assertCountersSane(t, env)

	for seed := int64(1); seed <= 6; seed++ {
		truncateAnalytics(t, env)
		rng := rand.New(rand.NewSource(seed))
		var stream []advEvent
		for _, e := range events {
			for c := 1 + rng.Intn(3); c > 0; c-- { // 1..3 copies, same EventID
				stream = append(stream, e)
			}
		}
		rng.Shuffle(len(stream), func(i, j int) { stream[i], stream[j] = stream[j], stream[i] })
		deliverWithRetry(t, env, stream)
		assertCountersSane(t, env)
		if got := stateDump(t, env, true); got != want {
			t.Fatalf("seed %d: state diverged from canonical\n--- want\n%s\n--- got\n%s", seed, want, got)
		}
	}
}

func TestAdvAnalytics_UserRoleEvents_OutOfOrderAndDuplicated(t *testing.T) {
	env := setup(t)
	uid := uuid.New()
	registered := ev(10, "identity.user_registered", map[string]any{"user_id": uid, "role": "student"})
	promoted := ev(20, "identity.user_role_changed", map[string]any{"user_id": uid, "role": "instructor"})
	demoted := ev(30, "identity.user_role_changed", map[string]any{"user_id": uid, "role": "student"})

	orders := [][]advEvent{
		{demoted, promoted, registered},
		{promoted, registered, demoted, registered, promoted},
		{registered, demoted, promoted, demoted},
	}
	for i, order := range orders {
		truncateAnalytics(t, env)
		for _, e := range order {
			if err := deliver(env, e); err != nil {
				t.Fatal(err)
			}
		}
		var role string
		env.DB.Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, uid).Row().Scan(&role)
		if role != "student" { // highest outbox id (30) is the demotion
			t.Fatalf("order %d: final role %q, want student (event with highest outbox id wins)", i, role)
		}
		var students, instructors int64
		env.DB.Raw(`SELECT total_students, total_instructors FROM platform_snapshot`).Row().Scan(&students, &instructors)
		if students != 1 || instructors != 0 {
			t.Fatalf("order %d: snapshot students=%d instructors=%d", i, students, instructors)
		}
	}

	// Raw events straight from the user_role_changed-before-user_registered
	// case: the registration (older id) must not overwrite the newer role.
	truncateAnalytics(t, env)
	_ = deliver(env, advEvent{outboxID: 50, eventID: uuid.New(), typ: "identity.user_role_changed", payload: map[string]any{"user_id": uid, "role": "instructor"}})
	_ = deliver(env, advEvent{outboxID: 40, eventID: uuid.New(), typ: "identity.user_registered", payload: map[string]any{"user_id": uid, "role": "student"}})
	var role string
	env.DB.Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, uid).Row().Scan(&role)
	if role != "instructor" {
		t.Fatalf("late registration overwrote a newer role change: %q", role)
	}
}

// course.course_completed for an enrollment the mirror has not seen yet must
// fail loudly (so the relay retries), write nothing, and succeed once the
// enrollment arrives.
func TestAdvAnalytics_CompletedBeforeEnrolled_FailsThenSucceedsAfterEnrolled(t *testing.T) {
	env := setup(t)
	course, enr, stu := uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	completed := ev(2, "progress.course_completed", map[string]any{"enrollment_id": enr, "student_id": stu, "completed_at": at.Add(time.Minute)})
	enrolled := ev(1, "enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": at})

	if err := deliver(env, completed); err == nil {
		t.Fatal("completion for an unknown enrollment must return an error so the relay retries")
	}
	var n int64
	env.DB.Raw(`SELECT (SELECT count(*) FROM enrollment_mirror) + (SELECT count(*) FROM course_stats) + (SELECT count(*) FROM processed_events)`).Row().Scan(&n)
	if n != 0 {
		t.Fatalf("failed delivery left %d rows behind (not atomic)", n)
	}
	if err := deliver(env, enrolled); err != nil {
		t.Fatal(err)
	}
	if err := deliver(env, completed); err != nil {
		t.Fatal(err)
	}
	var completions, total int64
	env.DB.Raw(`SELECT completions, total_enrollments FROM course_stats WHERE course_id = ?`, course).Row().Scan(&completions, &total)
	if completions != 1 || total != 1 {
		t.Fatalf("completions=%d total=%d", completions, total)
	}
	assertCountersSane(t, env)
}

// Malformed payloads must error (retry then give up), never silently zero a
// counter or panic.
func TestAdvAnalytics_MalformedAndHostilePayloads(t *testing.T) {
	env := setup(t)
	for _, typ := range []string{"identity.user_registered", "course.course_published", "enrollment.student_enrolled", "enrollment.student_withdrawn", "progress.course_completed"} {
		for _, raw := range []string{`not json`, `[]`, `{"user_id": 5}`, `{"course_id":"nope"}`} {
			for _, h := range analytics.Handlers() {
				if !h.EventTypes[typ] {
					continue
				}
				err := env.DB.Transaction(func(tx *gorm.DB) error {
					return h.Handle(context.Background(), tx, relay.DecodedEvent{OutboxID: 1, EventID: uuid.New(), EventType: typ, Payload: []byte(raw)})
				})
				if err == nil && (raw == `not json` || raw == `[]`) {
					t.Errorf("%s accepted payload %q", typ, raw)
				}
			}
		}
	}
	// Nothing malformed may have produced rows with the nil UUID except via {} style payloads.
	var neg int64
	env.DB.Raw(`SELECT count(*) FROM course_stats WHERE total_enrollments < 0`).Row().Scan(&neg)
	if neg != 0 {
		t.Fatal("negative counters")
	}
}
