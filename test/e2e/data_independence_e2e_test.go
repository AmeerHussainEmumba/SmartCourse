//go:build e2e

package e2e_test

import (
	"sort"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestE2E_DataIndependence_RolesAndOutboxes checks the data-independence
// boundary of docs/plans/microservices-split.md sections 1 and 3 through the
// real stack. Each binary connects as its own Postgres role, so a probe that
// reads another schema through a service's own connection cannot be made from
// outside; the assertions are Postgres catalog queries run as the database
// owner, plus where the events of real requests landed.
//
// Assertions:
//   - Every service role (svc_identity, svc_course, svc_analytics,
//     svc_notification) has no USAGE or CREATE on, and no SELECT, INSERT,
//     UPDATE or DELETE on any table of, any other service's schema; and does
//     have SELECT and INSERT on its own tables (so the check is not vacuous).
//   - The running binaries really connect as those roles: each role has live
//     connections, which only a service binary of that service holds.
//   - The `platform` schema no longer exists, and outbox, processed_events and
//     idempotency_keys exist only in the schemas of the service that owns each
//     (identity.outbox, course.outbox, course.processed_events,
//     course.idempotency_keys, analytics.processed_events,
//     notification.processed_events); no table outside the service schemas
//     has an event_id column.
//   - A registration's outbox row (identity.user_registered) is in
//     identity.outbox and in no other table; an enrollment's
//     (enrollment.student_enrolled) is in course.outbox and in no other
//     table; the consumers' markers are in the consuming service's own
//     processed_events, not in the producer's.
func TestE2E_DataIndependence_RolesAndOutboxes(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	student := s.NewStudent(t)
	course := s.PublishedCourse(t, inst, 1)
	enrollment := s.Enroll(t, student, course)
	s.WaitSettled(t, time.Minute)

	schemas := []string{"identity", "course", "analytics", "notification"}

	t.Run("roles have no privileges on the other schemas", func(t *testing.T) {
		for _, svc := range schemas {
			role := "svc_" + svc
			for _, other := range schemas {
				if other == svc {
					continue
				}
				for _, priv := range []string{"USAGE", "CREATE"} {
					if n := s.MustCount(t, `SELECT count(*) FROM pg_namespace n WHERE n.nspname = ? AND has_schema_privilege(?::name, n.oid, ?::text)`, other, role, priv); n != 0 {
						t.Errorf("%s has %s on schema %s", role, priv, other)
					}
				}
				var leaks []string
				if err := s.DB().Raw(`SELECT n.nspname || '.' || c.relname || ' ' || p.priv
					FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
					CROSS JOIN (VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE')) AS p(priv)
					WHERE n.nspname = ? AND c.relkind IN ('r', 'p') AND has_table_privilege(?::name, c.oid, p.priv)`, other, role).Scan(&leaks).Error; err != nil {
					t.Fatal(err)
				}
				if len(leaks) != 0 {
					t.Errorf("%s has privileges on %s tables: %v", role, other, leaks)
				}
			}
			// Positive control: the role can work in its own schema.
			var own int64
			if err := s.DB().Raw(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = ? AND c.relkind = 'r' AND has_table_privilege(?::name, c.oid, 'SELECT') AND has_table_privilege(?::name, c.oid, 'INSERT')`,
				svc, role, role).Row().Scan(&own); err != nil {
				t.Fatal(err)
			}
			total := s.MustCount(t, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relkind = 'r'`, svc)
			if total == 0 || own != total {
				t.Errorf("%s can SELECT and INSERT on %d of the %d tables of its own schema %s, want all", role, own, total, svc)
			}
		}
	})

	t.Run("each service binary connects as its own role", func(t *testing.T) {
		for _, svc := range schemas {
			if n := s.MustCount(t, `SELECT count(*) FROM pg_stat_activity WHERE usename = ?`, "svc_"+svc); n == 0 {
				t.Errorf("no live connection as svc_%s: the %s binaries are not connecting as their role", svc, svc)
			}
		}
	})

	t.Run("there is no shared platform schema or shared event table", func(t *testing.T) {
		if n := s.MustCount(t, `SELECT count(*) FROM pg_namespace WHERE nspname = 'platform'`); n != 0 {
			t.Errorf("the platform schema still exists")
		}
		var got []string
		if err := s.DB().Raw(`SELECT table_schema || '.' || table_name FROM information_schema.tables
			WHERE table_name IN ('outbox', 'processed_events', 'idempotency_keys') ORDER BY 1`).Scan(&got).Error; err != nil {
			t.Fatal(err)
		}
		want := []string{"analytics.processed_events", "course.idempotency_keys", "course.outbox", "course.processed_events", "identity.outbox", "notification.processed_events"}
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("outbox, processed_events and idempotency_keys tables: %v, want exactly %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("outbox, processed_events and idempotency_keys tables: %v, want exactly %v", got, want)
			}
		}
		var shared []string
		if err := s.DB().Raw(`SELECT table_schema || '.' || table_name FROM information_schema.columns
			WHERE column_name = 'event_id' AND table_schema NOT IN ('identity', 'course', 'analytics', 'notification')`).Scan(&shared).Error; err != nil {
			t.Fatal(err)
		}
		if len(shared) != 0 {
			t.Errorf("tables outside the service schemas with an event_id column: %v", shared)
		}
	})

	t.Run("events are in the owning service's outbox and nowhere else", func(t *testing.T) {
		// Registration: identity.user_registered, aggregate = the new user.
		for _, c := range []struct {
			what, schema, query string
			id                  any
		}{
			{"registration", "identity", `SELECT count(*) FROM identity.outbox WHERE event_type = 'identity.user_registered' AND aggregate_id = ?`, student.ID},
			{"enrollment", "course", `SELECT count(*) FROM course.outbox WHERE event_type = 'enrollment.student_enrolled' AND convert_from(payload, 'UTF8')::jsonb ->> 'enrollment_id' = ?`, enrollment.String()},
		} {
			if n := s.MustCount(t, c.query, c.id); n != 1 {
				t.Errorf("the %s event is in %s.outbox %d times, want 1", c.what, c.schema, n)
			}
		}
		// And not in the other service's outbox.
		if n := s.MustCount(t, `SELECT count(*) FROM course.outbox WHERE event_type LIKE 'identity.%'`); n != 0 {
			t.Errorf("course.outbox holds %d identity events", n)
		}
		if n := s.MustCount(t, `SELECT count(*) FROM identity.outbox WHERE event_type NOT LIKE 'identity.%'`); n != 0 {
			t.Errorf("identity.outbox holds %d events that are not identity's", n)
		}
		// The enrollment event id is the only place it appears across all outbox
		// tables of the database: one row in total, in course.outbox.
		eventID, err := s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrollment.String())
		if err != nil {
			t.Fatal(err)
		}
		if n := s.MustCount(t, `SELECT (SELECT count(*) FROM identity.outbox WHERE event_id = ?) + (SELECT count(*) FROM course.outbox WHERE event_id = ?)`, eventID, eventID); n != 1 {
			t.Errorf("the enrollment event id is in %d outbox rows in total, want 1", n)
		}
		// The consumers' markers: in the consumer's service, not the producer's.
		for _, c := range []struct{ consumer, schema string }{
			{"analytics-aggregator", "analytics"}, {"notification-dispatcher", "notification"},
		} {
			// Polled: once, in one of two runs, the row was not there yet right
			// after WaitSettled reported lag 0 (observed, not explained).
			harness.Eventually(t, 30*time.Second, c.consumer+"'s marker in "+c.schema+".processed_events", func() error {
				if n := s.MustCount(t, `SELECT count(*) FROM `+c.schema+`.processed_events WHERE consumer_name = ? AND event_id = ?`, c.consumer, eventID); n != 1 {
					return harness.Errf("%d rows, want 1", n)
				}
				return nil
			})
			if n := s.MustCount(t, `SELECT count(*) FROM course.processed_events WHERE consumer_name = ? AND event_id = ?`, c.consumer, eventID); n != 0 {
				t.Errorf("course.processed_events holds %d markers of %s", n, c.consumer)
			}
		}
	})
}
