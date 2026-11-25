//go:build integration

package testenv_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// probeTables are the tables each role is tried against in every OTHER
// service's schema. They include the integration tables (outbox,
// processed_events, idempotency_keys), which the split moved into the owning
// service's schema and which must never be reachable from another service.
var probeTables = map[string][]string{
	"identity":     {"users", "refresh_tokens", "outbox"},
	"course":       {"courses", "enrollments", "outbox", "processed_events", "idempotency_keys"},
	"analytics":    {"course_mirror", "course_stats", "processed_events"},
	"notification": {"recipients", "deliveries", "processed_events"},
}

// isDenied reports whether err is Postgres's insufficient_privilege error.
func isDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), "permission denied")
}

// firstColumn returns a column of schema.table (owner view) so an UPDATE can
// be written without touching any value.
func firstColumn(t *testing.T, owner *gorm.DB, schema, table string) string {
	t.Helper()
	var col string
	err := owner.Raw(`SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position LIMIT 1`, schema, table).Row().Scan(&col)
	if err != nil || col == "" {
		t.Fatalf("no columns for %s.%s (err %v): is the probe table list stale?", schema, table, err)
	}
	return col
}

// TestRolesCannotTouchOtherServiceSchemas is the enforcement test of the
// microservices split (docs/plans/microservices-split.md section 3.2): every
// service role against every OTHER service's schema must get "permission
// denied" for SELECT, INSERT, UPDATE, DELETE and CREATE TABLE, including the
// outbox and processed_events tables specifically. One row of the table is
// "identity cannot SELECT course.courses".
func TestRolesCannotTouchOtherServiceSchemas(t *testing.T) {
	env := testenv.Setup(t)

	type row struct {
		name, role, target, sql string
	}
	var rows []row
	for _, role := range testenv.Services {
		for _, target := range testenv.Services {
			if target == role {
				continue
			}
			for _, table := range probeTables[target] {
				q := testenv.Qualify(target, table)
				col := `"` + firstColumn(t, env.DB, target, table) + `"`
				rows = append(rows,
					row{fmt.Sprintf("%s cannot SELECT %s.%s", role, target, table), role, target, `SELECT * FROM ` + q},
					row{fmt.Sprintf("%s cannot INSERT %s.%s", role, target, table), role, target, `INSERT INTO ` + q + ` DEFAULT VALUES`},
					row{fmt.Sprintf("%s cannot UPDATE %s.%s", role, target, table), role, target, `UPDATE ` + q + ` SET ` + col + ` = ` + col},
					row{fmt.Sprintf("%s cannot DELETE %s.%s", role, target, table), role, target, `DELETE FROM ` + q},
				)
			}
			rows = append(rows,
				row{fmt.Sprintf("%s cannot CREATE TABLE in %s", role, target), role, target,
					`CREATE TABLE ` + testenv.Qualify(target, "intruder") + ` (id int)`},
				row{fmt.Sprintf("%s cannot TRUNCATE %s.%s", role, target, probeTables[target][0]), role, target,
					`TRUNCATE ` + testenv.Qualify(target, probeTables[target][0])},
			)
		}
	}

	conns := map[string]*gorm.DB{}
	for _, role := range testenv.Services {
		conns[role] = env.ServiceDB(t, role)
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			err := conns[r.role].Exec(r.sql).Error
			if !isDenied(err) {
				t.Fatalf("svc_%s ran %q against schema %s: want permission denied, got %v", r.role, r.sql, r.target, err)
			}
		})
	}

	// Nothing slipped through: no intruder table anywhere.
	var n int
	if err := env.DB.Raw(`SELECT count(*) FROM information_schema.tables WHERE table_name = 'intruder'`).Row().Scan(&n); err != nil || n != 0 {
		t.Fatalf("intruder tables exist: %d (err %v)", n, err)
	}
}

// TestRolesCannotReachOtherSchemasByName covers the namespace side: a role
// pointing its search_path at another service's schema still gets denied, and
// the owner connection resolves none of the colliding names unqualified.
func TestRolesCannotReachOtherSchemasByName(t *testing.T) {
	env := testenv.Setup(t)

	for _, name := range []string{"outbox", "processed_events", "idempotency_keys"} {
		var n int
		err := env.DB.Raw(`SELECT count(*) FROM ` + name).Row().Scan(&n)
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("owner connection resolved unqualified %q (err %v): the name-collision hazard guard is broken", name, err)
		}
	}

	for _, role := range testenv.Services {
		for _, target := range testenv.Services {
			if target == role {
				continue
			}
			// The role's pool, but with the OTHER service's schema first on the
			// search_path: unqualified names resolve there and are still denied.
			g, err := db.Open(config.PostgresConfig{DSN: env.ServiceDSN(t, role), MaxOpenConns: 2, MaxIdleConns: 1, SearchPath: target + ",public"})
			if err != nil {
				t.Fatalf("open svc_%s with search_path %s: %v", role, target, err)
			}
			t.Cleanup(func() {
				if sqlDB, err := g.DB(); err == nil {
					_ = sqlDB.Close()
				}
			})
			t.Run(role+" via search_path "+target, func(t *testing.T) {
				for _, table := range probeTables[target] {
					// Postgres skips a search_path schema the role has no USAGE
					// on, so the name does not resolve at all ("does not
					// exist"); either error is a refusal, success is the bug.
					err := g.Exec(`SELECT * FROM ` + table).Error
					if err == nil || (!isDenied(err) && !strings.Contains(err.Error(), "does not exist")) {
						t.Fatalf("svc_%s read %s.%s through search_path: want an error, got %v", role, target, table, err)
					}
				}
			})
		}
	}
}

// TestEachRoleCanDoItsOwnWork proves the roles are usable: in its own schema
// each role writes and reads back its outbox / processed_events in a
// transaction, runs DML, uses citext and pg_trgm, and sees tables the owner
// creates later (default privileges). It also proves a role has no DDL.
func TestEachRoleCanDoItsOwnWork(t *testing.T) {
	env := testenv.Setup(t)

	hasOutbox := map[string]bool{"identity": true, "course": true}
	hasInbox := map[string]bool{"course": true, "analytics": true, "notification": true}

	for _, role := range testenv.Services {
		t.Run(role, func(t *testing.T) {
			g := env.ServiceDB(t, role)

			if hasOutbox[role] {
				eventID := uuid.New()
				err := g.Transaction(func(tx *gorm.DB) error {
					if err := tx.Exec(`INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload)
						VALUES (?, 'test', ?, ?, 1, '\x01')`, eventID, uuid.New(), role+".thing_happened").Error; err != nil {
						return err
					}
					var et string
					return tx.Raw(`SELECT event_type FROM outbox WHERE event_id = ?`, eventID).Row().Scan(&et)
				})
				if err != nil {
					t.Fatalf("outbox write/read in a transaction: %v", err)
				}
				var n int
				if err := g.Raw(`SELECT count(*) FROM outbox WHERE event_id = ?`, eventID).Row().Scan(&n); err != nil || n != 1 {
					t.Fatalf("committed outbox row: n=%d err=%v", n, err)
				}
				// Lands in this service's schema, not another's.
				if err := env.DB.Raw(`SELECT count(*) FROM `+testenv.Qualify(role, "outbox")+` WHERE event_id = ?`, eventID).Row().Scan(&n); err != nil || n != 1 {
					t.Fatalf("row not in %s.outbox: n=%d err=%v", role, n, err)
				}
				// Sequence use, UPDATE and DELETE.
				if err := g.Exec(`UPDATE outbox SET attempts = attempts + 1 WHERE event_id = ?`, eventID).Error; err != nil {
					t.Fatalf("update outbox: %v", err)
				}
				if err := g.Exec(`DELETE FROM outbox WHERE event_id = ?`, eventID).Error; err != nil {
					t.Fatalf("delete outbox: %v", err)
				}
			} else if err := g.Exec(`SELECT 1 FROM outbox`).Error; err == nil || !strings.Contains(err.Error(), "does not exist") {
				t.Fatalf("%s has no outbox; unqualified outbox must fail with 'does not exist', got %v", role, err)
			}

			if hasInbox[role] {
				eventID := uuid.New()
				err := g.Transaction(func(tx *gorm.DB) error {
					if err := tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES ('probe', ?)`, eventID).Error; err != nil {
						return err
					}
					var n int
					if err := tx.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = 'probe' AND event_id = ?`, eventID).Row().Scan(&n); err != nil {
						return err
					}
					if n != 1 {
						return fmt.Errorf("read back %d rows", n)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("processed_events write/read in a transaction: %v", err)
				}
				var n int
				if err := env.DB.Raw(`SELECT count(*) FROM `+testenv.Qualify(role, "processed_events")+` WHERE event_id = ?`, eventID).Row().Scan(&n); err != nil || n != 1 {
					t.Fatalf("row not in %s.processed_events: n=%d err=%v", role, n, err)
				}
			} else if err := g.Exec(`SELECT 1 FROM processed_events`).Error; err == nil || !strings.Contains(err.Error(), "does not exist") {
				t.Fatalf("%s has no processed_events; unqualified use must fail with 'does not exist', got %v", role, err)
			}

			// An aborted transaction leaves nothing behind.
			if hasInbox[role] {
				ev := uuid.New()
				_ = g.Transaction(func(tx *gorm.DB) error {
					_ = tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES ('rolled-back', ?)`, ev).Error
					return fmt.Errorf("abort")
				})
				var n int
				_ = g.Raw(`SELECT count(*) FROM processed_events WHERE event_id = ?`, ev).Row().Scan(&n)
				if n != 0 {
					t.Fatalf("rolled-back processed_events row persisted")
				}
			}

			// Extensions in public: citext and pg_trgm functions and operators.
			var ci bool
			if err := g.Raw(`SELECT 'Ada@Example.COM'::citext = 'ada@example.com'::citext`).Row().Scan(&ci); err != nil || !ci {
				t.Fatalf("citext comparison: %v %v", ci, err)
			}
			var sim bool
			if err := g.Raw(`SELECT similarity('postgres', 'postgre') > 0.5 AND 'postgres' % 'postgrs'`).Row().Scan(&sim); err != nil || !sim {
				t.Fatalf("pg_trgm similarity: %v %v", sim, err)
			}

			// The role's own tables that need the extensions.
			switch role {
			case "identity":
				id := uuid.New()
				if err := g.Exec(`INSERT INTO users (id, email, password_hash, full_name) VALUES (?, 'Ada@Example.COM', 'x', 'Ada')`, id).Error; err != nil {
					t.Fatalf("insert user with citext email: %v", err)
				}
				var n int
				if err := g.Raw(`SELECT count(*) FROM users WHERE email = 'ada@example.com'`).Row().Scan(&n); err != nil || n != 1 {
					t.Fatalf("case-insensitive email lookup: n=%d err=%v", n, err)
				}
				if err := g.Exec(`DELETE FROM users WHERE id = ?`, id).Error; err != nil {
					t.Fatalf("delete user: %v", err)
				}
			case "course":
				var n int
				if err := g.Raw(`SELECT count(*) FROM course_search_index WHERE title % 'golang' AND similarity(title, 'golang') > 0.3`).Row().Scan(&n); err != nil {
					t.Fatalf("trigram query on course_search_index: %v", err)
				}
			}

			// No DDL: the role owns nothing and cannot create in its own schema.
			if err := g.Exec(`CREATE TABLE ddl_probe (id int)`).Error; !isDenied(err) {
				t.Fatalf("role created a table in its own schema: %v", err)
			}

			// Default privileges: a table the OWNER creates later is usable by
			// this role and by no other.
			future := "future_" + role
			if err := env.DB.Exec(`CREATE TABLE ` + testenv.Qualify(role, future) + ` (id int)`).Error; err != nil {
				t.Fatalf("owner create future table: %v", err)
			}
			if err := g.Exec(`INSERT INTO ` + future + ` VALUES (1)`).Error; err != nil {
				t.Fatalf("role cannot use a table created after the grants (default privileges): %v", err)
			}
			for _, other := range testenv.Services {
				if other == role {
					continue
				}
				if err := env.ServiceDB(t, other).Exec(`SELECT * FROM ` + testenv.Qualify(role, future)).Error; !isDenied(err) {
					t.Fatalf("svc_%s can read future table %s.%s: %v", other, role, future, err)
				}
			}
		})
	}
}
