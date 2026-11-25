// Package testenv spins up real Postgres and Redis containers for
// integration and concurrency tests (RFC §18.2: "mocks are not used for the
// database — constraint violations, transaction semantics, and partial
// index behaviour are precisely what needs verifying, and a mock asserts
// only that the code called the method it was written to call").
//
// Requires Docker. Every module's integration tests call Setup once per
// test (or per subtest) — testcontainers-go handles container lifecycle
// and t.Cleanup handles teardown, so callers never manage this by hand.
//
// Service tests should act as the service: Env.ServiceDB connects as that
// service's Postgres role with search_path "<schema>,public". Env.DB is the
// OWNER connection and carries no service schema on its search_path (see
// README, "Name-collision hazard").
package testenv

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
)

// Env is one test's infrastructure.
//
// DB is the OWNER connection (the database owner that runs migrations, a
// superuser in the test container). Its search_path is OwnerSearchPath
// ("public"), so an unqualified outbox, processed_events or idempotency_keys
// fails with "relation does not exist" instead of silently resolving to the
// wrong service's table. Use ServiceDB to act as a service, and Qualify (or
// write schema.table) for owner-side fixtures and assertions.
type Env struct {
	DB *gorm.DB
	// Owner is the same owner connection DB starts as. A test that replaces DB
	// with a ServiceDB connection (see the helpers in libs/relay's tests) keeps
	// the owner here for DDL fixtures and cross-schema assertions.
	Owner *gorm.DB
	RDB   *redis.Client

	dsn string // owner DSN, no search_path
}

// OwnerSearchPath is the search_path of Env.DB. It deliberately contains no
// service schema: outbox exists in identity and course, processed_events in
// course, analytics and notification, and idempotency_keys in course (more
// can follow), and a combined search_path silently resolves such a name to
// the FIRST schema listed. With only `public` on the path, every unqualified
// use of a service table fails loudly.
const OwnerSearchPath = "public"

// Services lists the service schemas that have a role, in migration order.
var Services = []string{"identity", "course", "analytics", "notification"}

// Setup starts fresh Postgres and Redis containers, creates the per-service
// roles (scripts/db-roles.sh, run inside the container), applies every
// migration in migrations/, and returns connected clients. Each call gets its
// own containers: tests don't share state, which is what makes the
// concurrency tests (enrollment) trustworthy.
func Setup(t *testing.T) *Env {
	t.Helper()
	ctx := context.Background()

	pg := startPostgres(t)
	pg.provisionRoles(t)
	if err := applyMigrations(pg.dsn, migrationServices...); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	// Same order as `make migrate-up`: roles, migrations, roles again. The
	// second run proves the script is idempotent and covers existing tables.
	pg.provisionRoles(t)

	gdb, err := db.Open(dbConfigFor(pg.dsn))
	if err != nil {
		t.Fatalf("open gorm connection: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := gdb.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine",
		tcredis.WithSnapshotting(0, 0), // no persistence needed for tests
	)
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = redisContainer.Terminate(context.Background()) })

	redisAddr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get redis connection string: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: trimRedisScheme(redisAddr)})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	return &Env{DB: gdb, Owner: gdb, RDB: rdb, dsn: pg.dsn}
}

// migrationServices is the fixed apply order (schema-per-service, ADR-0024):
// platform first (citext/pg_trgm live in `public`, needed before
// identity/course's tables that use them), cutover last (it moves and drops
// the platform tables, so every service's own tables must exist). Matches
// Makefile's MIGRATION_SERVICES.
var migrationServices = []string{"platform", "identity", "course", "analytics", "notification", "cutover"}

// applyMigrations runs the given migration sets against one database, each
// tracked by its own x-migrations-table so they don't collide on a single
// shared schema_migrations row (same scheme Makefile's migrate-up uses).
// Tests always apply every set regardless of which module is under test:
// fixtures commonly span schemas (e.g. an enrollment test seeding a row into
// identity.users).
func applyMigrations(dsn string, sets ...string) error {
	for _, set := range sets {
		m, err := newMigrator(dsn, set)
		if err != nil {
			return err
		}
		err = m.Up()
		_, _ = m.Close()
		if err != nil && err != migrate.ErrNoChange {
			return fmt.Errorf("run %s migrations: %w", set, err)
		}
	}
	return nil
}

func newMigrator(dsn, set string) (*migrate.Migrate, error) {
	root, err := migrationsRoot()
	if err != nil {
		return nil, err
	}
	m, err := migrate.New("file://"+filepath.Join(root, set), dsn+"&x-migrations-table="+set+"_schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("create migrator for %s: %w", set, err)
	}
	return m, nil
}

// migrationsRoot resolves migrations/ relative to this source file rather
// than the working directory, so tests pass regardless of which package
// invokes them.
func migrationsRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve caller for migrations path")
	}
	// libs/testenv/testenv.go -> repo root -> migrations
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(root, "migrations"), nil
}

// dbConfigFor builds the OWNER pool. See OwnerSearchPath for why it carries no
// service schema.
func dbConfigFor(dsn string) config.PostgresConfig {
	return config.PostgresConfig{
		DSN:          dsn,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		SearchPath:   OwnerSearchPath,
	}
}

func trimRedisScheme(addr string) string {
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		return addr[len(prefix):]
	}
	return addr
}
