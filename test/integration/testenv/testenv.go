// Package testenv spins up real Postgres and Redis containers for
// integration and concurrency tests (RFC §18.2: "mocks are not used for the
// database — constraint violations, transaction semantics, and partial
// index behaviour are precisely what needs verifying, and a mock asserts
// only that the code called the method it was written to call").
//
// Requires Docker. Every module's integration tests call Setup once per
// test (or per subtest) — testcontainers-go handles container lifecycle
// and t.Cleanup handles teardown, so callers never manage this by hand.
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
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/db"
)

type Env struct {
	DB  *gorm.DB
	RDB *redis.Client
}

// Setup starts fresh Postgres and Redis containers, applies every migration
// in migrations/, and returns connected clients. Each call gets its own
// containers — tests don't share state, which is what makes the
// concurrency tests (enrollment) trustworthy.
func Setup(t *testing.T) *Env {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("smartcourse_test"),
		tcpostgres.WithUsername("smartcourse"),
		tcpostgres.WithPassword("smartcourse"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get postgres connection string: %v", err)
	}

	if err := applyMigrations(dsn); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	gdb, err := db.Open(dbConfigFor(dsn))
	if err != nil {
		t.Fatalf("open gorm connection: %v", err)
	}

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

	return &Env{DB: gdb, RDB: rdb}
}

// migrationServices is the fixed apply order (schema-per-service,
// ADR-0024): platform first (citext/pg_trgm live in `public`, needed
// before identity/course's tables that use them), analytics last (nothing
// else depends on it) — matches Makefile's MIGRATION_SERVICES.
var migrationServices = []string{"platform", "identity", "course", "analytics"}

// applyMigrations runs all four services' migrations against one database,
// each tracked by its own x-migrations-table so they don't collide on a
// single shared schema_migrations row (same scheme Makefile's migrate-up
// uses). Tests always apply every service's schema regardless of which
// module is under test — fixtures commonly span schemas (e.g. an
// enrollment test seeding a row into identity.users), same as before
// ADR-0024 when it was one flat migrations/ directory.
func applyMigrations(dsn string) error {
	root, err := migrationsRoot()
	if err != nil {
		return err
	}
	for _, svc := range migrationServices {
		svcDSN := dsn + "&x-migrations-table=" + svc + "_schema_migrations"
		m, err := migrate.New("file://"+filepath.Join(root, svc), svcDSN)
		if err != nil {
			return fmt.Errorf("create migrator for %s: %w", svc, err)
		}
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			return fmt.Errorf("run %s migrations: %w", svc, err)
		}
	}
	return nil
}

// migrationsRoot resolves migrations/ relative to this source file rather
// than the working directory, so tests pass regardless of which package
// invokes them.
func migrationsRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve caller for migrations path")
	}
	// test/integration/testenv/testenv.go -> repo root -> migrations
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(root, "migrations"), nil
}

// dbConfigFor's search_path spans every schema: unlike a real binary, test
// code routinely reaches into more than one service's tables (fixtures
// seeding identity.users for a course/enrollment test, assertions reading
// analytics' mirror tables directly), so tests keep every bare table name
// resolvable the same way they were before ADR-0024's schema split.
func dbConfigFor(dsn string) config.PostgresConfig {
	return config.PostgresConfig{
		DSN:          dsn,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		SearchPath:   "identity,course,analytics,platform,public",
	}
}

func trimRedisScheme(addr string) string {
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		return addr[len(prefix):]
	}
	return addr
}
