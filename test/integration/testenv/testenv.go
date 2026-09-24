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

func applyMigrations(dsn string) error {
	path, err := migrationsPath()
	if err != nil {
		return err
	}
	m, err := migrate.New("file://"+path, dsn)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

// migrationsPath resolves migrations/ relative to this source file rather
// than the working directory, so tests pass regardless of which package
// invokes them.
func migrationsPath() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve caller for migrations path")
	}
	// test/integration/testenv/testenv.go -> repo root -> migrations
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(root, "migrations"), nil
}

func dbConfigFor(dsn string) config.PostgresConfig {
	return config.PostgresConfig{DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 5}
}

func trimRedisScheme(addr string) string {
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		return addr[len(prefix):]
	}
	return addr
}
