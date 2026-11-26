package testenv

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
)

// testPG is a bare Postgres container: no roles, no migrations.
type testPG struct {
	c   *tcpostgres.PostgresContainer
	dsn string // owner DSN with ?sslmode=disable
}

func startPostgres(t *testing.T) *testPG {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("smartcourse_test"),
		tcpostgres.WithUsername("smartcourse"),
		tcpostgres.WithPassword("smartcourse"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get postgres connection string: %v", err)
	}
	return &testPG{c: c, dsn: dsn}
}

// rolePassword is the test-only password of a service role. Test containers
// are ephemeral and local; real passwords come from the environment
// (scripts/db-roles.sh).
func rolePassword(svc string) string { return "pw-" + svc }

func roleName(svc string) string { return "svc_" + svc }

// provisionRoles runs the real scripts/db-roles.sh inside the container (the
// postgres image ships psql 16, the script reads passwords from the
// environment), so tests exercise the same grants as `make migrate-up`.
func (p *testPG) provisionRoles(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller for scripts path")
	}
	script := filepath.Join(filepath.Dir(thisFile), "..", "..", "scripts", "db-roles.sh")
	if err := p.c.CopyFileToContainer(ctx, script, "/tmp/db-roles.sh", 0o755); err != nil {
		t.Fatalf("copy db-roles.sh into container: %v", err)
	}
	var env []string
	for _, svc := range Services {
		env = append(env, "SVC_"+strings.ToUpper(svc)+"_PASSWORD="+rolePassword(svc))
	}
	code, out, err := p.c.Exec(ctx, []string{"sh", "/tmp/db-roles.sh"}, tcexec.WithEnv(env))
	if err != nil {
		t.Fatalf("run db-roles.sh: %v", err)
	}
	if code != 0 {
		b, _ := io.ReadAll(out)
		t.Fatalf("db-roles.sh exited %d: %s", code, b)
	}
}

// serviceDSN rewrites the owner DSN to log in as the service's role.
func serviceDSN(ownerDSN, svc string) (string, error) {
	u, err := url.Parse(ownerDSN)
	if err != nil {
		return "", fmt.Errorf("parse owner DSN: %w", err)
	}
	u.User = url.UserPassword(roleName(svc), rolePassword(svc))
	return u.String(), nil
}

// ServiceDSN returns the DSN of the given service's role ("identity",
// "course", "analytics" or "notification"), for a test that starts a binary or
// opens its own pool. It has no search_path; use "<schema>,public".
func (e *Env) ServiceDSN(t *testing.T, svc string) string {
	t.Helper()
	requireService(t, svc)
	dsn, err := serviceDSN(e.dsn, svc)
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

// ServiceDB opens a pool connected as the service's own Postgres role (for
// example svc_course) with search_path "<schema>,public". This is the
// connection a real service binary has, so unqualified outbox,
// processed_events and idempotency_keys resolve to that service's tables, and
// anything outside its schema fails with "permission denied". Service tests
// should use this rather than Env.DB. The pool is closed on test cleanup.
func (e *Env) ServiceDB(t *testing.T, svc string) *gorm.DB {
	t.Helper()
	requireService(t, svc)
	dsn := e.ServiceDSN(t, svc)
	g, err := db.Open(config.PostgresConfig{DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 5, SearchPath: svc + ",public"})
	if err != nil {
		t.Fatalf("open %s connection: %v", roleName(svc), err)
	}
	t.Cleanup(func() {
		if sqlDB, err := g.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	var who string
	if err := g.Raw(`SELECT current_user`).Row().Scan(&who); err != nil || who != roleName(svc) {
		t.Fatalf("ServiceDB(%q) connected as %q (err %v), want %q", svc, who, err, roleName(svc))
	}
	return g
}

// Qualify returns the schema-qualified, quoted name of a table for owner-side
// SQL, e.g. Qualify("course", "outbox") is "course"."outbox". Env.DB has no
// service schema on its search_path, so owner fixtures and assertions must
// qualify.
func Qualify(schema, table string) string {
	return `"` + strings.ReplaceAll(schema, `"`, `""`) + `"."` + strings.ReplaceAll(table, `"`, `""`) + `"`
}

func requireService(t *testing.T, svc string) {
	t.Helper()
	for _, s := range Services {
		if s == svc {
			return
		}
	}
	t.Fatalf("testenv: unknown service %q (want one of %v)", svc, Services)
}
