package testenv

import (
	"fmt"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
)

// OpenWithSearchPath opens an extra connection pool to the same database with
// the given search_path, built through libs/db exactly like a real service
// binary does (POSTGRES_SEARCH_PATH -> DSN startup parameter), so a test can
// prove a service works with ONLY its own schemas resolvable (ADR-0024).
// The pool is closed on test cleanup.
func (e *Env) OpenWithSearchPath(t *testing.T, searchPath string) *gorm.DB {
	t.Helper()
	d, ok := e.DB.Dialector.(*postgres.Dialector)
	if !ok {
		t.Fatalf("unexpected dialector %T", e.DB.Dialector)
	}
	g, err := db.Open(config.PostgresConfig{DSN: d.DSN, MaxOpenConns: 10, MaxIdleConns: 5, SearchPath: searchPath})
	if err != nil {
		t.Fatalf("open connection with search_path %q: %v", searchPath, err)
	}
	t.Cleanup(func() {
		if sqlDB, err := g.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	var got string
	if err := g.Raw(`SHOW search_path`).Row().Scan(&got); err != nil {
		t.Fatalf("show search_path: %v", err)
	}
	if want := normalizeSearchPath(searchPath); normalizeSearchPath(got) != want {
		t.Fatalf("search_path not applied: got %q want %q", got, searchPath)
	}
	return g
}

func normalizeSearchPath(s string) string {
	out := ""
	for _, r := range s {
		if r != ' ' && r != '"' {
			out += string(r)
		}
	}
	return fmt.Sprint(out)
}
