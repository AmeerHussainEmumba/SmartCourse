//go:build integration

// What this file checks, against real Postgres: migration 000007 reverses
// cleanly (the `certificates` table from 000003 survives, only the columns,
// constraint and indexes 000007 added go), and applying it again over rows
// that already exist backfills student_id/course_id from the enrollment so
// the NOT NULL columns cannot fail an upgrade of a populated database.
package certificate_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// execScript runs a multi-statement migration file over the simple protocol
// (the extended protocol database/sql would otherwise use takes one
// statement per call).
func execScript(t *testing.T, db *gorm.DB, name string) {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	script, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations", "course", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql.DB: %v", err)
	}
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Raw(func(driverConn any) error {
		pgConn := driverConn.(*stdlib.Conn).Conn().PgConn()
		_, err := pgConn.Exec(context.Background(), string(script)).ReadAll()
		return err
	}); err != nil {
		t.Fatalf("run %s: %v", name, err)
	}
}

func certificateColumns(t *testing.T, db *gorm.DB) map[string]bool {
	t.Helper()
	rows, err := db.Raw(`SELECT column_name FROM information_schema.columns WHERE table_schema = 'course' AND table_name = 'certificates'`).Rows()
	if err != nil {
		t.Fatalf("list columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		cols[c] = true
	}
	return cols
}

func TestMigration000007_DownThenUpAgain(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	execScript(t, env.Owner, "000007_certificates.down.sql")
	cols := certificateColumns(t, env.DB)
	for _, gone := range []string{"student_id", "course_id", "course_version_id"} {
		if cols[gone] {
			t.Errorf("down left column %s behind", gone)
		}
	}
	for _, kept := range []string{"id", "enrollment_id", "verification_code", "student_name", "course_title", "issued_at"} {
		if !cols[kept] {
			t.Errorf("down must not touch 000003's column %s", kept)
		}
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("down must keep existing certificate rows, got %d", n)
	}

	execScript(t, env.Owner, "000007_certificates.up.sql")
	var studentID, courseID string
	if err := env.DB.Raw(`SELECT student_id::text, course_id::text FROM certificates WHERE enrollment_id = ?`, f.enrollmentID).
		Row().Scan(&studentID, &courseID); err != nil {
		t.Fatalf("read backfilled row: %v", err)
	}
	if studentID != f.studentID.String() || courseID != f.courseID.String() {
		t.Fatalf("up must backfill student_id/course_id from the enrollment, got %s / %s", studentID, courseID)
	}

	// Reversible again, so a second down in a rollback rehearsal also works.
	execScript(t, env.Owner, "000007_certificates.down.sql")
	execScript(t, env.Owner, "000007_certificates.up.sql")
}
