//go:build integration

// Instructor ownership for per-course analytics (F-36 style, DR-022): the
// owner comes from course.course_published's instructor_id, via the real
// handler. A non-owner instructor gets 404, admins keep access, and a
// mirror row written before migration 000003 (no owner) is admin-only until
// the next course_published fills it in. Also the migration's down path.
package analytics_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func courseRoutes(courseID uuid.UUID) []string {
	base := "/api/v1/analytics/courses/" + courseID.String()
	return []string{base, base + "/daily-enrollments"}
}

func TestInstructorOwnership_CoursePublishedEventDecidesAccess(t *testing.T) {
	s := newSecEnv(t)
	course := uuid.New()
	owner, other := uuid.New(), uuid.New()
	tok := func(id uuid.UUID, role string) string { return tokenFor(t, id, role) }

	// Published by the owner; the owner appears in the mirror via the real handler.
	feed(t, s.env, ev(1, "course.course_published", map[string]any{"course_id": course, "instructor_id": owner}))

	for _, p := range courseRoutes(course) {
		if rec := s.get(p, tok(owner, "instructor")); rec.Code != 200 {
			t.Errorf("owner on %s: want 200, got %d", p, rec.Code)
		}
		if rec := s.get(p, tok(other, "instructor")); rec.Code != 404 {
			t.Errorf("non-owner instructor on %s: want 404, got %d", p, rec.Code)
		}
		if rec := s.get(p, tok(uuid.New(), "admin")); rec.Code != 200 {
			t.Errorf("admin on %s: want 200, got %d", p, rec.Code)
		}
		if rec := s.get(p, tok(uuid.New(), "student")); rec.Code != 404 {
			t.Errorf("unenrolled student on %s: want 404, got %d", p, rec.Code)
		}
		if rec := s.get(p, ""); rec.Code != 401 {
			t.Errorf("anonymous on %s: want 401, got %d", p, rec.Code)
		}
	}
	// The denial is indistinguishable from an unknown course (no existence leak).
	denied := s.get(courseRoutes(course)[0], tok(other, "instructor"))
	unknown := s.get(courseRoutes(uuid.New())[0], tok(other, "instructor"))
	if denied.Code != unknown.Code || denied.Body.String() != unknown.Body.String() {
		t.Errorf("non-owner denial %d %s differs from unknown-course denial %d %s", denied.Code, denied.Body.String(), unknown.Code, unknown.Body.String())
	}
}

func TestInstructorOwnership_RowWithoutOwner_AdminOnly_UntilNextPublished(t *testing.T) {
	s := newSecEnv(t)
	course := uuid.New()
	owner := uuid.New()
	tok := func(id uuid.UUID, role string) string { return tokenFor(t, id, role) }

	// A pre-migration mirror row: published, no instructor_id.
	if err := s.env.DB.Exec(`INSERT INTO course_mirror (course_id, published) VALUES (?, true)`, course).Error; err != nil {
		t.Fatal(err)
	}
	p := courseRoutes(course)[0]
	if rec := s.get(p, tok(owner, "instructor")); rec.Code != 404 {
		t.Errorf("instructor on an ownerless row: want 404, got %d", rec.Code)
	}
	if rec := s.get(p, tok(uuid.New(), "admin")); rec.Code != 200 {
		t.Errorf("admin on an ownerless row: want 200, got %d", rec.Code)
	}

	// A republish whose payload names the instructor fills the owner in.
	feed(t, s.env, ev(10, "course.course_published", map[string]any{"course_id": course, "instructor_id": owner}))
	if rec := s.get(p, tok(owner, "instructor")); rec.Code != 200 {
		t.Errorf("owner after republish: want 200, got %d", rec.Code)
	}

	// A later event without the field must not erase the owner.
	feed(t, s.env, ev(11, "course.course_published", map[string]any{"course_id": course}))
	if rec := s.get(p, tok(owner, "instructor")); rec.Code != 200 {
		t.Errorf("owner after an event without instructor_id: want 200, got %d", rec.Code)
	}
	// The nil UUID is "no owner", not an owner.
	other := uuid.New()
	feed(t, s.env, ev(12, "course.course_published", map[string]any{"course_id": course, "instructor_id": uuid.Nil}))
	if rec := s.get(p, tok(other, "instructor")); rec.Code != 404 {
		t.Errorf("non-owner after a nil-owner event: want 404, got %d", rec.Code)
	}
	if rec := s.get(p, tok(owner, "instructor")); rec.Code != 200 {
		t.Errorf("owner after a nil-owner event: want 200, got %d", rec.Code)
	}
}

func TestInstructorOwnership_UnknownCourse_InstructorGets404(t *testing.T) {
	s := newSecEnv(t)
	if rec := s.get(courseRoutes(uuid.New())[0], s.token("instructor")); rec.Code != 404 {
		t.Errorf("instructor on a course analytics has never seen: want 404, got %d", rec.Code)
	}
}

// migrationStatements splits a migration file into its statements, dropping
// comment lines, so each can be executed on its own.
func migrationStatements(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "analytics", name))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	var out []string
	for _, st := range strings.Split(strings.Join(kept, "\n"), ";") {
		if strings.TrimSpace(st) != "" {
			out = append(out, st)
		}
	}
	return out
}

func TestMigration000003_DownRemovesColumn_UpRestoresIt_KeepsRows(t *testing.T) {
	s := newSecEnv(t)
	owner, course := uuid.New(), uuid.New()
	feed(t, s.env, ev(1, "course.course_published", map[string]any{"course_id": course, "instructor_id": owner}))

	hasColumn := func() bool {
		var n int64
		if err := s.env.DB.Raw(`SELECT count(*) FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'course_mirror' AND column_name = 'instructor_id'`).Row().Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	apply := func(name string) {
		t.Helper()
		// The migration files SET search_path for their own session; do it on
		// one pinned connection and restore the test search_path after.
		sqlDB, err := s.env.Owner.DB()
		if err != nil {
			t.Fatal(err)
		}
		conn, err := sqlDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = conn.ExecContext(t.Context(), `SET search_path TO public`)
			_ = conn.Close()
		}()
		for _, st := range migrationStatements(t, name) {
			if _, err := conn.ExecContext(t.Context(), st); err != nil {
				t.Fatalf("%s: %v\n%s", name, err, st)
			}
		}
	}

	if !hasColumn() {
		t.Fatal("precondition: instructor_id should exist after the up migration")
	}
	apply("000003_course_mirror_instructor.down.sql")
	if hasColumn() {
		t.Fatal("down migration left instructor_id behind")
	}
	var published bool
	if err := s.env.DB.Raw(`SELECT published FROM course_mirror WHERE course_id = ?`, course).Row().Scan(&published); err != nil || !published {
		t.Fatalf("down migration must keep the mirror rows: published=%v err=%v", published, err)
	}
	apply("000003_course_mirror_instructor.up.sql")
	if !hasColumn() {
		t.Fatal("up migration after down did not restore instructor_id")
	}
	var got *uuid.UUID
	if err := s.env.DB.Raw(`SELECT instructor_id FROM course_mirror WHERE course_id = ?`, course).Row().Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("the restored column must start NULL for existing rows (no backfill source), got %v", got)
	}
}
