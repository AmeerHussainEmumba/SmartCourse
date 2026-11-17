//go:build integration && bugrepro

// Reproducers for REAL findings in the course module (course, enrollment,
// progress). Excluded from the default suite; run with
//
//	GOWORK=off go test -tags='integration bugrepro' -run TestBug ./...
//
// Each test FAILS against the current code and should pass once fixed.
package securitytest_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// DeleteModule/DeleteLesson never check the module/lesson belongs to the
// DRAFT version (UpdateModule/AddLesson/UpdateLesson do, via errNoDraft), so
// the owner can delete content of the PUBLISHED, supposedly immutable version
// (ADR-0010): live content disappears, ON DELETE CASCADE drops lessons, and
// enrolled students' progress denominators change.
func TestBug_OwnerCanDeleteModuleAndLessonsOfPublishedVersion(t *testing.T) {
	w := newWorld(t)
	pub := w.newPublished(instrA)
	rec := w.do(instrA, "DELETE", "/api/v1/lessons/"+pub.LessonID.String(), nil)
	if okStatus(rec.Code) {
		t.Errorf("DELETE /lessons/:id on a PUBLISHED lesson succeeded (%d)", rec.Code)
	}
	rec = w.do(instrA, "DELETE", "/api/v1/modules/"+pub.ModuleID.String(), nil)
	if okStatus(rec.Code) {
		t.Errorf("DELETE /modules/:id on a PUBLISHED module succeeded (%d); the live course now has no modules", rec.Code)
	}
}

// RequireAuth trusts the role claim for the whole access-token TTL. A demoted
// (or suspended) instructor keeps authoring/publishing/archiving rights until
// the 15 min token expires: only the admin routes call RequireFreshPrivilege.
func TestBug_DemotedInstructor_StaleTokenStillWrites(t *testing.T) {
	w := newWorld(t)
	// Simulate what identity.UpdateUserRole does: bump token_version to 2 in Redis.
	if err := w.env.RDB.Set(context.Background(), "user:tv:"+w.actors[instrA].id.String(), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	rec := w.do(instrA, "POST", "/api/v1/courses", map[string]any{"title": "Still here", "slug": "stale-" + strings.ToLower(uuid.NewString()[:6])})
	if okStatus(rec.Code) {
		t.Fatalf("an instructor whose token_version is stale (role revoked) created a course: %d. Instructor write routes should use RequireFreshPrivilege (or a tv check)", rec.Code)
	}
}

// Enrollment.Withdraw reads status OUTSIDE the transaction, then decrements
// courses.enrollment_count UNCONDITIONALLY even when its status UPDATE (guarded by status='active')
// matched zero rows. N concurrent withdrawals of ONE enrollment decrement the
// counter N times: seats are leaked, so a limited course is later oversold
// (or the CHECK enrollment_count >= 0 fires -> 500).
func TestBug_Withdraw_ConcurrentDoubleWithdraw_LeaksSeats(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	w.env.DB.Exec(`UPDATE courses SET enrollment_limit = 2 WHERE id = ?`, fx.CourseID)
	w.publish(fx.CourseID, instrA)
	eA := w.enroll(studentA, fx.CourseID)
	w.enroll(studentB, fx.CourseID) // course now full: 2/2

	var wg sync.WaitGroup
	gate := make(chan struct{})
	codes := make(chan int, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			codes <- w.do(studentA, "DELETE", "/api/v1/enrollments/"+eA.String(), nil).Code
		}()
	}
	close(gate)
	wg.Wait()
	close(codes)
	var fives int
	for c := range codes {
		if c >= 500 {
			fives++
		}
	}
	count := w.scalar(`SELECT enrollment_count::text FROM courses WHERE id = ?`, fx.CourseID)
	active := w.scalar(`SELECT count(*)::text FROM enrollments WHERE course_id = ? AND status = 'active'`, fx.CourseID)
	t.Logf("after 16 concurrent withdrawals of one enrollment: enrollment_count=%s active_rows=%s 5xx=%d", count, active, fives)
	if count != active || fives > 0 {
		t.Fatalf("enrollment_count=%s but %s active enrollments (5xx=%d): the counter drifted", count, active, fives)
	}
}

// pgx/Postgres reject NUL (0x00) in text; nothing strips it, so user input
// that is valid JSON/URL produces 500s (should be 400).
func TestBug_NULBytes_Cause500(t *testing.T) {
	w := newWorld(t)
	w.newPublished(instrA)
	for _, p := range []string{
		"/api/v1/courses/search?q=" + url.QueryEscape("a\x00b"),
		"/api/v1/courses?q=" + url.QueryEscape("a\x00b"),
		"/api/v1/courses?category=" + url.QueryEscape("a\x00b"),
		"/api/v1/courses/" + url.PathEscape("a\x00b"),
	} {
		if rec := w.do(anon, "GET", p, nil); rec.Code >= 500 {
			t.Errorf("GET %s -> %d", p, rec.Code)
		}
	}
	for name, body := range map[string]map[string]any{
		"POST /courses title":       {"title": "a\u0000b", "slug": "nul-" + strings.ToLower(uuid.NewString()[:6])},
		"POST /courses description": {"title": "t", "slug": "nul-" + strings.ToLower(uuid.NewString()[:6]), "description": "a\u0000b"},
	} {
		if rec := w.do(instrA, "POST", "/api/v1/courses", body); rec.Code >= 500 {
			t.Errorf("%s -> %d", name, rec.Code)
		}
	}
	fx := w.newDraft(instrA)
	for name, c := range map[string]map[string]any{
		"lesson content NUL value": {"a": "x\u0000y"},
		"lesson content NUL key":   {"\u0000": "v"},
		"lesson content nested":    {"k": nestMap(5000)}, // < 64 KiB as text but > 64 KiB as jsonb: CHECK violation
	} {
		rec := w.do(instrA, "POST", "/api/v1/modules/"+fx.ModuleID.String()+"/lessons", map[string]any{"title": "L", "position": 20 + len(name), "content_type": "text", "content": c})
		if rec.Code >= 500 {
			t.Errorf("%s -> %d", name, rec.Code)
		}
	}
}

// ILIKE pattern built from raw user input: a trailing backslash is an
// invalid LIKE pattern (Postgres error -> 500), and % / _ act as wildcards
// (q=% matches every course).
func TestBug_CatalogueSearch_LikePatternInjection(t *testing.T) {
	w := newWorld(t)
	w.newPublished(instrA)
	w.newPublished(instrB)
	if rec := w.do(anon, "GET", "/api/v1/courses?q="+url.QueryEscape(`abc\`), nil); rec.Code >= 500 {
		t.Errorf("q ending in a backslash -> %d (invalid LIKE pattern)", rec.Code)
	}
	rec := w.do(anon, "GET", "/api/v1/courses?q="+url.QueryEscape("%"), nil)
	if strings.Count(rec.Body.String(), `"course_id"`) >= 2 {
		t.Errorf("q=%% matched every published course: user-controlled LIKE wildcards are not escaped")
	}
}

// Integers are not range-checked against the INT (32-bit) columns.
func TestBug_OutOfRangeIntegers_Cause500(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	big := 3000000000
	cases := map[string]struct {
		path string
		body map[string]any
	}{
		"enrollment_limit": {"/api/v1/courses", map[string]any{"title": "t", "slug": "big-" + strings.ToLower(uuid.NewString()[:6]), "enrollment_limit": big}},
		"module position":  {"/api/v1/courses/" + fx.CourseID.String() + "/modules", map[string]any{"title": "t", "position": big}},
		"lesson position":  {"/api/v1/modules/" + fx.ModuleID.String() + "/lessons", map[string]any{"title": "t", "position": big, "content_type": "text"}},
		"lesson duration":  {"/api/v1/modules/" + fx.ModuleID.String() + "/lessons", map[string]any{"title": "t", "position": 3, "content_type": "text", "duration_s": big}},
	}
	for name, c := range cases {
		if rec := w.do(instrA, "POST", c.path, c.body); rec.Code >= 500 {
			t.Errorf("%s=%d -> %d (want 400)", name, big, rec.Code)
		}
	}
}

// No request-body cap and several unbounded string fields: one authenticated
// instructor can store arbitrarily large rows (category/tags have no max length).
func TestBug_UnboundedStringFields_And_NoBodyLimit(t *testing.T) {
	w := newWorld(t)
	huge := strings.Repeat("A", 8<<20)
	rec := w.do(instrA, "POST", "/api/v1/courses", map[string]any{"title": "t", "slug": "huge-" + strings.ToLower(uuid.NewString()[:6]), "category": huge})
	if okStatus(rec.Code) {
		t.Fatalf("an 8 MiB category was accepted and stored (%d)", rec.Code)
	}
}

// Ownership denial reveals existence: 403 not_owner for a real course id vs
// 404 for an unknown id, so any instructor (self-service role? no: admin
// granted) can probe which UUIDs are courses. Low severity.
func TestBug_OwnershipCheck_DistinguishesExistingFromMissingCourses(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	real := w.do(instrB, "PATCH", "/api/v1/courses/"+fx.CourseID.String(), map[string]any{"title": "x"})
	ghost := w.do(instrB, "PATCH", "/api/v1/courses/"+uuid.NewString(), map[string]any{"title": "x"})
	if real.Code == ghost.Code {
		return
	}
	t.Fatalf("existing foreign course -> %d %s ; unknown course -> %d %s", real.Code, strings.TrimSpace(real.Body.String()), ghost.Code, strings.TrimSpace(ghost.Body.String()))
}

var _ = fmt.Sprint
