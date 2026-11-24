//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// The worker as deployed: buildRelay over a connection with the worker's real
// POSTGRES_SEARCH_PATH (platform,course,analytics,public - no identity), all
// three modules' handlers registered together, fed real outbox rows. Every
// event must be processed without a single give-up, and every module's own
// tables must show the effect.
func TestBuildRelay_EndToEnd_OnWorkerSearchPath(t *testing.T) {
	env := testenv.Setup(t)
	conn := env.OpenWithSearchPath(t, "platform,course,analytics,public")
	rl := buildRelay(&infra.Dependencies{DB: conn})

	instructor, student := uuid.New(), uuid.New()
	courseID, enrollmentID := uuid.New(), uuid.New()
	// A real course row for the enrollment archive handler to lock.
	versionID := uuid.New()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if err := env.DB.Exec(q, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, 'worker-e2e')`, courseID, instructor)
	mustExec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'T', '{}')`, versionID, courseID)
	mustExec(`UPDATE courses SET published_version_id = ?, enrollment_count = 1 WHERE id = ?`, versionID, courseID)
	mustExec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`, enrollmentID, student, courseID)

	write := func(typ string, payload map[string]any) {
		t.Helper()
		if err := env.DB.Transaction(func(tx *gorm.DB) error {
			return outbox.Write(context.Background(), tx, outbox.Event{AggregateType: "x", AggregateID: uuid.New(), EventType: typ, SchemaVersion: 1, Payload: payload})
		}); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().UTC().Add(-time.Hour)
	write("identity.user_registered", map[string]any{"user_id": instructor, "role": "instructor", "full_name": "Worker Instr"})
	write("identity.user_profile_updated", map[string]any{"user_id": instructor, "full_name": "Worker Instr 2"})
	write("identity.user_role_changed", map[string]any{"user_id": student, "role": "student", "status": "active"})
	write("course.course_published", map[string]any{"course_id": courseID})
	write("enrollment.student_enrolled", map[string]any{"enrollment_id": enrollmentID, "student_id": student, "course_id": courseID, "enrolled_at": at})
	write("course.course_archived", map[string]any{"course_id": courseID})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int64
		env.DB.Raw(`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Row().Scan(&n)
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("%d events never published", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if p, f, g := rl.Stats(); g != 0 {
		t.Fatalf("relay gave up on %d events (processed=%d failed=%d)", g, p, f)
	}
	scalar := func(q string, args ...any) int64 {
		var n int64
		if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}
	if scalar(`SELECT count(*) FROM instructor_mirror WHERE user_id = ?`, instructor) != 1 {
		t.Error("course instructor_mirror not fed")
	}
	if scalar(`SELECT count(*) FROM user_role_mirror`) != 2 || scalar(`SELECT total_courses_published FROM platform_snapshot`) != 1 {
		t.Error("analytics mirrors/snapshot not fed")
	}
	if scalar(`SELECT count(*) FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID) != 1 {
		t.Error("analytics enrollment_mirror not fed")
	}
	if scalar(`SELECT count(*) FROM enrollments WHERE id = ? AND status = 'withdrawn'`, enrollmentID) != 1 || scalar(`SELECT enrollment_count FROM courses WHERE id = ?`, courseID) != 0 {
		t.Error("enrollment archive handler did not withdraw the enrollment / fix the counter")
	}
}
