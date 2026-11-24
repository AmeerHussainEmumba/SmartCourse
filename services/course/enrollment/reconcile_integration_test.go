//go:build integration

package enrollment_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func storedCount(t *testing.T, env *testenv.Env, courseID uuid.UUID) int {
	t.Helper()
	var n int
	if err := env.DB.Raw(`SELECT enrollment_count FROM courses WHERE id = ?`, courseID).Row().Scan(&n); err != nil {
		t.Fatalf("read enrollment_count: %v", err)
	}
	return n
}

func corrupt(t *testing.T, env *testenv.Env, courseID uuid.UUID, to int) {
	t.Helper()
	if err := env.DB.Exec(`UPDATE courses SET enrollment_count = ? WHERE id = ?`, to, courseID).Error; err != nil {
		t.Fatalf("corrupt counter: %v", err)
	}
}

// Three students enroll (counter 3), one withdraws (2), one completes. The
// counter is then corrupted by hand, up and down, and one pass must heal both
// courses. The completed enrollment keeps its seat, as Enroll/Withdraw treat it.
func TestReconcileEnrollmentCounts_HealsHandCorruptedCounter(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	courseA, courseB, healthy := seedCourse(t, env, nil), seedCourse(t, env, nil), seedCourse(t, env, nil)
	students := []uuid.UUID{seedStudent(t, env), seedStudent(t, env), seedStudent(t, env)}
	for _, s := range students {
		if _, err := svc.Enroll(ctx, s, courseA); err != nil {
			t.Fatalf("Enroll: %v", err)
		}
	}
	if _, err := svc.Enroll(ctx, students[0], courseB); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if _, err := svc.Enroll(ctx, students[0], healthy); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if err := svc.Withdraw(ctx, students[0], mustEnrollmentID(t, env, students[0], courseA)); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if err := env.DB.Exec(`UPDATE enrollments SET status = 'completed', completed_at = now() WHERE student_id = ? AND course_id = ?`, students[1], courseA).Error; err != nil {
		t.Fatal(err)
	}
	if got := storedCount(t, env, courseA); got != 2 {
		t.Fatalf("setup: counter = %d, want 2 (one withdrawn of three)", got)
	}

	corrupt(t, env, courseA, 9) // inflated: seats would look taken
	corrupt(t, env, courseB, 0) // deflated: the course would oversell

	res, err := enrollment.ReconcileEnrollmentCounts(ctx, env.DB)
	if err != nil {
		t.Fatalf("ReconcileEnrollmentCounts: %v", err)
	}
	if got := storedCount(t, env, courseA); got != 2 {
		t.Errorf("course A counter = %d after reconcile, want 2", got)
	}
	if got := storedCount(t, env, courseB); got != 1 {
		t.Errorf("course B counter = %d after reconcile, want 1", got)
	}
	if got := storedCount(t, env, healthy); got != 1 {
		t.Errorf("untouched course's counter = %d, want 1", got)
	}
	if res.Checked != 2 || len(res.Repaired) != 2 || res.Errors != 0 {
		t.Fatalf("result = %+v, want 2 checked, 2 repaired, 0 errors", res)
	}
	for _, d := range res.Repaired {
		if (d.CourseID == courseA && (d.Stored != 9 || d.Actual != 2)) || (d.CourseID == courseB && (d.Stored != 0 || d.Actual != 1)) {
			t.Errorf("drift report %+v is wrong", d)
		}
	}

	// A second pass has nothing to do.
	again, err := enrollment.ReconcileEnrollmentCounts(ctx, env.DB)
	if err != nil || again.Checked != 0 || len(again.Repaired) != 0 {
		t.Fatalf("second pass = %+v, %v; want a no-op", again, err)
	}
}

// The repair takes the course row lock first (ADR-0012): while another
// transaction holds the row, the pass must wait, and once released it
// recounts, so an enrollment committed meanwhile is not overwritten with a
// stale count.
func TestReconcileEnrollmentCounts_WaitsForTheCourseRowLock(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	courseID := seedCourse(t, env, nil)
	student := seedStudent(t, env)
	corrupt(t, env, courseID, 5) // no enrollments exist: the right value is 0

	locked := make(chan struct{})
	release := make(chan struct{})
	txDone := make(chan error, 1)
	go func() {
		txDone <- env.DB.Transaction(func(tx *gorm.DB) error {
			var id uuid.UUID
			if err := tx.Raw(`SELECT id FROM courses WHERE id = ? FOR UPDATE`, courseID).Row().Scan(&id); err != nil {
				return err
			}
			close(locked)
			<-release
			// An enrollment that commits while the reconciler is waiting.
			if err := tx.Exec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`,
				uuid.Must(uuid.NewV7()), student, courseID).Error; err != nil {
				return err
			}
			return tx.Exec(`UPDATE courses SET enrollment_count = 1 WHERE id = ?`, courseID).Error
		})
	}()
	<-locked

	reconDone := make(chan error, 1)
	go func() {
		_, err := enrollment.ReconcileEnrollmentCounts(ctx, env.DB)
		reconDone <- err
	}()
	// Bounded absence check: the pass must still be waiting on the row lock.
	select {
	case err := <-reconDone:
		t.Fatalf("reconcile finished (%v) while another transaction held the course row", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-txDone; err != nil {
		t.Fatalf("lock holder: %v", err)
	}
	select {
	case err := <-reconDone:
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile never finished after the lock was released")
	}
	if got := storedCount(t, env, courseID); got != 1 {
		t.Fatalf("counter = %d, want 1 (the enrollment committed under the lock)", got)
	}
}
