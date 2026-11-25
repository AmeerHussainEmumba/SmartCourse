//go:build integration

// Reproducer: Service.Withdraw reads the enrollment's status BEFORE taking
// the course row lock (service.go: GetByID + status check, then
// lockCourseForEnrollment). Two concurrent withdrawals of the same
// enrollment (double-click, client retry) both pass the "status == active"
// check, then run serially under the lock: the first withdraws the row, the
// second's UPDATE ... WHERE status='active' matches 0 rows - but the code
// does not look at RowsAffected, so it still decrements enrollment_count and
// writes a second enrollment.student_withdrawn event. The counter drifts
// below the number of held seats (freeing seats that are not free), and with
// a single enrollment the CHECK (enrollment_count >= 0) turns it into a 500.
//
// Determinism: the test holds the course row lock itself, starts both
// withdrawals (they read status, then block on the lock), waits until
// Postgres reports both waiting, then releases.
package enrollment_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func TestBugrepro_ConcurrentDoubleWithdraw_DriftsEnrollmentCount(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()
	courseID := seedCourse(t, env, nil)

	a, b, c := newStudentID(), newStudentID(), newStudentID()
	ea, err := svc.Enroll(ctx, a, courseID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Enroll(ctx, b, courseID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Enroll(ctx, c, courseID); err != nil {
		t.Fatal(err)
	}

	hold := env.DB.Begin()
	if err := hold.Exec(`SELECT 1 FROM courses WHERE id = ? FOR UPDATE`, courseID).Error; err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = svc.Withdraw(ctx, a, ea.ID)
		}(i)
	}
	deadline := time.Now().Add(15 * time.Second)
	for scalar(t, env, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE '%FOR UPDATE%'`) < 2 {
		if time.Now().After(deadline) {
			hold.Rollback()
			t.Fatal("both withdrawals never reached the row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := hold.Commit().Error; err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	held := scalar(t, env, `SELECT count(*) FROM enrollments WHERE course_id = ? AND status IN ('active','completed')`, courseID)
	count := scalar(t, env, `SELECT enrollment_count FROM courses WHERE id = ?`, courseID)
	events := scalar(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'enrollment.student_withdrawn' AND aggregate_id = ?`, courseID)
	if count != held {
		t.Errorf("enrollment_count=%d but %d seats are held: a duplicate withdraw decremented the counter twice", count, held)
	}
	if events != 1 {
		t.Errorf("%d enrollment.student_withdrawn events for ONE withdrawn enrollment", events)
	}
	_ = errs
}
