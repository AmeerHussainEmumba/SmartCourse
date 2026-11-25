//go:build integration

// Adversarial seat-limit tests (ADR-0012). The existing suite proves 30
// goroutines / 10 seats. These attack the rest of the state space: far more
// contenders, enroll/withdraw/re-enroll churn, archived and missing courses,
// archive racing with enroll, prerequisites under concurrency, a limit
// lowered below the current count.
package enrollment_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func newStudentID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func scalar(t *testing.T, env *testenv.Env, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// assertSeatInvariants is the invariant set every test below ends with:
// the counter equals the rows that hold a seat (active or completed), never
// exceeds the limit, no student holds two seats, and the outbox ledger
// (enrolled - withdrawn) agrees.
func assertSeatInvariants(t *testing.T, env *testenv.Env, courseID uuid.UUID, limit int) {
	t.Helper()
	count := scalar(t, env, `SELECT enrollment_count FROM courses WHERE id = ?`, courseID)
	held := scalar(t, env, `SELECT count(*) FROM enrollments WHERE course_id = ? AND status IN ('active','completed')`, courseID)
	if count != held {
		t.Errorf("enrollment_count=%d but %d enrollment rows hold a seat (counter drift)", count, held)
	}
	if limit > 0 && held > int64(limit) {
		t.Errorf("%d seats held, limit is %d", held, limit)
	}
	if dup := scalar(t, env, `SELECT count(*) FROM (SELECT student_id FROM enrollments WHERE course_id = ? AND status IN ('active','completed') GROUP BY student_id HAVING count(*) > 1) d`, courseID); dup != 0 {
		t.Errorf("%d students hold more than one seat", dup)
	}
	enrolled := scalar(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'enrollment.student_enrolled' AND aggregate_id = ?`, courseID)
	withdrawn := scalar(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'enrollment.student_withdrawn' AND aggregate_id = ?`, courseID)
	rows := scalar(t, env, `SELECT count(*) FROM enrollments WHERE course_id = ?`, courseID)
	if enrolled != rows {
		t.Errorf("%d enrollment rows but %d student_enrolled events", rows, enrolled)
	}
	if enrolled-withdrawn != held {
		t.Errorf("outbox ledger enrolled(%d)-withdrawn(%d)=%d, seats held=%d", enrolled, withdrawn, enrolled-withdrawn, held)
	}
	if orphan := scalar(t, env, `SELECT count(*) FROM enrollments e WHERE e.course_id = ? AND NOT EXISTS (SELECT 1 FROM lesson_progress lp WHERE lp.enrollment_id = e.id)`, courseID); orphan != 0 {
		t.Errorf("%d enrollments have no lesson_progress rows", orphan)
	}
}

func errCode(err error) string {
	if err == nil {
		return "ok"
	}
	if ae, ok := apperrors.As(err); ok {
		return ae.Code
	}
	return "NON_APP_ERROR: " + err.Error()
}

func TestAdv_SeatLimit_150Contenders_25Seats(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	const seats, contenders = 25, 150
	limit := seats
	courseID := seedCourse(t, env, &limit)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[string]int{}
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.Enroll(context.Background(), newStudentID(), courseID)
			mu.Lock()
			codes[errCode(err)]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if codes["ok"] != seats || codes["course_full"] != contenders-seats || len(codes) != 2 {
		t.Fatalf("outcome distribution wrong: %v", codes)
	}
	assertSeatInvariants(t, env, courseID, seats)
}

func TestAdv_Churn_EnrollWithdrawReenroll_SameStudentsRacing(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	const seats, students, rounds = 5, 14, 8
	limit := seats
	courseID := seedCourse(t, env, &limit)

	ids := make([]uuid.UUID, students)
	for i := range ids {
		ids[i] = newStudentID()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int32
	var badMu sync.Mutex
	var badMsgs []string
	allowed := map[string]bool{"ok": true, "course_full": true, "already_enrolled": true}
	note := func(op string, err error) {
		c := errCode(err)
		if !allowed[c] {
			bad.Add(1)
			badMu.Lock()
			badMsgs = append(badMsgs, op+": "+c)
			badMu.Unlock()
		}
	}
	// Two goroutines per student: both enroll, withdraw, re-enroll the same
	// student concurrently (double-click + churn).
	for _, sid := range ids {
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(sid uuid.UUID, g int) {
				defer wg.Done()
				<-start
				for r := 0; r < rounds; r++ {
					e, err := svc.Enroll(context.Background(), sid, courseID)
					note("enroll", err)
					if err == nil && (r+g)%2 == 0 {
						note("withdraw", svc.Withdraw(context.Background(), sid, e.ID))
					}
				}
			}(sid, g)
		}
	}
	close(start)
	wg.Wait()

	if bad.Load() != 0 {
		t.Fatalf("unexpected (non-domain) outcomes: %v", badMsgs)
	}
	assertSeatInvariants(t, env, courseID, seats)
}

func TestAdv_ArchivedUnpublishedAndMissingCoursesRejectEnrollment(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	archived := seedCourse(t, env, nil)
	if err := env.DB.Exec(`UPDATE courses SET archived_at = now() WHERE id = ?`, archived).Error; err != nil {
		t.Fatal(err)
	}
	unpublished := seedCourse(t, env, nil)
	if err := env.DB.Exec(`UPDATE courses SET published_version_id = NULL WHERE id = ?`, unpublished).Error; err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		id   uuid.UUID
		want string
	}{
		{"archived", archived, "course_not_published"},
		{"unpublished", unpublished, "course_not_published"},
		{"missing", uuid.New(), "course_not_found"},
		{"nil uuid", uuid.Nil, "course_not_found"},
	}
	for _, c := range cases {
		var wg sync.WaitGroup
		got := make([]string, 10)
		for i := range got {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := svc.Enroll(ctx, newStudentID(), c.id)
				got[i] = errCode(err)
			}(i)
		}
		wg.Wait()
		for _, g := range got {
			if g != c.want {
				t.Errorf("%s: got %q want %q", c.name, g, c.want)
			}
		}
	}
	if n := scalar(t, env, `SELECT count(*) FROM enrollments`); n != 0 {
		t.Fatalf("%d enrollments were created against unenrollable courses", n)
	}
	if n := scalar(t, env, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("%d outbox events leaked from rejected enrollments", n)
	}
}

// A course is archived while 60 students race to enroll. Afterwards (once the
// real relay has processed course_archived) nobody may hold a seat, the
// counter must be zero, and no enrollment may exist that was created after
// the archive committed.
func TestAdv_ArchiveRacingWithEnroll_NoSeatsLeftAfterRelay(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	courseID := seedCourse(t, env, nil)

	const contenders = 60
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[string]int{}
	var archivedAt time.Time
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.Enroll(context.Background(), newStudentID(), courseID)
			mu.Lock()
			codes[errCode(err)]++
			mu.Unlock()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Exactly what course.Service.ArchiveCourse does: archived_at + event, one transaction.
		err := env.DB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(`UPDATE courses SET archived_at = now() WHERE id = ?`, courseID).Error; err != nil {
				return err
			}
			return outbox.Write(context.Background(), tx, outbox.Event{
				AggregateType: "course", AggregateID: courseID, EventType: "course.course_archived",
				SchemaVersion: 1, Payload: map[string]any{"course_id": courseID},
			})
		})
		if err != nil {
			t.Errorf("archive: %v", err)
		}
	}()
	close(start)
	wg.Wait()
	for c := range codes {
		if c != "ok" && c != "course_not_published" {
			t.Fatalf("unexpected outcome during archive race: %v", codes)
		}
	}
	env.DB.Raw(`SELECT archived_at FROM courses WHERE id = ?`, courseID).Row().Scan(&archivedAt)

	rl := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond))
	for _, h := range enrollment.Handlers() {
		rl.Register(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	deadline := time.Now().Add(20 * time.Second)
	for scalar(t, env, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) != 0 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("relay never drained the outbox")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if n := scalar(t, env, `SELECT count(*) FROM enrollments WHERE course_id = ? AND status = 'active'`, courseID); n != 0 {
		t.Fatalf("%d active enrollments remain on an archived course after the archive handler ran", n)
	}
	if n := scalar(t, env, `SELECT enrollment_count FROM courses WHERE id = ?`, courseID); n != 0 {
		t.Fatalf("enrollment_count=%d on an archived, fully-withdrawn course", n)
	}
	if n := scalar(t, env, `SELECT count(*) FROM enrollments WHERE course_id = ? AND status <> 'withdrawn'`, courseID); n != 0 {
		t.Fatalf("%d non-withdrawn rows", n)
	}
	if codes["ok"]+codes["course_not_published"] != contenders {
		t.Fatalf("lost callers: %v", codes)
	}
}

func TestAdv_Prerequisites_UnderConcurrency(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()
	prereq := seedCourse(t, env, nil)
	target := seedCourse(t, env, nil)
	if err := env.DB.Exec(`INSERT INTO course_prerequisites (course_id, prerequisite_id) VALUES (?, ?)`, target, prereq).Error; err != nil {
		t.Fatal(err)
	}

	// 1) Nobody has completed the prerequisite: all 30 concurrent attempts are refused.
	var wg sync.WaitGroup
	var unmet atomic.Int32
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Enroll(ctx, newStudentID(), target); errCode(err) == "prerequisite_not_met" {
				unmet.Add(1)
			}
		}()
	}
	wg.Wait()
	if unmet.Load() != 30 {
		t.Fatalf("expected 30 prerequisite_not_met, got %d", unmet.Load())
	}

	// 2) Active (not completed) or withdrawn prerequisite enrollment does not count.
	active, withdrawn := newStudentID(), newStudentID()
	if _, err := svc.Enroll(ctx, active, prereq); err != nil {
		t.Fatal(err)
	}
	e, err := svc.Enroll(ctx, withdrawn, prereq)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Withdraw(ctx, withdrawn, e.ID); err != nil {
		t.Fatal(err)
	}
	for _, sid := range []uuid.UUID{active, withdrawn} {
		if _, err := svc.Enroll(ctx, sid, target); errCode(err) != "prerequisite_not_met" {
			t.Fatalf("student with non-completed prerequisite got %q", errCode(err))
		}
	}

	// 3) A student who completed it can enroll exactly once under 8 racing attempts.
	done := newStudentID()
	if _, err := svc.Enroll(ctx, done, prereq); err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`UPDATE enrollments SET status = 'completed', completed_at = now() WHERE student_id = ? AND course_id = ?`, done, prereq).Error; err != nil {
		t.Fatal(err)
	}
	var ok, already atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch errCode(func() error { _, err := svc.Enroll(ctx, done, target); return err }()) {
			case "ok":
				ok.Add(1)
			case "already_enrolled":
				already.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || already.Load() != 7 {
		t.Fatalf("expected 1 ok + 7 already_enrolled, got ok=%d already=%d", ok.Load(), already.Load())
	}
	assertSeatInvariants(t, env, target, 0)

	// 4) A completed enrollment still blocks re-enrollment (partial index covers 'completed').
	if _, err := svc.Enroll(ctx, done, prereq); errCode(err) != "already_enrolled" {
		t.Fatalf("re-enrolling after completion should be already_enrolled, got %q", errCode(err))
	}
}

func TestAdv_LimitLoweredBelowCurrentCount(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()
	limit := 10
	courseID := seedCourse(t, env, &limit)

	ids := make([]uuid.UUID, 6)
	enr := make([]uuid.UUID, 6)
	for i := range ids {
		ids[i] = newStudentID()
		e, err := svc.Enroll(ctx, ids[i], courseID)
		if err != nil {
			t.Fatal(err)
		}
		enr[i] = e.ID
	}
	if err := env.DB.Exec(`UPDATE courses SET enrollment_limit = 3 WHERE id = ?`, courseID).Error; err != nil {
		t.Fatal(err)
	}
	// Existing students keep their seats; nobody new gets in while count(6) >= limit(3).
	var wg sync.WaitGroup
	var full atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Enroll(ctx, newStudentID(), courseID); errCode(err) == "course_full" {
				full.Add(1)
			}
		}()
	}
	wg.Wait()
	if full.Load() != 20 {
		t.Fatalf("expected all 20 refused, got %d", full.Load())
	}
	assertSeatInvariants(t, env, courseID, 0)

	// Withdraw 4 -> count 2; exactly one new seat opens (limit 3).
	for i := 0; i < 4; i++ {
		if err := svc.Withdraw(ctx, ids[i], enr[i]); err != nil {
			t.Fatal(err)
		}
	}
	var ok, fullAgain atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch errCode(func() error { _, err := svc.Enroll(ctx, newStudentID(), courseID); return err }()) {
			case "ok":
				ok.Add(1)
			case "course_full":
				fullAgain.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || fullAgain.Load() != 9 {
		t.Fatalf("expected exactly 1 seat to reopen, got ok=%d full=%d", ok.Load(), fullAgain.Load())
	}
	assertSeatInvariants(t, env, courseID, 3)
}

func TestAdv_WithdrawSequentialEdgeCases_NoCountDrift(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()
	courseID := seedCourse(t, env, nil)

	a, b := newStudentID(), newStudentID()
	ea, _ := svc.Enroll(ctx, a, courseID)
	eb, _ := svc.Enroll(ctx, b, courseID)

	if err := svc.Withdraw(ctx, a, ea.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.Withdraw(ctx, a, ea.ID); err != nil { // second (sequential) withdraw is a documented no-op
		t.Fatalf("repeat withdraw should be idempotent: %v", err)
	}
	if got := errCode(svc.Withdraw(ctx, a, eb.ID)); got != "not_your_enrollment" {
		t.Fatalf("withdrawing someone else's enrollment: %q", got)
	}
	if got := errCode(svc.Withdraw(ctx, a, uuid.New())); got != "not_found" {
		t.Fatalf("withdrawing a missing enrollment: %q", got)
	}
	// A completed enrollment cannot be withdrawn (and must not free a seat).
	if err := env.DB.Exec(`UPDATE enrollments SET status='completed', completed_at=now() WHERE id = ?`, eb.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.Withdraw(ctx, b, eb.ID); err != nil {
		t.Fatal(err)
	}
	assertSeatInvariants(t, env, courseID, 0)
}
