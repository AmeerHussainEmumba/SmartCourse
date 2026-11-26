//go:build integration

// What this file checks, against real Postgres (RFC §18.2, §18.3): a
// completion event issues exactly one certificate with the course title and
// student name snapshotted; a duplicate, replayed or CONCURRENT delivery never
// produces a second row (UNIQUE (enrollment_id) is the guarantee, the
// relay's processed_events marker the second line); events for unknown or
// withdrawn enrollments are acknowledged with a skip instead of failing; and
// an issued certificate survives a completed enrollment going back to
// `active` (RFC §8.3, assumption A5). Run with `make test-integration`; see
// docs/guides/testing.md.
package certificate_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

type fixture struct {
	studentID    uuid.UUID
	courseID     uuid.UUID
	versionID    uuid.UUID
	enrollmentID uuid.UUID
}

// seedCompletedEnrollment builds a published course titled "Go Concurrency"
// and a completed enrollment for a student whose mirrored name is "Ada
// Lovelace", via raw SQL so this file depends on no other module (RFC §4.4
// applies to test code too).
func seedCompletedEnrollment(t *testing.T, env *testenv.Env) fixture {
	t.Helper()
	return seedEnrollment(t, env, "completed", "Ada Lovelace")
}

// seedEnrollment seeds with the given enrollment status; studentName "" means
// no instructor_mirror row (the name has not been mirrored yet).
func seedEnrollment(t *testing.T, env *testenv.Env, status, studentName string) fixture {
	t.Helper()
	f := fixture{
		studentID:    uuid.Must(uuid.NewV7()),
		courseID:     uuid.Must(uuid.NewV7()),
		versionID:    uuid.Must(uuid.NewV7()),
		enrollmentID: uuid.Must(uuid.NewV7()),
	}
	instructorID := uuid.Must(uuid.NewV7())
	if studentName != "" {
		mustExec(t, env.DB, `INSERT INTO instructor_mirror (user_id, full_name) VALUES (?, ?)`, f.studentID, studentName)
	}
	mustExec(t, env.DB, `INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, ?)`, f.courseID, instructorID, f.courseID.String())
	mustExec(t, env.DB, `INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'Go Concurrency', '{}')`,
		f.versionID, f.courseID)
	mustExec(t, env.DB, `UPDATE courses SET published_version_id = ? WHERE id = ?`, f.versionID, f.courseID)
	switch status {
	case "completed":
		mustExec(t, env.DB, `INSERT INTO enrollments (id, student_id, course_id, status, completed_at) VALUES (?, ?, ?, 'completed', now())`,
			f.enrollmentID, f.studentID, f.courseID)
	case "withdrawn":
		mustExec(t, env.DB, `INSERT INTO enrollments (id, student_id, course_id, status, withdrawn_at) VALUES (?, ?, ?, 'withdrawn', now())`,
			f.enrollmentID, f.studentID, f.courseID)
	default:
		mustExec(t, env.DB, `INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, ?)`,
			f.enrollmentID, f.studentID, f.courseID, status)
	}
	return f
}

func mustExec(t *testing.T, db *gorm.DB, sql string, args ...any) {
	t.Helper()
	if err := db.Exec(sql, args...).Error; err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func countCertificates(t *testing.T, db *gorm.DB, where string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(`SELECT count(*) FROM certificates WHERE `+where, args...).Row().Scan(&n); err != nil {
		t.Fatalf("count certificates: %v", err)
	}
	return n
}

func completedEvent(t *testing.T, enrollmentID, studentID uuid.UUID) relay.DecodedEvent {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "completed_at": time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "enrollment", AggregateID: enrollmentID,
		EventType: "progress.course_completed", SchemaVersion: 1, Payload: payload,
	}
}

// deliver runs the registered handler for evt the way relay.runOne does: in a
// transaction of its own, without the processed_events marker, so these tests
// exercise the database guarantee on its own.
func deliver(t *testing.T, db *gorm.DB, evt relay.DecodedEvent) error {
	t.Helper()
	hs := certificate.Handlers()
	if len(hs) != 1 || !hs[0].EventTypes["progress.course_completed"] {
		t.Fatalf("Handlers() should register exactly one handler for progress.course_completed, got %+v", hs)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		return hs[0].Handle(context.Background(), tx, evt)
	})
}

func TestHandler_IssuesCertificateWithSnapshot(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)

	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	list, err := certificate.NewService(certificate.NewRepository(env.DB)).ListMine(context.Background(), f.studentID)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected exactly 1 certificate, got %d (err %v)", len(list), err)
	}
	c := list[0]
	if c.StudentName != "Ada Lovelace" || c.CourseTitle != "Go Concurrency" {
		t.Fatalf("snapshot wrong: name %q title %q", c.StudentName, c.CourseTitle)
	}
	if c.EnrollmentID != f.enrollmentID || c.CourseID != f.courseID || c.StudentID != f.studentID {
		t.Fatalf("certificate points at the wrong ids: %+v", c)
	}
	if c.CourseVersionID == nil || *c.CourseVersionID != f.versionID {
		t.Fatalf("expected the completed version %s pinned, got %v", f.versionID, c.CourseVersionID)
	}
	if len(c.VerificationCode) != 26 || strings.ToUpper(c.VerificationCode) != c.VerificationCode {
		t.Fatalf("unexpected verification code shape %q", c.VerificationCode)
	}
	if c.IssuedAt.IsZero() {
		t.Fatal("issued_at should default to now()")
	}
}

func TestHandler_SnapshotSurvivesRenameAndRetitle(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	mustExec(t, env.DB, `UPDATE instructor_mirror SET full_name = 'Someone Else' WHERE user_id = ?`, f.studentID)
	mustExec(t, env.DB, `UPDATE course_versions SET title = 'Renamed Course' WHERE id = ?`, f.versionID)

	svc := certificate.NewService(certificate.NewRepository(env.DB))
	list, _ := svc.ListMine(context.Background(), f.studentID)
	if len(list) != 1 || list[0].StudentName != "Ada Lovelace" || list[0].CourseTitle != "Go Concurrency" {
		t.Fatalf("certificate must keep its issue-time snapshot, got %+v", list)
	}
}

func TestHandler_ReplayProducesNoSecondRow(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	evt := completedEvent(t, f.enrollmentID, f.studentID)

	for i := 0; i < 3; i++ {
		if err := deliver(t, env.DB, evt); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("expected 1 certificate after replays, got %d", n)
	}
	var code1 string
	env.DB.Raw(`SELECT verification_code FROM certificates WHERE enrollment_id = ?`, f.enrollmentID).Row().Scan(&code1)

	// A second event with a different event_id for the same enrollment (a
	// producer-side duplicate, which processed_events cannot catch) must
	// also leave the original row untouched.
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("duplicate-with-new-id delivery: %v", err)
	}
	var code2 string
	env.DB.Raw(`SELECT verification_code FROM certificates WHERE enrollment_id = ?`, f.enrollmentID).Row().Scan(&code2)
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 || code1 != code2 {
		t.Fatalf("duplicate event changed state: rows=%d code %q -> %q", n, code1, code2)
	}
}

// TestHandler_TwentyConcurrentDeliveries_ExactlyOneCertificate is the F13
// test: 20 transactions run the handler for the same enrollment at once,
// released together. Without UNIQUE (enrollment_id) (or with a
// check-then-insert that has no constraint behind it) several would pass the
// "does one exist?" read and each insert.
func TestHandler_TwentyConcurrentDeliveries_ExactlyOneCertificate(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	evt := completedEvent(t, f.enrollmentID, f.studentID)

	const n = 20
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- deliver(t, env.DB, evt)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("a concurrent delivery failed (it must be a clean no-op): %v", err)
		}
	}
	if got := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); got != 1 {
		t.Fatalf("expected exactly 1 certificate from %d concurrent deliveries, got %d", n, got)
	}
}

func TestDatabase_UniqueEnrollmentAndCodeFormatAreEnforced(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)

	insert := func(code string) error {
		return env.DB.Exec(`INSERT INTO certificates (id, enrollment_id, student_id, course_id, verification_code, student_name, course_title)
			VALUES (?, ?, ?, ?, ?, 'n', 't')`, uuid.New(), f.enrollmentID, f.studentID, f.courseID, code).Error
	}
	if err := insert("SHORT"); err == nil {
		t.Fatal("a short verification code must violate the format CHECK")
	}
	if err := insert(strings.Repeat("A", 26)); err != nil {
		t.Fatalf("well-formed insert: %v", err)
	}
	if err := insert(strings.Repeat("B", 26)); err == nil {
		t.Fatal("a second certificate for one enrollment must violate UNIQUE (enrollment_id)")
	}
}

func TestHandler_UnknownEnrollment_AcknowledgedNotFailed(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	if err := deliver(t, env.DB, completedEvent(t, uuid.New(), uuid.New())); err != nil {
		t.Fatalf("an event for an unknown enrollment must be acknowledged, got error: %v", err)
	}
	if n := countCertificates(t, env.DB, `true`); n != 0 {
		t.Fatalf("expected no certificates, got %d", n)
	}
}

func TestHandler_WithdrawnEnrollment_AcknowledgedNotIssued(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedEnrollment(t, env, "withdrawn", "Ada Lovelace")
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("an event for a withdrawn enrollment must be acknowledged, got error: %v", err)
	}
	if n := countCertificates(t, env.DB, `true`); n != 0 {
		t.Fatalf("a withdrawn enrollment must not be issued a certificate, got %d", n)
	}
}

func TestHandler_StudentMismatch_AcknowledgedNotIssued(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, uuid.New())); err != nil {
		t.Fatalf("a mismatched student_id must be acknowledged, got error: %v", err)
	}
	if n := countCertificates(t, env.DB, `true`); n != 0 {
		t.Fatalf("expected no certificate for an event naming a different student, got %d", n)
	}
}

func TestHandler_MalformedPayload_ReturnsError(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	for name, payload := range map[string]string{
		"not json":       `{"enrollment_id": `,
		"missing ids":    `{}`,
		"bad uuid":       `{"enrollment_id":"nope","student_id":"nope"}`,
		"only the enrol": `{"enrollment_id":"` + uuid.NewString() + `"}`,
	} {
		evt := relay.DecodedEvent{EventID: uuid.New(), EventType: "progress.course_completed", Payload: json.RawMessage(payload)}
		if err := deliver(t, env.DB, evt); err == nil {
			t.Errorf("%s: expected an error so the relay records the bad event", name)
		}
	}
}

// A student whose name has not been mirrored yet gets an error (retried by
// the relay), never a certificate with a blank or invented name.
func TestHandler_NameNotMirroredYet_ErrorsAndIssuesNothing(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedEnrollment(t, env, "completed", "")
	err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID))
	if err == nil {
		t.Fatal("expected an error while no display name is mirrored")
	}
	if n := countCertificates(t, env.DB, `true`); n != 0 {
		t.Fatalf("no certificate may be issued without a name, got %d", n)
	}

	// Once the mirror catches up, a redelivery succeeds.
	mustExec(t, env.DB, `INSERT INTO instructor_mirror (user_id, full_name) VALUES (?, 'Ada Lovelace')`, f.studentID)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("redelivery after the mirror caught up: %v", err)
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("expected 1 certificate after redelivery, got %d", n)
	}
}

// RFC §8.3/§8.4 (assumption A5): a republish that adds a lesson moves a
// completed student back to incomplete; the certificate they already hold is
// never revoked, and a completion event arriving after that regression is
// still honoured because it reports a fact that was true when it was written.
func TestCertificate_SurvivesCompletionBeingRevoked(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	// Simulate the regression: enrollment goes back to active.
	mustExec(t, env.DB, `UPDATE enrollments SET status = 'active', completed_at = NULL WHERE id = ?`, f.enrollmentID)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("redelivery after regression: %v", err)
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, f.enrollmentID); n != 1 {
		t.Fatalf("issued certificate must remain (exactly one), got %d", n)
	}

	// And an event that is first handled after the regression still issues.
	g := seedEnrollment(t, env, "active", "Grace Hopper")
	if err := deliver(t, env.DB, completedEvent(t, g.enrollmentID, g.studentID)); err != nil {
		t.Fatalf("late event for a regressed enrollment: %v", err)
	}
	if n := countCertificates(t, env.DB, `enrollment_id = ?`, g.enrollmentID); n != 1 {
		t.Fatalf("a completion event handled after the enrollment regressed must still issue, got %d", n)
	}
}

func TestService_GetByIDAndList_OwnerOnly(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	other := seedCompletedEnrollment(t, env)
	for _, x := range []fixture{f, other} {
		if err := deliver(t, env.DB, completedEvent(t, x.enrollmentID, x.studentID)); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	svc := certificate.NewService(certificate.NewRepository(env.DB))
	ctx := context.Background()

	mine, err := svc.ListMine(ctx, f.studentID)
	if err != nil || len(mine) != 1 || mine[0].StudentID != f.studentID {
		t.Fatalf("ListMine must return only the caller's certificates, got %+v (err %v)", mine, err)
	}
	if _, err := svc.GetByID(ctx, f.studentID, mine[0].ID); err != nil {
		t.Fatalf("owner GetByID: %v", err)
	}
	_, errOther := svc.GetByID(ctx, other.studentID, mine[0].ID)
	_, errUnknown := svc.GetByID(ctx, other.studentID, uuid.New())
	if errOther == nil || errUnknown == nil || errOther.Error() != errUnknown.Error() {
		t.Fatalf("non-owner and unknown id must be the same not-found error, got %v vs %v", errOther, errUnknown)
	}
}

func TestService_Verify(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	f := seedCompletedEnrollment(t, env)
	if err := deliver(t, env.DB, completedEvent(t, f.enrollmentID, f.studentID)); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	var code string
	env.DB.Raw(`SELECT verification_code FROM certificates WHERE enrollment_id = ?`, f.enrollmentID).Row().Scan(&code)

	svc := certificate.NewService(certificate.NewRepository(env.DB))
	c, err := svc.Verify(context.Background(), code)
	if err != nil || c.StudentName != "Ada Lovelace" {
		t.Fatalf("Verify(valid): %+v, %v", c, err)
	}
	if _, err := svc.Verify(context.Background(), strings.Repeat("A", 26)); err == nil {
		t.Fatal("unknown well-formed code must be not found")
	}
	if _, err := svc.Verify(context.Background(), "abc"); err == nil {
		t.Fatal("malformed code must be rejected")
	}
}
