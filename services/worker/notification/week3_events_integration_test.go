//go:build integration

// What this file checks, against real Postgres: the two consumers added for
// the Week 3 event gaps. progress.certificate_issued mails the student and
// course.publish_failed mails the instructor; each writes ONE delivery per
// event however it is delivered, renders safely (HTML escaping, no header
// injection through the course title or the failure reason), parks for an
// unknown recipient, and reaches the provider exactly once.
package notification

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func sweepAndDeliver(t *testing.T, env *testenv.Env, prov Provider) {
	t.Helper()
	enq := &memEnqueuer{}
	if _, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil {
		t.Fatal(err)
	}
	handle := NewDeliveryHandler(env.DB, prov)
	for _, task := range enq.drain() {
		if err := handle(context.Background(), task); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
}

func messagesWithSubject(prov *recordingProvider, part string) []Message {
	var out []Message
	for _, m := range prov.sends() {
		if strings.Contains(m.Subject, part) {
			out = append(out, m)
		}
	}
	return out
}

func TestCertificateIssued_MailsTheStudentOnce(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	e := evt("progress.certificate_issued", 2, map[string]any{
		"certificate_id": uuid.New(), "enrollment_id": uuid.New(), "student_id": student,
		"course_id": uuid.New(), "course_title": "Go <b>Concurrency</b>\r\nBcc: victim@evil.test",
		"issued_at": "2026-11-24T10:00:00Z",
	})

	// Repeated and concurrent delivery of one event: one delivery row.
	mustApply(t, env.DB, e)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = apply(env.DB, e) }()
	}
	wg.Wait()
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE event_id = ? AND kind = ?`, e.EventID, KindCertificateIssued); n != 1 {
		t.Fatalf("certificate deliveries for one event: %d, want 1", n)
	}

	prov := &recordingProvider{}
	sweepAndDeliver(t, env, prov)
	sweepAndDeliver(t, env, prov) // nothing left to send
	mails := messagesWithSubject(prov, "certificate")
	if len(mails) != 1 {
		t.Fatalf("certificate mails sent: %d, want 1 (sends: %+v)", len(mails), prov.sends())
	}
	m := mails[0]
	if m.To != "ada@example.com" || strings.ContainsAny(m.Subject, "\r\n") {
		t.Errorf("recipient %q subject %q", m.To, m.Subject)
	}
	if strings.Contains(m.HTML, "<b>") || !strings.Contains(m.HTML, "&lt;b&gt;") {
		t.Errorf("title not escaped in HTML: %s", m.HTML)
	}
	if strings.Contains(m.Text, "\nBcc:") {
		t.Errorf("title kept its line break in the text part: %q", m.Text)
	}
}

func TestCertificateIssued_UnknownStudentIsParkedThenDelivered(t *testing.T) {
	env := testenv.Setup(t)
	student := uuid.New()
	mustApply(t, env.DB, evt("progress.certificate_issued", 1, map[string]any{
		"certificate_id": uuid.New(), "enrollment_id": uuid.New(), "student_id": student,
		"course_id": uuid.New(), "course_title": "Go", "issued_at": "2026-11-24T10:00:00Z",
	}))
	if d := deliveries(t, env.DB); len(d) != 1 || d[0].Status != statusParked || d[0].Kind != KindCertificateIssued {
		t.Fatalf("deliveries = %+v, want one parked certificate_issued", d)
	}
	register(t, env.DB, student, "late@example.com", "Late", 2)
	prov := &recordingProvider{}
	sweepAndDeliver(t, env, prov)
	if got := messagesWithSubject(prov, "certificate"); len(got) != 1 || got[0].To != "late@example.com" {
		t.Fatalf("after the recipient arrived: %+v", prov.sends())
	}
}

func TestCertificateIssued_MalformedPayloadIsAnError(t *testing.T) {
	env := testenv.Setup(t)
	if err := apply(env.DB, evt("progress.certificate_issued", 1, map[string]any{"student_id": 7})); err == nil {
		t.Error("a payload that does not decode must be an error")
	}
	if err := apply(env.DB, evt("progress.certificate_issued", 2, map[string]any{"course_title": "x"})); err == nil {
		t.Error("a payload without a student must be an error")
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`); n != 0 {
		t.Errorf("a rejected event wrote %d deliveries", n)
	}
}

func TestPublishFailed_MailsTheInstructorOnce(t *testing.T) {
	env := testenv.Setup(t)
	instructor := uuid.New()
	register(t, env.DB, instructor, "grace@example.com", "Grace", 1)
	e := evt("course.publish_failed", 2, map[string]any{
		"course_id": uuid.New(), "version_id": uuid.New(), "version_number": 2,
		"title": "Go Concurrency", "instructor_id": instructor,
		"failed_activity": "VerifyAssets", "reason": "asset <script>alert(1)</script> missing\r\nBcc: victim@evil.test",
		"failed_at": "2026-11-24T10:00:00Z",
	})
	for range 3 {
		mustApply(t, env.DB, e)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries WHERE event_id = ? AND kind = ?`, e.EventID, KindPublishFailed); n != 1 {
		t.Fatalf("publish_failed deliveries for one event: %d, want 1", n)
	}
	prov := &recordingProvider{}
	sweepAndDeliver(t, env, prov)
	mails := messagesWithSubject(prov, "failed")
	if len(mails) != 1 {
		t.Fatalf("publish_failed mails: %d, want 1 (sends: %+v)", len(mails), prov.sends())
	}
	m := mails[0]
	if m.To != "grace@example.com" || !strings.Contains(m.Subject, "Go Concurrency") || strings.ContainsAny(m.Subject, "\r\n") {
		t.Errorf("recipient %q subject %q", m.To, m.Subject)
	}
	if !strings.Contains(m.Text, "VerifyAssets") || !strings.Contains(m.Text, "missing") {
		t.Errorf("the mail does not say what failed: %q", m.Text)
	}
	if strings.Contains(m.HTML, "<script>") || !strings.Contains(m.HTML, "&lt;script&gt;") {
		t.Errorf("reason not escaped in HTML: %s", m.HTML)
	}
	if strings.Contains(m.Text, "\nBcc:") {
		t.Errorf("reason kept its line break: %q", m.Text)
	}
}

func TestPublishFailed_WithoutAnInstructorWritesNoDelivery(t *testing.T) {
	env := testenv.Setup(t)
	mustApply(t, env.DB, evt("course.publish_failed", 1, map[string]any{
		"course_id": uuid.New(), "version_id": uuid.New(), "version_number": 1, "title": "T", "reason": "x",
	}))
	if n := count(t, env.DB, `SELECT count(*) FROM notification.deliveries`); n != 0 {
		t.Fatalf("deliveries = %d, want 0 when the event names no instructor", n)
	}
	if err := apply(env.DB, evt("course.publish_failed", 2, map[string]any{"course_id": 5})); err == nil {
		t.Error("a payload that does not decode must be an error")
	}
}
