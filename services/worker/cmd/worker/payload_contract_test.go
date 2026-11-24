package main

// Payload contract: the producing modules (services/course/{progress,
// certificate,course}) do not import libs/events, so nothing in their own tests
// can notice a payload field that the Protobuf schema does not have, or one
// that was renamed. The transport decodes an outbox payload STRICTLY into the
// schema (a field the schema lacks makes the event unencodable and the relay
// gives up on it). These tests run each producer's real payload struct through
// that exact codec and read the typed message back, so a drift fails here, in
// CI, rather than as an unencodable event in production.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	coursev1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/course/v1"
	progressv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/progress/v1"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
)

func contractCodec() *transport.Codec {
	s := transport.Schemas{EnvelopeIDs: map[string]uint32{}, PayloadVersions: map[string]int32{}}
	for i, topic := range events.Topics() {
		s.EnvelopeIDs[topic] = uint32(i + 1)
	}
	for _, ty := range events.Types() {
		s.PayloadVersions[ty.EventType] = 1
	}
	return transport.NewCodec(s)
}

// encodeAndOpen encodes payload as the relay would carry it and returns the
// typed message the consumers' schema reads back.
func encodeAndOpen(t *testing.T, eventType string, payload any) proto.Message {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	msg, err := contractCodec().Encode(relay.DecodedEvent{
		OutboxID: 1, EventID: uuid.New(), AggregateType: "enrollment", AggregateID: id,
		EventType: eventType, SchemaVersion: 1, Payload: raw, OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("%s: the producer's payload does not fit its schema: %v", eventType, err)
	}
	var env events.Envelope
	if _, err := events.DecodeInto(msg.Value, &env); err != nil {
		t.Fatal(err)
	}
	out, err := events.OpenPayload(&env)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPayloadContract_ProducersFitTheirSchemas(t *testing.T) {
	enrollment, student, lesson, cert, courseID, version, instructor := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	at := time.Date(2026, 11, 24, 10, 0, 0, 123456000, time.UTC)

	t.Run("progress.lesson_completed", func(t *testing.T) {
		got := encodeAndOpen(t, progress.EventLessonCompleted, progress.LessonCompletedPayload{
			EnrollmentID: enrollment, StudentID: student, LessonKey: lesson, CompletedAt: at,
		}).(*progressv1.LessonCompleted)
		if got.EnrollmentId != enrollment.String() || got.StudentId != student.String() || got.LessonKey != lesson.String() || !got.CompletedAt.AsTime().Equal(at) {
			t.Errorf("round trip lost data: %v", got)
		}
	})
	t.Run("progress.course_completed", func(t *testing.T) {
		got := encodeAndOpen(t, progress.EventCourseCompleted, progress.CourseCompletedPayload{
			EnrollmentID: enrollment, StudentID: student, CompletedAt: at,
		}).(*progressv1.CourseCompleted)
		if got.EnrollmentId != enrollment.String() || got.StudentId != student.String() || !got.CompletedAt.AsTime().Equal(at) {
			t.Errorf("round trip lost data: %v", got)
		}
	})
	t.Run("progress.certificate_issued", func(t *testing.T) {
		got := encodeAndOpen(t, certificate.EventCertificateIssued, certificate.CertificateIssuedPayload{
			CertificateID: cert, EnrollmentID: enrollment, StudentID: student, CourseID: courseID, CourseTitle: "Go", IssuedAt: at,
		}).(*progressv1.CertificateIssued)
		if got.CertificateId != cert.String() || got.EnrollmentId != enrollment.String() || got.StudentId != student.String() ||
			got.CourseId != courseID.String() || got.CourseTitle != "Go" || !got.IssuedAt.AsTime().Equal(at) {
			t.Errorf("round trip lost data: %v", got)
		}
	})
	t.Run("course.publish_failed", func(t *testing.T) {
		got := encodeAndOpen(t, course.EventPublishFailed, course.PublishFailedPayload{
			CourseID: courseID, VersionID: version, VersionNumber: 3, Title: "Go", InstructorID: instructor,
			FailedActivity: "VerifyAssets", Reason: "asset missing", FailedAt: at,
		}).(*coursev1.CoursePublishFailed)
		if got.CourseId != courseID.String() || got.VersionId != version.String() || got.VersionNumber != 3 || got.Title != "Go" ||
			got.InstructorId != instructor.String() || got.FailedActivity != "VerifyAssets" || got.Reason != "asset missing" || !got.FailedAt.AsTime().Equal(at) {
			t.Errorf("round trip lost data: %v", got)
		}
	})
}
