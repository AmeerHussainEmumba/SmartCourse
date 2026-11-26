//go:build integration

package course_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// course.course_published carries instructor_id (additive; unblocks
// instructor ownership in analytics without a cross-schema read, ADR-0024).
func TestCoursePublishedPayload_CarriesInstructorID(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "payload-instr", "Payload")
	authorDraft(t, svc, courseID, instr, 1, 1)

	if err := svc.PromoteAndPublish(context.Background(), courseID, draft, nil, "Payload"); err != nil {
		t.Fatal(err)
	}

	var raw []byte
	if err := env.DB.Raw(`SELECT payload FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID).
		Row().Scan(&raw); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var got struct {
		CourseID     uuid.UUID `json:"course_id"`
		VersionID    uuid.UUID `json:"version_id"`
		Title        string    `json:"title"`
		InstructorID uuid.UUID `json:"instructor_id"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if got.InstructorID != instr {
		t.Fatalf("instructor_id = %s, want %s (payload %s)", got.InstructorID, instr, raw)
	}
	if got.CourseID != courseID || got.VersionID != draft || got.Title != "Payload" {
		t.Fatalf("existing fields changed: %s", raw)
	}

	// Backward compatibility: the analytics handler decodes this event with
	// json.Unmarshal into a struct holding only course_id
	// (services/analytics/analytics/handlers.go handleCoursePublished), which
	// ignores unknown fields. This mirrors that decode so a switch to strict
	// decoding there, or a type change here, shows up. The end-to-end check
	// that the real handler still processes the event lives with that module.
	var oldConsumer struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(raw, &oldConsumer); err != nil || oldConsumer.CourseID != courseID {
		t.Fatalf("old consumer decode: id=%s err=%v", oldConsumer.CourseID, err)
	}
}
