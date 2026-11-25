package progress

import (
	"time"

	"github.com/google/uuid"
)

// Event types written to the outbox by this module. The payload structs below
// are the producing side of the contracts in libs/events (progress.v1): the
// JSON field names are the proto field names, which the transport decodes
// strictly. a payload contract test (lost in the split, see services/course/progress/README.md) used to keep the two in step
// (this module does not import libs/events, so course-api does not link
// Protobuf).
const (
	EventLessonCompleted = "progress.lesson_completed"
	EventCourseCompleted = "progress.course_completed"
)

// LessonCompletedPayload is progress.v1.LessonCompleted.
type LessonCompletedPayload struct {
	EnrollmentID uuid.UUID `json:"enrollment_id"`
	StudentID    uuid.UUID `json:"student_id"`
	LessonKey    uuid.UUID `json:"lesson_key"`
	CompletedAt  time.Time `json:"completed_at"`
}

// CourseCompletedPayload is progress.v1.CourseCompleted. completed_at lets
// analytics.enrollment_mirror carry its own completed_at without querying
// course.enrollments for it (ADR-0024).
type CourseCompletedPayload struct {
	EnrollmentID uuid.UUID `json:"enrollment_id"`
	StudentID    uuid.UUID `json:"student_id"`
	CompletedAt  time.Time `json:"completed_at"`
}
