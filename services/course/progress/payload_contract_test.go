// What this file checks: the producer side of the event contract. The payload
// structs in events.go are encoded as JSON by libs/outbox and decoded strictly
// by the relay's codec against the Protobuf messages in libs/events
// (progress.v1). This module's production code does not import libs/events (so
// course-api does not link Protobuf); this TEST may, because a _test import is
// not linked into any binary (scripts/check-link-graph.sh). Renaming a JSON
// tag, adding a field or dropping one fails here, in unit tests, with no
// Docker. It replaces the payload-contract test that lived in the retired
// services/worker.
package progress_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

func TestPayloadContract_ProgressEvents(t *testing.T) {
	now := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		eventType string
		payload   any
	}{
		{progress.EventLessonCompleted, progress.LessonCompletedPayload{
			EnrollmentID: uuid.New(), StudentID: uuid.New(), LessonKey: uuid.New(), CompletedAt: now}},
		{progress.EventCourseCompleted, progress.CourseCompletedPayload{
			EnrollmentID: uuid.New(), StudentID: uuid.New(), CompletedAt: now}},
	}
	for _, c := range cases {
		raw, err := json.Marshal(c.payload)
		if err != nil {
			t.Fatal(err)
		}
		eventstest.RequireConforms(t, c.eventType, raw)
	}
}
