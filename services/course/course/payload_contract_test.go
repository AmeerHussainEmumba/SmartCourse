// What this file checks: PublishFailedPayload (events.go) conforms to
// course.v1.CoursePublishFailed in libs/events. See
// services/course/progress/payload_contract_test.go for why a producer test
// may import libs/events although the production code does not. The other
// course events (course_published, course_archived) are written from maps;
// event_contract_integration_test.go checks them from the real outbox rows.
package course_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

func TestPayloadContract_PublishFailed(t *testing.T) {
	raw, err := json.Marshal(course.PublishFailedPayload{
		CourseID: uuid.New(), VersionID: uuid.New(), VersionNumber: 2, Title: "Intro",
		InstructorID: uuid.New(), FailedActivity: "PromoteAndPublish", Reason: "internal_error",
		FailedAt: time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	eventstest.RequireConforms(t, course.EventPublishFailed, raw)
}
