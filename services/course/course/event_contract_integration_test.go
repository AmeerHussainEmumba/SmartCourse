//go:build integration

// What this file checks, against real Postgres: every course event the course
// service writes to its outbox (course_published, course_archived,
// publish_failed), produced by the real service methods, conforms to the
// Protobuf contract in libs/events. course_published and course_archived are
// written from maps, so unlike PublishFailedPayload there is no struct for
// the unit test to inspect; the real row is the only place a renamed key
// shows up. See services/course/progress/payload_contract_test.go.
package course_test

import (
	"context"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestEventContract_CourseOutboxRowsConformToLibsEvents(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)

	// course_published.
	pubID, draft := newCourse(t, svc, instr, "contract-pub", "Contract Pub")
	authorDraft(t, svc, pubID, instr, 1, 1)
	if err := svc.PromoteAndPublish(ctx, pubID, draft, nil, "Contract Pub"); err != nil {
		t.Fatal(err)
	}
	// course_archived.
	arcID, _ := newCourse(t, svc, instr, "contract-arc", "Contract Arc")
	if err := svc.ArchiveCourse(ctx, arcID, instr); err != nil {
		t.Fatal(err)
	}
	// publish_failed.
	_, failDraft := newCourse(t, svc, instr, "contract-fail", "Contract Fail")
	if err := svc.MarkPublishFailed(ctx, failDraft, "asset not found", "VerifyAssets"); err != nil {
		t.Fatal(err)
	}

	rows, err := env.DB.Raw(`SELECT event_type, payload FROM outbox ORDER BY id`).Rows()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	seen := map[string]int{}
	for rows.Next() {
		var typ string
		var raw []byte
		if err := rows.Scan(&typ, &raw); err != nil {
			t.Fatal(err)
		}
		eventstest.RequireConforms(t, typ, raw)
		seen[typ]++
	}
	for _, typ := range []string{"course.course_published", "course.course_archived", "course.publish_failed"} {
		if seen[typ] == 0 {
			t.Errorf("no %s row was written, so its contract was not checked (saw %v)", typ, seen)
		}
	}
}
