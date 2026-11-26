//go:build integration

// What this file checks, against real Postgres: the enrollment events the
// service writes (student_enrolled, student_withdrawn) conform to the Protobuf
// contract in libs/events. They are written from maps, so the real outbox row
// is the only place a renamed key shows up. See
// services/course/progress/payload_contract_test.go.
package enrollment_test

import (
	"context"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func TestEventContract_EnrollmentOutboxRowsConformToLibsEvents(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()
	courseID := seedCourse(t, env, nil)
	studentID := seedStudent(t, env)

	e, err := svc.Enroll(ctx, studentID, courseID)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Withdraw(ctx, studentID, e.ID); err != nil {
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
	for _, typ := range []string{"enrollment.student_enrolled", "enrollment.student_withdrawn"} {
		if seen[typ] == 0 {
			t.Errorf("no %s row was written, so its contract was not checked (saw %v)", typ, seen)
		}
	}
}
