//go:build integration

package schemaisolation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// analytics-api runs with POSTGRES_SEARCH_PATH=analytics,public: not even
// platform is visible. Its handlers (run by the worker) and its read service
// must both work on that.
func TestSchemaIsolation_AnalyticsHandlersAndReadsOnOwnSchemaOnly(t *testing.T) {
	env := testenv.Setup(t)
	conn := env.ServiceDB(t, "analytics") // svc_analytics, search_path analytics,public
	ctx := context.Background()

	for _, q := range []string{`SELECT 1 FROM courses`, `SELECT 1 FROM users`, `SELECT 1 FROM outbox`, `SELECT 1 FROM enrollments`} {
		if err := conn.Exec(q).Error; err == nil {
			t.Fatalf("%q succeeded on an analytics-only search_path; isolation harness is broken", q)
		}
	}

	deliver := func(id int64, typ string, payload map[string]any) {
		t.Helper()
		raw, _ := json.Marshal(payload)
		for _, h := range analytics.Handlers() {
			if !h.EventTypes[typ] {
				continue
			}
			if err := conn.Transaction(func(tx *gorm.DB) error {
				return h.Handle(ctx, tx, relay.DecodedEvent{OutboxID: id, EventID: uuid.New(), EventType: typ, Payload: raw})
			}); err != nil {
				t.Fatalf("%s on analytics-only path: %v", typ, err)
			}
		}
	}
	course, enr, stu, usr := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Add(-time.Hour)
	deliver(1, "identity.user_registered", map[string]any{"user_id": usr, "role": "student"})
	deliver(2, "course.course_published", map[string]any{"course_id": course})
	deliver(3, "enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": at})
	deliver(4, "progress.course_completed", map[string]any{"enrollment_id": enr, "student_id": stu, "completed_at": at.Add(time.Minute)})
	e2 := uuid.New()
	deliver(5, "enrollment.student_enrolled", map[string]any{"enrollment_id": e2, "student_id": uuid.New(), "course_id": course, "enrolled_at": at})
	deliver(6, "enrollment.student_withdrawn", map[string]any{"enrollment_id": e2, "course_id": course})

	svc := analytics.NewService(analytics.NewRepository(conn))
	snap, err := svc.GetPlatformSnapshot(ctx)
	if err != nil || snap.TotalStudents != 1 || snap.TotalCoursesPublished != 1 {
		t.Fatalf("snapshot: %v %+v", err, snap)
	}
	st, err := svc.GetCourseStats(ctx, course)
	if err != nil || st.TotalEnrollments != 1 || st.Completions != 1 {
		t.Fatalf("stats: %v %+v", err, st)
	}
	days, err := svc.ListDailyEnrollments(ctx, course)
	if err != nil || len(days) == 0 {
		t.Fatalf("daily: %v %+v", err, days)
	}
}
