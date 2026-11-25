//go:build integration

package course_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// A retry that reclaims the version (same deterministic workflow ID, fresh
// claim time) between the reconciler's read and its release must not be
// released from the old claim.
func TestReconcile_ReclaimedBetweenReadAndRelease_NotOverwritten(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	var vid uuid.UUID
	fake := &reconcileFake{}
	fake.describe = func(string) (enumspb.WorkflowExecutionStatus, error) {
		if err := env.DB.Exec(`UPDATE course_versions SET publish_started_at = now() WHERE id = ?`, vid).Error; err != nil {
			t.Errorf("simulate reclaim: %v", err)
		}
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	_, vid = publishingVersion(t, env, svc, instr, "rec-reclaim")

	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil || res.Released != 0 || res.Lost != 1 {
		t.Fatalf("res=%+v err=%v, want the release to lose", res, err)
	}
	if st, _, _ := versionState(t, env, vid); st != "publishing" {
		t.Fatalf("state = %q, want publishing (new claim)", st)
	}
}
