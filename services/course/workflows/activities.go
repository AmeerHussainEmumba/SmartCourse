package workflows

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// Activities wraps services/course/course's Service with Temporal-callable
// methods, one per node in RFC §7.4's activity graph (see
// course.Service's "--- publish ---" comment for the full picture). Each
// method here does nothing but forward — the actual business logic lives
// on Service so it's exercised identically whether Temporal calls it (M2)
// or a future test calls it directly, and so services/course/course stays free of
// a Temporal SDK import (RFC §4.4's module-boundary discipline: course must
// not know this package exists, since the dependency can only run one way
// — see publishing.WorkflowName's doc comment).
type Activities struct {
	svc *course.Service
}

func NewActivities(svc *course.Service) *Activities { return &Activities{svc: svc} }

// failure is how every publish activity reports an error (S-08). The full
// error goes to the server log; what Temporal carries to failPublish, and
// from there to course_versions.publish_error, the course.publish_failed
// reason and the instructor's email, is a course.PublishFailureText: a fixed
// category, never raw error text (a storage SDK message with a host, a driver
// error, a lesson title). The error stays retryable, as before.
func failure(ctx context.Context, activity string, err error) error {
	if err == nil {
		return nil
	}
	slog.ErrorContext(ctx, "publish activity failed", slog.String("activity", activity), slog.Any("error", err))
	return temporal.NewApplicationError(course.PublishFailureText(err), course.PublishFailureType)
}

func (a *Activities) ValidateMetadata(ctx context.Context, versionID uuid.UUID) error {
	return failure(ctx, "ValidateMetadata", a.svc.ValidateMetadata(ctx, versionID))
}

func (a *Activities) VerifyAssets(ctx context.Context, versionID uuid.UUID) error {
	return failure(ctx, "VerifyAssets", a.svc.VerifyAssets(ctx, versionID))
}

func (a *Activities) ComputeMetadata(ctx context.Context, versionID uuid.UUID) error {
	return failure(ctx, "ComputeMetadata", a.svc.ComputeMetadata(ctx, versionID))
}

func (a *Activities) BuildOutline(ctx context.Context, courseID, versionID uuid.UUID) (*publishing.PublishOutline, error) {
	outline, err := a.svc.BuildOutline(ctx, courseID, versionID)
	return outline, failure(ctx, "BuildOutline", err)
}

// IndexSearch and WarmCache take publishing.PublishOutline by value, not
// pointer: Temporal's data converter marshals activity arguments to JSON
// for workflow history, and workflow.ExecuteActivity's own call already
// passed BuildOutline's *publishing.PublishOutline result across that same
// boundary once — a pointer here would just be a second indirection for
// data that's already copied.
func (a *Activities) IndexSearch(ctx context.Context, outline publishing.PublishOutline) error {
	return failure(ctx, "IndexSearch", a.svc.IndexSearch(ctx, &outline))
}

func (a *Activities) WarmCache(ctx context.Context, outline publishing.PublishOutline) error {
	return failure(ctx, "WarmCache", a.svc.WarmCache(ctx, &outline))
}

func (a *Activities) PromoteAndPublish(ctx context.Context, courseID, versionID uuid.UUID, oldVersionID *uuid.UUID, title string) error {
	return failure(ctx, "PromoteAndPublish", a.svc.PromoteAndPublish(ctx, courseID, versionID, oldVersionID, title))
}

// MarkPublishFailed takes the failed activity's name as its last argument for
// course.publish_failed. Workflows already running when this was added were
// scheduled with two arguments; see the README's deployment note.
func (a *Activities) MarkPublishFailed(ctx context.Context, versionID uuid.UUID, reason, failedActivity string) error {
	return a.svc.MarkPublishFailed(ctx, versionID, reason, failedActivity)
}
