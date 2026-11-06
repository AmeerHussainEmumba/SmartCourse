package workflows

import (
	"context"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
)

// Activities wraps internal/course's Service with Temporal-callable
// methods, one per node in RFC §7.4's activity graph (see
// course.Service's "--- publish ---" comment for the full picture). Each
// method here does nothing but forward — the actual business logic lives
// on Service so it's exercised identically whether Temporal calls it (M2)
// or a future test calls it directly, and so internal/course stays free of
// a Temporal SDK import (RFC §4.4's module-boundary discipline: course must
// not know this package exists, since the dependency can only run one way
// — see course.PublishWorkflowName's doc comment).
type Activities struct {
	svc *course.Service
}

func NewActivities(svc *course.Service) *Activities { return &Activities{svc: svc} }

func (a *Activities) ValidateMetadata(ctx context.Context, versionID uuid.UUID) error {
	return a.svc.ValidateMetadata(ctx, versionID)
}

func (a *Activities) VerifyAssets(ctx context.Context, versionID uuid.UUID) error {
	return a.svc.VerifyAssets(ctx, versionID)
}

func (a *Activities) ComputeMetadata(ctx context.Context, versionID uuid.UUID) error {
	return a.svc.ComputeMetadata(ctx, versionID)
}

func (a *Activities) BuildOutline(ctx context.Context, courseID, versionID uuid.UUID) (*course.PublishOutline, error) {
	return a.svc.BuildOutline(ctx, courseID, versionID)
}

// IndexSearch and WarmCache take course.PublishOutline by value, not
// pointer: Temporal's data converter marshals activity arguments to JSON
// for workflow history, and workflow.ExecuteActivity's own call already
// passed BuildOutline's *course.PublishOutline result across that same
// boundary once — a pointer here would just be a second indirection for
// data that's already copied.
func (a *Activities) IndexSearch(ctx context.Context, outline course.PublishOutline) error {
	return a.svc.IndexSearch(ctx, &outline)
}

func (a *Activities) WarmCache(ctx context.Context, outline course.PublishOutline) error {
	return a.svc.WarmCache(ctx, &outline)
}

func (a *Activities) PromoteAndPublish(ctx context.Context, courseID, versionID uuid.UUID, oldVersionID *uuid.UUID, title string) error {
	return a.svc.PromoteAndPublish(ctx, courseID, versionID, oldVersionID, title)
}

func (a *Activities) MarkPublishFailed(ctx context.Context, versionID uuid.UUID, reason string) error {
	return a.svc.MarkPublishFailed(ctx, versionID, reason)
}
