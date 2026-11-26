// Workflow Issues, the ninth FR-5 metric (DR-024, DR-026). The workflow
// engine's failures are course-schema facts this module may not read
// (ADR-0024), so they arrive as course.publish_failed events and are mirrored
// here (migration 000004): publish_failures holds one row per failed publish,
// workflow_issue_stats the per-workflow count GET /analytics/issues reports.
//
// What the number is: the count of publish attempts that ended in
// publish_failed since the event existed. It is a cumulative count of failures,
// not of versions currently stuck: a version that failed twice counts twice
// and one that later published successfully still counts. Failures before the
// event existed are not counted (there is nothing this schema may read to
// backfill them).
package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// PublishWorkflow is the workflow name course.publish_failed counts under in
// workflow_issue_stats: the Temporal type of the course publishing workflow
// (services/course/course.PublishWorkflowName). The event carries no workflow
// name because publishing is the only workflow that emits one; a second
// workflow's failure event would add its own constant here.
const PublishWorkflow = "PublishCourseWorkflow"

// PublishFailure is one course.publish_failed event as mirrored.
type PublishFailure struct {
	EventID        uuid.UUID
	CourseID       uuid.UUID
	VersionID      uuid.UUID
	VersionNumber  int
	FailedActivity string
	Reason         string
	FailedAt       time.Time
}

func handlePublishFailed(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID       uuid.UUID `json:"course_id"`
		VersionID      uuid.UUID `json:"version_id"`
		VersionNumber  int       `json:"version_number"`
		FailedActivity string    `json:"failed_activity"`
		Reason         string    `json:"reason"`
		FailedAt       time.Time `json:"failed_at"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.publish_failed payload: %w", err)
	}
	if payload.CourseID == uuid.Nil || payload.VersionID == uuid.Nil {
		return fmt.Errorf("course.publish_failed payload (outbox %d) is missing course_id or version_id", evt.OutboxID)
	}
	failedAt := payload.FailedAt
	if failedAt.IsZero() {
		failedAt = evt.OccurredAt
	}
	return RecordPublishFailure(ctx, tx, PublishWorkflow, PublishFailure{
		EventID: evt.EventID, CourseID: payload.CourseID, VersionID: payload.VersionID,
		VersionNumber: payload.VersionNumber, FailedActivity: payload.FailedActivity,
		Reason: payload.Reason, FailedAt: failedAt,
	})
}

// RecordPublishFailure mirrors one failed publish and recomputes the
// workflow's count from the mirror, in the caller's transaction. The count row
// is locked BEFORE the insert and the recompute, so two handlers recording
// failures at once serialise: the second counts the first's committed row
// instead of overwriting its total with a stale one (the rule
// RecomputeCourseStats follows). A redelivered event inserts nothing (event_id
// is the key) and recomputes the same number.
func RecordPublishFailure(ctx context.Context, tx *gorm.DB, workflow string, f PublishFailure) error {
	if err := lockWorkflowIssueStats(ctx, tx, workflow); err != nil {
		return err
	}
	if err := tx.WithContext(ctx).Exec(`
		INSERT INTO publish_failures (event_id, workflow, course_id, version_id, version_number, failed_activity, reason, failed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (event_id) DO NOTHING
	`, f.EventID, workflow, f.CourseID, f.VersionID, f.VersionNumber, f.FailedActivity, f.Reason, f.FailedAt).Error; err != nil {
		return err
	}
	return recomputeWorkflowIssueStats(ctx, tx, workflow)
}

func lockWorkflowIssueStats(ctx context.Context, tx *gorm.DB, workflow string) error {
	if err := tx.WithContext(ctx).Exec(`
		INSERT INTO workflow_issue_stats (workflow) VALUES (?) ON CONFLICT (workflow) DO NOTHING
	`, workflow).Error; err != nil {
		return err
	}
	return tx.WithContext(ctx).Exec(`SELECT 1 FROM workflow_issue_stats WHERE workflow = ? FOR UPDATE`, workflow).Error
}

// recomputeWorkflowIssueStats sets one workflow's count to the number of
// mirrored failures. The caller holds the row lock.
func recomputeWorkflowIssueStats(ctx context.Context, tx *gorm.DB, workflow string) error {
	return tx.WithContext(ctx).Exec(`
		UPDATE workflow_issue_stats
		SET failed = (SELECT count(*) FROM publish_failures WHERE workflow = ?), updated_at = now()
		WHERE workflow = ?
	`, workflow, workflow).Error
}

// ReconcileWorkflowIssueStats recomputes every workflow's count from the
// mirror, creating a count row for a workflow that has mirrored failures but
// none, so a hand-edited or lost count heals within one reconcile interval.
// Each workflow is recomputed in its own transaction under its row lock.
func ReconcileWorkflowIssueStats(ctx context.Context, db *gorm.DB) error {
	var workflows []string
	if err := db.WithContext(ctx).Raw(`
		SELECT workflow FROM workflow_issue_stats UNION SELECT workflow FROM publish_failures ORDER BY 1
	`).Scan(&workflows).Error; err != nil {
		return fmt.Errorf("list workflows to reconcile: %w", err)
	}
	var errs []error
	for _, w := range workflows {
		if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := lockWorkflowIssueStats(ctx, tx, w); err != nil {
				return err
			}
			return recomputeWorkflowIssueStats(ctx, tx, w)
		}); err != nil {
			errs = append(errs, fmt.Errorf("reconcile workflow issues for %s: %w", w, err))
		}
	}
	return errors.Join(errs...)
}

// workflowIssuesSource is the WorkflowIssuesSource over this module's own
// workflow_issue_stats: a read of its own schema, no cross-schema query.
type workflowIssuesSource struct{ db *gorm.DB }

// NewWorkflowIssuesSource returns the WorkflowIssuesSource analytics-api attaches
// to GET /analytics/issues.
func NewWorkflowIssuesSource(db *gorm.DB) WorkflowIssuesSource { return workflowIssuesSource{db} }

// SummarizeWorkflowIssues reports the failed-publish count per workflow and
// its total. A database error is wrapped in ErrSourceUnavailable: the route
// answers 503, not a made-up zero.
func (s workflowIssuesSource) SummarizeWorkflowIssues(ctx context.Context) (*WorkflowIssuesSummary, error) {
	var rows []struct {
		Workflow string
		Failed   int64
	}
	if err := s.db.WithContext(ctx).Raw(`SELECT workflow, failed FROM workflow_issue_stats`).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("%w: read workflow_issue_stats: %w", ErrSourceUnavailable, err)
	}
	sum := &WorkflowIssuesSummary{ByWorkflow: map[string]int64{}}
	for _, r := range rows {
		if r.Failed == 0 {
			continue
		}
		sum.Failed += r.Failed
		sum.ByWorkflow[r.Workflow] = r.Failed
	}
	return sum, nil
}
