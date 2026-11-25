package course

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"gorm.io/gorm"
)

// publishReconcileGrace is how old a `publishing` claim must be before
// ReconcileStalePublishes asks Temporal about it. Service.Publish starts the
// workflow immediately after claiming, so anything older than this that
// Temporal has never heard of belongs to a process that died in between. It is
// deliberately much shorter than publishClaimStaleAfter (15 minutes), which
// stays as the backstop for when reconciliation is not running (ADR-0028).
const publishReconcileGrace = time.Minute

// reconcileBatch bounds one reconciliation pass; leftovers wait for the next.
const reconcileBatch = 200

// ReconcileResult counts what one ReconcileStalePublishes pass did.
type ReconcileResult struct {
	Checked  int // stale `publishing` versions examined
	Released int // moved to publish_failed
	Running  int // workflow still running: left alone
	Lost     int // release skipped because the claim changed underneath us (a legitimate completion, retry or failure won)
	Errors   int // Temporal or database errors; those versions are retried next pass
}

// ReconcileStalePublishes releases `publishing` versions whose workflow is no
// longer alive, so a crash between ClaimPublish and the workflow start (or a
// workflow that ended without its activity recording a result) does not block
// republishing for the full 15-minute lease (ADR-0028's reversal trigger,
// option C used only for old claims, never on the publish hot path).
//
// For each version still `publishing` after publishReconcileGrace it describes
// the workflow by its deterministic ID:
//
//   - running (or continued-as-new): left alone;
//   - not found: the workflow never started (process died after the claim);
//   - completed, failed, cancelled, terminated or timed out: the workflow is
//     over but the version still says `publishing`, so its activity never wrote
//     the result;
//
// and, in the last two cases, releases the version to `publish_failed` with an
// explanatory publish_error so the instructor can simply publish again. The
// release is one UPDATE guarded on the same claim that was read, so a workflow
// that completes concurrently (state `ready`), a failure the workflow recorded
// itself, or a fresh retry is never overwritten, and running it twice or from
// two replicas is harmless. A Temporal error other than not-found leaves the
// version untouched (Temporal being down must not fail healthy publishes).
//
// Scheduled by the worker (services/worker/jobs, every 60 s by default).
func (s *Service) ReconcileStalePublishes(ctx context.Context) (ReconcileResult, error) {
	var res ReconcileResult
	stale, err := s.repo.ListStalePublishes(ctx, publishReconcileGrace, reconcileBatch)
	if err != nil {
		return res, fmt.Errorf("list stale publishes: %w", err)
	}
	var errs []error
	for _, sp := range stale {
		res.Checked++
		reason, release, err := s.publishLiveness(ctx, sp)
		if err != nil {
			res.Errors++
			errs = append(errs, fmt.Errorf("version %s: %w", sp.VersionID, err))
			continue
		}
		if !release {
			res.Running++
			continue
		}
		// The release and its course.publish_failed event commit together.
		var changed bool
		err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			changed, err = s.repo.WithTx(tx).ReleaseStalePublish(ctx, sp, reason)
			if err != nil || !changed {
				return err
			}
			return s.writePublishFailed(ctx, tx, sp.VersionID, FailedActivityReconciler, reason)
		})
		switch {
		case err != nil:
			res.Errors++
			errs = append(errs, fmt.Errorf("release version %s: %w", sp.VersionID, err))
		case changed:
			res.Released++
		default:
			res.Lost++
		}
	}
	return res, errors.Join(errs...)
}

// publishLiveness decides whether a stale claim should be released and why.
func (s *Service) publishLiveness(ctx context.Context, sp StalePublish) (reason string, release bool, err error) {
	if sp.WorkflowID == "" {
		return "publish was interrupted before its workflow was started; publish again", true, nil
	}
	desc, err := s.temporal.DescribeWorkflowExecution(ctx, sp.WorkflowID, "")
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return "publish was interrupted before its workflow was started (the server never ran it); publish again", true, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("describe workflow %s: %w", sp.WorkflowID, err)
	}
	status := desc.GetWorkflowExecutionInfo().GetStatus()
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "", false, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return fmt.Sprintf("publish workflow ended (%s) without recording a result; publish again", status), true, nil
	default:
		return "", false, fmt.Errorf("workflow %s has unknown status %s", sp.WorkflowID, status)
	}
}
