package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	libtemporal "github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/activities"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// activityRegistry is the part of a Temporal worker that registers activities.
type activityRegistry interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// registerActivities registers every publish activity under the NAME the
// contract gives it (libs/contracts/publishing), not under the Go method name:
// the publishing worker calls them by those strings, so a renamed method must
// not silently change what answers to them. The table lists the contract's
// names in the order of publishing.ActivityNames; a test keeps the two in step.
func registerActivities(r activityRegistry, a *activities.Activities) {
	for _, e := range []struct {
		name string
		fn   any
	}{
		{publishing.ActivityValidateMetadata, a.ValidateMetadata},
		{publishing.ActivityVerifyAssets, a.VerifyAssets},
		{publishing.ActivityComputeMetadata, a.ComputeMetadata},
		{publishing.ActivityBuildOutline, a.BuildOutline},
		{publishing.ActivityIndexSearch, a.IndexSearch},
		{publishing.ActivityWarmCache, a.WarmCache},
		{publishing.ActivityPromoteAndPublish, a.PromoteAndPublish},
		{publishing.ActivityMarkPublishFailed, a.MarkPublishFailed},
	} {
		r.RegisterActivityWithOptions(e.fn, activity.RegisterOptions{Name: e.name})
	}
}

// newCourseService builds the course service the Temporal activities and the
// stale-publish job use, with object storage attached the way course-api does
// it. Without a store, VerifyAssets fails every publish whose lessons reference
// an asset (fail closed), so a MinIO misconfiguration is logged loudly here but
// never takes the relay down.
func newCourseService(deps *infra.Dependencies, cfg *config.Config, tc temporalclient.Client) *course.Service {
	svc := course.NewService(course.NewRepository(deps.DB), deps.RDB, tc, cfg.Temporal.TaskQueue).
		WithActivityQueue(cfg.Temporal.ActivityQueue)
	if store, err := assets.New(cfg.MinIO); err != nil {
		slog.Error("asset storage unavailable; publishes whose lessons reference assets will fail VerifyAssets", slog.Any("error", err))
	} else {
		svc.WithAssetStore(store)
	}
	return svc
}

// runActivities runs the Temporal worker on the activity queue
// (cfg.Temporal.ActivityQueue) until ctx is cancelled. It builds a fresh client
// and worker on every attempt and retries with capped exponential backoff
// rather than giving up after one failure: Start can fail not only on local
// misconfiguration but on Temporal being reachable yet its namespace not
// existing yet (temporalio/auto-setup takes real time to create it while the
// stack comes up). A client/worker pair that has already failed to start can
// keep reporting the same stale "connection refused" for minutes after the
// server is back, so every retry is a genuine new dial.
//
// It returns an error only for a configuration that can never work (a bad
// HostPort or Namespace string, what the old worker exited on). It is
// registered as a critical component for that reason: a permanent
// misconfiguration must not leave publishing silently dead behind a healthy
// readiness probe.
func runActivities(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		tc, err := libtemporal.Connect(cfg)
		if err != nil {
			return fmt.Errorf("build temporal client: %w", err)
		}
		w := temporalworker.New(tc, cfg.Temporal.ActivityQueue, temporalworker.Options{})
		registerActivities(w, activities.NewActivities(newCourseService(deps, cfg, tc)))

		if err := w.Start(); err != nil {
			tc.Close()
			logger.Error("activities worker failed to start, will retry", slog.Any("error", err), slog.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		logger.Info("activities worker started", slog.String("activity_queue", cfg.Temporal.ActivityQueue))
		<-ctx.Done()
		w.Stop()
		tc.Close()
		return nil
	}
}
