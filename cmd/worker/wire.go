package main

import (
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/workflows"
)

// buildRelay wires every module's relay handlers into one Relay instance.
// internal/analytics, internal/course, and internal/enrollment register
// handlers today (enrollment withdraws active enrollments when a course is
// archived — see enrollment.handleCourseArchived; course keeps
// instructor_mirror current from identity's events — see
// course.handleUserRegistered, ADR-0024); internal/search's search-indexer
// and internal/notification's dispatcher are the natural next additions —
// the relay itself doesn't change when they land, only this list grows.
func buildRelay(deps *infra.Dependencies) *relay.Relay {
	rl := relay.New(deps.DB)
	for _, h := range analytics.Handlers() {
		rl.Register(h)
	}
	for _, h := range course.Handlers() {
		rl.Register(h)
	}
	for _, h := range enrollment.Handlers() {
		rl.Register(h)
	}
	return rl
}

// buildPublishWorker wires internal/workflows' PublishCourseWorkflow and its
// Activities onto a Temporal worker.Worker, polling cfg.Temporal.TaskQueue.
// Independent of buildRelay (one Temporal, one the in-process outbox relay);
// main.go runs them as two separate goroutines so a Temporal outage never
// takes the relay down with it (ADR-0009/0025).
func buildPublishWorker(deps *infra.Dependencies, cfg *config.Config, temporal temporalclient.Client) temporalworker.Worker {
	courseSvc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue)
	activities := workflows.NewActivities(courseSvc)

	w := temporalworker.New(temporal, cfg.Temporal.TaskQueue, temporalworker.Options{})
	// Registered under course.PublishWorkflowName explicitly, rather than
	// relying on the default (the Go function's own name) — that's the
	// same string course.Service.Publish uses to start it by name, which
	// is how it starts a workflow without importing this package (course
	// must not import internal/workflows; internal/workflows imports
	// course for Activities, so the dependency can only run one way).
	w.RegisterWorkflowWithOptions(workflows.PublishCourseWorkflow, workflow.RegisterOptions{Name: course.PublishWorkflowName})
	w.RegisterActivity(activities)
	return w
}
