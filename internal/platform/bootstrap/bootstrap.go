// Package bootstrap is the composition root (RFC §4.4) — the one place in
// the codebase allowed to import every domain module, because wiring them
// together into a running system is the one legitimate reason to need all
// of them at once. It lives under internal/platform, not under cmd/,
// specifically so four separate binaries (cmd/identity-api, cmd/course-api,
// cmd/analytics-api, cmd/worker — ADR-0022, ADR-0023) can each import it
// without one cmd package importing another, which Go doesn't allow
// between two `package main`s anyway.
//
// This does not change the underlying architecture: every module still
// shares one PostgreSQL connection and one set of Go packages — it's a
// modular monolith's code, built into four deployable artifacts instead of
// one. ADR-0001's reasoning for the shared-transaction, shared-codebase
// design is unaffected; only how the result gets built and deployed
// changed. See ADR-0022 and ADR-0023 for the full decision records.
package bootstrap

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/identity"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/db"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/router"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/workflows"
)

// Dependencies holds the shared infrastructure clients every service
// needs — connecting to Postgres/Redis is identical work for all four
// cmd/* binaries, so it lives here once instead of being duplicated across
// main.go files that would inevitably drift.
type Dependencies struct {
	DB  *gorm.DB
	RDB *redis.Client
}

func Connect(cfg *config.Config) (*Dependencies, error) {
	gdb, err := db.Open(cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	rdb, err := cache.Open(cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	return &Dependencies{DB: gdb, RDB: rdb}, nil
}

// ConnectTemporal builds a Temporal client for course-api (to start the
// publish workflow) and worker (to run it) only — identity-api and
// analytics-api never call this (ADR-0009: Temporal backs publishing and
// nothing else). client.NewLazyClient, not client.Dial: Dial eagerly
// contacts the server and fails if it's unreachable, which would make
// course-api's boot depend on Temporal for catalogue/detail/CRUD routes
// that have nothing to do with it. A lazy client defers that connection to
// the first actual publish, matching ADR-0009's own stated consequence —
// "Temporal down means publishing unavailable — though nothing else is
// affected."
func ConnectTemporal(cfg *config.Config) (temporalclient.Client, error) {
	c, err := temporalclient.NewLazyClient(temporalclient.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("build temporal client: %w", err)
	}
	return c, nil
}

// HealthCheckers returns the dependency checks for /health/ready
// (internal/platform/router). Shared because every service has the same
// dependencies, even though cmd/worker doesn't expose an HTTP health
// endpoint today.
func (d *Dependencies) HealthCheckers() map[string]router.Checker {
	return map[string]router.Checker{
		"postgres": func(ctx context.Context) error { return db.Ping(ctx, d.DB) },
		"redis":    func(ctx context.Context) error { return d.RDB.Ping(ctx).Err() },
	}
}

// RegisterIdentityRoutes wires internal/identity's HTTP routes onto r.
// Called by cmd/identity-api. Fully self-contained — no other module reads
// identity's tables directly; everyone else gets the caller's identity from
// the JWT claims (auth.FromContext), never a lookup into `users`. See
// ADR-0023 for why identity split out on its own first.
func RegisterIdentityRoutes(r *gin.Engine, deps *Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	identityRepo := identity.NewRepository(deps.DB)
	identitySvc := identity.NewService(identityRepo, deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	identity.RegisterRoutes(api, identitySvc, cfg.JWT, deps.RDB)
}

// RegisterCourseRoutes wires internal/course, internal/enrollment, and
// internal/progress's HTTP routes onto r. Called by cmd/course-api. The
// three stay together, not split into three more binaries, because
// enrollment locks the `courses` row in the SAME database transaction it
// writes an enrollment row in (ADR-0012's seat-limit guarantee) — that
// requires both to run in one process against one connection; splitting
// them would force that lock into a network call. See ADR-0023.
// temporal is passed in explicitly, not read off deps, because it's not a
// Dependencies field — see ConnectTemporal's doc comment for why
// identity-api/analytics-api never acquire one.
func RegisterCourseRoutes(r *gin.Engine, deps *Dependencies, cfg *config.Config, temporal temporalclient.Client) {
	api := r.Group("/api/v1")

	courseSvc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue)
	course.RegisterRoutes(api, courseSvc, cfg.JWT)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(deps.DB))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT)

	progressSvc := progress.NewService(progress.NewRepository(deps.DB))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)
}

// RegisterAnalyticsRoutes wires internal/analytics's HTTP routes onto r.
// Called by cmd/analytics-api. Read-only and already decoupled — analytics
// learns about the rest of the system through the relay/outbox, never a
// direct call into another module's service layer, which made it the
// lowest-risk candidate to split out first (ADR-0023).
func RegisterAnalyticsRoutes(r *gin.Engine, deps *Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	analyticsSvc := analytics.NewService(analytics.NewRepository(deps.DB))
	analytics.RegisterRoutes(api, analyticsSvc, cfg.JWT)
}

// BuildRelay wires every module's relay handlers into one Relay instance.
// Called only by cmd/worker. internal/analytics, internal/course, and
// internal/enrollment register handlers today (enrollment withdraws
// active enrollments when a course is archived — see enrollment.
// handleCourseArchived; course keeps instructor_mirror current from
// identity's events — see course.handleUserRegistered, ADR-0024);
// internal/search's search-indexer and internal/notification's dispatcher
// are the natural next additions — the relay itself doesn't change when
// they land, only this list grows.
func BuildRelay(deps *Dependencies) *relay.Relay {
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

// BuildPublishWorker wires internal/workflows' PublishCourseWorkflow and its
// Activities onto a Temporal worker.Worker, polling cfg.Temporal.TaskQueue.
// Called only by cmd/worker, the same way BuildRelay is — the two are
// independent (one Temporal, one the in-process outbox relay) and are run
// as two separate goroutines in cmd/worker/main.go so a Temporal outage
// never takes the relay down with it (ADR-0009/0025).
func BuildPublishWorker(deps *Dependencies, cfg *config.Config, temporal temporalclient.Client) temporalworker.Worker {
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
