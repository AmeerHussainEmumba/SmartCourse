// Package bootstrap is the composition root (RFC §4.4) — the one place in
// the codebase allowed to import every domain module, because wiring them
// together into a running system is the one legitimate reason to need all
// of them at once. It lives under internal/platform, not under cmd/,
// specifically so two separate binaries (cmd/api, cmd/worker — ADR-0022)
// can both import it without one cmd package importing another, which Go
// doesn't allow between two `package main`s anyway.
//
// This does not change the underlying architecture: every module still
// shares one PostgreSQL connection and one set of Go packages — it's a
// modular monolith's code, built into two deployable artifacts instead of
// one. ADR-0001's reasoning for the shared-transaction, shared-codebase
// design is unaffected; only how the result gets built and deployed
// changed. See ADR-0022 for the full decision record.
package bootstrap

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
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
)

// Dependencies holds the shared infrastructure clients both services
// need — connecting to Postgres/Redis is identical work for cmd/api and
// cmd/worker, so it lives here once instead of being duplicated in two
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

// HealthCheckers returns the dependency checks for /health/ready
// (internal/platform/router). Shared because both services have the same
// dependencies, even though only cmd/api exposes an HTTP health endpoint
// today.
func (d *Dependencies) HealthCheckers() map[string]router.Checker {
	return map[string]router.Checker{
		"postgres": func(ctx context.Context) error { return db.Ping(ctx, d.DB) },
		"redis":    func(ctx context.Context) error { return d.RDB.Ping(ctx).Err() },
	}
}

// RegisterModules wires every domain module's HTTP routes onto r. Called
// only by cmd/api — cmd/worker has no HTTP surface.
func RegisterModules(r *gin.Engine, deps *Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	identityRepo := identity.NewRepository(deps.DB)
	identitySvc := identity.NewService(identityRepo, deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	identity.RegisterRoutes(api, identitySvc, cfg.JWT, deps.RDB)

	courseSvc := course.NewService(course.NewRepository(deps.DB))
	course.RegisterRoutes(api, courseSvc, cfg.JWT)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(deps.DB))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT)

	progressSvc := progress.NewService(progress.NewRepository(deps.DB))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)

	analytics.RegisterRoutes(api, deps.DB, cfg.JWT)
}

// BuildRelay wires every module's relay handlers into one Relay instance.
// Called only by cmd/worker. Only internal/analytics registers handlers
// today; internal/search's search-indexer and internal/notification's
// dispatcher are the natural next additions — the relay itself doesn't
// change when they land, only this list grows.
func BuildRelay(deps *Dependencies) *relay.Relay {
	rl := relay.New(deps.DB)
	for _, h := range analytics.Handlers() {
		rl.Register(h)
	}
	return rl
}
