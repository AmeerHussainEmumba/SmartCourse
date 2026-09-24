package main

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/identity"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
)

// registerModules wires every module's routes onto the shared engine. Each
// module owns its own handler/service/repository wiring (RFC §4.3); this
// function is deliberately the only place that imports every module, so
// internal/platform/router never depends on domain code and module
// boundaries (RFC §4.4) stay enforceable by depguard.
func registerModules(r *gin.Engine, gdb *gorm.DB, rdb *redis.Client, cfg *config.Config) {
	api := r.Group("/api/v1")

	identityRepo := identity.NewRepository(gdb)
	identitySvc := identity.NewService(identityRepo, rdb, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	identity.RegisterRoutes(api, identitySvc, cfg.JWT, rdb)

	courseSvc := course.NewService(course.NewRepository(gdb))
	course.RegisterRoutes(api, courseSvc, cfg.JWT)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(gdb))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT)

	progressSvc := progress.NewService(progress.NewRepository(gdb))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)

	analytics.RegisterRoutes(api, gdb, cfg.JWT)
}

// buildRelay wires every module's relay handlers into one Relay instance
// (RFC §4.4 — this file, the composition root, is the only place allowed
// to know every module exists). Only internal/analytics registers
// handlers today; internal/search's search-indexer and
// internal/notification's dispatcher are the natural next additions —
// the relay itself doesn't change when they land, only this list grows.
func buildRelay(gdb *gorm.DB) *relay.Relay {
	rl := relay.New(gdb)
	for _, h := range analytics.Handlers() {
		rl.Register(h)
	}
	return rl
}
