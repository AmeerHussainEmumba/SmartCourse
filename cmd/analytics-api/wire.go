package main

import (
	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
)

// registerRoutes wires internal/analytics's HTTP routes onto r. Read-only
// and already decoupled — analytics learns about the rest of the system
// through the relay/outbox, never a direct call into another module's
// service layer, which made it the lowest-risk candidate to split out first
// (ADR-0023).
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	analyticsSvc := analytics.NewService(analytics.NewRepository(deps.DB))
	analytics.RegisterRoutes(api, analyticsSvc, cfg.JWT)
}
