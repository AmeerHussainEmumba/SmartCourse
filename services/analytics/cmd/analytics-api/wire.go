package main

import (
	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// registerRoutes wires services/analytics/analytics's HTTP routes onto r. Read-only
// and already decoupled — analytics learns about the rest of the system
// through the relay/outbox, never a direct call into another module's
// service layer, which made it the lowest-risk candidate to split out first
// (ADR-0023).
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	// Redis serves the Most Popular Courses ranking the reconciler keeps; every
	// read falls back to Postgres if it is empty or down. The dead-letter and
	// workflow sources for GET /analytics/issues are not attached here yet (see
	// analytics/issues.go), so that route answers 503 until they are.
	analyticsSvc := analytics.NewService(analytics.NewRepository(deps.DB)).WithPopularCache(deps.RDB)
	analytics.RegisterRoutes(api, analyticsSvc, cfg.JWT)
}
