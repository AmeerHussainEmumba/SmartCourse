package main

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/failedevents"
)

// registerRoutes wires services/analytics/analytics's HTTP routes onto r. Read-only
// and already decoupled — analytics learns about the rest of the system
// through the relay/outbox, never a direct call into another module's
// service layer, which made it the lowest-risk candidate to split out first
// (ADR-0023). The returned func releases what the routes hold (the MongoDB
// connection); call it on shutdown.
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config) (closeFn func(context.Context)) {
	api := r.Group("/api/v1")

	// Redis serves the Most Popular Courses ranking the reconciler keeps; every
	// read falls back to Postgres if it is empty or down.
	analyticsSvc := analytics.NewService(analytics.NewRepository(deps.DB)).WithPopularCache(deps.RDB)
	closeFn = func(context.Context) {}

	// GET /analytics/issues reads failed_events from MongoDB (ADR-0031; the one
	// HTTP service allowed to link libs/mongostore). The connection is made on
	// first use, so a MongoDB outage never delays startup or touches another
	// route; the issues route answers 503 while it is unreachable, and so it
	// does when MONGO_URI is unset. No workflow-failure source is attached: those
	// counts live in the course schema, which analytics cannot read (see
	// analytics/issues.go), so the report's workflow section is null.
	if cfg.Mongo.Configured() {
		lazy := failedevents.NewLazyStore(mongostore.Config{
			URI: cfg.Mongo.URI, Database: cfg.Mongo.Database, ConnectTimeout: cfg.Mongo.ConnectTimeout,
			ServerSelectionTimeout: cfg.Mongo.ServerSelectionTimeout, OpTimeout: cfg.Mongo.OpTimeout,
		})
		analyticsSvc = analyticsSvc.WithIssueSources(failedevents.NewSource(lazy), nil)
		closeFn = func(ctx context.Context) { _ = lazy.Close(ctx) }
	}
	analytics.RegisterRoutes(api, analyticsSvc, cfg.JWT)
	return closeFn
}
