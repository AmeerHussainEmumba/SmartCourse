package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/failedevents"
)

// shutdownTimeout bounds the HTTP server's drain.
const shutdownTimeout = 15 * time.Second

// buildRouter builds the public engine: health and the analytics routes. It
// deliberately does not mount /metrics: that is served only on the internal
// listener (METRICS_HTTP_ADDR) that libs/runtime starts (design §6).
func buildRouter(cfg *config.Config, deps *infra.Dependencies, obs *infra.Observability) (*gin.Engine, func(context.Context), error) {
	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		AllowedOrigins: cfg.CORSAllowedOrigins,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
		Observe:        obs.Middleware(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("invalid router configuration: %w", err)
	}
	router.RegisterHealth(r, deps.HealthCheckers())
	closeRoutes := registerRoutes(r, deps, cfg)
	return r, closeRoutes, nil
}

// wire registers the HTTP server as the one critical component.
func wire(p *runtime.Process) error {
	r, closeRoutes, err := buildRouter(p.Config, infra.Of(p.Deps), p.Obs)
	if err != nil {
		return err
	}
	cfg := p.Config
	srv := router.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})
	p.OnShutdown("routes", func(ctx context.Context) error { closeRoutes(ctx); return nil })
	p.Go("http", func(ctx context.Context) error {
		errc := make(chan error, 1)
		go func() { errc <- srv.ListenAndServe() }()
		p.Logger.Info("analytics-api listening", slog.Int("port", cfg.HTTPPort))
		select {
		case err := <-errc:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			return srv.Shutdown(sctx)
		}
	}, runtime.Critical())
	return nil
}

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
	// does when MONGO_URI is unset. The workflow section comes from
	// analytics' own workflow_issue_stats, fed by course.publish_failed events
	// (analytics/workflow_issues.go, DR-026), so it needs no MongoDB and no
	// read of the course schema.
	workflowIssues := analytics.NewWorkflowIssuesSource(deps.DB)
	analyticsSvc = analyticsSvc.WithIssueSources(nil, workflowIssues)
	if cfg.Mongo.Configured() {
		lazy := failedevents.NewLazyStore(mongostore.Config{
			URI: cfg.Mongo.URI, Database: cfg.Mongo.Database, ConnectTimeout: cfg.Mongo.ConnectTimeout,
			ServerSelectionTimeout: cfg.Mongo.ServerSelectionTimeout, OpTimeout: cfg.Mongo.OpTimeout,
		})
		analyticsSvc = analyticsSvc.WithIssueSources(failedevents.NewSource(lazy), workflowIssues)
		closeFn = func(ctx context.Context) { _ = lazy.Close(ctx) }
	}
	analytics.RegisterRoutes(api, analyticsSvc, cfg.JWT)
	return closeFn
}
