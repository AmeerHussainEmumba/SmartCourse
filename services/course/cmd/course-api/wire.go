package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	libtemporal "github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

// shutdownGrace bounds the HTTP server's drain of in-flight requests.
const shutdownGrace = 15 * time.Second

// wire is course-api's runtime wiring: the Temporal client (lazy, so an outage
// never blocks start-up; only a publish call needs it), the public router and
// the HTTP server as a critical component. /metrics is not on this router: the
// runtime serves it on the internal listener.
func wire(p *runtime.Process) error {
	cfg := p.Config
	// The tracing interceptor makes StartWorkflow a span whose context the
	// workflow run continues in the publishing worker; added only when tracing is on.
	libtemporal.Observe(libtemporal.Observability{Tracing: p.Obs.TracingEnabled()})
	tc, err := libtemporal.Connect(cfg)
	if err != nil {
		return fmt.Errorf("build temporal client: %w", err)
	}
	p.OnShutdown("temporal", func(context.Context) error { tc.Close(); return nil })

	r, err := newRouter(p, tc)
	if err != nil {
		return err
	}
	srv := router.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})
	p.Go("http", func(ctx context.Context) error {
		errc := make(chan error, 1)
		go func() {
			p.Logger.Info("course-api listening", slog.Int("port", cfg.HTTPPort))
			errc <- srv.ListenAndServe()
		}()
		select {
		case err := <-errc:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
		}
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		return srv.Shutdown(shutCtx)
	}, runtime.Critical())
	return nil
}

// newRouter builds the public router: the same middleware stack and routes as
// before the split, with public health but no /metrics.
func newRouter(p *runtime.Process, tc temporalclient.Client) (*gin.Engine, error) {
	cfg := p.Config
	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		AllowedOrigins: cfg.CORSAllowedOrigins,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
		Observe:        p.Obs.Middleware(),
	})
	if err != nil {
		return nil, fmt.Errorf("invalid router configuration: %w", err)
	}
	router.RegisterHealth(r, infra.Of(p.Deps).HealthCheckers())
	registerRoutes(r, infra.Of(p.Deps), cfg, tc)
	return r, nil
}

// registerRoutes wires services/course/course, services/course/enrollment, and
// services/course/progress's HTTP routes onto r. The three stay together, not split
// into three more binaries, because enrollment locks the `courses` row in
// the SAME database transaction it writes an enrollment row in (ADR-0012's
// seat-limit guarantee) — that requires both to run in one process against
// one connection; splitting them would force that lock into a network call.
// See ADR-0023.
// temporal is passed in explicitly, not read off deps — see
// libs/temporal for why identity-api/analytics-api never
// acquire one.
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config, temporal temporalclient.Client) {
	api := r.Group("/api/v1")

	// Idempotency-Key (RFC §15.1) on the non-idempotent POST routes.
	idem := idempotency.Middleware(deps.DB, idempotency.Options{
		UserID: func(c *gin.Context) (uuid.UUID, bool) {
			id, ok := auth.FromContext(c.Request.Context())
			return id.UserID, ok
		},
	})

	courseSvc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue).
		WithActivityQueue(cfg.Temporal.ActivityQueue)
	// Object storage backs POST /assets/upload-url. If it cannot be built
	// (bad MinIO settings), upload-url answers 503; nothing else is affected.
	if store, err := assets.New(cfg.MinIO); err != nil {
		slog.Error("asset storage unavailable; POST /assets/upload-url will return 503", slog.Any("error", err))
	} else {
		courseSvc.WithAssetStore(store)
	}
	course.RegisterRoutes(api, courseSvc, cfg.JWT, idem)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(deps.DB))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT, idem)

	progressSvc := progress.NewService(progress.NewRepository(deps.DB))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)

	// The public verification route (RFC §8.4) takes a per-IP limit, 30 a
	// minute like the identity module's other token-guessing surfaces.
	certificateSvc := certificate.NewService(certificate.NewRepository(deps.DB))
	limiter := ratelimit.New(deps.RDB)
	certificate.RegisterRoutes(api, certificateSvc, cfg.JWT,
		limiter.Middleware("certificate-verify", ratelimit.PerMinute(30)))
}
