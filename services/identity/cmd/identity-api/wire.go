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
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

// shutdownGrace bounds the drain of in-flight requests (inside runtime's shutdown timeout).
const shutdownGrace = 15 * time.Second

// wire registers the one component identity-api runs: the public HTTP server.
// Its failure is critical (an API that is not serving must not look alive). The
// token-version re-seeder is NOT here: it runs in identity-worker.
func wire(p *runtime.Process) error {
	r, err := newRouter(p.Config, infra.Of(p.Deps), p.Obs)
	if err != nil {
		return err
	}
	srv := router.NewServer(fmt.Sprintf(":%d", p.Config.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  p.Config.HTTPReadTimeout,
		WriteTimeout: p.Config.HTTPWriteTimeout,
		IdleTimeout:  p.Config.HTTPIdleTimeout,
	})
	p.Go("http", func(ctx context.Context) error { return serve(ctx, p.Logger, srv) }, runtime.Critical())
	return nil
}

// serve runs srv until ctx is cancelled, then drains in-flight requests.
func serve(ctx context.Context, logger *slog.Logger, srv *http.Server) error {
	errc := make(chan error, 1)
	go func() {
		logger.Info("identity-api listening", slog.String("addr", srv.Addr))
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// ctx is already cancelled; the drain gets its own bound.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// newRouter builds the public router: the identity routes and the public
// health routes, with tracing and request metrics recorded by obs. It does not
// mount /metrics: that is served by the internal listener (libs/runtime).
func newRouter(cfg *config.Config, deps *infra.Dependencies, obs *infra.Observability) (*gin.Engine, error) {
	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		AllowedOrigins: cfg.CORSAllowedOrigins,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
		Observe:        obs.Middleware(),
	})
	if err != nil {
		return nil, fmt.Errorf("invalid router configuration: %w", err)
	}
	router.RegisterHealth(r, deps.HealthCheckers())
	registerRoutes(r, deps, cfg)
	return r, nil
}

// registerRoutes wires services/identity/identity's HTTP routes onto r. Fully
// self-contained: no other module reads identity's tables directly; everyone
// else gets the caller's identity from the JWT claims (auth.FromContext), never
// a lookup into `users`. See ADR-0023.
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	identityRepo := identity.NewRepository(deps.DB)
	identitySvc := identity.NewService(identityRepo, deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	identity.RegisterRoutes(api, identitySvc, cfg.JWT, deps.RDB)
}
