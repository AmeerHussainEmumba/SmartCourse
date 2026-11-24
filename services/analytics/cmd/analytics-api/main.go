// Command analytics-api serves services/analytics/analytics's HTTP routes only
// (ADR-0023 — splitting cmd/api into per-domain services, extending
// ADR-0022's two-binary split). Read-only, decoupled from every other
// module already (it learns about the rest of the system through the
// relay/outbox, not a direct call into another module), which is why it
// split out first. Same shape as services/identity/cmd/identity-api/main.go and
// services/course/cmd/course-api/main.go — thin, no business
// logic — just a narrower route registration.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Env, logging.LevelFromEnv())
	logger.Info("starting", slog.String("service", "analytics-api"), slog.String("env", cfg.Env))

	deps, err := infra.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	// Tracing and /metrics (ADR-0032). Never fails and never blocks start-up:
	// with no OTEL_EXPORTER_OTLP_ENDPOINT, or an unreachable collector, the
	// service simply runs untraced.
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "analytics-api", Outbox: false})

	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		AllowedOrigins: cfg.CORSAllowedOrigins,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
		Observe:        obs.Middleware(),
	})
	if err != nil {
		logger.Error("invalid router configuration", slog.Any("error", err))
		os.Exit(1)
	}
	router.RegisterHealth(r, deps.HealthCheckers())
	obs.RegisterMetricsRoute(r) // unauthenticated by design; see libs/infra Observability
	closeRoutes := registerRoutes(r, deps, cfg)

	srv := router.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("analytics-api listening", slog.Int("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("analytics-api server error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down analytics-api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer closeRoutes(shutdownCtx)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
	// Flush buffered spans after the last request; bounded so a collector
	// that is down cannot hold the process open.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	obs.Shutdown(flushCtx)
}
