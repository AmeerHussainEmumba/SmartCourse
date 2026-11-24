// Command course-api serves services/course/course, services/course/enrollment, and
// services/course/progress's HTTP routes (ADR-0023 — splitting cmd/api into
// per-domain services, extending ADR-0022's two-binary split). The three
// stay together, not three more binaries, because enrollment locks the
// `courses` row in the SAME database transaction it writes an enrollment
// row in (ADR-0012's seat-limit guarantee) — splitting them apart would
// force that lock into a network call. Same shape as
// services/identity/cmd/identity-api/main.go and services/analytics/cmd/analytics-api/main.go — thin, no
// business logic — just a narrower route registration.
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
	"github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Env, logging.LevelFromEnv())
	logger.Info("starting", slog.String("service", "course-api"), slog.String("env", cfg.Env))

	deps, err := infra.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	// Tracing and /metrics (ADR-0032). Never fails and never blocks start-up:
	// with no OTEL_EXPORTER_OTLP_ENDPOINT, or an unreachable collector, the
	// service simply runs untraced.
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "course-api", Outbox: true})

	// Built with client.NewLazyClient (temporal.Connect) — never
	// fails here even if Temporal isn't reachable yet; only an actual
	// publish call would (ADR-0009/0025).
	// The tracing interceptor makes StartWorkflow a span whose context the
	// workflow run continues in the worker; it is added only when tracing is on.
	temporal.Observe(temporal.Observability{Tracing: obs.TracingEnabled()})
	temporalClient, err := temporal.Connect(cfg)
	if err != nil {
		logger.Error("failed to build temporal client", slog.Any("error", err))
		os.Exit(1)
	}
	defer temporalClient.Close()

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
	registerRoutes(r, deps, cfg, temporalClient)

	srv := router.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("course-api listening", slog.Int("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("course-api server error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down course-api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
	// Flush buffered spans after the last request; bounded so a collector
	// that is down cannot hold the process open.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	obs.Shutdown(flushCtx)
}
