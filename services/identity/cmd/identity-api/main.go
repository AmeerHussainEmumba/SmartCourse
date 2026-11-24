// Command identity-api serves services/identity/identity's HTTP routes only
// (ADR-0023 — splitting cmd/api into per-domain services, extending
// ADR-0022's two-binary split). Fully self-contained: no other module
// reads identity's tables directly, and every other module gets the
// caller's identity from the JWT claims, never a lookup into `users`.
// Same shape as services/course/cmd/course-api/main.go and services/analytics/cmd/analytics-api/main.go —
// thin, no business logic — just a narrower route registration.
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
	logger.Info("starting", slog.String("service", "identity-api"), slog.String("env", cfg.Env))

	deps, err := infra.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	r, err := router.NewWithOptions(router.Options{
		Env:            cfg.Env,
		AllowedOrigins: cfg.CORSAllowedOrigins,
		TrustedProxies: cfg.TrustedProxies,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		HSTS:           cfg.EnableHSTS,
	})
	if err != nil {
		logger.Error("invalid router configuration", slog.Any("error", err))
		os.Exit(1)
	}
	router.RegisterHealth(r, deps.HealthCheckers())
	registerRoutes(r, deps, cfg)

	srv := router.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), r, router.ServerOptions{
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("identity-api listening", slog.Int("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("identity-api server error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down identity-api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
}
