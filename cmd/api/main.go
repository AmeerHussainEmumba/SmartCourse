// Command api is SmartCourse's HTTP service — one of two independently
// built, deployed, and run binaries (ADR-0022; cmd/worker is the other).
// It serves every synchronous route (RFC §3.4's sync/async boundary) and
// nothing else: no outbox relay, no background processing. Both binaries
// share the same internal packages and the same PostgreSQL database — see
// internal/platform/bootstrap's package comment for why this is still the
// modular monolith ADR-0001 describes, just built as two artifacts instead
// of one.
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

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/bootstrap"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Env, logging.LevelFromEnv())
	logger.Info("starting", slog.String("service", "api"), slog.String("env", cfg.Env))

	deps, err := bootstrap.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	r := router.New(cfg.Env, cfg.CORSAllowedOrigins)
	router.RegisterHealth(r, deps.HealthCheckers())
	bootstrap.RegisterModules(r, deps, cfg)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("api listening", slog.Int("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("api server error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
}
