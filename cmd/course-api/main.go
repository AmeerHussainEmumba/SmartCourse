// Command course-api serves internal/course, internal/enrollment, and
// internal/progress's HTTP routes (ADR-0023 — splitting cmd/api into
// per-domain services, extending ADR-0022's two-binary split). The three
// stay together, not three more binaries, because enrollment locks the
// `courses` row in the SAME database transaction it writes an enrollment
// row in (ADR-0012's seat-limit guarantee) — splitting them apart would
// force that lock into a network call. Same shape as
// cmd/identity-api/main.go and cmd/analytics-api/main.go — thin, no
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
	logger.Info("starting", slog.String("service", "course-api"), slog.String("env", cfg.Env))

	deps, err := bootstrap.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	r := router.New(cfg.Env, cfg.CORSAllowedOrigins)
	router.RegisterHealth(r, deps.HealthCheckers())
	bootstrap.RegisterCourseRoutes(r, deps, cfg)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

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
}
