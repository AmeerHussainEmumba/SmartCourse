// Command smartcourse is the single binary that runs in two modes
// (RFC §4.2): --mode=api serves HTTP, --mode=worker runs the outbox relay
// (internal/platform/relay), plus, later, Kafka consumers, Asynq workers,
// and the Temporal worker (M2/M3). One build, one image — API and workers
// can never drift apart on a shared domain change.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/db"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/router"
)

func main() {
	mode := flag.String("mode", "api", `run mode: "api" or "worker"`)
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}
	cfg.Mode = *mode

	logger := logging.New(cfg.Env, envLogLevel())
	logger.Info("starting", slog.String("mode", cfg.Mode), slog.String("env", cfg.Env))

	gdb, err := db.Open(cfg.Postgres)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	rdb, err := cache.Open(cfg.Redis)
	if err != nil {
		logger.Error("failed to connect to redis", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Mode {
	case "api":
		runAPI(ctx, cfg, gdb, rdb, logger)
	case "worker":
		runWorker(ctx, cfg, gdb, rdb, logger)
	default:
		logger.Error("unknown mode", slog.String("mode", cfg.Mode))
		os.Exit(1)
	}
}

func runAPI(ctx context.Context, cfg *config.Config, gdb *gorm.DB, rdb *redis.Client, logger *slog.Logger) {
	r := router.New(cfg.Env, cfg.CORSAllowedOrigins)

	router.RegisterHealth(r, map[string]router.Checker{
		"postgres": func(ctx context.Context) error { return db.Ping(ctx, gdb) },
		"redis":    func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
	})

	registerModules(r, gdb, rdb, cfg)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

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

func runWorker(ctx context.Context, cfg *config.Config, gdb *gorm.DB, rdb *redis.Client, logger *slog.Logger) {
	// Kafka consumers, Asynq pools, and the Temporal worker are still
	// M2/M3 scope (RFC §21.0). The outbox relay is not — see ADR-0021 for
	// why it was pulled forward as an in-process bridge ahead of Kafka.
	rl := buildRelay(gdb)
	logger.Info("outbox relay starting")
	rl.Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
	logger.Info("shutting down worker")
}

func envLogLevel() string {
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		return v
	}
	return "info"
}
