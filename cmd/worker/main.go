// Command worker is SmartCourse's background service — one of two
// independently built, deployed, and run binaries (ADR-0022; cmd/api is
// the other). It runs the outbox relay (internal/platform/relay) today,
// plus, later, Kafka consumers, Asynq pools, and the Temporal worker
// (M2/M3 — RFC §21.0). No HTTP surface at all.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/bootstrap"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Env, logging.LevelFromEnv())
	logger.Info("starting", slog.String("service", "worker"), slog.String("env", cfg.Env))

	deps, err := bootstrap.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Kafka consumers, Asynq pools, and the Temporal worker are still
	// M2/M3 scope (RFC §21.0). The outbox relay is not — see ADR-0021 for
	// why it was pulled forward as an in-process bridge ahead of Kafka.
	rl := bootstrap.BuildRelay(deps)
	logger.Info("outbox relay starting")
	rl.Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
	logger.Info("shutting down worker")
}
