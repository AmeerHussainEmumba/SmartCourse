// Command worker is SmartCourse's background service — one of four
// independently built, deployed, and run binaries (ADR-0022; the other
// three are the HTTP services, identity-api/course-api/analytics-api,
// split out of what used to be one cmd/api — ADR-0023). It runs two
// independent things side by side: the outbox relay
// (internal/platform/relay, ADR-0021) and, as of M2, the Temporal worker
// that executes PublishCourseWorkflow (internal/workflows, ADR-0009/0025).
// Kafka consumers and Asynq pools are still M3 scope (RFC §21.0). No HTTP
// surface at all.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	rl := bootstrap.BuildRelay(deps)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		logger.Info("outbox relay starting")
		rl.Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
	}()
	go func() {
		defer wg.Done()
		startPublishWorker(ctx, logger, deps, cfg)
	}()

	wg.Wait() // both goroutines return once ctx is cancelled
	logger.Info("shutting down worker")
}

// startPublishWorker builds a fresh Temporal client and worker on every
// attempt and retries with capped exponential backoff rather than giving
// up after one failure — Start() can fail not just on local
// misconfiguration but on Temporal being reachable yet its "default"
// namespace not existing yet (temporalio/auto-setup takes real time to
// create it, and `docker compose up` brings this binary up concurrently
// with it, not after it).
//
// Rebuilding the client every attempt, not reusing one across retries, is
// the fix for a second bug the first version of this retry loop had: a
// client/worker pair that has already failed to start can keep failing
// with the exact same stale "connection refused" error for minutes after
// the server is verifiably reachable again (confirmed live — `nc` to the
// server succeeded while the existing client/worker kept reporting the
// identical old error; restarting the whole process, which naturally
// builds a fresh client, connected within seconds). gRPC's own internal
// reconnect backoff on that client's connection runs on its own schedule,
// decoupled from this loop's; a fresh client each attempt means every
// retry here is a genuine new dial, not a question asked of a connection
// that's deliberately not trying yet.
func startPublishWorker(ctx context.Context, logger *slog.Logger, deps *bootstrap.Dependencies, cfg *config.Config) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		temporalClient, err := bootstrap.ConnectTemporal(cfg)
		if err != nil {
			logger.Error("failed to build temporal client", slog.Any("error", err))
			os.Exit(1) // only a local misconfiguration (bad HostPort/Namespace string) fails here, never server unavailability
		}
		pw := bootstrap.BuildPublishWorker(deps, cfg, temporalClient)

		if err := pw.Start(); err != nil {
			temporalClient.Close()
			logger.Error("publish worker failed to start, will retry", slog.Any("error", err), slog.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		logger.Info("publish worker started", slog.String("task_queue", cfg.Temporal.TaskQueue))
		<-ctx.Done()
		pw.Stop()
		temporalClient.Close()
		return
	}
}
