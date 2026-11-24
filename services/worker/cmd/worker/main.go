// Command worker is SmartCourse's background service — one of four
// independently built, deployed, and run binaries (ADR-0022; the other
// three are the HTTP services, identity-api/course-api/analytics-api,
// split out of what used to be one cmd/api — ADR-0023). It runs three
// independent things side by side: the outbox relay
// (libs/relay, ADR-0021), the Temporal worker that executes
// PublishCourseWorkflow (services/course/workflows, ADR-0009/0025), and the
// Asynq side (libs/asynqx, ADR-0013): one server per queue with its own
// bounded pool (RFC §10.2) and the scheduler for the maintenance jobs in
// services/worker/jobs. A failure in one never stops another. With
// EVENT_TRANSPORT=kafka (ADR-0029) the relay publishes to Kafka instead of
// running the handlers, and a consumer group per handler runs them from the
// topics (transport.go). The only HTTP surface is the admin port (services/worker/admin: health and failed-event replay).
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

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/jobs"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Env, logging.LevelFromEnv())
	logger.Info("starting", slog.String("service", "worker"), slog.String("env", cfg.Env))

	deps, err := infra.Connect(cfg)
	if err != nil {
		logger.Error("failed to connect to dependencies", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer startTelemetry(ctx, logger, deps, cfg)() // tracing, /metrics, Temporal interceptors; see telemetry_wire.go

	// MongoDB (archive, dead letters) and the admin surface run beside the
	// relay and never stop it: see mongo_wire.go.
	mg := newMongoWiring(cfg)
	// The dead-letter sink exists only when MongoDB is configured. Without a
	// store, a sink would keep every poison event unpublished forever; with
	// none, the relay logs and drops it as it did before Week 3.
	var sink relay.DeadLetterSink
	if cfg.Mongo.URI != "" {
		sink = deadLetterAdapter{mg.Sink()}
	}
	events, err := newEventTransport(ctx, logger, cfg, sink)
	if err != nil {
		logger.Error("event transport not ready", slog.Any("error", err))
		os.Exit(1)
	}
	defer events.Close()
	rl := buildRelay(deps, cfg, events.RelayOptions()...)
	replayable := consumerHandlers()
	if transportMode(cfg) == config.TransportKafka {
		replayable = append(replayable, registerArchiver(logger, cfg, mg, rl)...)
	}

	var wg sync.WaitGroup
	wg.Add(6)
	go func() {
		defer wg.Done()
		mg.run(ctx, logger)
	}()
	go func() {
		defer wg.Done()
		serveAdmin(ctx, logger, deps, cfg, mg, replayable)
	}()
	go func() {
		defer wg.Done()
		logger.Info("outbox relay starting", slog.String("transport", transportMode(cfg)))
		rl.Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
	}()
	go func() {
		defer wg.Done()
		events.Run(ctx, rl, deps.DB) // kafka mode only: one consumer group per registered handler
	}()
	go func() {
		defer wg.Done()
		startPublishWorker(ctx, logger, deps, cfg)
	}()
	go func() {
		defer wg.Done()
		startAsynq(ctx, logger, deps, cfg)
	}()

	wg.Wait() // every goroutine returns once ctx is cancelled
	logger.Info("shutting down worker")
}

// startAsynq runs the Asynq servers and the scheduler until ctx is cancelled,
// then stops the scheduler first (no new ticks) and drains the pools. Any
// failure here is logged and ends only this goroutine: the relay and the
// Temporal worker keep running, same independence as startPublishWorker. A
// missing piece (no Temporal client for the publish job, no analytics Runner)
// drops just that job.
//
// Start does not fail while Redis is down: Asynq logs and keeps trying to
// reach it, and the pools and the scheduler pick up when it returns.
func startAsynq(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("asynq goroutine panicked; relay and publish worker are unaffected", slog.Any("panic", r))
		}
	}()

	// The publish reconciliation asks Temporal about workflows, so the jobs
	// get their own (lazy) client; a Temporal outage fails that job's tick,
	// never the process.
	var publishes jobs.PublishReconciler
	if tc, err := temporal.Connect(cfg); err != nil {
		logger.Error("failed to build temporal client for the reconciliation job", slog.Any("error", err))
	} else {
		defer tc.Close()
		publishes = newCourseService(deps, cfg, tc)
	}
	js, err := buildJobs(deps, cfg, publishes, analyticsReconciler(deps))
	if err != nil {
		logger.Error("asynq jobs not started", slog.Any("error", err))
		return
	}
	servers, scheduler, err := buildAsynq(cfg, logger, js)
	if err != nil {
		logger.Error("asynq not started", slog.Any("error", err))
		return
	}
	closeNotify := wireNotifications(ctx, logger, servers, deps, cfg, newProvider(logger, cfg))
	defer closeNotify()
	if err := servers.Start(); err != nil {
		scheduler.Shutdown()
		logger.Error("asynq servers failed to start", slog.Any("error", err))
		return
	}
	if err := scheduler.Start(); err != nil {
		servers.Shutdown()
		scheduler.Shutdown()
		logger.Error("scheduler failed to start", slog.Any("error", err))
		return
	}
	logger.Info("asynq started", slog.Any("queues", servers.Queues()), slog.Int("scheduled_jobs", len(js)))
	<-ctx.Done()
	scheduler.Shutdown()
	servers.Shutdown()
	logger.Info("asynq stopped")
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
func startPublishWorker(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		temporalClient, err := temporal.Connect(cfg)
		if err != nil {
			logger.Error("failed to build temporal client", slog.Any("error", err))
			os.Exit(1) // only a local misconfiguration (bad HostPort/Namespace string) fails here, never server unavailability
		}
		pw := buildPublishWorker(deps, cfg, temporalClient)

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
