package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	runtime "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
	"github.com/AmeerHussainEmumba/SmartCourse/services/publishing/publish"
)

// worker is the part of a Temporal worker this binary uses.
type worker interface {
	publish.Registry
	Start() error
	Stop()
}

// backend is what the wiring needs from Temporal; tests replace it.
type backend struct {
	connect   func(*config.Config) (temporalclient.Client, error)
	newWorker func(c temporalclient.Client, queue string) worker
}

func realBackend() backend {
	return backend{
		connect: temporal.Connect,
		newWorker: func(c temporalclient.Client, queue string) worker {
			return temporalworker.New(c, queue, temporalworker.Options{})
		},
	}
}

func wire(p *runtime.Process) error { return wireWith(p, realBackend()) }

const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
)

// wireWith registers the one component (the Temporal worker with its
// start/retry loop) and the Temporal readiness check. Temporal is non-fatal for
// readiness: the process is up and serving, it only cannot run workflows yet.
func wireWith(p *runtime.Process, b backend) error {
	temporal.Observe(temporal.Observability{Tracing: p.Obs.TracingEnabled(), Metrics: p.Obs.Metrics})
	s := &state{}
	p.Ready("temporal", s.check, runtime.NonFatal())
	p.Go("publish-worker", func(ctx context.Context) error {
		return runWorker(ctx, p.Logger, p.Config, b, s, initialBackoff)
	})
	return nil
}

// state is the connected client, once the worker has started on it.
type state struct {
	mu sync.RWMutex
	c  temporalclient.Client
}

func (s *state) set(c temporalclient.Client) { s.mu.Lock(); s.c = c; s.mu.Unlock() }

func (s *state) check(ctx context.Context) error {
	s.mu.RLock()
	c := s.c
	s.mu.RUnlock()
	if c == nil {
		return errors.New("temporal worker not started")
	}
	_, err := c.CheckHealth(ctx, &temporalclient.CheckHealthRequest{})
	return err
}

// runWorker builds a fresh Temporal client and worker on every attempt and
// retries with capped exponential backoff until the worker starts: Start can
// fail while the server is unreachable or its namespace does not exist yet
// (temporalio/auto-setup takes time to create it, and compose starts this
// binary concurrently). A fresh client per attempt, never a reused one: a
// client that already failed can keep reporting the same stale "connection
// refused" after the server is back (observed live in the old worker). An
// outage never ends the process: it shows as degraded in /health/ready.
func runWorker(ctx context.Context, logger *slog.Logger, cfg *config.Config, b backend, s *state, backoff time.Duration) error {
	for {
		c, err := b.connect(cfg)
		if err != nil {
			// Only a local misconfiguration (a bad host:port or namespace
			// string) fails here, never server unavailability.
			return err
		}
		w := b.newWorker(c, cfg.Temporal.TaskQueue)
		publish.Register(w)

		if err := w.Start(); err != nil {
			c.Close()
			logger.Error("publish worker failed to start, will retry", slog.Any("error", err), slog.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		s.set(c)
		logger.Info("publish worker started", slog.String("task_queue", cfg.Temporal.TaskQueue))
		<-ctx.Done()
		w.Stop()
		s.set(nil)
		c.Close()
		return nil
	}
}
