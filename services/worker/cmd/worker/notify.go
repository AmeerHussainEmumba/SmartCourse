package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/notification"
)

// deliveryTimeout bounds one delivery attempt. It must stay below the
// notification package's claim lease (5 minutes by default), so a live attempt
// is never mistaken for an abandoned one.
const deliveryTimeout = 2 * time.Minute

// sweepInterval is how often pending deliveries are (re-)enqueued. Handlers
// only write delivery rows; the sweep is the single enqueue path, which is also
// what repairs a crash between commit and enqueue or a flushed Redis.
const sweepInterval = 2 * time.Second

// newProvider builds the mail provider from cfg, or returns nil when SMTP_HOST
// is unset (deliveries then stay pending and a warning says so). The local
// stack points it at Mailpit; no real mail provider is integrated by decision.
func newProvider(logger *slog.Logger, cfg *config.Config) notification.Provider {
	if !cfg.SMTP.Configured() {
		return nil
	}
	p, err := notification.NewSMTPProvider(cfg.SMTP)
	if err != nil {
		logger.Error("smtp provider not built; notification deliveries will stay pending", slog.Any("error", err))
		return nil
	}
	return p
}

// wireNotifications registers the delivery task on the critical queue and
// starts the pending-delivery sweeper. It must run before servers.Start
// (Register is refused afterwards). The returned func closes the enqueue client.
// With a nil provider nothing is registered or swept.
func wireNotifications(ctx context.Context, logger *slog.Logger, servers *asynqx.Servers, deps *infra.Dependencies, cfg *config.Config, provider notification.Provider) (closeFn func()) {
	closeFn = func() {}
	if provider == nil {
		logger.Warn("notification delivery not wired: no mail provider (set SMTP_HOST); deliveries stay pending")
		return closeFn
	}
	deliver := notification.NewDeliveryHandler(deps.DB, provider)
	err := servers.Register(notification.TaskTypeDeliver, asynqx.QueueCritical, func(ctx context.Context, payload []byte) error {
		err := deliver(ctx, payload)
		if errors.Is(err, notification.ErrPermanent) {
			// Retrying cannot help a permanent failure (a 5xx reply, a bad address).
			return fmt.Errorf("%w: %w", asynqx.SkipRetry, err)
		}
		return err
	})
	if err != nil {
		logger.Error("notification delivery task not registered", slog.Any("error", err))
		return closeFn
	}
	client := asynqx.NewClient(asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB))
	go notification.RunSweeper(ctx, deps.DB, notificationEnqueuer{client}, sweepInterval, notification.SweepOptions{})
	return func() { _ = client.Close() }
}

// taskIDBucket is how long one task ID stays the same for one delivery. It
// matches the sweeper's default RequeueAfter: inside the window a second
// enqueue of the same row is a harmless duplicate, and after it a fresh ID lets
// the sweep re-enqueue a row whose earlier task was archived (an Asynq task ID
// keeps existing while the task sits in the archive, so a fixed ID would make
// every later enqueue a silent duplicate and the row would stay pending).
const taskIDBucket = 5 * time.Minute

// deliveryTaskID derives the Asynq task ID from the payload and the current
// time bucket.
func deliveryTaskID(taskType string, payload []byte, now time.Time) string {
	return asynqx.DeterministicID(taskType, string(payload), strconv.FormatInt(now.Unix()/int64(taskIDBucket/time.Second), 10))
}

// notificationEnqueuer adapts asynqx.Client to notification.Enqueuer. Enqueuing
// a row the sweep already queued within the same time bucket is a no-op that
// asynqx reports as a duplicate, not an error.
type notificationEnqueuer struct{ c *asynqx.Client }

func (e notificationEnqueuer) Enqueue(ctx context.Context, taskType string, payload []byte) error {
	_, err := e.c.Enqueue(ctx, asynqx.Job{
		Type:    taskType,
		Payload: payload,
		ID:      deliveryTaskID(taskType, payload, time.Now()),
		Queue:   asynqx.QueueCritical,
		Timeout: deliveryTimeout,
	})
	return err
}
