package config

import (
	"fmt"
	"time"
)

// AsynqConfig sizes the worker's Asynq pools and retry policy (libs/asynqx,
// RFC §10.2, ADR-0013). Only the worker binary runs Asynq, but every binary
// reads the same environment, so the checks below run in Load for all of them.
type AsynqConfig struct {
	// Per-queue pool sizes (ASYNQ_CRITICAL_CONCURRENCY, ASYNQ_DEFAULT_CONCURRENCY,
	// ASYNQ_LOW_CONCURRENCY): RFC §10.2's 10 / 5 / 3. The pools that touch
	// Postgres count against POSTGRES_MAX_OPEN_CONNS; Load does not enforce
	// that, because which handlers use the database is the worker's wiring.
	CriticalConcurrency int
	DefaultConcurrency  int
	LowConcurrency      int

	// MaxRetry is the retry budget of scheduled jobs (ASYNQ_MAX_RETRY, default 5):
	// retries after the first attempt, before Asynq archives the task.
	MaxRetry int
	// BackoffBase and BackoffMax shape the retry delay, Base * 2^n capped at
	// Max (ASYNQ_BACKOFF_BASE, ASYNQ_BACKOFF_MAX).
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// ShutdownTimeout bounds the drain of in-flight tasks (ASYNQ_SHUTDOWN_TIMEOUT).
	ShutdownTimeout time.Duration
}

// JobsConfig holds the scheduled jobs' intervals and retention (the worker's
// services/worker/jobs). Intervals are whole minutes that divide the hour, or
// whole hours that divide the day, because the scheduler runs cron
// expressions that every worker replica must agree on (CronSpec).
type JobsConfig struct {
	ReconcilePublishesInterval time.Duration // JOBS_RECONCILE_PUBLISHES_INTERVAL, default 1m
	AnalyticsReconcileInterval time.Duration // JOBS_ANALYTICS_RECONCILE_INTERVAL, default 1m
	EnrollmentCountInterval    time.Duration // JOBS_ENROLLMENT_COUNT_INTERVAL, default 1h
	IdempotencyPruneInterval   time.Duration // JOBS_IDEMPOTENCY_PRUNE_INTERVAL, default 1h
	OutboxPruneInterval        time.Duration // JOBS_OUTBOX_PRUNE_INTERVAL, default 1h

	// OutboxRetention is how long a published outbox row is kept before the
	// prune job may delete it (OUTBOX_RETENTION, default 168h = 7 days).
	OutboxRetention time.Duration
}

const (
	defaultAsynqCritical = 10
	defaultAsynqDefault  = 5
	defaultAsynqLow      = 3
	maxAsynqConcurrency  = 256
	maxAsynqRetry        = 25

	minOutboxRetention = time.Hour
	maxOutboxRetention = 365 * 24 * time.Hour
)

// CronSpec renders interval as the cron expression the asynqx scheduler
// takes, or reports why it cannot. Supported: a whole number of minutes that
// divides 60 (every N minutes, on the minute marks), or a whole number of hours
// that divides 24 (every N hours, at minute 0). "@every" is not an option: it
// counts from each process's start, so replicas would never agree on a tick.
func CronSpec(interval time.Duration) (string, error) {
	switch {
	case interval < time.Minute:
		return "", fmt.Errorf("interval %v is below one minute", interval)
	case interval < time.Hour:
		m := int(interval / time.Minute)
		if interval%time.Minute != 0 || 60%m != 0 {
			return "", fmt.Errorf("interval %v must be a whole number of minutes that divides 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30), or whole hours", interval)
		}
		if m == 1 {
			return "* * * * *", nil
		}
		return fmt.Sprintf("*/%d * * * *", m), nil
	default:
		h := int(interval / time.Hour)
		if interval%time.Hour != 0 || h > 24 || 24%h != 0 {
			return "", fmt.Errorf("interval %v must be a whole number of hours that divides 24 (1, 2, 3, 4, 6, 8, 12, 24)", interval)
		}
		switch h {
		case 1:
			return "0 * * * *", nil
		case 24:
			return "0 0 * * *", nil
		}
		return fmt.Sprintf("0 */%d * * *", h), nil
	}
}

// loadAsynq reads and validates the Asynq and scheduled-job settings, adding
// to errs. It takes Load's own parsers so the error wording is uniform.
func loadAsynq(
	optInt func(string, int) int,
	optDuration func(string, time.Duration) time.Duration,
	errs *[]string,
) (AsynqConfig, JobsConfig) {
	bad := func(format string, args ...any) { *errs = append(*errs, fmt.Sprintf(format, args...)) }
	inRange := func(key string, v, lo, hi int) {
		if v < lo || v > hi {
			bad("%s must be between %d and %d", key, lo, hi)
		}
	}

	a := AsynqConfig{
		CriticalConcurrency: optInt("ASYNQ_CRITICAL_CONCURRENCY", defaultAsynqCritical),
		DefaultConcurrency:  optInt("ASYNQ_DEFAULT_CONCURRENCY", defaultAsynqDefault),
		LowConcurrency:      optInt("ASYNQ_LOW_CONCURRENCY", defaultAsynqLow),
		MaxRetry:            optInt("ASYNQ_MAX_RETRY", 5),
		BackoffBase:         optDuration("ASYNQ_BACKOFF_BASE", 2*time.Second),
		BackoffMax:          optDuration("ASYNQ_BACKOFF_MAX", 10*time.Minute),
		ShutdownTimeout:     optDuration("ASYNQ_SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	inRange("ASYNQ_CRITICAL_CONCURRENCY", a.CriticalConcurrency, 1, maxAsynqConcurrency)
	inRange("ASYNQ_DEFAULT_CONCURRENCY", a.DefaultConcurrency, 1, maxAsynqConcurrency)
	inRange("ASYNQ_LOW_CONCURRENCY", a.LowConcurrency, 1, maxAsynqConcurrency)
	inRange("ASYNQ_MAX_RETRY", a.MaxRetry, 0, maxAsynqRetry)
	if a.BackoffBase <= 0 {
		bad("ASYNQ_BACKOFF_BASE must be greater than zero")
	}
	if a.BackoffMax <= 0 {
		bad("ASYNQ_BACKOFF_MAX must be greater than zero")
	}
	if a.BackoffBase > 0 && a.BackoffMax > 0 && a.BackoffMax < a.BackoffBase {
		bad("ASYNQ_BACKOFF_MAX must be at least ASYNQ_BACKOFF_BASE")
	}
	if a.ShutdownTimeout <= 0 {
		bad("ASYNQ_SHUTDOWN_TIMEOUT must be greater than zero")
	}

	j := JobsConfig{
		ReconcilePublishesInterval: optDuration("JOBS_RECONCILE_PUBLISHES_INTERVAL", time.Minute),
		AnalyticsReconcileInterval: optDuration("JOBS_ANALYTICS_RECONCILE_INTERVAL", time.Minute),
		EnrollmentCountInterval:    optDuration("JOBS_ENROLLMENT_COUNT_INTERVAL", time.Hour),
		IdempotencyPruneInterval:   optDuration("JOBS_IDEMPOTENCY_PRUNE_INTERVAL", time.Hour),
		OutboxPruneInterval:        optDuration("JOBS_OUTBOX_PRUNE_INTERVAL", time.Hour),
		OutboxRetention:            optDuration("OUTBOX_RETENTION", 7*24*time.Hour),
	}
	for key, d := range map[string]time.Duration{
		"JOBS_RECONCILE_PUBLISHES_INTERVAL": j.ReconcilePublishesInterval,
		"JOBS_ANALYTICS_RECONCILE_INTERVAL": j.AnalyticsReconcileInterval,
		"JOBS_ENROLLMENT_COUNT_INTERVAL":    j.EnrollmentCountInterval,
		"JOBS_IDEMPOTENCY_PRUNE_INTERVAL":   j.IdempotencyPruneInterval,
		"JOBS_OUTBOX_PRUNE_INTERVAL":        j.OutboxPruneInterval,
	} {
		if _, err := CronSpec(d); err != nil {
			bad("%s: %v", key, err)
		}
	}
	if j.OutboxRetention < minOutboxRetention || j.OutboxRetention > maxOutboxRetention {
		bad("OUTBOX_RETENTION must be between %v and %v", minOutboxRetention, maxOutboxRetention)
	}
	return a, j
}
