package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_AsynqAndJobs_Defaults(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	a, j := cfg.Asynq, cfg.Jobs
	if a.CriticalConcurrency != 10 || a.DefaultConcurrency != 5 || a.LowConcurrency != 3 {
		t.Errorf("pools = %d/%d/%d, want RFC 10.2's 10/5/3", a.CriticalConcurrency, a.DefaultConcurrency, a.LowConcurrency)
	}
	if a.MaxRetry != 5 || a.BackoffBase != 2*time.Second || a.BackoffMax != 10*time.Minute || a.ShutdownTimeout != 30*time.Second {
		t.Errorf("retry/backoff/shutdown defaults = %+v", a)
	}
	if j.OutboxRetention != 7*24*time.Hour {
		t.Errorf("OutboxRetention = %v, want 7 days", j.OutboxRetention)
	}
	if j.ReconcilePublishesInterval != time.Minute || j.AnalyticsReconcileInterval != time.Minute ||
		j.EnrollmentCountInterval != time.Hour || j.IdempotencyPruneInterval != time.Hour || j.OutboxPruneInterval != time.Hour {
		t.Errorf("interval defaults = %+v", j)
	}
}

func TestLoad_AsynqAndJobs_Overrides(t *testing.T) {
	setValidEnv(t)
	t.Setenv("ASYNQ_CRITICAL_CONCURRENCY", "12")
	t.Setenv("ASYNQ_LOW_CONCURRENCY", "1")
	t.Setenv("ASYNQ_MAX_RETRY", "0")
	t.Setenv("ASYNQ_BACKOFF_BASE", "500ms")
	t.Setenv("OUTBOX_RETENTION", "48h")
	t.Setenv("JOBS_OUTBOX_PRUNE_INTERVAL", "6h")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Asynq.CriticalConcurrency != 12 || cfg.Asynq.LowConcurrency != 1 || cfg.Asynq.MaxRetry != 0 ||
		cfg.Asynq.BackoffBase != 500*time.Millisecond || cfg.Jobs.OutboxRetention != 48*time.Hour || cfg.Jobs.OutboxPruneInterval != 6*time.Hour {
		t.Fatalf("overrides not applied: %+v %+v", cfg.Asynq, cfg.Jobs)
	}
}

func TestLoad_AsynqAndJobs_Rejects(t *testing.T) {
	cases := []struct{ key, val string }{
		{"ASYNQ_CRITICAL_CONCURRENCY", "0"},
		{"ASYNQ_DEFAULT_CONCURRENCY", "-1"},
		{"ASYNQ_LOW_CONCURRENCY", "257"},
		{"ASYNQ_LOW_CONCURRENCY", "three"},
		{"ASYNQ_MAX_RETRY", "-1"},
		{"ASYNQ_MAX_RETRY", "26"},
		{"ASYNQ_BACKOFF_BASE", "0s"},
		{"ASYNQ_BACKOFF_BASE", "20m"}, // above the 10m default max
		{"ASYNQ_BACKOFF_MAX", "-5s"},
		{"ASYNQ_SHUTDOWN_TIMEOUT", "0s"},
		{"ASYNQ_SHUTDOWN_TIMEOUT", "soon"},
		{"OUTBOX_RETENTION", "30m"},
		{"OUTBOX_RETENTION", "9000h"},
		{"JOBS_RECONCILE_PUBLISHES_INTERVAL", "30s"},
		{"JOBS_ANALYTICS_RECONCILE_INTERVAL", "7m"},
		{"JOBS_ENROLLMENT_COUNT_INTERVAL", "90m"},
		{"JOBS_IDEMPOTENCY_PRUNE_INTERVAL", "5h"},
		{"JOBS_OUTBOX_PRUNE_INTERVAL", "48h"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.val, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(c.key, c.val)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), c.key) {
				t.Fatalf("want an error naming %s, got %v", c.key, err)
			}
		})
	}
}

func TestCronSpec(t *testing.T) {
	ok := map[time.Duration]string{
		time.Minute:      "* * * * *",
		5 * time.Minute:  "*/5 * * * *",
		30 * time.Minute: "*/30 * * * *",
		time.Hour:        "0 * * * *",
		6 * time.Hour:    "0 */6 * * *",
		24 * time.Hour:   "0 0 * * *",
	}
	for d, want := range ok {
		got, err := CronSpec(d)
		if err != nil || got != want {
			t.Errorf("CronSpec(%v) = %q, %v; want %q", d, got, err, want)
		}
	}
	for _, d := range []time.Duration{0, -time.Minute, 59 * time.Second, 90 * time.Second, 7 * time.Minute, 45 * time.Minute, 90 * time.Minute, 5 * time.Hour, 25 * time.Hour, 48 * time.Hour} {
		if got, err := CronSpec(d); err == nil {
			t.Errorf("CronSpec(%v) = %q, want an error", d, got)
		}
	}
}

func TestLoad_ProcessedEventsRetention(t *testing.T) {
	setValidEnv(t)
	t.Setenv("PROCESSED_EVENTS_RETENTION", "")
	cfg, err := Load()
	if err != nil || cfg.Jobs.ProcessedEventsRetention != 336*time.Hour {
		t.Fatalf("default = %v, %v; want 336h", cfg.Jobs.ProcessedEventsRetention, err)
	}
	t.Setenv("PROCESSED_EVENTS_RETENTION", "48h")
	cfg, err = Load()
	if err != nil || cfg.Jobs.ProcessedEventsRetention != 48*time.Hour {
		t.Fatalf("48h = %v, %v", cfg.Jobs.ProcessedEventsRetention, err)
	}
	for _, bad := range []string{"0", "0s", "-1h", "soon", "14d"} {
		t.Setenv("PROCESSED_EVENTS_RETENTION", bad)
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "PROCESSED_EVENTS_RETENTION") {
			t.Errorf("PROCESSED_EVENTS_RETENTION=%q: want a startup error, got %v", bad, err)
		}
	}
}
