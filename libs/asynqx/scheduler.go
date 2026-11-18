package asynqx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	// TickSkewTolerance is the clock and scheduling skew between replicas that
	// still resolves to the same tick (and so the same task ID). Cron specs here
	// have minute resolution, far above this.
	TickSkewTolerance = 10 * time.Second

	// DefaultTickRetention keeps a completed tick's task ID in Redis so a
	// replica whose clock fires a little late cannot re-enqueue a tick that
	// already ran and was deleted.
	DefaultTickRetention = 5 * time.Minute

	enqueueTimeout = 10 * time.Second
)

// ErrDuplicateEntry is returned when an entry name is registered twice.
var ErrDuplicateEntry = errors.New("asynqx: scheduler entry already registered")

// SchedulerConfig configures a Scheduler.
type SchedulerConfig struct {
	Redis RedisOpt
	// Location interprets cron specs; nil means UTC.
	Location *time.Location
	// TickRetention overrides DefaultTickRetention.
	TickRetention time.Duration
	// OnAttempt, if set, is called each time this scheduler tries to enqueue a
	// tick, before the enqueue. With N replicas it fires N times per tick while
	// Redis ends up holding one task. Keep it short.
	OnAttempt func(name, taskID string)
	Logger    *slog.Logger
}

// Entry is one periodic job.
type Entry struct {
	// Name identifies the entry in the tick task ID, so it must be the same
	// string on every replica and unique per entry.
	Name string
	// Spec is a standard 5-field cron expression or @hourly/@daily/...
	// "@every" is rejected: it is relative to each process's start time, so
	// replicas would never agree on a tick.
	Spec    string
	Type    string
	Payload []byte
	Queue   string // empty means low (maintenance), RFC §6.7
	// MaxRetry and Timeout as in Job; zero values mean the Job defaults.
	MaxRetry int
	Timeout  time.Duration
}

// Scheduler enqueues periodic jobs. Every replica of a worker may run one with
// the same entries: each tick's task ID is derived from the entry name and the
// tick time, so the replicas' enqueues collapse to a single task.
//
// It is a cron loop feeding Client.Enqueue rather than asynq.Scheduler: Asynq's
// scheduler fixes a task's options at registration, so it cannot give each tick
// its own task ID, and its heartbeat reads those options concurrently (so they
// cannot be rewritten per tick either; the race detector finds it). The cost is
// that entries do not appear in Asynq's own scheduler view (asynqmon).
type Scheduler struct {
	cfg    SchedulerConfig
	client *Client
	log    *slog.Logger
	loc    *time.Location
	cron   *cron.Cron

	mu      sync.Mutex
	names   map[string]struct{}
	started bool

	ctx    context.Context
	cancel context.CancelFunc
	closed sync.Once

	duplicates atomic.Int64
}

// NewScheduler builds a scheduler. Register entries, then Start or Run.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	if cfg.Redis == nil {
		return nil, errors.New("asynqx: SchedulerConfig.Redis is required")
	}
	if cfg.TickRetention < 0 {
		return nil, errors.New("asynqx: negative TickRetention")
	}
	if cfg.TickRetention == 0 {
		cfg.TickRetention = DefaultTickRetention
	}
	loc := cfg.Location
	if loc == nil {
		loc = time.UTC
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		cfg: cfg, log: log, loc: loc,
		client: NewClient(cfg.Redis),
		cron:   cron.New(cron.WithLocation(loc)),
		names:  map[string]struct{}{},
		ctx:    ctx, cancel: cancel,
	}, nil
}

// Register adds a periodic job. It must be called before Start. Registering
// the same Name twice on one scheduler is an error (it would otherwise enqueue
// twice per tick from one process).
func (sc *Scheduler) Register(e Entry) error {
	if e.Name == "" || e.Type == "" {
		return fmt.Errorf("%w: entry needs Name and Type", ErrInvalidJob)
	}
	if strings.HasPrefix(strings.TrimSpace(e.Spec), "@every") {
		return fmt.Errorf("%w: @every is not tick-aligned across replicas; use a cron expression", ErrInvalidJob)
	}
	sched, err := cron.ParseStandard(e.Spec)
	if err != nil {
		return fmt.Errorf("%w: spec %q: %v", ErrInvalidJob, e.Spec, err)
	}
	queue := e.Queue
	if queue == "" {
		queue = QueueLow
	}
	proto := Job{Type: e.Type, Payload: e.Payload, ID: "validate", Queue: queue, MaxRetry: e.MaxRetry, Timeout: e.Timeout, Retention: sc.cfg.TickRetention}
	if _, _, err := proto.options(); err != nil {
		return err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.started {
		return errors.New("asynqx: Register after Start")
	}
	if _, dup := sc.names[e.Name]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateEntry, e.Name)
	}
	sc.names[e.Name] = struct{}{}
	sc.cron.Schedule(sched, cron.FuncJob(func() { sc.fire(e, sched, proto) }))
	return nil
}

// TickID returns the task ID a replica enqueues for the tick at slot.
// Exported so callers and tests can assert on it.
func TickID(name string, slot time.Time) string {
	return DeterministicID("asynqx-tick", name, strconv.FormatInt(slot.Unix(), 10))
}

// tickSlot maps the moment a tick fired to the scheduled tick time: the first
// scheduled time after (now - TickSkewTolerance). All replicas firing within
// the tolerance of one tick resolve to that tick.
func tickSlot(s cron.Schedule, now time.Time) time.Time {
	return s.Next(now.Add(-TickSkewTolerance))
}

func (sc *Scheduler) fire(e Entry, sched cron.Schedule, job Job) {
	job.ID = TickID(e.Name, tickSlot(sched, time.Now().In(sc.loc)))
	if sc.cfg.OnAttempt != nil {
		sc.cfg.OnAttempt(e.Name, job.ID)
	}
	ctx, cancel := context.WithTimeout(sc.ctx, enqueueTimeout)
	defer cancel()
	res, err := sc.client.Enqueue(ctx, job)
	switch {
	case err != nil:
		sc.log.Error("asynqx: scheduled enqueue failed", "entry", e.Name, "err", err)
	case res.Duplicate:
		sc.duplicates.Add(1) // another replica already enqueued this tick
	}
}

// DuplicateCount is how many of this scheduler's tick enqueues were no-ops
// because another replica had already enqueued the tick.
func (sc *Scheduler) DuplicateCount() int64 { return sc.duplicates.Load() }

// Start begins firing entries in the background.
func (sc *Scheduler) Start() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.started {
		return errors.New("asynqx: scheduler already started")
	}
	sc.started = true
	sc.cron.Start()
	return nil
}

// Shutdown stops firing, waits for an in-progress enqueue, and releases Redis.
// It is safe to call more than once.
func (sc *Scheduler) Shutdown() {
	sc.closed.Do(func() {
		<-sc.cron.Stop().Done()
		sc.cancel()
		_ = sc.client.Close()
	})
}

// Run starts the scheduler and blocks until ctx is cancelled.
func (sc *Scheduler) Run(ctx context.Context) error {
	if err := sc.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	sc.Shutdown()
	return nil
}
