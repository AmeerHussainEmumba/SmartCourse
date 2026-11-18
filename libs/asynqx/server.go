package asynqx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
)

// Server defaults.
const (
	DefaultConcurrency     = 10 // RFC §10.2: the critical pool size
	DefaultShutdownTimeout = 30 * time.Second
)

// HandlerFunc processes one task. Returning an error schedules a retry (or, on
// the last attempt, exhausts the task); returning nil completes it.
type HandlerFunc func(ctx context.Context, t *Task) error

// FailedTask describes a task that will not be retried again.
type FailedTask struct {
	ID       string
	Type     string
	Queue    string
	Payload  []byte
	Retried  int // retries performed before this final attempt
	MaxRetry int
	Err      error // the final attempt's error; errors.Is(Err, ErrPanic) for a panic
}

// ServerConfig configures a worker Server.
type ServerConfig struct {
	Redis RedisOpt

	// Concurrency bounds simultaneously executing handlers across all queues
	// of this server. Zero means DefaultConcurrency; negative is an error.
	// Asynq sizes one pool per server, not per queue: to give a queue its own
	// pool (RFC §10.2), run a second Server with a single-entry Queues map.
	Concurrency int

	// Queues maps queue name to weight. Nil means DefaultQueueWeights (6:3:1).
	Queues map[string]int

	// ShutdownTimeout is how long Run waits for in-flight handlers after its
	// context is cancelled. Handlers still running at the deadline have their
	// context cancelled and their task returned to the queue, to be retried
	// (at-least-once, RFC §10.2). Zero means DefaultShutdownTimeout.
	ShutdownTimeout time.Duration

	// Backoff computes retry delays. Zero value means DefaultBackoff.
	Backoff BackoffConfig
	// RetryDelay overrides Backoff entirely when set.
	RetryDelay RetryDelayFunc

	// OnExhausted is called once for a task whose final attempt failed (retries
	// used up, or SkipRetry), so the caller can park it, for example as a failed
	// event. It runs on the worker goroutine before the task is archived; keep
	// it short. A panic inside the hook is recovered and logged.
	OnExhausted func(ctx context.Context, f FailedTask)

	// DelayedTaskCheckInterval is how often scheduled and retry-waiting tasks
	// are moved to pending. Zero keeps Asynq's default (5s), which is also the
	// granularity of retry timing.
	DelayedTaskCheckInterval time.Duration

	Logger *slog.Logger
}

// Server runs handlers against the queues. Register handlers with Handle
// before Run or Start.
type Server struct {
	cfg ServerConfig
	srv *asynq.Server
	mux *asynq.ServeMux
	log *slog.Logger

	mu      sync.Mutex
	started bool

	panics    atomic.Int64
	exhausted atomic.Int64
}

// NewServer validates the config and builds the server. It does not touch
// Redis until Start/Run.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Redis == nil {
		return nil, errors.New("asynqx: ServerConfig.Redis is required")
	}
	switch {
	case cfg.Concurrency < 0:
		return nil, errors.New("asynqx: negative Concurrency")
	case cfg.Concurrency == 0:
		cfg.Concurrency = DefaultConcurrency
	}
	if cfg.Queues == nil {
		cfg.Queues = DefaultQueueWeights()
	}
	if len(cfg.Queues) == 0 {
		return nil, errors.New("asynqx: empty Queues")
	}
	for q, w := range cfg.Queues {
		if !validQueue(q) {
			return nil, fmt.Errorf("asynqx: unknown queue %q", q)
		}
		if w <= 0 {
			return nil, fmt.Errorf("asynqx: queue %q has non-positive weight %d", q, w)
		}
	}
	switch {
	case cfg.ShutdownTimeout < 0:
		return nil, errors.New("asynqx: negative ShutdownTimeout")
	case cfg.ShutdownTimeout == 0:
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.RetryDelay == nil {
		b := cfg.Backoff
		if b == (BackoffConfig{}) {
			b = DefaultBackoff
		}
		cfg.RetryDelay = b.Func()
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	s := &Server{cfg: cfg, mux: asynq.NewServeMux(), log: log}
	s.srv = asynq.NewServer(cfg.Redis, asynq.Config{
		Concurrency:              cfg.Concurrency,
		Queues:                   cfg.Queues,
		StrictPriority:           false, // weighted, not strict: low must not starve
		ShutdownTimeout:          cfg.ShutdownTimeout,
		RetryDelayFunc:           cfg.RetryDelay,
		DelayedTaskCheckInterval: cfg.DelayedTaskCheckInterval,
		ErrorHandler:             asynq.ErrorHandlerFunc(s.onError),
		Logger:                   slogAdapter{log},
	})
	return s, nil
}

// Handle registers h for task type typ. h is wrapped so that a panic is
// recovered and returned as an error wrapping ErrPanic: the attempt fails and
// is retried like any other failure, and the worker process survives.
func (s *Server) Handle(typ string, h HandlerFunc) {
	s.mux.HandleFunc(typ, func(ctx context.Context, t *asynq.Task) (err error) {
		defer func() {
			if r := recover(); r != nil {
				s.panics.Add(1)
				s.log.Error("asynqx: handler panic", "type", typ, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				err = panicError(r)
			}
		}()
		return h(ctx, t)
	})
}

// Start begins processing in the background. Pair it with Shutdown; most
// callers want Run.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("asynqx: server already started")
	}
	if err := s.srv.Start(s.mux); err != nil {
		return fmt.Errorf("asynqx: start server: %w", err)
	}
	s.started = true
	return nil
}

// Shutdown stops pulling new tasks and waits up to ShutdownTimeout for
// in-flight handlers, then returns unfinished tasks to the queue. It blocks
// until done and is safe to call once after Start.
func (s *Server) Shutdown() {
	s.mu.Lock()
	started := s.started
	s.started = false
	s.mu.Unlock()
	if started {
		s.srv.Shutdown()
	}
}

// Run starts the server, blocks until ctx is cancelled, then drains as
// Shutdown does. It returns nil after a clean drain.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	s.Shutdown()
	return nil
}

// PanicCount is the number of handler panics recovered so far. A panic is also
// an ordinary failure (retry, then OnExhausted).
func (s *Server) PanicCount() int64 { return s.panics.Load() }

// ExhaustedCount is the number of tasks passed to OnExhausted so far.
func (s *Server) ExhaustedCount() int64 { return s.exhausted.Load() }

// onError is Asynq's error handler: called after every failed attempt. It
// detects the final one and invokes OnExhausted.
func (s *Server) onError(ctx context.Context, t *asynq.Task, err error) {
	if errors.Is(err, asynq.RevokeTask) {
		return // deliberate discard, not a failure to park
	}
	meta, ok := MetaFrom(ctx)
	if !ok {
		return
	}
	if !errors.Is(err, asynq.SkipRetry) && meta.Retried < meta.MaxRetry {
		return // a retry is still scheduled
	}
	s.exhausted.Add(1)
	s.log.Warn("asynqx: task exhausted retries", "id", meta.ID, "type", t.Type(), "queue", meta.Queue, "err", err)
	if s.cfg.OnExhausted == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("asynqx: OnExhausted panicked", "id", meta.ID, "panic", fmt.Sprint(r))
		}
	}()
	s.cfg.OnExhausted(ctx, FailedTask{
		ID: meta.ID, Type: t.Type(), Queue: meta.Queue, Payload: t.Payload(),
		Retried: meta.Retried, MaxRetry: meta.MaxRetry, Err: err,
	})
}

// slogAdapter lets Asynq log through slog.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Debug(args ...any) { a.l.Debug(fmt.Sprint(args...)) }
func (a slogAdapter) Info(args ...any)  { a.l.Info(fmt.Sprint(args...)) }
func (a slogAdapter) Warn(args ...any)  { a.l.Warn(fmt.Sprint(args...)) }
func (a slogAdapter) Error(args ...any) { a.l.Error(fmt.Sprint(args...)) }

// Fatal logs at error level. Asynq's own default logger exits the process
// here; a library must not.
func (a slogAdapter) Fatal(args ...any) { a.l.Error(fmt.Sprint(args...)) }
