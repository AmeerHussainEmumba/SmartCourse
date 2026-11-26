package asynqx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DefaultPoolSizes is RFC §10.2's table: critical 10 (bounded by provider
// rate limits), default 5, low 3 (bounded by the database connection budget).
func DefaultPoolSizes() map[string]int {
	return map[string]int{QueueCritical: 10, QueueDefault: 5, QueueLow: 3}
}

// queueOrder is the order Servers starts and reports queues in.
var queueOrder = []string{QueueCritical, QueueDefault, QueueLow}

// ErrUnknownQueue is returned by Servers.Register for a queue that has no pool.
var ErrUnknownQueue = errors.New("asynqx: no pool configured for queue")

// ServersConfig configures one Server per queue.
type ServersConfig struct {
	Redis RedisOpt

	// Namespace prefixes every queue the servers consume (see Namespace). Set
	// it once, to the owning service's name; empty means no prefix. Pools and
	// Register keep using the logical queue names.
	Namespace Namespace

	// Pools maps queue name to the size of that queue's own pool (the number of
	// handlers that may run at once for it). Nil means DefaultPoolSizes. Only
	// the queues listed here are served; a queue left out is never consumed.
	// The sum of the pools that touch Postgres must stay below the connection
	// pool's MaxOpenConns (RFC §10.2).
	Pools map[string]int

	// ShutdownTimeout, Backoff, RetryDelay, OnExhausted,
	// DelayedTaskCheckInterval and Logger apply to every server as in
	// ServerConfig.
	ShutdownTimeout          time.Duration
	Backoff                  BackoffConfig
	RetryDelay               RetryDelayFunc
	OnExhausted              func(ctx context.Context, f FailedTask)
	DelayedTaskCheckInterval time.Duration
	Logger                   *slog.Logger
}

// Servers runs one Server per queue so each queue has its own bounded pool.
// Asynq sizes concurrency per server, not per queue, and weighted queues inside
// one server share its pool, so a slow queue could occupy slots meant for
// another: separate servers are what make RFC §10.2's per-queue sizes real.
// The 6:3:1 weights (RFC §6.7) only matter inside a shared pool, so each
// server here serves exactly its own queue.
//
// Packages register handlers by task type with Register, which takes a plain
// func over the payload, so they need not import Asynq.
type Servers struct {
	ns      Namespace
	byQueue map[string]*Server

	mu      sync.Mutex
	types   map[string]string // task type -> queue
	started bool
}

// NewServers validates the config and builds a server for every pool. It does
// not touch Redis until Start/Run.
func NewServers(cfg ServersConfig) (*Servers, error) {
	if cfg.Redis == nil {
		return nil, errors.New("asynqx: ServersConfig.Redis is required")
	}
	if err := ValidateNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	pools := cfg.Pools
	if pools == nil {
		pools = DefaultPoolSizes()
	}
	if len(pools) == 0 {
		return nil, errors.New("asynqx: empty Pools")
	}
	for q, n := range pools {
		if !validQueue(q) {
			return nil, fmt.Errorf("asynqx: unknown queue %q", q)
		}
		if n <= 0 {
			return nil, fmt.Errorf("asynqx: queue %q has non-positive pool size %d", q, n)
		}
	}
	if err := cfg.Backoff.Validate(); err != nil {
		return nil, err
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	ss := &Servers{ns: cfg.Namespace, byQueue: map[string]*Server{}, types: map[string]string{}}
	for _, q := range queueOrder {
		n, ok := pools[q]
		if !ok {
			continue
		}
		srv, err := NewServer(ServerConfig{
			Redis:                    cfg.Redis,
			Namespace:                cfg.Namespace,
			Concurrency:              n,
			Queues:                   map[string]int{q: 1},
			ShutdownTimeout:          cfg.ShutdownTimeout,
			Backoff:                  cfg.Backoff,
			RetryDelay:               cfg.RetryDelay,
			OnExhausted:              cfg.OnExhausted,
			DelayedTaskCheckInterval: cfg.DelayedTaskCheckInterval,
			Logger:                   log.With("queue", QueueName(cfg.Namespace, q)),
		})
		if err != nil {
			return nil, fmt.Errorf("queue %s: %w", q, err)
		}
		ss.byQueue[q] = srv
	}
	return ss, nil
}

// Register routes tasks of taskType, pulled from the logical queue (QueueLow,
// not a prefixed name), to fn. fn receives the
// task payload; returning an error retries the task (SkipRetry archives it),
// and a panic is recovered as in Server.Handle. A task type belongs to one
// queue: registering it twice is an error. Register before Start.
func (ss *Servers) Register(taskType, queue string, fn func(ctx context.Context, payload []byte) error) error {
	if taskType == "" || fn == nil {
		return errors.New("asynqx: Register needs a task type and a handler")
	}
	srv, ok := ss.byQueue[queue]
	if !ok {
		return fmt.Errorf("%w: %q (task %q)", ErrUnknownQueue, queue, taskType)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.started {
		return errors.New("asynqx: Register after Start")
	}
	if prev, dup := ss.types[taskType]; dup {
		return fmt.Errorf("asynqx: task type %q already registered on queue %q", taskType, prev)
	}
	ss.types[taskType] = queue
	srv.Handle(taskType, func(ctx context.Context, t *Task) error { return fn(ctx, t.Payload()) })
	return nil
}

// Namespace is the queue prefix the servers were built with ("" for none).
func (ss *Servers) Namespace() Namespace { return ss.ns }

// Queues lists the Redis names of the queues that have a pool (namespace
// included), in critical, default, low order.
func (ss *Servers) Queues() []string {
	var out []string
	for _, q := range ss.logicalQueues() {
		out = append(out, QueueName(ss.ns, q))
	}
	return out
}

func (ss *Servers) logicalQueues() []string {
	var out []string
	for _, q := range queueOrder {
		if _, ok := ss.byQueue[q]; ok {
			out = append(out, q)
		}
	}
	return out
}

// Start begins processing on every queue in the background. If one server
// fails to start, the ones already started are shut down and the error is
// returned.
func (ss *Servers) Start() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.started {
		return errors.New("asynqx: servers already started")
	}
	var begun []*Server
	for _, q := range ss.logicalQueues() {
		srv := ss.byQueue[q]
		if err := srv.Start(); err != nil {
			shutdownAll(begun)
			return fmt.Errorf("queue %s: %w", q, err)
		}
		begun = append(begun, srv)
	}
	ss.started = true
	return nil
}

// Shutdown drains every pool at once, so the wait is bounded by one
// ShutdownTimeout, not one per queue. Safe to call more than once.
func (ss *Servers) Shutdown() {
	ss.mu.Lock()
	started := ss.started
	ss.started = false
	ss.mu.Unlock()
	if !started {
		return
	}
	all := make([]*Server, 0, len(ss.byQueue))
	for _, q := range ss.logicalQueues() {
		all = append(all, ss.byQueue[q])
	}
	shutdownAll(all)
}

func shutdownAll(srvs []*Server) {
	var wg sync.WaitGroup
	for _, s := range srvs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Shutdown()
		}()
	}
	wg.Wait()
}

// Run starts every server, blocks until ctx is cancelled, then drains them.
func (ss *Servers) Run(ctx context.Context) error {
	if err := ss.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	ss.Shutdown()
	return nil
}

// PanicCount is the number of handler panics recovered across all queues.
func (ss *Servers) PanicCount() (n int64) {
	for _, s := range ss.byQueue {
		n += s.PanicCount()
	}
	return n
}

// ExhaustedCount is the number of tasks passed to OnExhausted across all queues.
func (ss *Servers) ExhaustedCount() (n int64) {
	for _, s := range ss.byQueue {
		n += s.ExhaustedCount()
	}
	return n
}
