package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer defaults.
const (
	DefaultMaxInFlight     = 10 // RFC §10.2: a bounded pool, sized below the DB pool
	DefaultMaxAttempts     = 5
	DefaultShutdownTimeout = 30 * time.Second

	// commitTimeout bounds one offset commit. Commits use their own context so
	// the progress made before a shutdown or a cancelled handler is still saved.
	commitTimeout = 10 * time.Second
	// cancelGrace is how long Run waits for handlers to return after their
	// context was cancelled at the shutdown deadline, before giving up on them.
	cancelGrace = 5 * time.Second
)

// Handler processes one record. Returning nil lets the runner commit the
// record's offset; returning an error retries it with backoff, up to
// ConsumerConfig.MaxAttempts. The context is cancelled only when the runner
// gives up on in-flight work (shutdown deadline, partition lost); a handler
// that returns because of that is not counted as a failure.
//
// Delivery is at-least-once: a record can be handed to a handler again after a
// crash, a rebalance or a shutdown deadline, so handlers must be idempotent
// (RFC §6.6).
type Handler func(ctx context.Context, rec Record) error

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	// Brokers are the seed broker addresses (host:port). Required.
	Brokers []string
	// GroupID is the consumer group. Required: it names the consumer (RFC §6.5)
	// and is what offsets are stored under.
	GroupID string
	// Topics to subscribe to. Required.
	Topics []string
	// Handler is called for every record. Required.
	Handler Handler
	// ClientID is reported to the broker. Optional.
	ClientID string

	// MaxInFlight bounds how many handlers run at once across all partitions
	// of this consumer. Zero means DefaultMaxInFlight; negative is an error.
	// Records of one partition are always handled one at a time, in order, so
	// the effective parallelism is also capped by the partitions assigned.
	MaxInFlight int

	// MaxAttempts is the handler's retry budget: how many times one record is
	// tried (first call included) before OnExhausted runs and the offset
	// advances. Zero means DefaultMaxAttempts; negative is an error. A record
	// being retried holds its partition (order is kept) but not a MaxInFlight
	// slot, and no other partition.
	MaxAttempts int
	// Backoff is the delay between attempts. Zero value means DefaultBackoff.
	Backoff BackoffConfig

	// OnExhausted is called once when a record has used its budget, after the
	// last failed attempt. This is the dead-letter seam (RFC §6.9, ADR-0031):
	// park the record somewhere durable. It runs on the partition's goroutine
	// and holds a MaxInFlight slot.
	//
	// Returning nil means the record is dealt with and the offset advances, so a
	// poison record never stalls its partition. A hook whose store is down
	// should not return nil (that drops the record): it should hold, retrying
	// with backoff, until the store accepts the record, and return an error only
	// when ctx is done (the consumer is stopping). A non-nil error leaves the
	// record uncommitted: it is redelivered with a fresh budget to the next
	// consumer of the group, and this partition's worker stops. A panic in the
	// hook is recovered, logged, and the record is skipped. A nil OnExhausted
	// means the record is only logged and skipped.
	OnExhausted func(ctx context.Context, rec Record, err error) error

	// ShutdownTimeout is how long Run waits for in-flight handlers once its
	// context is cancelled (and how long a partition revoked by a rebalance
	// waits for its handler). Handlers still running at the deadline have
	// their context cancelled and their record is not committed. Zero means
	// DefaultShutdownTimeout.
	ShutdownTimeout time.Duration

	// SessionTimeout is the group session timeout. Zero keeps franz-go's
	// default (45s); the broker enforces its own minimum (6s by default). The
	// heartbeat interval is set to a third of it.
	SessionTimeout time.Duration

	// StartAtEnd makes a group with no committed offset start at the end of
	// each partition instead of the beginning. The default (beginning) is the
	// safe one for event consumers: a new group sees the whole retained log.
	StartAtEnd bool

	Logger *slog.Logger
}

func (c ConsumerConfig) validate() error {
	if len(c.Brokers) == 0 {
		return errInvalid("Brokers is required")
	}
	if c.GroupID == "" {
		return errInvalid("GroupID is required")
	}
	if len(c.Topics) == 0 {
		return errInvalid("Topics is required")
	}
	for _, t := range c.Topics {
		if t == "" {
			return errInvalid("Topics contains an empty name")
		}
	}
	if c.Handler == nil {
		return errInvalid("Handler is required")
	}
	if c.MaxInFlight < 0 {
		return errNegative("MaxInFlight")
	}
	if c.MaxAttempts < 0 {
		return errNegative("MaxAttempts")
	}
	if c.ShutdownTimeout < 0 {
		return errNegative("ShutdownTimeout")
	}
	if c.SessionTimeout < 0 {
		return errNegative("SessionTimeout")
	}
	return c.Backoff.validate()
}

// Stats is a snapshot of a Consumer's counters, for callers that export
// metrics (this package depends on no metrics library).
type Stats struct {
	Handled   int64 // handler calls that returned nil
	Failed    int64 // handler calls that returned an error (retries included)
	Exhausted int64 // records that used their whole budget
	Panics    int64 // handler or hook panics recovered
	InFlight  int64 // handlers (and hooks) running right now
}

// Consumer runs one handler over the records of a consumer group.
type Consumer struct {
	cfg     ConsumerConfig
	log     *slog.Logger
	started atomic.Bool

	handled, failed, exhausted, panics, inFlight atomic.Int64
}

// NewConsumer validates cfg. Nothing connects until Run.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{cfg: cfg, log: log.With("component", "kafka-consumer", "group", cfg.GroupID)}, nil
}

// Stats returns the current counters.
func (c *Consumer) Stats() Stats {
	return Stats{
		Handled:   c.handled.Load(),
		Failed:    c.failed.Load(),
		Exhausted: c.exhausted.Load(),
		Panics:    c.panics.Load(),
		InFlight:  c.inFlight.Load(),
	}
}

type topicPartition struct {
	topic string
	part  int32
}

// session is the state of one Run call.
type session struct {
	c  *Consumer
	cl *kgo.Client

	// workCtx is the parent of every handler context. It outlives the context
	// passed to Run (which only means "stop pulling") and is cancelled when the
	// runner gives up on in-flight work.
	workCtx context.Context
	sem     chan struct{} // MaxInFlight slots

	mu      sync.Mutex
	workers map[topicPartition]*worker
}

// Run joins the group and processes records until ctx is cancelled. It blocks.
// Cancelling ctx stops fetching, lets each in-flight handler finish (and its
// offset commit), cancels the handlers still running after ShutdownTimeout,
// and leaves the group. It returns nil on a clean drain, ErrDrainTimeout if
// handlers had to be cancelled, or the error that prevented starting. Run may
// be called once per Consumer.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	s := &session{
		c:       c,
		workCtx: workCtx,
		sem:     make(chan struct{}, c.cfg.MaxInFlight),
		workers: make(map[topicPartition]*worker),
	}

	start := kgo.NewOffset().AtStart()
	if c.cfg.StartAtEnd {
		start = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ConsumerGroup(c.cfg.GroupID),
		kgo.ConsumeTopics(c.cfg.Topics...),
		kgo.ConsumeResetOffset(start),
		// Same reason as the producer's: a restarting broker can close a
		// fresh connection at once, which must not be read as a TLS error.
		kgo.AlwaysRetryEOF(),
		// Offsets are committed by the workers, after the handler, and by
		// nothing else (at-least-once).
		kgo.DisableAutoCommit(),
		// A rebalance cannot start between a poll and the dispatch of its
		// records to the workers; the revoke callback then waits for them.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
	}
	if c.cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.cfg.ClientID))
	}
	if c.cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(c.cfg.SessionTimeout), kgo.HeartbeatInterval(c.cfg.SessionTimeout/3))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: new consumer client: %w", err)
	}
	s.cl = cl

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, part int32, err error) {
			// franz-go retries these itself; they are worth a line, not a stop.
			c.log.Warn("fetch error", "topic", topic, "partition", part, "error", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) > 0 {
				s.dispatch(p)
			}
		})
		cl.AllowRebalance()
	}
	cl.AllowRebalance()

	drained := s.stop(s.takeAll(), c.cfg.ShutdownTimeout)
	cl.Close() // leaves the group
	if !drained {
		return ErrDrainTimeout
	}
	return nil
}

// dispatch hands a partition's records to its worker and pauses the partition
// until the worker has finished them, which is the backpressure: at most one
// fetched batch per partition is waiting at a time.
func (s *session) dispatch(p kgo.FetchTopicPartition) {
	k := topicPartition{p.Topic, p.Partition}
	s.mu.Lock()
	w := s.workers[k]
	if w == nil {
		w = newWorker(s, k)
		s.workers[k] = w
		go w.loop()
	}
	s.mu.Unlock()

	s.cl.PauseFetchPartitions(map[string][]int32{k.topic: {k.part}})
	w.push(p.Records)
}

// onRevoked runs when a rebalance takes partitions away. The workers of those
// partitions finish their current record, commit it and stop, so the next
// owner starts exactly after the last handled record.
func (s *session) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	ws := s.take(revoked)
	if !s.stop(ws, s.c.cfg.ShutdownTimeout) {
		s.c.log.Warn("handlers cancelled at the rebalance deadline", "partitions", len(ws))
	}
	s.resume(revoked)
}

// onLost runs when the group already gave the partitions away (session
// expired): committing would fail, and the new owner is already reading, so
// the workers are cancelled rather than waited for.
func (s *session) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	ws := s.take(lost)
	for _, w := range ws {
		w.skipCommit.Store(true)
		w.cancel()
	}
	s.stop(ws, s.c.cfg.ShutdownTimeout)
	s.resume(lost)
}

// take removes and returns the workers of the given partitions.
func (s *session) take(parts map[string][]int32) []*worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ws []*worker
	for topic, ps := range parts {
		for _, p := range ps {
			k := topicPartition{topic, p}
			if w := s.workers[k]; w != nil {
				ws = append(ws, w)
				delete(s.workers, k)
			}
		}
	}
	return ws
}

func (s *session) takeAll() []*worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := make([]*worker, 0, len(s.workers))
	for k, w := range s.workers {
		ws = append(ws, w)
		delete(s.workers, k)
	}
	return ws
}

// resume un-pauses partitions whose worker is gone: a pause outlives the
// assignment, and a partition that comes back must not stay paused.
func (s *session) resume(parts map[string][]int32) {
	s.cl.ResumeFetchPartitions(parts)
}

// stop asks the workers to finish their current record and exit, waits up to
// timeout, then cancels the handlers still running and waits a short grace.
// It reports whether every worker exited before the timeout.
func (s *session) stop(ws []*worker, timeout time.Duration) bool {
	if len(ws) == 0 {
		return true
	}
	for _, w := range ws {
		w.signalStop()
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for i, w := range ws {
		select {
		case <-w.done:
			continue
		case <-deadline.C:
		}
		// Deadline: cancel everything not finished, then give the cancelled
		// handlers a moment to return so their goroutines do not leak.
		for _, rest := range ws[i:] {
			rest.cancel()
		}
		grace := time.NewTimer(cancelGrace)
		defer grace.Stop()
		for _, rest := range ws[i:] {
			select {
			case <-rest.done:
			case <-grace.C:
				s.c.log.Error("handler did not return after cancellation; abandoning it",
					"topic", rest.tp.topic, "partition", rest.tp.part)
			}
		}
		return false
	}
	return true
}

// worker handles the records of one partition, one at a time, in order.
type worker struct {
	s  *session
	tp topicPartition

	ctx    context.Context
	cancel context.CancelFunc

	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	// skipCommit is set when the partition was lost: a commit would be
	// rejected, and the new owner decides where to resume.
	skipCommit atomic.Bool

	wake chan struct{}
	mu   sync.Mutex
	// queue holds fetched batches not yet handled. The partition is paused
	// while it is non-empty, so it stays at one batch in practice.
	queue [][]*kgo.Record
}

func newWorker(s *session, tp topicPartition) *worker {
	ctx, cancel := context.WithCancel(s.workCtx)
	return &worker{
		s: s, tp: tp, ctx: ctx, cancel: cancel,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
}

func (w *worker) signalStop() { w.stopOnce.Do(func() { close(w.stopCh) }) }

func (w *worker) stopped() bool {
	select {
	case <-w.stopCh:
		return true
	default:
		return false
	}
}

func (w *worker) push(recs []*kgo.Record) {
	w.mu.Lock()
	w.queue = append(w.queue, recs)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *worker) pop() []*kgo.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return nil
	}
	b := w.queue[0]
	w.queue = w.queue[1:]
	return b
}

func (w *worker) loop() {
	defer close(w.done)
	defer w.cancel()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.wake:
		}
		for batch := w.pop(); batch != nil; batch = w.pop() {
			if !w.process(batch) {
				return
			}
		}
	}
}

// process handles one batch in order and commits the last record that was
// completed (handled, or skipped after exhaustion). It reports false if the
// worker was told to stop before the batch was finished.
func (w *worker) process(batch []*kgo.Record) bool {
	var last *kgo.Record
	finished := true
	for _, rec := range batch {
		if w.stopped() || !w.handle(rec) {
			finished = false
			break
		}
		last = rec
	}
	if last != nil && !w.skipCommit.Load() {
		w.commit(last)
	}
	if finished {
		// Only a worker that stays alive resumes its partition; a stopped one
		// leaves it paused for the revoke path or the shutdown to deal with.
		w.s.cl.ResumeFetchPartitions(map[string][]int32{w.tp.topic: {w.tp.part}})
	}
	return finished
}

func (w *worker) commit(rec *kgo.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	if err := w.s.cl.CommitRecords(ctx, rec); err != nil {
		// Not fatal: the next commit covers this offset, and if the partition
		// was taken away the new owner re-reads from the last commit.
		w.s.c.log.Warn("offset commit failed", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
	}
}

// handle runs the handler for one record with retries and reports whether the
// record is done: handled, or skipped after its budget ran out. False means it
// was abandoned (stop or cancellation) and must stay uncommitted.
func (w *worker) handle(rec *kgo.Record) bool {
	c := w.s.c
	r := toRecord(rec)
	var attemptErrs []error // every failed attempt, handed to OnExhausted
	for attempt := 0; ; attempt++ {
		if !w.acquire() {
			return false
		}
		err := w.call(func() error { return c.cfg.Handler(w.ctx, r) })
		w.release()
		if err == nil {
			c.handled.Add(1)
			return true
		}
		if w.ctx.Err() != nil || w.stopped() {
			// Abandoned, not failed: the record is redelivered with a fresh budget.
			return false
		}
		c.failed.Add(1)
		attemptErrs = append(attemptErrs, err)
		c.log.Warn("handler failed", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
			"attempt", attempt+1, "max_attempts", c.cfg.MaxAttempts, "error", err)
		if attempt+1 >= c.cfg.MaxAttempts {
			return w.exhaust(r, &ExhaustedError{Attempts: attemptErrs})
		}
		if !w.sleep(c.cfg.Backoff.Delay(attempt)) {
			return false
		}
	}
}

// exhaust runs the OnExhausted hook for a record that used its budget. It
// reports false if the worker was stopped before the hook could run, or if the
// hook returned an error: the record then stays uncommitted (redelivered with a
// fresh budget), not skipped.
func (w *worker) exhaust(r Record, err error) bool {
	c := w.s.c
	if !w.acquire() {
		return false
	}
	defer w.release()
	c.exhausted.Add(1)
	c.log.Error("record exhausted its retry budget; skipping it", "topic", r.Topic, "partition", r.Partition,
		"offset", r.Offset, "attempts", c.cfg.MaxAttempts, "error", err)
	if c.cfg.OnExhausted == nil {
		return true
	}
	var hookErr error
	if herr := w.call(func() error { hookErr = c.cfg.OnExhausted(w.ctx, r, err); return nil }); herr != nil {
		c.log.Error("OnExhausted hook panicked; the record is skipped anyway", "topic", r.Topic,
			"partition", r.Partition, "offset", r.Offset, "error", herr)
		return true
	}
	if hookErr != nil {
		c.log.Error("OnExhausted hook did not deal with the record; leaving it uncommitted for redelivery", "topic", r.Topic,
			"partition", r.Partition, "offset", r.Offset, "error", hookErr)
		return false
	}
	return true
}

// acquire takes a MaxInFlight slot. It gives up if the worker is stopped or
// cancelled while waiting.
func (w *worker) acquire() bool {
	select {
	case w.s.sem <- struct{}{}:
		w.s.c.inFlight.Add(1)
		return true
	case <-w.stopCh:
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *worker) release() {
	w.s.c.inFlight.Add(-1)
	<-w.s.sem
}

// sleep waits d, and reports false if the worker was stopped or cancelled first.
func (w *worker) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.stopCh:
		return false
	case <-w.ctx.Done():
		return false
	}
}

// call runs fn, turning a panic into an error wrapping ErrPanic so that it is an
// ordinary failure (retry, then OnExhausted), never a crash of the process.
func (w *worker) call(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			w.s.c.panics.Add(1)
			w.s.c.log.Error("handler panicked", "topic", w.tp.topic, "partition", w.tp.part, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("%w: %v", ErrPanic, p)
		}
	}()
	return fn()
}

func toRecord(rec *kgo.Record) Record {
	r := Record{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Timestamp: rec.Timestamp.UnixMilli(),
	}
	if len(rec.Headers) > 0 {
		r.Headers = make([]Header, len(rec.Headers))
		for i, h := range rec.Headers {
			r.Headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return r
}
