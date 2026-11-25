package eventbus

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

const (
	// handleTimeout bounds one handler call (decode, transaction, handler), the
	// same 30 s the relay gives a row. A handler stuck past it fails
	// the attempt instead of holding its partition forever.
	handleTimeout = 30 * time.Second
	// parkTries is how often OnExhausted tries the sink before it gives up and
	// logs the event: the kafka runner advances the offset whatever the hook
	// does (libs/kafka README), so the retries are all the protection there is.
	parkTries = 3
	// restartBackoff separates restarts of a consumer whose Run returned an error.
	restartBackoff = 2 * time.Second
)

// consumerSettings is what a Consumers runs with: the Kafka section of the
// process configuration plus the options. Zero tuning values take libs/kafka's
// defaults.
type consumerSettings struct {
	Brokers []string
	// MaxInFlight, MaxAttempts, Backoff* and ShutdownTimeout map to
	// kafka.ConsumerConfig and apply to every group.
	MaxInFlight     int
	MaxAttempts     int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	ShutdownTimeout time.Duration
	Sink            relay.DeadLetterSink
	Logger          *slog.Logger
	Tracing         bool
	Metrics         *telemetry.Metrics
}

// ConsumerOption tunes NewConsumers.
type ConsumerOption func(*consumerSettings)

// WithSink sets where a record whose handler used its budget is parked. Without
// one the record is logged and dropped, which is the documented gap
// (relay.DeadLetterSink).
func WithSink(s relay.DeadLetterSink) ConsumerOption { return func(c *consumerSettings) { c.Sink = s } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) ConsumerOption { return func(c *consumerSettings) { c.Logger = l } }

// WithTracing makes each record a CONSUMER span continuing the trace in its
// traceparent header (or, without one, the envelope's trace ids); the handler's
// DB and Redis spans nest under it. Off starts no span.
func WithTracing(on bool) ConsumerOption { return func(c *consumerSettings) { c.Tracing = on } }

// WithMetrics counts event_processed_total per group and outcome (processed,
// duplicate, failed, dead_lettered). A nil m is no metrics.
func WithMetrics(m *telemetry.Metrics) ConsumerOption {
	return func(c *consumerSettings) { c.Metrics = m }
}

// deliverFunc runs one handler for one decoded event and reports whether the
// event had already been processed (the handler did not run). It is the one
// thing the two modes differ in.
type deliverFunc func(ctx context.Context, h relay.Handler, evt relay.DecodedEvent) (duplicate bool, err error)

// Consumers runs one Kafka consumer group per consumer name: the group id is the
// ConsumerName and it subscribes to the topics of its handlers' event types and
// to its own retry topic, retry.<ConsumerName> (docs/plans/microservices-split.md
// §4.1: the audit service republishes a parked event there). Records from the
// retry topic take exactly the same path as any other: decode, relay.Deliver,
// offset commit, so a replay that was already applied is absorbed by
// processed_events.
// Each record is decoded to a relay.DecodedEvent and given to
// relay.Deliver, so the handler runs in a
// transaction that also writes processed_events, and only when that commits does
// libs/kafka commit the offset. A handler that fails past MaxAttempts is parked
// through the sink and the offset advances, so a poison record never stalls its
// partition.
//
// Per-aggregate order: the producer keys by aggregate id, a partition is handled
// by one goroutine one record at a time, and a retried record holds its
// partition. Bounded in-flight: MaxInFlight handlers per group.
//
// Two modes share all of the above (NewConsumers, NewStreamConsumers) and differ
// only in how a handler is delivered: with a Postgres transaction and the
// processed_events marker, or plainly (see NewStreamConsumers).
type Consumers struct {
	cfg     consumerSettings
	deliver deliverFunc
	groups  []group
	log     *slog.Logger

	parkFailures atomic.Int64

	mu    sync.Mutex
	stats map[string]kafka.Stats
	live  map[string]*kafka.Consumer
}

// group is the handlers registered under one ConsumerName. The relay allows
// several (analytics registers one per event family under "analytics-aggregator",
// sharing one processed_events key space), and a consumer group is named by the
// consumer, so they are one group subscribed to the union of their topics.
type group struct {
	name     string
	handlers []relay.Handler
	topics   []string
}

// NewConsumers builds the consumer groups for a service's own handlers (it
// needs no *relay.Relay: a service that only consumes, or whose relay only
// publishes, passes the handlers directly) and validates that every handler's
// event types map to a topic. cfg supplies the brokers and the tuning
// (KAFKA_CONSUMER_*). It starts nothing.
//
// Each handler runs through relay.DeliverEvent on db: in one transaction with
// its processed_events marker, so a redelivery or a replay is absorbed.
func NewConsumers(cfg config.KafkaConfig, handlers []relay.Handler, db *gorm.DB, opts ...ConsumerOption) (*Consumers, error) {
	if db == nil {
		return nil, fmt.Errorf("eventbus: consumers need a database")
	}
	return newConsumers(cfg, handlers, func(ctx context.Context, h relay.Handler, evt relay.DecodedEvent) (bool, error) {
		return relay.DeliverEvent(ctx, db, h, evt)
	}, opts)
}

// NewStreamConsumers is NewConsumers for a service with no Postgres, whose
// idempotency lives elsewhere (the audit archiver dedups by event id in
// MongoDB). A handler is delivered WITHOUT a transaction and without a
// processed_events marker: relay.Handler.Handle receives a nil *gorm.DB, so a
// handler registered here must not use it, and it must be safe to run more than
// once for one event (Kafka delivers at least once, and a replay through the
// retry topic redelivers an event that was already handled). A handler panic is
// recovered and returned as an error. Everything else is identical to
// NewConsumers: the group per consumer name, the retry topic subscription,
// bounded in-flight, the retry budget, parking through the sink with one history
// entry per attempt, the relay.Source in the handler's context, tracing and
// metrics (the "duplicate" outcome never occurs).
func NewStreamConsumers(cfg config.KafkaConfig, handlers []relay.Handler, opts ...ConsumerOption) (*Consumers, error) {
	return newConsumers(cfg, handlers, deliverStream, opts)
}

func newConsumers(cfg config.KafkaConfig, handlers []relay.Handler, deliver deliverFunc, opts []ConsumerOption) (*Consumers, error) {
	s := consumerSettings{
		Brokers: cfg.Brokers, MaxInFlight: cfg.ConsumerMaxInFlight, MaxAttempts: cfg.ConsumerMaxAttempts,
		BackoffBase: cfg.ConsumerBackoffBase, BackoffMax: cfg.ConsumerBackoffMax, ShutdownTimeout: cfg.ConsumerShutdownTimeout,
	}
	for _, o := range opts {
		o(&s)
	}
	if len(s.Brokers) == 0 {
		return nil, fmt.Errorf("eventbus: consumers need brokers")
	}
	r := &Consumers{
		cfg: s, deliver: deliver,
		stats: map[string]kafka.Stats{}, live: map[string]*kafka.Consumer{},
		log: s.Logger,
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	byName := map[string]int{}
	for _, h := range handlers {
		ts, err := TopicsFor(h)
		if err != nil {
			return nil, err
		}
		i, ok := byName[h.ConsumerName]
		if !ok {
			i = len(r.groups)
			byName[h.ConsumerName] = i
			r.groups = append(r.groups, group{name: h.ConsumerName})
		}
		g := &r.groups[i]
		g.handlers = append(g.handlers, h)
		for _, t := range ts {
			if !slices.Contains(g.topics, t) {
				g.topics = append(g.topics, t)
			}
		}
		slices.Sort(g.topics)
	}
	for i := range r.groups {
		g := &r.groups[i]
		g.topics = append(g.topics, topics.Retry(g.name))
	}
	return r, nil
}

// Run starts every group and blocks until ctx is cancelled and each has drained
// (up to ShutdownTimeout). A group whose Run fails is restarted after a short
// pause: one consumer's trouble does not stop the others.
func (r *Consumers) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, g := range r.groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runGroup(ctx, g)
		}()
	}
	wg.Wait()
}

// ensureTimeout bounds creating a group's retry topic at its start.
const ensureTimeout = 15 * time.Second

func (r *Consumers) runGroup(ctx context.Context, g group) {
	// The retry topic is provisioned by topics.Ensure / kafka-init for the known
	// consumers; creating it here (idempotent) means a new consumer name works
	// before anyone remembers to add it to the list. A failure is not fatal: the
	// broker may just be down, and the consumer retries until it is up.
	ectx, cancel := context.WithTimeout(ctx, ensureTimeout)
	if err := topics.EnsureNamed(ectx, r.cfg.Brokers, topics.Retry(g.name)); err != nil {
		r.log.Warn("could not ensure the retry topic", slog.String("consumer", g.name), slog.Any("error", err))
	}
	cancel()
	for ctx.Err() == nil {
		c, err := kafka.NewConsumer(kafka.ConsumerConfig{
			Brokers:         r.cfg.Brokers,
			GroupID:         g.name,
			Topics:          g.topics,
			ClientID:        "smartcourse-" + g.name,
			Handler:         func(ctx context.Context, rec kafka.Record) error { return r.handle(ctx, g, rec) },
			OnExhausted:     func(ctx context.Context, rec kafka.Record, err error) { r.exhausted(ctx, g, rec, err) },
			MaxInFlight:     r.cfg.MaxInFlight,
			MaxAttempts:     r.cfg.MaxAttempts,
			Backoff:         kafka.BackoffConfig{Base: r.cfg.BackoffBase, Max: r.cfg.BackoffMax, Jitter: kafka.DefaultBackoff.Jitter},
			ShutdownTimeout: r.cfg.ShutdownTimeout,
			Logger:          r.log,
		})
		if err != nil {
			r.log.Error("kafka consumer not started", slog.String("consumer", g.name), slog.Any("error", err))
			return // a configuration error: restarting cannot fix it
		}
		r.mu.Lock()
		r.live[g.name] = c
		r.mu.Unlock()
		r.log.Info("kafka consumer started", slog.String("consumer", g.name), slog.Any("topics", g.topics))

		err = c.Run(ctx)

		r.mu.Lock()
		r.stats[g.name] = c.Stats()
		delete(r.live, g.name)
		r.mu.Unlock()
		if err != nil {
			r.log.Error("kafka consumer stopped", slog.String("consumer", g.name), slog.Any("error", err))
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartBackoff):
		}
	}
}

// handle is the kafka.Handler for one consumer group: decode, then run every
// handler of the group that registered for the event type through
// relay.Deliver (a record whose type no handler of
// the group registered for is skipped, since the topic carries other types). One
// handler's failure does not stop the others for this record; a retry re-runs
// them all, and Deliver's processed_events check makes the finished ones no-ops.
func (r *Consumers) handle(ctx context.Context, g group, rec kafka.Record) (err error) {
	evt, schemaID, err := decodeFramed(rec)
	if err != nil {
		r.count(g.name, telemetry.OutcomeFailed)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()
	// Where the record came from, for handlers that keep it (relay.SourceFrom).
	ctx = relay.WithSource(ctx, SourceOf(rec, schemaID))
	ctx = logging.WithContext(ctx, logging.WithEventID(logging.FromContext(ctx), evt.EventID.String()))
	if r.cfg.Tracing {
		var span trace.Span
		ctx, span = telemetry.StartConsume(ctx, consumeTraceparent(rec, evt), g.name, rec.Topic, evt.EventType, evt.EventID.String())
		defer func() { telemetry.EndSpan(span, err) }()
	}
	var errs []error
	for _, h := range g.handlers {
		if !h.Handles(evt.EventType) {
			continue
		}
		duplicate, err := r.deliver(ctx, h, evt)
		r.count(g.name, outcomeOf(duplicate, err))
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// deliverStream runs h with no transaction (NewStreamConsumers), recovering a
// panic into an error like relay.DeliverEvent does.
func deliverStream(ctx context.Context, h relay.Handler, evt relay.DecodedEvent) (duplicate bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			logging.FromContext(ctx).Error("event handler panicked",
				slog.String("consumer", h.ConsumerName), slog.String("event_id", evt.EventID.String()),
				slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			duplicate, err = false, fmt.Errorf("handler panic: %v", p)
		}
	}()
	return false, h.Handle(ctx, nil, evt)
}

// count records one handler run in event_processed_total, when metrics are on.
func (r *Consumers) count(consumer string, outcome telemetry.Outcome) {
	if r.cfg.Metrics != nil {
		r.cfg.Metrics.EventProcessed(consumer, outcome)
	}
}

// Topics returns the topics group subscribes to: its handlers' source topics
// and its retry topic. It is nil for a name that is not a group.
func (r *Consumers) Topics(group string) []string {
	for _, g := range r.groups {
		if g.name == group {
			return slices.Clone(g.topics)
		}
	}
	return nil
}

// Groups returns the consumer group ids, one per consumer name.
func (r *Consumers) Groups() []string {
	out := make([]string, len(r.groups))
	for i, g := range r.groups {
		out[i] = g.name
	}
	return out
}

// LagGauges feeds kafka_consumer_lag{group,topic} for the runner's groups from
// the brokers; run it with telemetry.StartPoller, and call the returned func
// when done. Labels are the group (one per consumer name) and the topic (the
// event topics and each group's retry topic): both fixed by code, so the series count is bounded
// however many partitions the topics have. The partition is not a label (it
// would depend on topic configuration and churn with rebalances) and neither
// is an offset; the value is the total over the topic's partitions.
func (r *Consumers) LagGauges(m *telemetry.Metrics) (telemetry.GaugeCollector, func(), error) {
	lr, err := kafka.NewLagReader(r.cfg.Brokers)
	if err != nil {
		return nil, nil, err
	}
	groups := r.Groups()
	return telemetry.ConsumerLagGauges(m, func(ctx context.Context) ([]telemetry.ConsumerLag, error) {
		lags, err := lr.Lag(ctx, groups...)
		if err != nil {
			return nil, err
		}
		out := make([]telemetry.ConsumerLag, len(lags))
		for i, l := range lags {
			out[i] = telemetry.ConsumerLag{Group: l.Group, Topic: l.Topic, Lag: l.Lag}
		}
		return out, nil
	}), lr.Close, nil
}

// exhausted parks a record whose handler used its budget. The offset advances
// when it returns whatever happens (libs/kafka), so it retries the sink a few
// times and, if the sink stays down, logs the event in full and counts the
// failure: that is the one remaining way an event leaves the stream unparked,
// and it is loud.
func (r *Consumers) exhausted(ctx context.Context, g group, rec kafka.Record, cause error) {
	// The sink sees the record's coordinates as a handler does (the audit sink
	// stores them with the parked event).
	ctx = relay.WithSource(ctx, recordSource(rec))
	d := relay.DeadLetter{Event: stubEvent(rec), Consumer: g.name, Err: cause, History: kafka.AttemptErrors(cause), Attempts: r.attempts()}
	if r.cfg.Sink == nil {
		r.log.Error("kafka consumer gave up on a record; no dead-letter sink is configured, the record is dropped",
			slog.String("consumer", g.name), slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset), slog.String("event_id", d.Event.EventID.String()), slog.Any("error", cause))
		return
	}
	var err error
	for i := 0; i < parkTries; i++ {
		if err = parkSafely(ctx, r.cfg.Sink, d); err == nil {
			r.count(g.name, telemetry.OutcomeDeadLetter)
			r.log.Warn("kafka consumer parked a record in the dead-letter sink",
				slog.String("consumer", g.name), slog.String("event_id", d.Event.EventID.String()),
				slog.String("event_type", d.Event.EventType), slog.Any("error", cause))
			return
		}
		select {
		case <-ctx.Done():
			i = parkTries
		case <-time.After(time.Duration(i+1) * 200 * time.Millisecond):
		}
	}
	r.parkFailures.Add(1)
	r.log.Error("kafka consumer could not park a record; it is skipped and only this log keeps it",
		slog.String("consumer", g.name), slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset), slog.String("event_id", d.Event.EventID.String()),
		slog.String("event_type", d.Event.EventType), slog.String("payload", string(d.Event.Payload)),
		slog.Any("handler_error", cause), slog.Any("park_error", err))
}

func (r *Consumers) attempts() int {
	if r.cfg.MaxAttempts > 0 {
		return r.cfg.MaxAttempts
	}
	return kafka.DefaultMaxAttempts
}

func parkSafely(ctx context.Context, s relay.DeadLetterSink, d relay.DeadLetter) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("dead-letter sink panicked: %v", p)
		}
	}()
	return s.Park(ctx, d)
}

// stubEvent is the event handed to the sink: the decoded one when the record
// decodes, otherwise a stand-in carrying the ids from the headers and the raw
// bytes (base64) with their coordinates, so an undecodable record is parked with
// everything needed to look at it.
func stubEvent(rec kafka.Record) relay.DecodedEvent {
	if evt, err := Decode(rec); err == nil {
		return evt
	}
	var evt relay.DecodedEvent
	if v, ok := rec.Header(HeaderEventID); ok {
		evt.EventID, _ = uuid.Parse(string(v))
	}
	if v, ok := rec.Header(HeaderEventType); ok {
		evt.EventType = string(v)
	}
	if len(rec.Key) > 0 {
		evt.AggregateID, _ = uuid.Parse(string(rec.Key))
	}
	if evt.EventID == uuid.Nil {
		// A stable id from the record's coordinates, so the sink's idempotency
		// key (event id, consumer) still tells two undecodable records apart.
		evt.EventID = uuid.NewSHA1(uuid.NameSpaceURL, fmt.Appendf(nil, "kafka://%s/%d/%d", rec.Topic, rec.Partition, rec.Offset))
	}
	evt.OccurredAt = time.UnixMilli(rec.Timestamp).UTC()
	evt.Payload, _ = json.Marshal(map[string]any{"undecodable_record": map[string]any{
		"topic": rec.Topic, "partition": rec.Partition, "offset": rec.Offset,
		"value_base64": base64.StdEncoding.EncodeToString(rec.Value),
	}})
	return evt
}

// ParkFailures counts records the sink refused on every try.
func (r *Consumers) ParkFailures() int64 { return r.parkFailures.Load() }

// Stats returns each consumer's counters (the last run's, once it has stopped).
func (r *Consumers) Stats() map[string]kafka.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]kafka.Stats, len(r.stats)+len(r.live))
	for k, v := range r.stats {
		out[k] = v
	}
	for k, c := range r.live {
		out[k] = c.Stats()
	}
	return out
}
