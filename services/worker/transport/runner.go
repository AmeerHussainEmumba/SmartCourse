package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

const (
	// handleTimeout bounds one handler call (decode, transaction, handler), the
	// same 30 s the in-process relay gives a row. A handler stuck past it fails
	// the attempt instead of holding its partition forever.
	handleTimeout = 30 * time.Second
	// parkTries is how often OnExhausted tries the sink before it gives up and
	// logs the event: the kafka runner advances the offset whatever the hook
	// does (libs/kafka README), so the retries are all the protection there is.
	parkTries = 3
	// restartBackoff separates restarts of a consumer whose Run returned an error.
	restartBackoff = 2 * time.Second
)

// RunnerConfig configures a Runner. Zero tuning values take libs/kafka's
// defaults.
type RunnerConfig struct {
	Brokers []string
	// MaxInFlight, MaxAttempts, Backoff* and ShutdownTimeout map to
	// kafka.ConsumerConfig and apply to every group.
	MaxInFlight     int
	MaxAttempts     int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	ShutdownTimeout time.Duration
	// Sink receives a record whose handler used its budget. Nil keeps the
	// log-and-skip behaviour, which is the documented gap (relay.DeadLetterSink).
	Sink   relay.DeadLetterSink
	Logger *slog.Logger
	// Tracing makes each record a CONSUMER span continuing the trace in its
	// traceparent header (or, without one, the envelope's trace ids); the
	// handler's DB and Redis spans nest under it. Off starts no span.
	Tracing bool
	// Metrics, when set, counts event_processed_total per group and outcome
	// (processed, duplicate, failed, dead_lettered).
	Metrics *telemetry.Metrics
}

// Runner runs one Kafka consumer group per consumer name: the group id is the
// ConsumerName and it subscribes to the topics of its handlers' event types.
// Each record is decoded to a relay.DecodedEvent and given to
// relay.Deliver, the code the in-process relay uses, so the handler runs in a
// transaction that also writes processed_events, and only when that commits does
// libs/kafka commit the offset. A handler that fails past MaxAttempts is parked
// through the sink and the offset advances, so a poison record never stalls its
// partition.
//
// Per-aggregate order: the producer keys by aggregate id, a partition is handled
// by one goroutine one record at a time, and a retried record holds its
// partition. Bounded in-flight: MaxInFlight handlers per group.
type Runner struct {
	cfg    RunnerConfig
	db     *gorm.DB
	groups []group
	log    *slog.Logger

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

// NewRunner validates that every handler's event types map to a topic. It
// starts nothing.
func NewRunner(cfg RunnerConfig, db *gorm.DB, handlers []relay.Handler) (*Runner, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("transport: runner needs brokers")
	}
	if db == nil {
		return nil, fmt.Errorf("transport: runner needs a database")
	}
	r := &Runner{
		cfg: cfg, db: db,
		stats: map[string]kafka.Stats{}, live: map[string]*kafka.Consumer{},
		log: cfg.Logger,
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
	return r, nil
}

// Run starts every group and blocks until ctx is cancelled and each has drained
// (up to ShutdownTimeout). A group whose Run fails is restarted after a short
// pause: one consumer's trouble does not stop the others.
func (r *Runner) Run(ctx context.Context) {
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

func (r *Runner) runGroup(ctx context.Context, g group) {
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
// relay.Deliver (as the in-process relay does; a record whose type no handler of
// the group registered for is skipped, since the topic carries other types). One
// handler's failure does not stop the others for this record; a retry re-runs
// them all, and Deliver's processed_events check makes the finished ones no-ops.
func (r *Runner) handle(ctx context.Context, g group, rec kafka.Record) (err error) {
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
		duplicate, err := relay.DeliverEvent(ctx, r.db, h, evt)
		r.count(g.name, outcomeOf(duplicate, err))
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// count records one handler run in event_processed_total, when metrics are on.
func (r *Runner) count(consumer string, outcome telemetry.Outcome) {
	if r.cfg.Metrics != nil {
		r.cfg.Metrics.EventProcessed(consumer, outcome)
	}
}

// Groups returns the consumer group ids, one per consumer name.
func (r *Runner) Groups() []string {
	out := make([]string, len(r.groups))
	for i, g := range r.groups {
		out[i] = g.name
	}
	return out
}

// LagGauges feeds kafka_consumer_lag{group,topic} for the runner's groups from
// the brokers; run it with telemetry.StartPoller, and call the returned func
// when done. Labels are the group (one per consumer name) and the topic (the
// four provisioned ones): both fixed by code, so the series count is bounded
// however many partitions the topics have. The partition is not a label (it
// would depend on topic configuration and churn with rebalances) and neither
// is an offset; the value is the total over the topic's partitions.
func (r *Runner) LagGauges(m *telemetry.Metrics) (telemetry.GaugeCollector, func(), error) {
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
func (r *Runner) exhausted(ctx context.Context, g group, rec kafka.Record, cause error) {
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

func (r *Runner) attempts() int {
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
func (r *Runner) ParkFailures() int64 { return r.parkFailures.Load() }

// Stats returns each consumer's counters (the last run's, once it has stopped).
func (r *Runner) Stats() map[string]kafka.Stats {
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
