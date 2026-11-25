// Package consume runs the audit-archiver's consumer group.
//
// Why this is not eventbus.NewConsumers. That runner hands every record to
// relay.DeliverEvent, which runs the handler and writes a `processed_events`
// marker in one Postgres transaction, so it demands a *gorm.DB. The audit
// service has no Postgres (design §2: its idempotency is the event id in
// MongoDB: ArchiveEvent is first-write-wins and AppendAuditOnce is keyed by
// event id), so there is no database to give it. This runner is the same loop
// minus the transaction: decode with eventbus.Decode, attach the record's
// relay.Source to the context (the archiver stores topic, partition, offset,
// key and schema id), retry up to the configured budget, then park through the
// dead-letter sink and advance the offset. When libs/eventbus gains a mode
// without a database, this package should be replaced by it (open item in the
// service README).
//
// The group is `audit-archiver` and subscribes to every event topic plus
// `retry.audit-archiver`, so replaying a parked archive event works the same as
// for any other consumer.
package consume

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

const (
	// handleTimeout bounds one handler call, as in libs/eventbus.
	handleTimeout = 30 * time.Second
	// parkTries is how often exhausted tries the sink before logging the event.
	parkTries = 3
	// restartBackoff separates restarts of a consumer whose Run returned an error.
	restartBackoff = 2 * time.Second
	ensureTimeout  = 15 * time.Second
)

// Options configures New.
type Options struct {
	Kafka   config.KafkaConfig
	Sink    relay.DeadLetterSink
	Logger  *slog.Logger
	Tracing bool
	Metrics *telemetry.Metrics
}

// Runner is one consumer group over a relay.Handler.
type Runner struct {
	o       Options
	handler relay.Handler
	topics  []string
	log     *slog.Logger

	parkFailures atomic.Int64

	mu    sync.Mutex
	stats kafka.Stats
	live  *kafka.Consumer
}

// New builds the runner for h (its ConsumerName is the group id). It starts
// nothing.
func New(h relay.Handler, o Options) (*Runner, error) {
	if len(o.Kafka.Brokers) == 0 {
		return nil, errors.New("consume: no Kafka brokers")
	}
	ts, err := eventbus.TopicsFor(h)
	if err != nil {
		return nil, err
	}
	ts = append(ts, topics.Retry(h.ConsumerName))
	r := &Runner{o: o, handler: h, topics: ts, log: o.Logger}
	if r.log == nil {
		r.log = slog.Default()
	}
	return r, nil
}

// Topics returns the subscribed topics (event topics and the retry topic).
func (r *Runner) Topics() []string { return append([]string(nil), r.topics...) }

// ParkFailures counts records the sink refused on every try.
func (r *Runner) ParkFailures() int64 { return r.parkFailures.Load() }

// Stats returns the consumer's counters (the last run's, once it has stopped).
func (r *Runner) Stats() kafka.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.live != nil {
		return r.live.Stats()
	}
	return r.stats
}

// Run consumes until ctx is cancelled and the group has drained. A consumer
// whose Run fails is restarted after a short pause.
func (r *Runner) Run(ctx context.Context) {
	name := r.handler.ConsumerName
	ectx, cancel := context.WithTimeout(ctx, ensureTimeout)
	if err := topics.EnsureNamed(ectx, r.o.Kafka.Brokers, topics.Retry(name)); err != nil {
		r.log.Warn("could not ensure the retry topic", slog.String("consumer", name), slog.Any("error", err))
	}
	cancel()
	k := r.o.Kafka
	for ctx.Err() == nil {
		c, err := kafka.NewConsumer(kafka.ConsumerConfig{
			Brokers:         k.Brokers,
			GroupID:         name,
			Topics:          r.topics,
			ClientID:        "smartcourse-" + name,
			Handler:         r.handle,
			OnExhausted:     r.exhausted,
			MaxInFlight:     k.ConsumerMaxInFlight,
			MaxAttempts:     k.ConsumerMaxAttempts,
			Backoff:         kafka.BackoffConfig{Base: k.ConsumerBackoffBase, Max: k.ConsumerBackoffMax, Jitter: kafka.DefaultBackoff.Jitter},
			ShutdownTimeout: k.ConsumerShutdownTimeout,
			Logger:          r.log,
		})
		if err != nil {
			r.log.Error("kafka consumer not started", slog.String("consumer", name), slog.Any("error", err))
			return // a configuration error: restarting cannot fix it
		}
		r.mu.Lock()
		r.live = c
		r.mu.Unlock()
		r.log.Info("kafka consumer started", slog.String("consumer", name), slog.Any("topics", r.topics))

		err = c.Run(ctx)

		r.mu.Lock()
		r.stats, r.live = c.Stats(), nil
		r.mu.Unlock()
		if err != nil {
			r.log.Error("kafka consumer stopped", slog.String("consumer", name), slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartBackoff):
		}
	}
}

// sourceOf is the record's relay.Source; the schema id comes from the frame
// and is 0 for a record whose frame cannot be read.
func sourceOf(rec kafka.Record) relay.Source {
	var id uint32
	if f, err := events.Decode(rec.Value); err == nil {
		id = f.SchemaID
	}
	return eventbus.SourceOf(rec, id)
}

func (r *Runner) handle(ctx context.Context, rec kafka.Record) (err error) {
	name := r.handler.ConsumerName
	evt, err := eventbus.Decode(rec)
	if err != nil {
		r.count(telemetry.OutcomeFailed)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()
	ctx = relay.WithSource(ctx, sourceOf(rec))
	ctx = logging.WithContext(ctx, logging.WithEventID(logging.FromContext(ctx), evt.EventID.String()))
	if r.o.Tracing {
		var span trace.Span
		ctx, span = telemetry.StartConsume(ctx, traceparent(rec, evt), name, rec.Topic, evt.EventType, evt.EventID.String())
		defer func() { telemetry.EndSpan(span, err) }()
	}
	if !r.handler.Handles(evt.EventType) {
		return nil // the topic carries types this consumer did not register for
	}
	if err := r.safeHandle(ctx, evt); err != nil {
		r.count(telemetry.OutcomeFailed)
		return err
	}
	r.count(telemetry.OutcomeProcessed)
	return nil
}

func (r *Runner) safeHandle(ctx context.Context, evt relay.DecodedEvent) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return r.handler.Handle(ctx, nil, evt) // the archiver uses no transaction
}

func traceparent(rec kafka.Record, evt relay.DecodedEvent) string {
	if v, ok := rec.Header(eventbus.HeaderTraceparent); ok && len(v) > 0 {
		return string(v)
	}
	return telemetry.TraceparentFromIDs(evt.TraceID, evt.SpanID)
}

func (r *Runner) count(outcome telemetry.Outcome) {
	if r.o.Metrics != nil {
		r.o.Metrics.EventProcessed(r.handler.ConsumerName, outcome)
	}
}

// exhausted parks a record whose budget is spent. It is called once; the
// offset advances when it returns whatever happens, so it retries the sink a
// few times and, if the sink stays down, logs the event in full and counts the
// failure: the one way an event leaves the stream unparked, and it is loud.
func (r *Runner) exhausted(ctx context.Context, rec kafka.Record, cause error) {
	name := r.handler.ConsumerName
	ctx = relay.WithSource(ctx, sourceOf(rec))
	d := relay.DeadLetter{Event: stubEvent(rec), Consumer: name, Err: cause, History: kafka.AttemptErrors(cause), Attempts: r.attempts()}
	if r.o.Sink == nil {
		r.log.Error("kafka consumer gave up on a record; no dead-letter sink is configured, the record is dropped",
			slog.String("consumer", name), slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset), slog.String("event_id", d.Event.EventID.String()), slog.Any("error", cause))
		return
	}
	var err error
	for i := 0; i < parkTries; i++ {
		if err = parkSafely(ctx, r.o.Sink, d); err == nil {
			r.count(telemetry.OutcomeDeadLetter)
			r.log.Warn("kafka consumer parked a record in the dead-letter sink",
				slog.String("consumer", name), slog.String("event_id", d.Event.EventID.String()),
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
		slog.String("consumer", name), slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset), slog.String("event_id", d.Event.EventID.String()),
		slog.String("event_type", d.Event.EventType), slog.String("payload", string(d.Event.Payload)),
		slog.Any("handler_error", cause), slog.Any("park_error", err))
}

func (r *Runner) attempts() int {
	if r.o.Kafka.ConsumerMaxAttempts > 0 {
		return r.o.Kafka.ConsumerMaxAttempts
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
// bytes (base64) with their coordinates, so an undecodable record is parked
// with everything needed to look at it. Replay refuses such a record.
func stubEvent(rec kafka.Record) relay.DecodedEvent {
	if evt, err := eventbus.Decode(rec); err == nil {
		return evt
	}
	var evt relay.DecodedEvent
	if v, ok := rec.Header(eventbus.HeaderEventID); ok {
		evt.EventID, _ = uuid.Parse(string(v))
	}
	if v, ok := rec.Header(eventbus.HeaderEventType); ok {
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
