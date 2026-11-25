//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/archive"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/consume"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/deadletter"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/replay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stack is Postgres (only for the consumer group of the service being replayed
// to: the audit service itself has none), Kafka, MongoDB and the schema ids a
// real registry would give.
type stack struct {
	t       *testing.T
	pg      *testenv.Env
	kafka   *testenv.KafkaEnv
	mongo   *testenv.MongoEnv
	schemas eventbus.Schemas
	store   *mongostore.Store
	wiring  *store.Wiring
}

func newStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{t: t, pg: testenv.Setup(t), kafka: testenv.SetupKafka(t), mongo: testenv.SetupMongo(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := topics.Ensure(ctx, s.kafka.Brokers); err != nil {
		t.Fatalf("topics.Ensure: %v", err)
	}
	reg := transporttest.NewRegistry(t)
	var err error
	if s.schemas, err = eventbus.LoadSchemas(ctx, registry.New(reg.URL)); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	st, err := mongostore.Open(ctx, mongostore.Config{URI: s.mongo.URI, ServerSelectionTimeout: 2 * time.Second, OpTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	s.store = st
	s.wiring = store.New(config.MongoConfig{})
	s.wiring.Use(st)
	// The downstream service's marker and effect tables.
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS public.processed_events (consumer_name TEXT NOT NULL, event_id UUID NOT NULL, processed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer_name, event_id))`,
		`CREATE TABLE IF NOT EXISTS public.test_effects (consumer TEXT NOT NULL, event_id UUID NOT NULL, n SERIAL)`,
	} {
		if err := s.pg.DB.Exec(q).Error; err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func (s *stack) kafkaCfg() config.KafkaConfig {
	return config.KafkaConfig{
		Brokers: s.kafka.Brokers, ConsumerMaxInFlight: 2, ConsumerMaxAttempts: 2,
		ConsumerBackoffBase: 20 * time.Millisecond, ConsumerBackoffMax: 100 * time.Millisecond, ConsumerShutdownTimeout: 5 * time.Second,
	}
}

func (s *stack) publisher(timeout time.Duration) *eventbus.Publisher {
	s.t.Helper()
	p, err := eventbus.NewPublisher(eventbus.PublisherConfig{Brokers: s.kafka.Brokers, ProduceTimeout: timeout, Schemas: s.schemas})
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(p.Close)
	return p
}

func (s *stack) eventually(d time.Duration, what string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %s waiting for: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *stack) count(q string, args ...any) int64 {
	s.t.Helper()
	var n int64
	if err := s.pg.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		s.t.Fatalf("count %q: %v", q, err)
	}
	return n
}

// eventOf is a valid event of type tp (an empty payload message is valid).
func eventOf(tp events.Type, actor *uuid.UUID) relay.DecodedEvent {
	payload, _ := protojson.Marshal(tp.New())
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "agg", AggregateID: uuid.New(), EventType: tp.EventType,
		Payload: payload, OccurredAt: time.Now().UTC(), TraceID: "trace", ActorID: actor,
	}
}

func profileEvent(agg uuid.UUID, name string) relay.DecodedEvent {
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "user", AggregateID: agg, EventType: "identity.user_profile_updated",
		Payload: []byte(fmt.Sprintf(`{"user_id":%q,"full_name":%q}`, agg, name)), OccurredAt: time.Now().UTC(),
	}
}

func (s *stack) publish(pub *eventbus.Publisher, evts ...relay.DecodedEvent) {
	s.t.Helper()
	for _, e := range evts {
		if err := pub.Publish(context.Background(), e); err != nil {
			s.t.Fatalf("publish %s: %v", e.EventType, err)
		}
	}
}

func (s *stack) archived(id uuid.UUID) *mongostore.Event {
	e, err := s.store.GetEvent(context.Background(), id.String())
	if err != nil {
		return nil
	}
	return e
}

// startArchiver runs the audit-archiver over st (the store the archiver
// writes to) and parks its own failures in the real store.
func (s *stack) startArchiver(st archive.Store) func() {
	s.t.Helper()
	handlers := archive.Handlers(st)
	r, err := consume.New(handlers[0], consume.Options{
		Kafka: s.kafkaCfg(), Logger: quiet,
		Sink: deadletter.ForRelay(deadletter.NewSink(s.wiring)),
	})
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	s.t.Cleanup(stop)
	return stop
}

// ---- the archiver end to end ----

// Every event type reaches the archive with its Kafka coordinates and the
// frame's schema id (carried from the relay.Source seam), the audit types also
// reach audit_log with their actor, a duplicate delivery leaves one document,
// and after a restart (a new consumer group member over events already
// archived, plus new ones) every event is there exactly once.
func TestArchiver_EndToEnd_CompletenessIdempotencyAndRestart(t *testing.T) {
	s := newStack(t)
	pub := s.publisher(15 * time.Second)
	stop := s.startArchiver(s.wiring)

	actor := uuid.New()
	var evts []relay.DecodedEvent
	for _, tp := range events.Types() {
		evts = append(evts, eventOf(tp, &actor))
	}
	s.publish(pub, evts...)
	s.publish(pub, evts[0]) // the same event again: a duplicate delivery

	for _, e := range evts {
		s.eventually(60*time.Second, "archive of "+e.EventType, func() bool { return s.archived(e.EventID) != nil })
	}
	for _, e := range evts {
		doc := s.archived(e.EventID)
		tp, _ := events.Lookup(e.EventType)
		if doc.Topic != tp.Topic || doc.Key != e.AggregateID.String() || doc.Partition == nil || doc.Offset == nil {
			t.Errorf("%s: kafka source not stored: topic %q key %q partition %v offset %v", e.EventType, doc.Topic, doc.Key, doc.Partition, doc.Offset)
		}
		if uint32(doc.SchemaID) != s.schemas.EnvelopeIDs[tp.Topic] || doc.Headers["schema_id_source"] != "registry_frame" {
			t.Errorf("%s: schema id %d (%s), want the frame's %d", e.EventType, doc.SchemaID, doc.Headers["schema_id_source"], s.schemas.EnvelopeIDs[tp.Topic])
		}
	}
	for _, typ := range archive.AuditTypes() {
		var found bool
		for _, e := range evts {
			if e.EventType != typ {
				continue
			}
			found = true
			s.eventually(30*time.Second, "audit entry for "+typ, func() bool { return s.auditCount(e.EventID) == 1 })
		}
		if !found {
			t.Errorf("audit type %s is not in the catalogue the test publishes", typ)
		}
	}

	// Restart: stop, publish more while nothing consumes, start a new archiver.
	stop()
	more := []relay.DecodedEvent{eventOf(events.Types()[0], nil), eventOf(events.Types()[3], nil)}
	s.publish(pub, more...)
	s.startArchiver(s.wiring)
	for _, e := range more {
		s.eventually(60*time.Second, "archive after restart of "+e.EventType, func() bool { return s.archived(e.EventID) != nil })
	}
	// The duplicate and any redelivery left exactly one document per event
	// (the event id is the document id).
	if n, want := s.rawCount(mongostore.CollEvents, bson.D{}), int64(len(evts)+len(more)); n != want {
		t.Errorf("%d archive documents, want %d (one per distinct event)", n, want)
	}
}

// rawCount counts documents with the driver directly (mongostore has no such view).
func (s *stack) rawCount(coll string, filter bson.D) int64 {
	s.t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(s.mongo.URI))
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = c.Disconnect(context.Background()) }()
	n, err := c.Database(mongostore.DefaultDatabase).Collection(coll).CountDocuments(context.Background(), filter)
	if err != nil {
		s.t.Fatal(err)
	}
	return n
}

func (s *stack) auditCount(eventID uuid.UUID) int {
	return int(s.rawCount(mongostore.CollAuditLog, bson.D{{Key: "event_id", Value: eventID.String()}}))
}

// ---- parking the archiver's own failures, and replaying them ----

// flakyStore fails ArchiveEvent for event ids in the set until Heal.
type flakyStore struct {
	inner  archive.Store
	mu     sync.Mutex
	failed map[string]bool
	calls  atomic.Int64
}

func (f *flakyStore) ArchiveEvent(ctx context.Context, e mongostore.Event) (bool, error) {
	f.calls.Add(1)
	f.mu.Lock()
	bad := f.failed[e.ID]
	f.mu.Unlock()
	if bad {
		return false, errors.New("mongo: simulated outage")
	}
	return f.inner.ArchiveEvent(ctx, e)
}

func (f *flakyStore) AppendAuditOnce(ctx context.Context, a mongostore.AuditEntry) (bool, error) {
	return f.inner.AppendAuditOnce(ctx, a)
}

func (f *flakyStore) heal() {
	f.mu.Lock()
	f.failed = map[string]bool{}
	f.mu.Unlock()
}

// The archiver's own failure is parked through the mongostore sink with the
// frame's schema id and topic, and replaying it republishes to
// retry.audit-archiver, where the archiver's own group archives it.
func TestArchiver_OwnFailureIsParkedAndReplayedThroughItsRetryTopic(t *testing.T) {
	s := newStack(t)
	pub := s.publisher(15 * time.Second)
	evt := profileEvent(uuid.New(), "Ada")
	flaky := &flakyStore{inner: s.wiring, failed: map[string]bool{evt.EventID.String(): true}}
	s.startArchiver(flaky)
	s.publish(pub, evt)

	var parked *mongostore.FailedEvent
	s.eventually(60*time.Second, "the archiver to park its failure", func() bool {
		rec, err := s.store.GetFailedEvent(context.Background(), evt.EventID.String(), topics.ConsumerAuditArchiver)
		parked = rec
		return err == nil
	})
	if parked.SchemaID == 0 || parked.Topic != topics.Identity || parked.SchemaVersion == 0 {
		t.Fatalf("parked record lacks what a replay needs: schema id %d, topic %q, version %d", parked.SchemaID, parked.Topic, parked.SchemaVersion)
	}
	if s.archived(evt.EventID) != nil {
		t.Fatal("the failing event was archived")
	}

	flaky.heal()
	res, err := replay.New(s.wiring, pub).Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Replayed() || res.Outcomes[0].Topic != topics.Retry(topics.ConsumerAuditArchiver) {
		t.Fatalf("replay: %+v %v", res, err)
	}
	s.eventually(60*time.Second, "the replayed event to be archived from the retry topic", func() bool {
		d := s.archived(evt.EventID)
		return d != nil && d.Topic == topics.Retry(topics.ConsumerAuditArchiver)
	})
}

// An undecodable record is parked with a stand-in and replay refuses it
// (finding B-01): nothing is produced and it stays parked.
func TestArchiver_UndecodableRecordIsParkedAndReplayIsRefused(t *testing.T) {
	s := newStack(t)
	pub := s.publisher(15 * time.Second)
	s.startArchiver(s.wiring)

	p, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: s.kafka.Brokers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	eid := uuid.New()
	if _, err := p.Produce(context.Background(), kafka.Message{
		Topic: topics.Identity, Key: []byte(uuid.NewString()), Value: []byte("not a framed envelope"),
		Headers: []kafka.Header{{Key: eventbus.HeaderEventID, Value: []byte(eid.String())}},
	}); err != nil {
		t.Fatal(err)
	}
	s.eventually(60*time.Second, "the undecodable record to be parked", func() bool {
		_, err := s.store.GetFailedEvent(context.Background(), eid.String(), topics.ConsumerAuditArchiver)
		return err == nil
	})
	res, err := replay.New(s.wiring, pub).Replay(context.Background(), eid.String())
	if err != nil || !res.Refused() {
		t.Fatalf("replay of an undecodable record: %+v %v", res, err)
	}
	rec, _ := s.store.GetFailedEvent(context.Background(), eid.String(), topics.ConsumerAuditArchiver)
	if rec.Status != mongostore.StatusParked {
		t.Fatalf("status %s, want parked", rec.Status)
	}
}

// ---- replay against a real consumer group of another service ----

func (s *stack) effectsOf(consumer string, eventID uuid.UUID) int64 {
	return s.count(`SELECT count(*) FROM public.test_effects WHERE consumer = ? AND event_id = ?`, consumer, eventID)
}

func effectHandler(consumer string) relay.Handler {
	return relay.NewHandler(consumer, []string{"identity.user_profile_updated"}, func(_ context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		return tx.Exec(`INSERT INTO public.test_effects (consumer, event_id) VALUES (?, ?)`, consumer, evt.EventID).Error
	})
}

func (s *stack) startConsumers(handlers ...relay.Handler) {
	s.t.Helper()
	c, err := eventbus.NewConsumers(s.kafkaCfg(), handlers, s.pg.DB, eventbus.WithLogger(quiet))
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	s.t.Cleanup(func() { cancel(); <-done })
}

func (s *stack) park(consumer string, evt relay.DecodedEvent) {
	s.t.Helper()
	err := deadletter.NewSink(s.wiring).Park(context.Background(), deadletter.Parked{
		Event: evt, Consumer: consumer, SchemaID: int32(s.schemas.EnvelopeIDs[topics.Identity]), Topic: topics.Identity,
		Err: errors.New("db down"), Attempts: 8,
	})
	if err != nil {
		s.t.Fatal(err)
	}
}

// A real consumer group on retry.<consumer> applies the replayed event once,
// the other consumer's group (same event type, different name) does not see it,
// a second replay is a no-op, and a concurrent burst of replays marks once and
// still applies once (processed_events absorbs the extra records).
func TestReplay_RepublishesToTheConsumersRetryTopic_AppliedOnce(t *testing.T) {
	s := newStack(t)
	owner, bystander := topics.ConsumerCourseInstructorMirror, topics.ConsumerAnalyticsAggregator
	s.startConsumers(effectHandler(owner), effectHandler(bystander))
	pub := s.publisher(15 * time.Second)
	r := replay.New(s.wiring, pub)

	evt := profileEvent(uuid.New(), "Ada")
	evt.SchemaVersion = int(s.schemas.PayloadVersions[evt.EventType])
	s.park(owner, evt)

	res, err := r.Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Replayed() || res.Outcomes[0].Topic != topics.Retry(owner) {
		t.Fatalf("replay: %+v %v", res, err)
	}
	s.eventually(60*time.Second, "the owner's group to apply the retry record", func() bool { return s.effectsOf(owner, evt.EventID) == 1 })
	time.Sleep(2 * time.Second) // give a wrongly routed record time to show up
	if n := s.effectsOf(bystander, evt.EventID); n != 0 {
		t.Fatalf("another consumer's group applied the retry record (%d)", n)
	}
	if n := s.count(`SELECT count(*) FROM public.processed_events WHERE consumer_name = ? AND event_id = ?`, owner, evt.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1 (the idempotent path)", n)
	}
	rec, _ := s.store.GetFailedEvent(context.Background(), evt.EventID.String(), owner)
	if rec.Status != mongostore.StatusReplayed {
		t.Fatalf("status %s, want replayed", rec.Status)
	}

	// A second replay: a clean no-op that says so, nothing more is produced or applied.
	res, err = r.Replay(context.Background(), evt.EventID.String())
	if err != nil || res.Replayed() || res.Outcomes[0].Status != replay.StatusAlreadyReplayed {
		t.Fatalf("second replay: %+v %v", res, err)
	}

	// Concurrent burst on a fresh record: marked once, applied once.
	burst := profileEvent(uuid.New(), "Grace")
	burst.SchemaVersion = evt.SchemaVersion
	s.park(owner, burst)
	const n = 6
	var wg sync.WaitGroup
	var replayed atomic.Int64
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Replay(context.Background(), burst.EventID.String())
			if err != nil {
				t.Error(err)
				return
			}
			if res.Replayed() {
				replayed.Add(1)
			}
		}()
	}
	wg.Wait()
	if replayed.Load() != 1 {
		t.Fatalf("%d callers reported replayed, want 1", replayed.Load())
	}
	s.eventually(60*time.Second, "the burst's event to be applied", func() bool { return s.effectsOf(owner, burst.EventID) >= 1 })
	time.Sleep(3 * time.Second) // any extra retry records have been consumed by now
	if got := s.effectsOf(owner, burst.EventID); got != 1 {
		t.Fatalf("applied %d times, want exactly once however many records were produced", got)
	}
}

// A produce failure (no broker reachable) leaves the record parked, nothing is
// marked, and the same record replays once the broker is reachable.
func TestReplay_ProduceFailureLeavesTheRecordParked(t *testing.T) {
	s := newStack(t)
	evt := profileEvent(uuid.New(), "Ada")
	evt.SchemaVersion = int(s.schemas.PayloadVersions[evt.EventType])
	s.park(topics.ConsumerCourseInstructorMirror, evt)

	dead, err := eventbus.NewPublisher(eventbus.PublisherConfig{Brokers: []string{"127.0.0.1:1"}, ProduceTimeout: 2 * time.Second, Schemas: s.schemas})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dead.Close)
	res, err := replay.New(s.wiring, dead, replay.WithProduceTimeout(3*time.Second)).Replay(context.Background(), evt.EventID.String())
	if !errors.Is(err, replay.ErrUnavailable) || res.Outcomes[0].Status != replay.StatusProduceFailed {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	rec, _ := s.store.GetFailedEvent(context.Background(), evt.EventID.String(), topics.ConsumerCourseInstructorMirror)
	if rec.Status != mongostore.StatusParked {
		t.Fatalf("status %s after a failed produce, want parked", rec.Status)
	}
	res, err = replay.New(s.wiring, s.publisher(15*time.Second)).Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Replayed() {
		t.Fatalf("replay with the broker reachable: %+v %v", res, err)
	}
}
