package replay_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/replay"
)

const consumer = topics.ConsumerCourseInstructorMirror

// fakeStore is an in-memory failed_events with mongostore's MarkReplayed
// semantics (one conditional transition).
type fakeStore struct {
	mu   sync.Mutex
	recs map[string]*mongostore.FailedEvent // by event id + consumer
	// events records the order of the interesting calls across store and producer.
	log       *[]string
	markErr   error
	listErr   error
	markCalls atomic.Int64
}

func key(eventID, c string) string { return eventID + "|" + c }

func (f *fakeStore) add(rec mongostore.FailedEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recs == nil {
		f.recs = map[string]*mongostore.FailedEvent{}
	}
	rec.ID = fmt.Sprintf("%024d", len(f.recs)+1)
	if rec.Status == "" {
		rec.Status = mongostore.StatusParked
	}
	f.recs[key(rec.EventID, rec.Consumer)] = &rec
}

func (f *fakeStore) ListFailedEvents(_ context.Context, fl mongostore.FailedFilter, _ string, _ int) (*mongostore.FailedPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	page := &mongostore.FailedPage{}
	for _, r := range f.recs {
		if r.EventID == fl.EventID {
			c := *r
			c.Payload = nil
			page.Items = append(page.Items, c)
		}
	}
	return page, nil
}

func (f *fakeStore) GetFailedEvent(_ context.Context, eventID, c string) (*mongostore.FailedEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.recs[key(eventID, c)]
	if !ok {
		return nil, mongostore.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (f *fakeStore) MarkReplayed(_ context.Context, eventID, c string, at time.Time) (bool, error) {
	f.markCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.log != nil {
		*f.log = append(*f.log, "mark")
	}
	if f.markErr != nil {
		return false, f.markErr
	}
	r, ok := f.recs[key(eventID, c)]
	if !ok {
		return false, mongostore.ErrNotFound
	}
	if r.Status != mongostore.StatusParked {
		return false, nil
	}
	r.Status, r.ReplayedAt = mongostore.StatusReplayed, &at
	return true, nil
}

func (f *fakeStore) status(eventID, c string) mongostore.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recs[key(eventID, c)].Status
}

type fakeProducer struct {
	mu    sync.Mutex
	log   *[]string
	err   error
	delay time.Duration
	sent  []sentRecord
}

type sentRecord struct {
	consumer string
	schemaID uint32
	evt      relay.DecodedEvent
}

func (p *fakeProducer) PublishRetry(_ context.Context, c string, schemaID uint32, evt relay.DecodedEvent) error {
	time.Sleep(p.delay)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.log != nil {
		*p.log = append(*p.log, "produce")
	}
	if p.err != nil {
		return p.err
	}
	p.sent = append(p.sent, sentRecord{c, schemaID, evt})
	return nil
}

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sent)
}

func validRecord(eventID uuid.UUID) mongostore.FailedEvent {
	agg := uuid.New()
	return mongostore.FailedEvent{
		EventID: eventID.String(), Consumer: consumer, EventType: "identity.user_profile_updated",
		Topic: topics.Identity, AggregateType: "user", AggregateID: agg.String(),
		Payload:  []byte(fmt.Sprintf(`{"user_id":%q,"full_name":"Ada"}`, agg)),
		OutboxID: 42, SchemaID: 7, SchemaVersion: 1, OccurredAt: time.Now().UTC(), TraceID: "trace-1", SpanID: "span-1",
	}
}

func newReplayer(s *fakeStore, p *fakeProducer, opts ...replay.Option) *replay.Replayer {
	return replay.New(s, p, opts...)
}

// The ordering argument: the retry record is produced first and only then is
// the record marked replayed. Mutation check: swap the two steps in replayOne
// and this fails.
func TestReplay_ProducesBeforeMarking(t *testing.T) {
	var order []string
	s, p := &fakeStore{log: &order}, &fakeProducer{log: &order}
	id := uuid.New()
	s.add(validRecord(id))
	res, err := newReplayer(s, p).Replay(context.Background(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "produce,mark" {
		t.Fatalf("order = %v, want produce then mark", order)
	}
	if !res.Replayed() || res.Outcomes[0].Status != replay.StatusReplayed || res.Outcomes[0].Topic != topics.Retry(consumer) {
		t.Fatalf("result = %+v", res)
	}
	got := p.sent[0]
	if got.consumer != consumer || got.schemaID != 7 || got.evt.OutboxID != 42 || got.evt.TraceID != "trace-1" || got.evt.EventID != id {
		t.Fatalf("produced %+v", got)
	}
	// And the record really encodes to the retry topic.
	msg, err := eventbus.EncodeForRetry(got.consumer, got.schemaID, got.evt)
	if err != nil || msg.Topic != topics.Retry(consumer) {
		t.Fatalf("encode: %v %q", err, msg.Topic)
	}
}

// A produce failure leaves the record parked and is reported as unavailable;
// nothing is marked.
func TestReplay_ProduceFailureLeavesRecordParked(t *testing.T) {
	s, p := &fakeStore{}, &fakeProducer{err: errors.New("broker down")}
	id := uuid.New()
	s.add(validRecord(id))
	res, err := newReplayer(s, p).Replay(context.Background(), id.String())
	if !errors.Is(err, replay.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if s.markCalls.Load() != 0 || s.status(id.String(), consumer) != mongostore.StatusParked {
		t.Fatal("the record was marked although nothing was produced")
	}
	if res == nil || res.Outcomes[0].Status != replay.StatusProduceFailed {
		t.Fatalf("result = %+v", res)
	}
}

// Produced but the mark failed: the record stays parked, the outcome says so,
// and a second replay produces again (absorbed downstream) and then marks.
func TestReplay_MarkFailureAfterProduceStaysParkedAndIsRepeatable(t *testing.T) {
	s, p := &fakeStore{markErr: errors.New("mongo down")}, &fakeProducer{}
	id := uuid.New()
	s.add(validRecord(id))
	r := newReplayer(s, p)
	res, err := r.Replay(context.Background(), id.String())
	if !errors.Is(err, replay.ErrUnavailable) || res.Outcomes[0].Status != replay.StatusProducedNotMarked {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if s.status(id.String(), consumer) != mongostore.StatusParked {
		t.Fatal("record not parked")
	}
	s.markErr = nil
	res, err = r.Replay(context.Background(), id.String())
	if err != nil || !res.Replayed() {
		t.Fatalf("second replay: %+v %v", res, err)
	}
	if p.count() != 2 {
		t.Fatalf("produced %d times, want 2 (the duplicate is the documented price)", p.count())
	}
}

// B-01: a record that cannot be decoded or encoded for a retry is refused,
// nothing is produced, and it stays parked.
func TestReplay_UndecodableRecordIsRefused(t *testing.T) {
	cases := map[string]func(*mongostore.FailedEvent){
		"payload is not JSON":         func(r *mongostore.FailedEvent) { r.Payload = []byte("\x00\xff{not json") },
		"payload does not match type": func(r *mongostore.FailedEvent) { r.Payload = []byte(`{"unknown_field":1}`) },
		"no schema id":                func(r *mongostore.FailedEvent) { r.SchemaID = 0 },
		"no schema version":           func(r *mongostore.FailedEvent) { r.SchemaVersion = 0 },
		"unknown event type":          func(r *mongostore.FailedEvent) { r.EventType = "nope.unknown" },
		"empty event type (stub)":     func(r *mongostore.FailedEvent) { r.EventType = "" },
		"no payload":                  func(r *mongostore.FailedEvent) { r.Payload = nil },
		"bad aggregate id":            func(r *mongostore.FailedEvent) { r.AggregateID = "not-a-uuid" },
		"unknown consumer":            func(r *mongostore.FailedEvent) { r.Consumer = "not-a-consumer" },
		"path-like consumer":          func(r *mongostore.FailedEvent) { r.Consumer = "../course-instructor-mirror" },
		"the archiver's stub record": func(r *mongostore.FailedEvent) {
			r.EventType, r.Payload = "", []byte(`{"undecodable_record":{"topic":"identity","partition":0,"offset":3,"value_base64":"AAEC"}}`)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s, p := &fakeStore{}, &fakeProducer{}
			id := uuid.New()
			rec := validRecord(id)
			mutate(&rec)
			s.add(rec)
			res, err := newReplayer(s, p).Replay(context.Background(), id.String())
			if err != nil {
				t.Fatalf("a refusal is an outcome, not an error: %v", err)
			}
			if !res.Refused() || res.Outcomes[0].Error == "" {
				t.Fatalf("result = %+v, want a refusal with a reason", res)
			}
			if p.count() != 0 || s.markCalls.Load() != 0 || s.status(id.String(), rec.Consumer) != mongostore.StatusParked {
				t.Fatalf("a refused record was produced (%d) or marked (%d)", p.count(), s.markCalls.Load())
			}
		})
	}
}

func TestReplay_AlreadyReplayedIsANoOp(t *testing.T) {
	s, p := &fakeStore{}, &fakeProducer{}
	id := uuid.New()
	rec := validRecord(id)
	rec.Status = mongostore.StatusReplayed
	s.add(rec)
	res, err := newReplayer(s, p).Replay(context.Background(), id.String())
	if err != nil || res.Replayed() || res.Outcomes[0].Status != replay.StatusAlreadyReplayed {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if p.count() != 0 {
		t.Fatal("an already replayed record was produced again")
	}
}

func TestReplay_UnknownAndMalformedIDs(t *testing.T) {
	s, p := &fakeStore{}, &fakeProducer{}
	r := newReplayer(s, p)
	if _, err := r.Replay(context.Background(), uuid.NewString()); !errors.Is(err, replay.ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	if _, err := r.Replay(context.Background(), "../x"); !errors.Is(err, replay.ErrInvalidID) {
		t.Fatalf("malformed id: %v", err)
	}
	var nilR *replay.Replayer
	if _, err := nilR.Replay(context.Background(), uuid.NewString()); !errors.Is(err, replay.ErrNoStore) {
		t.Fatalf("nil replayer: %v", err)
	}
}

func TestReplay_StoreDownOnListIsUnavailable(t *testing.T) {
	s, p := &fakeStore{listErr: errors.New("mongo down")}, &fakeProducer{}
	res, err := newReplayer(s, p).Replay(context.Background(), uuid.NewString())
	if !errors.Is(err, replay.ErrUnavailable) || res != nil {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// One refused record does not stop the other consumers' records of the same
// event; each gets its own outcome.
func TestReplay_PerConsumerOutcomes(t *testing.T) {
	s, p := &fakeStore{}, &fakeProducer{}
	id := uuid.New()
	good, bad := validRecord(id), validRecord(id)
	bad.Consumer, bad.SchemaID = topics.ConsumerAnalyticsAggregator, 0
	s.add(good)
	s.add(bad)
	res, err := newReplayer(s, p).Replay(context.Background(), id.String())
	if err != nil || len(res.Outcomes) != 2 || !res.Replayed() || !res.Refused() {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if p.count() != 1 {
		t.Fatalf("produced %d, want 1", p.count())
	}
}

// The documented guarantee under N concurrent replays of one record: exactly
// one caller reports "replayed" (MarkReplayed transitions once), the others
// "already_replayed"; the retry record is produced at least once and at most N
// times; a call that starts after the mark produces nothing.
func TestReplay_ConcurrentDoubleReplay_MarksOnceProducesAtMostN(t *testing.T) {
	const n = 8
	s, p := &fakeStore{}, &fakeProducer{delay: 20 * time.Millisecond}
	id := uuid.New()
	s.add(validRecord(id))
	r := newReplayer(s, p)
	var wg sync.WaitGroup
	var replayed, already atomic.Int64
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := r.Replay(context.Background(), id.String())
			if err != nil {
				t.Error(err)
				return
			}
			switch res.Outcomes[0].Status {
			case replay.StatusReplayed:
				replayed.Add(1)
			case replay.StatusAlreadyReplayed:
				already.Add(1)
			default:
				t.Errorf("status %q", res.Outcomes[0].Status)
			}
		}()
	}
	close(start)
	wg.Wait()
	if replayed.Load() != 1 || already.Load() != n-1 {
		t.Fatalf("replayed=%d already=%d, want 1 and %d", replayed.Load(), already.Load(), n-1)
	}
	if c := p.count(); c < 1 || c > n {
		t.Fatalf("produced %d, want between 1 and %d", c, n)
	}
	before := p.count()
	if _, err := r.Replay(context.Background(), id.String()); err != nil {
		t.Fatal(err)
	}
	if p.count() != before {
		t.Fatal("a replay after the mark produced again")
	}
}
