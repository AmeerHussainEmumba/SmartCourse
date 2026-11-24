package harness

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
)

// KRecord is one record read from a topic.
type KRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
}

// EventID is the event_id header the relay sets on every record.
func (r KRecord) EventID() string { return r.Headers["event_id"] }

// EventType is the event_type header.
func (r KRecord) EventType() string { return r.Headers["event_type"] }

// Envelope decodes the value (registry frame, then the Envelope) and returns
// it with the schema id from the frame.
func (r KRecord) Envelope() (*events.Envelope, uint32, error) {
	var env events.Envelope
	id, err := events.DecodeInto(r.Value, &env)
	if err != nil {
		return nil, 0, fmt.Errorf("decode %s[%d]@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	return &env, id, nil
}

// ReadTopic reads every record of topic from the start up to the end offsets
// it has when the call begins. It reads with its own client and no consumer
// group, so it never disturbs the offsets of the services under test. A topic
// that does not exist yet reads as empty.
func (s *Stack) ReadTopic(topic string) ([]KRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.Kafka.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets of %s: %w", topic, err)
	}
	want := map[int32]int64{} // partition -> last offset to see
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			want[o.Partition] = o.Offset - 1
		}
	})
	var out []KRecord
	for len(want) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("read %s: %w (still waiting for partitions %v)", topic, err, want)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return out, fmt.Errorf("read %s: %v", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			last, open := want[r.Partition]
			if !open {
				return
			}
			rec := KRecord{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: r.Value, Headers: map[string]string{}}
			for _, h := range r.Headers {
				rec.Headers[h.Key] = string(h.Value)
			}
			out = append(out, rec)
			if r.Offset >= last {
				delete(want, r.Partition)
			}
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Offset < out[j].Offset
	})
	return out, nil
}

// ReadTopics reads several topics (see ReadTopic) and concatenates them.
func (s *Stack) ReadTopics(topics ...string) ([]KRecord, error) {
	var all []KRecord
	for _, topic := range topics {
		recs, err := s.ReadTopic(topic)
		if err != nil {
			return nil, err
		}
		all = append(all, recs...)
	}
	return all, nil
}

// EventTopics are the four topics of RFC 6.5.
var EventTopics = events.Topics()

// ConsumerLag is the total lag of a consumer group (the consumer name, for the
// worker) over its subscribed topics: records not yet committed. A group the
// broker does not know yet, or one that has not committed, reports an error
// the caller can poll through.
func (s *Stack) ConsumerLag(group string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Kafka.Brokers...))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	lags, err := kadm.NewClient(cl).Lag(ctx, group)
	if err != nil {
		return 0, err
	}
	l, ok := lags[group]
	if !ok {
		return 0, fmt.Errorf("group %s unknown to the broker", group)
	}
	if err := l.Error(); err != nil {
		return 0, err
	}
	if l.State != "Stable" && l.Lag.IsEmpty() {
		return 0, fmt.Errorf("group %s is %s with no offsets", group, l.State)
	}
	return l.Lag.Total(), nil
}

// EventProducer writes records to the event topics the way the relay does, but
// without the relay: a test-only producer that bypasses the outbox. It lets a
// scenario re-deliver a record (E3) or inject one the services would never
// produce (E4). It frames values with the real registry ids.
type EventProducer struct {
	s   *Stack
	cl  *kgo.Client
	reg *registry.Client
}

// NewEventProducer builds a producer against the stack's broker and registry.
func (s *Stack) NewEventProducer(t testing.TB) *EventProducer {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Kafka.Brokers...))
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(cl.Close)
	return &EventProducer{s: s, cl: cl, reg: registry.New(s.RegistryURL)}
}

// Redeliver produces a copy of rec: the same topic, key, value bytes and
// headers, so the same event id and outbox id, as an at-least-once redelivery.
func (p *EventProducer) Redeliver(t testing.TB, rec KRecord) {
	t.Helper()
	r := &kgo.Record{Topic: rec.Topic, Key: []byte(rec.Key), Value: rec.Value}
	for k, v := range rec.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	p.produce(t, r)
}

// Produce writes a new event (a fresh event id) of eventType whose payload is
// the protobuf JSON payloadJSON, keyed by aggregateID, with outboxID in the
// outbox_id header (the mirror handlers' ordering guard compares it with what
// they stored, so pass a value above any real outbox id to make it "newest").
// It returns the event id.
func (p *EventProducer) Produce(t testing.TB, eventType string, aggregateID uuid.UUID, payloadJSON string, outboxID int64) string {
	t.Helper()
	typ, ok := events.Lookup(eventType)
	if !ok {
		t.Fatalf("unknown event type %q", eventType)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload := typ.New()
	if err := (protojson.UnmarshalOptions{}).Unmarshal([]byte(payloadJSON), payload); err != nil {
		t.Fatalf("payload of %s: %v", eventType, err)
	}
	payloadVersion, err := p.reg.LatestVersion(ctx, typ.Subject())
	if err != nil {
		t.Fatalf("registry version of %s: %v", typ.Subject(), err)
	}
	latest, err := p.reg.LatestSchema(ctx, events.EnvelopeSubject(typ.Topic))
	if err != nil {
		t.Fatalf("registry envelope schema of %s: %v", typ.Topic, err)
	}
	eventID := uuid.NewString()
	env, err := events.NewEnvelope(typ, &events.Envelope{
		EventId:       eventID,
		SchemaVersion: int32(payloadVersion),
		AggregateType: "e2e",
		AggregateId:   aggregateID.String(),
		OccurredAt:    timestamppb.Now(),
	}, payload)
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	value, err := events.Encode(uint32(latest.ID), env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	p.produce(t, &kgo.Record{
		Topic: typ.Topic,
		Key:   []byte(aggregateID.String()),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(eventID)},
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "outbox_id", Value: []byte(fmt.Sprint(outboxID))},
		},
	})
	return eventID
}

func (p *EventProducer) produce(t testing.TB, r *kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce to %s: %v", r.Topic, err)
	}
}
