package transport_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport/transporttest"
)

// samplePayloads are what the services write to the outbox for each event type
// (the outbox.Write call sites), the input the relay has to encode.
func samplePayloads(agg uuid.UUID) map[string]map[string]any {
	now := time.Date(2026, 11, 20, 9, 30, 15, 123456000, time.FixedZone("PKT", 5*3600))
	return map[string]map[string]any{
		"identity.user_registered":      {"user_id": agg, "email": "ada@example.com", "role": "student", "full_name": "Ada Lovelace"},
		"identity.user_profile_updated": {"user_id": agg, "full_name": "Ada L."},
		"identity.user_role_changed":    {"user_id": agg, "role": "instructor", "status": "active"},
		"course.course_published":       {"course_id": agg, "version_id": uuid.New(), "title": "Distributed Systems", "instructor_id": uuid.New()},
		"course.course_archived":        {"course_id": agg},
		"enrollment.student_enrolled":   {"enrollment_id": uuid.New(), "student_id": uuid.New(), "course_id": agg, "enrolled_at": now},
		"enrollment.student_withdrawn":  {"enrollment_id": uuid.New(), "student_id": uuid.New(), "course_id": agg},
		"progress.course_completed":     {"enrollment_id": agg, "student_id": uuid.New(), "completed_at": now},
	}
}

func testSchemas(t *testing.T) transport.Schemas {
	t.Helper()
	reg := transporttest.NewRegistry(t)
	s, err := transport.LoadSchemas(context.Background(), registry.New(reg.URL))
	if err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	return s
}

func rowFor(t *testing.T, eventType string, payload map[string]any, agg uuid.UUID) relay.DecodedEvent {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	return relay.DecodedEvent{
		OutboxID: 77, EventID: uuid.New(), AggregateType: "x", AggregateID: agg, EventType: eventType, SchemaVersion: 1,
		Payload: b, OccurredAt: time.Date(2026, 11, 20, 4, 30, 15, 0, time.UTC), TraceID: strings.Repeat("a", 32), SpanID: strings.Repeat("b", 16), ActorID: &actor,
	}
}

func TestTopicFor_RoutesByEventTypePrefix(t *testing.T) {
	cases := map[string]string{
		"identity.user_registered":     topics.Identity,
		"course.course_published":      topics.Course,
		"enrollment.student_enrolled":  topics.Enrollment, // aggregate_type is "course", the topic follows the prefix
		"progress.course_completed":    topics.Progress,
		"course.some_future_event":     topics.Course,
		"enrollment.student_withdrawn": topics.Enrollment,
	}
	for et, want := range cases {
		got, err := transport.TopicFor(et)
		if err != nil || got != want {
			t.Errorf("TopicFor(%q) = %q, %v; want %q", et, got, err, want)
		}
	}
	for _, bad := range []string{"", "nodot", "billing.invoice_paid", ".x"} {
		if _, err := transport.TopicFor(bad); err == nil {
			t.Errorf("TopicFor(%q) must fail", bad)
		}
	}
}

// The routing table and libs/events' catalog are two lists of the same fact;
// they must not drift.
func TestTopicFor_AgreesWithEventCatalogAndTopicList(t *testing.T) {
	known := map[string]bool{}
	for _, n := range topics.Names() {
		known[n] = true
	}
	for _, et := range events.Types() {
		got, err := transport.TopicFor(et.EventType)
		if err != nil || got != et.Topic {
			t.Errorf("%s: routing says %q (%v), catalog says %q", et.EventType, got, err, et.Topic)
		}
		if !known[got] {
			t.Errorf("%s routes to %q, which libs/kafka/topics does not provision", et.EventType, got)
		}
	}
}

func TestTopicsFor_HandlerSubscribesToItsEventTypesTopics(t *testing.T) {
	h := relay.NewHandler("c", []string{"enrollment.student_enrolled", "enrollment.student_withdrawn", "course.course_archived", "progress.course_completed"}, nil)
	got, err := transport.TopicsFor(h)
	want := []string{topics.Course, topics.Enrollment, topics.Progress}
	if err != nil || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("TopicsFor = %v, %v; want %v", got, err, want)
	}
	if _, err := transport.TopicsFor(relay.NewHandler("c", []string{"nope.x"}, nil)); err == nil {
		t.Error("an event type with no topic must be an error")
	}
	if _, err := transport.TopicsFor(relay.NewHandler("c", nil, nil)); err == nil {
		t.Error("a handler with no event types must be an error")
	}
}

func TestCodec_EveryEventTypeRoundTripsToTheHandlerInput(t *testing.T) {
	codec := transport.NewCodec(testSchemas(t))
	agg := uuid.New()
	payloads := samplePayloads(agg)
	if len(payloads) != len(events.Types()) {
		t.Fatalf("samplePayloads covers %d of %d event types", len(payloads), len(events.Types()))
	}
	for _, et := range events.Types() {
		t.Run(et.EventType, func(t *testing.T) {
			row := rowFor(t, et.EventType, payloads[et.EventType], agg)
			msg, err := codec.Encode(row)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if msg.Topic != et.Topic || string(msg.Key) != agg.String() {
				t.Fatalf("topic=%s key=%s, want %s keyed by the aggregate id %s", msg.Topic, msg.Key, et.Topic, agg)
			}
			rec := kafka.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers}
			got, err := transport.Decode(rec)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.EventID != row.EventID || got.EventType != row.EventType || got.AggregateID != agg ||
				got.AggregateType != "x" || got.OutboxID != 77 || got.TraceID != row.TraceID || got.SpanID != row.SpanID ||
				got.ActorID == nil || *got.ActorID != *row.ActorID || !got.OccurredAt.Equal(row.OccurredAt) {
				t.Fatalf("metadata did not survive:\n got  %+v\n want %+v", got, row)
			}
			if got.SchemaVersion != 1 {
				t.Errorf("SchemaVersion = %d, want the registry version 1", got.SchemaVersion)
			}
			// Same keys and the same values, compared as JSON the way a handler
			// reads them; timestamps are compared as instants.
			var in, out map[string]any
			if err := json.Unmarshal(row.Payload, &in); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(got.Payload, &out); err != nil {
				t.Fatalf("decoded payload is not JSON: %v (%s)", err, got.Payload)
			}
			if len(in) != len(out) {
				t.Fatalf("payload keys differ:\n in  %v\n out %v", in, out)
			}
			for k, v := range in {
				ov, ok := out[k]
				if !ok {
					t.Fatalf("key %q lost: %s", k, got.Payload)
				}
				if strings.HasSuffix(k, "_at") {
					a, _ := time.Parse(time.RFC3339Nano, v.(string))
					b, err := time.Parse(time.RFC3339Nano, ov.(string))
					if err != nil || !a.Equal(b) {
						t.Fatalf("%s: %v vs %v (%v)", k, v, ov, err)
					}
					continue
				}
				if v != ov {
					t.Fatalf("%s: %v vs %v", k, v, ov)
				}
			}
		})
	}
}

func TestCodec_FrameCarriesTheRegistrySchemaIDAndVersion(t *testing.T) {
	reg := transporttest.NewRegistry(t)
	schemas, err := transport.LoadSchemas(context.Background(), registry.New(reg.URL))
	if err != nil {
		t.Fatal(err)
	}
	agg := uuid.New()
	msg, err := transport.NewCodec(schemas).Encode(rowFor(t, "course.course_archived", map[string]any{"course_id": agg}, agg))
	if err != nil {
		t.Fatal(err)
	}
	f, err := events.Decode(msg.Value)
	if err != nil {
		t.Fatal(err)
	}
	if int(f.SchemaID) != reg.ID(events.EnvelopeSubject(topics.Course)) {
		t.Errorf("frame schema id %d, want the envelope subject's id %d", f.SchemaID, reg.ID(events.EnvelopeSubject(topics.Course)))
	}
	var env events.Envelope
	if _, err := events.DecodeInto(msg.Value, &env); err != nil || env.SchemaVersion != 1 {
		t.Errorf("envelope schema_version = %d (%v), want the payload subject's registry version 1", env.SchemaVersion, err)
	}
}

func TestCodec_UnencodableRowsAreMarkedAndNamed(t *testing.T) {
	codec := transport.NewCodec(testSchemas(t))
	agg := uuid.New()
	cases := map[string]relay.DecodedEvent{
		"unknown event type":  rowFor(t, "course.course_created", map[string]any{"course_id": agg}, agg),
		"no topic prefix":     rowFor(t, "billing.paid", map[string]any{}, agg),
		"field not in schema": rowFor(t, "course.course_archived", map[string]any{"course_id": agg, "surprise": 1}, agg),
		"wrong type":          rowFor(t, "course.course_archived", map[string]any{"course_id": 42}, agg),
		"not json":            {EventID: uuid.New(), AggregateID: agg, EventType: "course.course_archived", Payload: []byte("{oops")},
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := codec.Encode(row)
			if !errors.Is(err, transport.ErrUnencodable) {
				t.Fatalf("want ErrUnencodable, got %v", err)
			}
			if relay.IsTransient(err) {
				t.Fatal("an unencodable row must not be transient: no retry can fix it")
			}
		})
	}
}

func TestDecode_RejectsGarbage(t *testing.T) {
	for name, v := range map[string][]byte{"empty": nil, "not a frame": []byte("hello world"), "bad magic": {1, 0, 0, 0, 1, 0, 1}} {
		if _, err := transport.Decode(kafka.Record{Value: v}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestLoadSchemas_FailsClearlyWhenASubjectIsMissing(t *testing.T) {
	reg := transporttest.NewRegistry(t)
	reg.Remove("smartcourse.course.v1.CoursePublished")
	reg.Remove("progress.events-value")
	_, err := transport.LoadSchemas(context.Background(), registry.New(reg.URL))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"smartcourse.course.v1.CoursePublished", "progress.events-value", "register-schemas"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestLoadSchemas_FailsWhenTheRegistryIsUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Port 1 on loopback refuses the connection.
	_, err := transport.LoadSchemas(ctx, registry.New("http://127.0.0.1:1"))
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Fatalf("want an error naming the registry address, got %v", err)
	}
}
