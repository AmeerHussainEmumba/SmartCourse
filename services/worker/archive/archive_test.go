package archive_test

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/archive"
)

type fakeStore struct {
	mu     sync.Mutex
	events []mongostore.Event
	audits []mongostore.AuditEntry
}

func (f *fakeStore) ArchiveEvent(_ context.Context, e mongostore.Event) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return true, nil
}

func (f *fakeStore) AppendAuditOnce(_ context.Context, a mongostore.AuditEntry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, a)
	return true, nil
}

func deliver(t *testing.T, ctx context.Context, store archive.Store, evt relay.DecodedEvent, opts ...archive.Option) {
	t.Helper()
	hs := archive.Handlers(store, opts...)
	if len(hs) != 1 || !hs[0].Handles(evt.EventType) {
		t.Fatalf("the archiver does not handle %s", evt.EventType)
	}
	if err := hs[0].Handle(ctx, nil, evt); err != nil {
		t.Fatalf("handle %s: %v", evt.EventType, err)
	}
}

func eventOf(eventType string, resourceKey string, resource uuid.UUID) relay.DecodedEvent {
	payload, _ := json.Marshal(map[string]any{resourceKey: resource.String()})
	return relay.DecodedEvent{
		EventID: uuid.New(), EventType: eventType, AggregateType: "agg", AggregateID: uuid.New(), SchemaVersion: 1,
		Payload: payload, OccurredAt: time.Now().UTC(), TraceID: "t",
	}
}

// RFC §4.6 marks CoursePublished, CoursePublishFailed, CourseArchived and
// CertificateIssued "archive + audit"; §5.4 adds role changes. Everything else
// in the catalogue is archive only. The set is pinned so that a change to either
// side is a deliberate one.
func TestAuditTypes_MatchTheRFC(t *testing.T) {
	want := []string{
		"course.course_archived", "course.course_published", "course.publish_failed",
		"identity.user_role_changed", "progress.certificate_issued",
	}
	got := archive.AuditTypes()
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Fatalf("audited types = %v, want %v", got, want)
	}
	for _, ty := range got {
		if _, ok := events.Lookup(ty); !ok {
			t.Errorf("audited type %s is not in the event catalogue", ty)
		}
	}
}

// One test per audited type: the entry names the resource the payload points at
// (not the aggregate id), the actor when there is one, and the action is the type.
func TestAuditedTypes_EachWritesAnAuditEntry(t *testing.T) {
	cases := []struct{ eventType, key, resourceType string }{
		{"identity.user_role_changed", "user_id", "user"},
		{"course.course_published", "course_id", "course"},
		{"course.course_archived", "course_id", "course"},
		{"course.publish_failed", "course_id", "course"},
		{"progress.certificate_issued", "certificate_id", "certificate"},
	}
	if len(cases) != len(archive.AuditTypes()) {
		t.Fatalf("%d cases for %d audited types: add the missing one", len(cases), len(archive.AuditTypes()))
	}
	for _, c := range cases {
		t.Run(c.eventType, func(t *testing.T) {
			store := &fakeStore{}
			resource, actor := uuid.New(), uuid.New()
			evt := eventOf(c.eventType, c.key, resource)
			evt.ActorID = &actor
			deliver(t, context.Background(), store, evt)
			if len(store.events) != 1 || len(store.audits) != 1 {
				t.Fatalf("%d archive documents and %d audit entries, want 1 and 1", len(store.events), len(store.audits))
			}
			a := store.audits[0]
			if a.EventID != evt.EventID.String() || a.Action != c.eventType || a.ResourceType != c.resourceType ||
				a.ResourceID != resource.String() || a.ActorID != actor.String() || a.After == nil {
				t.Errorf("audit entry = %+v, want event %s, action %s, %s %s, actor %s, the payload as after", a, evt.EventID, c.eventType, c.resourceType, resource, actor)
			}
		})
	}
}

func TestSystemEventsAuditedAsSystem_AndOthersNotAudited(t *testing.T) {
	store := &fakeStore{}
	deliver(t, context.Background(), store, eventOf("progress.certificate_issued", "certificate_id", uuid.New()))
	if len(store.audits) != 1 || store.audits[0].ActorID != "system" {
		t.Errorf("an event with no actor must be audited as system: %+v", store.audits)
	}
	audited := map[string]bool{}
	for _, ty := range archive.AuditTypes() {
		audited[ty] = true
	}
	for _, ty := range events.Types() {
		if audited[ty.EventType] {
			continue
		}
		s := &fakeStore{}
		deliver(t, context.Background(), s, eventOf(ty.EventType, "x", uuid.New()))
		if len(s.events) != 1 || len(s.audits) != 0 {
			t.Errorf("%s: %d archive documents, %d audit entries; want archive only", ty.EventType, len(s.events), len(s.audits))
		}
	}
}

// E13: over Kafka the document carries the frame's schema id and the record's
// topic, key, partition and offset (0 and 0 included).
func TestArchive_StoresTheKafkaSource(t *testing.T) {
	store := &fakeStore{}
	ctx := relay.WithSource(context.Background(), relay.Source{Topic: "course.events", Partition: 0, Offset: 0, Key: "agg-1", SchemaID: 17})
	deliver(t, ctx, store, eventOf("course.course_published", "course_id", uuid.New()),
		archive.WithSchemaResolver(func(string, int) int32 { return 99 })) // the frame's id wins
	d := store.events[0]
	if d.SchemaID != 17 || d.Headers["schema_id_source"] != "registry_frame" {
		t.Errorf("schema id %d (%s), want the frame's 17 (registry_frame)", d.SchemaID, d.Headers["schema_id_source"])
	}
	if d.Topic != "course.events" || d.Key != "agg-1" || d.Partition == nil || *d.Partition != 0 || d.Offset == nil || *d.Offset != 0 {
		t.Errorf("source = topic %q key %q partition %v offset %v, want course.events, agg-1, 0, 0", d.Topic, d.Key, d.Partition, d.Offset)
	}
}

// In-process mode has no registry id and no coordinates: schema_id is 0 and
// marked as such, and no Kafka field is stored. A resolver, if given, is used.
func TestArchive_InProcessStoresNoSource(t *testing.T) {
	store := &fakeStore{}
	deliver(t, context.Background(), store, eventOf("course.course_published", "course_id", uuid.New()))
	d := store.events[0]
	if d.SchemaID != 0 || d.Headers["schema_id_source"] != "none" {
		t.Errorf("schema id %d (%s), want 0 marked none", d.SchemaID, d.Headers["schema_id_source"])
	}
	if d.Topic != "" || d.Key != "" || d.Partition != nil || d.Offset != nil {
		t.Errorf("in-process document has Kafka fields: %+v", d)
	}
	deliver(t, context.Background(), store, eventOf("course.course_published", "course_id", uuid.New()),
		archive.WithSchemaResolver(func(string, int) int32 { return 5 }))
	if d := store.events[1]; d.SchemaID != 5 || d.Headers["schema_id_source"] != "resolver" {
		t.Errorf("with a resolver: schema id %d (%s), want 5 (resolver)", d.SchemaID, d.Headers["schema_id_source"])
	}
}
