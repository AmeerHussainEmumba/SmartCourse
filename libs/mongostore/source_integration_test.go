//go:build integration

package mongostore_test

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// The Kafka coordinates of an archived event (RFC §6.8). Partition 0 and offset
// 0 are real values and must be stored; an event with no coordinates (in-process
// delivery) stores none.
func TestArchiveEvent_StoresKafkaCoordinates(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()
	raw := rawClient(t, env.URI).Collection(mongostore.CollEvents)

	p, o := int32(0), int64(0)
	if _, err := s.ArchiveEvent(ctx, mongostore.Event{
		ID: "e-kafka", EventType: "course.course_published", SchemaID: 7, Payload: []byte("x"),
		Topic: "course.events", Key: "agg-1", Partition: &p, Offset: &o,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "e-inproc", EventType: "course.course_published", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	var k bson.M
	if err := raw.FindOne(ctx, bson.D{{Key: "_id", Value: "e-kafka"}}).Decode(&k); err != nil {
		t.Fatal(err)
	}
	if k["topic"] != "course.events" || k["key"] != "agg-1" || k["partition"] != int32(0) || k["offset"] != int64(0) || k["schema_id"] != int32(7) {
		t.Errorf("kafka event document = %v, want topic, key, partition 0, offset 0 and schema_id 7", k)
	}
	var n bson.M
	if err := raw.FindOne(ctx, bson.D{{Key: "_id", Value: "e-inproc"}}).Decode(&n); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"topic", "key", "partition", "offset"} {
		if _, ok := n[f]; ok {
			t.Errorf("in-process event document has %s; nothing should be stored without a broker", f)
		}
	}
}

// Audit entries written with AppendAudit carry no event_id, so the unique
// partial index on event_id (AppendAuditOnce) does not apply: the same resource
// can be audited again and again, one record per attempt (admin replay).
func TestAppendAudit_RepeatedForOneResourceRecordsEveryAttempt(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.AppendAudit(ctx, mongostore.AuditEntry{
			ActorID: "admin-1", ActorRole: "admin", Action: "failed_event_replayed",
			ResourceType: "failed_event", ResourceID: "evt-1", At: time.Now().UTC(),
			After: bson.M{"consumer": "audit-archiver", "outcome": "failed"},
		}); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if n := count(t, rawClient(t, env.URI), mongostore.CollAuditLog, bson.D{{Key: "resource_id", Value: "evt-1"}}); n != 3 {
		t.Errorf("%d audit records for three attempts, want 3", n)
	}
}
