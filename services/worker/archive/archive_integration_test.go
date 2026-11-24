//go:build integration

// The audit-archiver against real MongoDB, driven by the real relay over a
// real outbox (RFC §18.2). What is checked: every event type in the catalogue
// reaches the archive with enough to interpret it later (RFC §6.8); the audit
// log gets the sensitive types with their actor; redelivery, including after
// a "restart" (a new store connection and relay over events delivered before),
// leaves exactly one document per event; and with MongoDB down the archiver
// fails by returning errors while the other handlers on the same event still
// do their work.
package archive_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/archive"
)

func openStore(t *testing.T, uri string) *mongostore.Store {
	t.Helper()
	s, err := mongostore.Open(context.Background(), mongostore.Config{URI: uri, ServerSelectionTimeout: time.Second, OpTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("open mongostore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func rawDB(t *testing.T, uri string) *mongo.Database {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c.Database(mongostore.DefaultDatabase)
}

func count(t *testing.T, d *mongo.Database, coll string, filter any) int64 {
	t.Helper()
	n, err := d.Collection(coll).CountDocuments(context.Background(), filter)
	if err != nil {
		t.Fatalf("count %s: %v", coll, err)
	}
	return n
}

// writeOneOfEach writes one outbox row per catalogue type. The role-changed
// event carries an actor, the way identity writes it for an admin's change.
func writeOneOfEach(t *testing.T, env *testenv.Env, actor uuid.UUID) map[string]uuid.UUID {
	t.Helper()
	aggregates := map[string]uuid.UUID{}
	for _, ty := range events.Types() {
		agg := uuid.New()
		aggregates[ty.EventType] = agg
		evt := outbox.Event{
			AggregateType: strings.SplitN(ty.EventType, ".", 2)[0], AggregateID: agg, EventType: ty.EventType, SchemaVersion: 1,
			Payload: map[string]any{"user_id": agg, "course_id": agg, "role": "instructor", "full_name": "N"},
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7",
		}
		if ty.EventType == "identity.user_role_changed" {
			evt.ActorID = &actor
		}
		if err := env.DB.Transaction(func(tx *gorm.DB) error { return outbox.Write(context.Background(), tx, evt) }); err != nil {
			t.Fatalf("write %s: %v", ty.EventType, err)
		}
	}
	return aggregates
}

func runRelay(t *testing.T, env *testenv.Env, handlers []relay.Handler, opts ...relay.Option) (stop func()) {
	t.Helper()
	rl := relay.New(env.DB, append([]relay.Option{relay.WithWorkerCount(1), relay.WithPollInterval(30 * time.Millisecond)}, opts...)...)
	for _, h := range handlers {
		rl.Register(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); rl.Run(ctx) }()
	return func() { cancel(); wg.Wait() }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestArchiver_CoversEveryCatalogueType_WithSchemaAndAudit(t *testing.T) {
	env, mongoEnv := testenv.Setup(t), testenv.SetupMongo(t)
	store, raw := openStore(t, mongoEnv.URI), rawDB(t, mongoEnv.URI)
	actor := uuid.New()
	aggregates := writeOneOfEach(t, env, actor)
	want := int64(len(events.Types()))

	stop := runRelay(t, env, archive.Handlers(store, archive.WithSchemaResolver(func(string, int) int32 { return 4242 })))
	defer stop()
	waitFor(t, "every event archived", func() bool { return count(t, raw, mongostore.CollEvents, bson.D{}) == want })

	for _, ty := range events.Types() {
		var doc mongostore.Event
		err := raw.Collection(mongostore.CollEvents).FindOne(context.Background(), bson.D{{Key: "event_type", Value: ty.EventType}}).Decode(&doc)
		if err != nil {
			t.Fatalf("%s not archived: %v", ty.EventType, err)
		}
		if doc.AggregateID != aggregates[ty.EventType].String() || doc.SchemaVersion != 1 || doc.SchemaID != 4242 ||
			doc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || len(doc.Payload) == 0 || doc.OccurredAt.IsZero() {
			t.Errorf("%s: archived document incomplete: %+v", ty.EventType, doc)
		}
		if doc.Headers["schema_subject"] != ty.Subject() || doc.Headers["payload_encoding"] != "json" {
			t.Errorf("%s: headers %v lack the schema subject / encoding needed to interpret the payload", ty.EventType, doc.Headers)
		}
	}

	// Audit: exactly the sensitive types, with the actor on the role change.
	waitFor(t, "audit records", func() bool {
		return count(t, raw, mongostore.CollAuditLog, bson.D{}) == int64(len(archive.AuditTypes()))
	})
	var entry mongostore.AuditEntry
	if err := raw.Collection(mongostore.CollAuditLog).FindOne(context.Background(), bson.D{{Key: "action", Value: "identity.user_role_changed"}}).Decode(&entry); err != nil {
		t.Fatalf("no audit record for the role change: %v", err)
	}
	if entry.ActorID != actor.String() || entry.ResourceType != "user" || entry.ResourceID != aggregates["identity.user_role_changed"].String() || entry.After == nil {
		t.Errorf("role change audit record = %+v, want the actor, the user and the payload", entry)
	}
	if n := count(t, raw, mongostore.CollAuditLog, bson.D{{Key: "action", Value: "identity.user_registered"}}); n != 0 {
		t.Errorf("a non-sensitive event produced %d audit records", n)
	}
}

// The archive stays complete and duplicate-free across a restart: events
// delivered before are delivered again (processed_events and published_at
// cleared, as after losing the consumer's position) through a new store
// connection and a new relay.
func TestArchiver_RedeliveryAfterRestart_LeavesOneDocumentPerEvent(t *testing.T) {
	env, mongoEnv := testenv.Setup(t), testenv.SetupMongo(t)
	raw := rawDB(t, mongoEnv.URI)
	writeOneOfEach(t, env, uuid.New())
	want := int64(len(events.Types()))

	storeA := openStore(t, mongoEnv.URI)
	stop := runRelay(t, env, archive.Handlers(storeA))
	waitFor(t, "first delivery", func() bool { return count(t, raw, mongostore.CollEvents, bson.D{}) == want })
	waitFor(t, "outbox drained", func() bool {
		var n int64
		env.DB.Raw(`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Row().Scan(&n)
		return n == 0
	})
	stop()
	audits := count(t, raw, mongostore.CollAuditLog, bson.D{})
	first := map[string]time.Time{}
	cur, _ := raw.Collection(mongostore.CollEvents).Find(context.Background(), bson.D{})
	var docs []mongostore.Event
	_ = cur.All(context.Background(), &docs)
	for _, d := range docs {
		first[d.ID] = d.ArchivedAt
	}
	_ = storeA.Close(context.Background())

	// "Restart": new connection, new relay, everything delivered again.
	if err := env.DB.Exec(`DELETE FROM processed_events WHERE consumer_name = ?`, archive.Consumer).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`UPDATE outbox SET published_at = NULL, attempts = 0`).Error; err != nil {
		t.Fatal(err)
	}
	storeB := openStore(t, mongoEnv.URI)
	stop = runRelay(t, env, archive.Handlers(storeB))
	defer stop()
	waitFor(t, "second delivery", func() bool {
		var n int64
		env.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ?`, archive.Consumer).Row().Scan(&n)
		return n == want
	})

	if n := count(t, raw, mongostore.CollEvents, bson.D{}); n != want {
		t.Errorf("archive has %d documents after redelivery, want %d", n, want)
	}
	if n := count(t, raw, mongostore.CollAuditLog, bson.D{}); n != audits {
		t.Errorf("audit log has %d records after redelivery, want %d", n, audits)
	}
	cur, _ = raw.Collection(mongostore.CollEvents).Find(context.Background(), bson.D{})
	docs = nil
	_ = cur.All(context.Background(), &docs)
	for _, d := range docs {
		if !d.ArchivedAt.Equal(first[d.ID]) {
			t.Errorf("event %s was overwritten by the redelivery (first write must win)", d.ID)
		}
	}
}

// MongoDB down: the archiver returns errors (so a transport retries and may
// park), never panics, and a handler for the same event from another consumer
// still commits its effect and its marker.
func TestArchiver_MongoDown_FailsOnlyItself(t *testing.T) {
	env, mongoEnv := testenv.Setup(t), testenv.SetupMongo(t)
	store := openStore(t, mongoEnv.URI)
	mongoEnv.Stop(t)

	user := uuid.New()
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "user", AggregateID: user, EventType: "identity.user_profile_updated", SchemaVersion: 1,
			Payload: map[string]any{"user_id": user, "full_name": "Written Despite Mongo"},
		})
	}); err != nil {
		t.Fatal(err)
	}
	handlers := append(archive.Handlers(store), course.Handlers()...)
	stop := runRelay(t, env, handlers, relay.WithMaxAttempts(1000))
	defer stop()

	waitFor(t, "the course handler's effect while MongoDB is down", func() bool {
		var name string
		env.DB.Raw(`SELECT COALESCE(max(full_name), '') FROM instructor_mirror WHERE user_id = ?`, user).Row().Scan(&name)
		return name == "Written Despite Mongo"
	})
	var archiverMarkers int64
	env.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ?`, archive.Consumer).Row().Scan(&archiverMarkers)
	if archiverMarkers != 0 {
		t.Errorf("the archiver wrote %d markers although MongoDB is down", archiverMarkers)
	}
}

func TestHandler_ErrorsInsteadOfPanicking(t *testing.T) {
	evt := relay.DecodedEvent{EventID: uuid.New(), EventType: "identity.user_registered", AggregateID: uuid.New(), Payload: []byte(`{}`)}
	call := func(h relay.Handler) error { return h.Handle(context.Background(), nil, evt) }

	if err := call(archive.Handlers(nil)[0]); !errors.Is(err, archive.ErrNoStore) {
		t.Errorf("nil store: want ErrNoStore, got %v", err)
	}
	var closed *mongostore.Store // a nil *Store behind the interface reports ErrClosed
	if err := call(archive.Handlers(closed)[0]); !errors.Is(err, mongostore.ErrClosed) {
		t.Errorf("nil *Store: want ErrClosed, got %v", err)
	}
	if err := call(archive.Handlers(panicStore{})[0]); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Errorf("a panicking store must become an error, got %v", err)
	}
}

type panicStore struct{}

func (panicStore) ArchiveEvent(context.Context, mongostore.Event) (bool, error) { panic("boom") }
func (panicStore) AppendAuditOnce(context.Context, mongostore.AuditEntry) (bool, error) {
	return false, fmt.Errorf("unused")
}
