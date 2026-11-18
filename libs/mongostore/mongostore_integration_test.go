//go:build integration

// What this file checks, against a real MongoDB (RFC §18.2: no mocks for the
// store whose behaviour is the point): that archiving is idempotent, that 20
// goroutines parking the same event yield ONE document whose history has 20
// entries (not 1, not 20 documents), that MarkReplayed transitions exactly
// once under contention, that keyset pagination neither skips nor repeats
// while documents change underneath it, that large payloads round-trip, and
// that an unavailable MongoDB produces prompt errors rather than hangs or
// panics. Run with `go test -tags=integration ./...`.
package mongostore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

const goroutines = 20

func openStore(t *testing.T, uri string) *mongostore.Store {
	t.Helper()
	s, err := mongostore.Open(context.Background(), mongostore.Config{URI: uri})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// rawClient bypasses the store so tests can inspect what is really stored.
func rawClient(t *testing.T, uri string) *mongo.Database {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("raw connect: %v", err)
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

// runConcurrently releases n goroutines at the same instant.
func runConcurrently(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn(i)
		}()
	}
	close(start)
	wg.Wait()
}

func TestArchiveEventIsIdempotent(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()
	occurred := time.Now().UTC().Truncate(time.Millisecond)

	e := mongostore.Event{
		ID: "evt-1", EventType: "StudentEnrolled", SchemaID: 42, SchemaVersion: 3,
		AggregateType: "enrollment", AggregateID: "agg-1", OccurredAt: occurred,
		TraceID: "trace-1", Payload: []byte{1, 2, 3}, Headers: map[string]string{"k": "v"},
	}
	inserted, err := s.ArchiveEvent(ctx, e)
	if err != nil || !inserted {
		t.Fatalf("first archive: inserted=%v err=%v", inserted, err)
	}
	e2 := e
	e2.Payload = []byte{9, 9, 9}
	inserted, err = s.ArchiveEvent(ctx, e2)
	if err != nil || inserted {
		t.Fatalf("second archive: inserted=%v err=%v, want false,nil", inserted, err)
	}

	got, err := s.GetEvent(ctx, "evt-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SchemaID != 42 || got.SchemaVersion != 3 || !bytes.Equal(got.Payload, []byte{1, 2, 3}) || !got.OccurredAt.Equal(occurred) || got.Headers["k"] != "v" {
		t.Fatalf("first write must win and keep the schema id, got %+v", got)
	}
	if _, err := s.GetEvent(ctx, "missing"); !errors.Is(err, mongostore.ErrNotFound) {
		t.Fatalf("missing event: err=%v, want ErrNotFound", err)
	}

	// Concurrent archive of a new id: exactly one caller inserts.
	var inserts atomic.Int32
	runConcurrently(goroutines, func(int) {
		ins, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "evt-2", EventType: "X", Payload: []byte("p")})
		if err != nil {
			t.Errorf("concurrent archive: %v", err)
		}
		if ins {
			inserts.Add(1)
		}
	})
	if inserts.Load() != 1 {
		t.Fatalf("concurrent archive inserted %d times, want 1", inserts.Load())
	}
	if n := count(t, rawClient(t, env.URI), mongostore.CollEvents, bson.D{}); n != 2 {
		t.Fatalf("events collection holds %d documents, want 2", n)
	}
}

func TestOpenCreatesIndexesIdempotentlyWithoutTTL(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	openStore(t, env.URI) // second Open over existing indexes must succeed

	d := rawClient(t, env.URI)
	for _, coll := range []string{mongostore.CollEvents, mongostore.CollAuditLog, mongostore.CollFailedEvents} {
		cur, err := d.Collection(coll).Indexes().List(context.Background())
		if err != nil {
			t.Fatalf("list indexes %s: %v", coll, err)
		}
		var specs []bson.M
		if err := cur.All(context.Background(), &specs); err != nil {
			t.Fatalf("read indexes %s: %v", coll, err)
		}
		uniqueCompound := false
		for _, sp := range specs {
			if _, ttl := sp["expireAfterSeconds"]; ttl {
				t.Errorf("%s has a TTL index %v; the archive is permanent", coll, sp["name"])
			}
			if u, _ := sp["unique"].(bool); u && sp["name"] == "event_id_1_consumer_1" {
				uniqueCompound = true
			}
		}
		if coll == mongostore.CollFailedEvents && !uniqueCompound {
			t.Errorf("failed_events lacks the unique (event_id, consumer) index: %v", specs)
		}
	}
}

func TestAppendAudit(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	id, err := s.AppendAudit(ctx, mongostore.AuditEntry{
		ActorID: "u1", ActorRole: "admin", Action: "role.change", ResourceType: "user", ResourceID: "u2",
		Before: bson.M{"role": "student"}, After: bson.M{"role": "instructor"}, IP: "10.0.0.1", TraceID: "t",
	})
	if err != nil || id == "" {
		t.Fatalf("append audit: id=%q err=%v", id, err)
	}
	var doc bson.M
	oid, _ := bson.ObjectIDFromHex(id)
	if err := rawClient(t, env.URI).Collection(mongostore.CollAuditLog).FindOne(ctx, bson.D{{Key: "_id", Value: oid}}).Decode(&doc); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if doc["actor_id"] != "u1" || doc["action"] != "role.change" {
		t.Fatalf("unexpected audit doc %v", doc)
	}
	if _, err := s.AppendAudit(ctx, mongostore.AuditEntry{Action: "x"}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Fatalf("invalid audit: err=%v, want ErrInvalid", err)
	}
}

func park(s *mongostore.Store, eventID, consumer string, n int) (*mongostore.FailedEvent, error) {
	return s.ParkFailedEvent(context.Background(), mongostore.ParkInput{
		EventID: eventID, Consumer: consumer, EventType: "LessonCompleted", Topic: "course.events",
		AggregateType: "course", AggregateID: "c1", Payload: []byte("payload"),
		Attempts: 3, Err: fmt.Sprintf("boom %d", n),
	})
}

func TestParkFailedEventConcurrentSameEventYieldsOneDocument(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	baseline := goleak.IgnoreCurrent()

	runConcurrently(goroutines, func(i int) {
		if _, err := park(s, "evt-1", "analytics", i); err != nil {
			t.Errorf("park %d: %v", i, err)
		}
	})
	// A different consumer of the same event is a separate document.
	if _, err := park(s, "evt-1", "notifications", 0); err != nil {
		t.Fatalf("park other consumer: %v", err)
	}

	// Counted through the store, not a second client, so the leak check below
	// sees only this store's goroutines.
	one, err := s.ListFailedEvents(context.Background(), mongostore.FailedFilter{EventID: "evt-1", Consumer: "analytics"}, "", 10)
	if err != nil || len(one.Items) != 1 {
		t.Fatalf("documents for (evt-1, analytics) = %v, err %v; want exactly 1", one, err)
	}
	got, err := s.GetFailedEvent(context.Background(), "evt-1", "analytics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Errors) != goroutines {
		t.Fatalf("error history has %d entries, want %d", len(got.Errors), goroutines)
	}
	if got.Attempts != goroutines*3 {
		t.Fatalf("attempts=%d, want %d", got.Attempts, goroutines*3)
	}
	if got.Status != mongostore.StatusParked || string(got.Payload) != "payload" || got.FirstFailedAt.After(got.LastFailedAt) {
		t.Fatalf("unexpected document %+v", got)
	}
	all, err := s.ListFailedEvents(context.Background(), mongostore.FailedFilter{}, "", 10)
	if err != nil || len(all.Items) != 2 {
		t.Fatalf("failed_events holds %v, err %v; want 2 documents", all, err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := goleak.Find(baseline); err != nil {
		t.Fatalf("goroutines leaked after Close: %v", err)
	}
}

func TestParkFailedEventCapsHistoryButKeepsAttempts(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	for i := range mongostore.MaxErrorHistory + 5 {
		if _, err := park(s, "evt-1", "c", i); err != nil {
			t.Fatalf("park: %v", err)
		}
	}
	got, err := s.GetFailedEvent(context.Background(), "evt-1", "c")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Errors) != mongostore.MaxErrorHistory || got.Attempts != (mongostore.MaxErrorHistory+5)*3 {
		t.Fatalf("history=%d attempts=%d", len(got.Errors), got.Attempts)
	}
	if last := got.Errors[len(got.Errors)-1].Message; last != fmt.Sprintf("boom %d", mongostore.MaxErrorHistory+4) {
		t.Fatalf("newest entry not kept, last=%q", last)
	}
}

func TestMarkReplayedTransitionsExactlyOnceUnderConcurrency(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	if _, err := park(s, "evt-1", "analytics", 0); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	runConcurrently(goroutines, func(int) {
		ok, err := s.MarkReplayed(ctx, "evt-1", "analytics", time.Now())
		if err != nil {
			t.Errorf("mark replayed: %v", err)
		}
		if ok {
			winners.Add(1)
		}
	})
	if winners.Load() != 1 {
		t.Fatalf("%d callers saw the parked->replayed transition, want exactly 1", winners.Load())
	}
	got, _ := s.GetFailedEvent(ctx, "evt-1", "analytics")
	if got.Status != mongostore.StatusReplayed || got.ReplayedAt == nil {
		t.Fatalf("not replayed: %+v", got)
	}

	if ok, err := s.MarkReplayed(ctx, "evt-1", "analytics", time.Now()); ok || err != nil {
		t.Fatalf("already replayed: ok=%v err=%v, want false,nil", ok, err)
	}
	if _, err := s.MarkReplayed(ctx, "nope", "analytics", time.Now()); !errors.Is(err, mongostore.ErrNotFound) {
		t.Fatalf("unknown event: err=%v, want ErrNotFound", err)
	}

	// Failing again after a replay re-parks it, and it can be replayed again.
	again, err := park(s, "evt-1", "analytics", 1)
	if err != nil || again.Status != mongostore.StatusParked || again.ReplayedAt != nil {
		t.Fatalf("re-park: %+v err=%v", again, err)
	}
	if ok, _ := s.MarkReplayed(ctx, "evt-1", "analytics", time.Now()); !ok {
		t.Fatalf("a re-parked event must be replayable again")
	}
}

func TestListFailedEventsPaginationIsStableUnderChange(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	const total = 25
	want := make([]string, 0, total) // newest first
	for i := range total {
		if _, err := park(s, fmt.Sprintf("evt-%02d", i), "analytics", 0); err != nil {
			t.Fatal(err)
		}
		want = append([]string{fmt.Sprintf("evt-%02d", i)}, want...)
	}
	if _, err := park(s, "other", "notifications", 0); err != nil {
		t.Fatal(err)
	}

	var got []string
	cursor := ""
	for page := 0; ; page++ {
		res, err := s.ListFailedEvents(ctx, mongostore.FailedFilter{Consumer: "analytics"}, cursor, 7)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, f := range res.Items {
			if len(f.Payload) != 0 {
				t.Fatalf("list must not return payloads")
			}
			got = append(got, f.EventID)
		}
		// Churn between pages: re-park the oldest (bumps last_failed_at),
		// replay another, add a brand-new event. None may disturb the walk.
		_, _ = park(s, "evt-00", "analytics", 9)
		_, _ = s.MarkReplayed(ctx, "evt-05", "analytics", time.Now())
		_, _ = park(s, fmt.Sprintf("late-%d", page), "analytics", 0)
		if res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
	}
	// The walk must contain every original event exactly once, in order;
	// events added mid-walk are newer than the cursor and are not visited.
	if len(got) != total {
		t.Fatalf("walk returned %d items, want %d: %v", len(got), total, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %s want %s", i, got[i], want[i])
		}
	}

	if _, err := s.ListFailedEvents(ctx, mongostore.FailedFilter{}, "not-a-cursor", 5); !errors.Is(err, mongostore.ErrInvalid) {
		t.Fatalf("bad cursor: err=%v, want ErrInvalid", err)
	}
	parked, err := s.ListFailedEvents(ctx, mongostore.FailedFilter{Status: mongostore.StatusReplayed}, "", 100)
	if err != nil || len(parked.Items) != 1 || parked.Items[0].EventID != "evt-05" {
		t.Fatalf("status filter: %+v err=%v", parked, err)
	}
}

func TestCountFailedSince(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()
	now := time.Now().UTC()

	in := func(id string, at time.Time) mongostore.ParkInput {
		return mongostore.ParkInput{EventID: id, Consumer: "c", EventType: "T", Err: "e", FailedAt: at}
	}
	for _, p := range []mongostore.ParkInput{in("old", now.Add(-48*time.Hour)), in("recent1", now.Add(-time.Hour)), in("recent2", now.Add(-time.Minute))} {
		if _, err := s.ParkFailedEvent(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.MarkReplayed(ctx, "recent2", "c", now); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		since  time.Time
		status mongostore.Status
		want   int64
	}{
		{now.Add(-24 * time.Hour), "", 2},
		{now.Add(-24 * time.Hour), mongostore.StatusParked, 1},
		{now.Add(-72 * time.Hour), "", 3},
		{now.Add(time.Hour), "", 0},
	} {
		n, err := s.CountFailedSince(ctx, tc.since, tc.status)
		if err != nil || n != tc.want {
			t.Errorf("CountFailedSince(%v, %q) = %d, %v; want %d", tc.since, tc.status, n, err, tc.want)
		}
	}
}

func TestLargePayloads(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	big := bytes.Repeat([]byte{0xAB, 0xCD, 0x01}, 3<<20) // ~9 MiB
	if ok, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "big", EventType: "T", Payload: big}); err != nil || !ok {
		t.Fatalf("archive big: ok=%v err=%v", ok, err)
	}
	ev, err := s.GetEvent(ctx, "big")
	if err != nil || !bytes.Equal(ev.Payload, big) {
		t.Fatalf("big event round trip failed: err=%v", err)
	}
	if _, err := s.ParkFailedEvent(ctx, mongostore.ParkInput{EventID: "big", Consumer: "c", EventType: "T", Payload: big, Err: "e"}); err != nil {
		t.Fatalf("park big: %v", err)
	}
	f, err := s.GetFailedEvent(ctx, "big", "c")
	if err != nil || !bytes.Equal(f.Payload, big) {
		t.Fatalf("big failed event round trip failed: err=%v", err)
	}
	// Re-parking returns without shipping the payload back.
	again, err := s.ParkFailedEvent(ctx, mongostore.ParkInput{EventID: "big", Consumer: "c", Err: "e2"})
	if err != nil || len(again.Payload) != 0 || len(again.Errors) != 2 {
		t.Fatalf("re-park big: %+v err=%v", again, err)
	}

	tooBig := make([]byte, mongostore.MaxPayloadBytes+1)
	if _, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "huge", EventType: "T", Payload: tooBig}); !errors.Is(err, mongostore.ErrPayloadTooLarge) {
		t.Fatalf("archive huge: err=%v, want ErrPayloadTooLarge", err)
	}
	if _, err := s.ParkFailedEvent(ctx, mongostore.ParkInput{EventID: "huge", Consumer: "c", Payload: tooBig}); !errors.Is(err, mongostore.ErrPayloadTooLarge) {
		t.Fatalf("park huge: err=%v, want ErrPayloadTooLarge", err)
	}
}

func TestInvalidInputAndClosedStoreNeverPanic(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	if _, err := s.ArchiveEvent(ctx, mongostore.Event{}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Errorf("empty event: %v", err)
	}
	if _, err := s.ParkFailedEvent(ctx, mongostore.ParkInput{}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Errorf("empty park: %v", err)
	}
	if _, err := mongostore.Open(ctx, mongostore.Config{}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Errorf("empty config: %v", err)
	}

	if err := s.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "a", EventType: "T"}); !errors.Is(err, mongostore.ErrClosed) {
		t.Errorf("closed store: %v", err)
	}
	var nilStore *mongostore.Store
	if err := nilStore.Ping(ctx); !errors.Is(err, mongostore.ErrClosed) {
		t.Errorf("nil store: %v", err)
	}
}

func TestMongoUnavailableFailsPromptlyAndNeverPanics(t *testing.T) {
	env := testenv.SetupMongo(t)
	s, err := mongostore.Open(context.Background(), mongostore.Config{
		URI: env.URI, ServerSelectionTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("ping while up: %v", err)
	}

	env.Stop(t)

	const bound = 4 * time.Second
	ops := map[string]func(ctx context.Context) error{
		"ping": s.Ping,
		"archive": func(ctx context.Context) error {
			_, err := s.ArchiveEvent(ctx, mongostore.Event{ID: "a", EventType: "T"})
			return err
		},
		"audit": func(ctx context.Context) error {
			_, err := s.AppendAudit(ctx, mongostore.AuditEntry{ActorID: "a", Action: "x", ResourceType: "r"})
			return err
		},
		"park": func(ctx context.Context) error { _, err := park(s, "e", "c", 0); return err },
		"list": func(ctx context.Context) error {
			_, err := s.ListFailedEvents(ctx, mongostore.FailedFilter{}, "", 10)
			return err
		},
		"get":     func(ctx context.Context) error { _, err := s.GetFailedEvent(ctx, "e", "c"); return err },
		"replay":  func(ctx context.Context) error { _, err := s.MarkReplayed(ctx, "e", "c", time.Now()); return err },
		"count":   func(ctx context.Context) error { _, err := s.CountFailedSince(ctx, time.Now(), ""); return err },
		"getevnt": func(ctx context.Context) error { _, err := s.GetEvent(ctx, "e"); return err },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			// Caller deadline shorter than the store's own timeouts must win.
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			begin := time.Now()
			err := op(ctx)
			if err == nil {
				t.Fatalf("%s succeeded with MongoDB stopped", name)
			}
			if d := time.Since(begin); d > bound {
				t.Fatalf("%s took %v, want under %v", name, d, bound)
			}
			// And with no caller deadline, the store's own bound applies.
			begin = time.Now()
			if err := op(context.Background()); err == nil {
				t.Fatalf("%s succeeded with MongoDB stopped (no deadline)", name)
			}
			if d := time.Since(begin); d > bound {
				t.Fatalf("%s without caller deadline took %v, want under %v", name, d, bound)
			}
		})
	}

	// Closing with MongoDB gone must not hang past the caller's deadline.
	cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	begin := time.Now()
	_ = s.Close(cctx)
	if d := time.Since(begin); d > 3*time.Second {
		t.Fatalf("Close took %v with MongoDB down", d)
	}
}

func TestOpenUnreachableReturnsPromptlyWithoutLeaks(t *testing.T) {
	defer goleak.VerifyNone(t)

	begin := time.Now()
	_, err := mongostore.Open(context.Background(), mongostore.Config{
		URI: "mongodb://127.0.0.1:1", ServerSelectionTimeout: 300 * time.Millisecond, ConnectTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("open succeeded against an unreachable server")
	}
	if d := time.Since(begin); d > 3*time.Second {
		t.Fatalf("open took %v, want prompt failure", d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := mongostore.Open(ctx, mongostore.Config{URI: "mongodb://127.0.0.1:1"}); err == nil {
		t.Fatal("open succeeded with an expired context")
	}
}
