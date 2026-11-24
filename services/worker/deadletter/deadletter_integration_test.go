//go:build integration

// Dead-letter parking and replay against real MongoDB, Postgres and Redis
// (RFC §18.2: no mocks for the stores whose behaviour is the point). The
// replay tests use the course module's real instructor-mirror handler, so the
// effect of a replay is a row in a real table, and the "applied once" claims
// are checked on that row, not on a counter the test wrote itself.
package deadletter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

const mirrorConsumer = "course-instructor-mirror"

type fixture struct {
	t     *testing.T
	pg    *testenv.Env
	mongo *testenv.MongoEnv
	store *mongostore.Store
	sink  *deadletter.Sink
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, pg: testenv.Setup(t), mongo: testenv.SetupMongo(t)}
	s, err := mongostore.Open(context.Background(), mongostore.Config{
		URI: f.mongo.URI, ServerSelectionTimeout: time.Second, OpTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("open mongostore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	f.store = s
	f.sink = deadletter.NewSink(s, deadletter.WithParkTimeout(3*time.Second))
	return f
}

func (f *fixture) replayer(handlers ...relay.Handler) *deadletter.Replayer {
	return deadletter.NewReplayer(f.pg.DB, f.store, handlers)
}

// profileEvent is an identity.user_profile_updated event, the kind the real
// course-instructor-mirror handler consumes.
func profileEvent(userID uuid.UUID, name string, outboxID int64) relay.DecodedEvent {
	payload, _ := json.Marshal(map[string]any{"user_id": userID, "full_name": name})
	return relay.DecodedEvent{
		OutboxID: outboxID, EventID: uuid.New(), AggregateType: "user", AggregateID: userID,
		EventType: "identity.user_profile_updated", SchemaVersion: 1, Payload: payload,
		OccurredAt: time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond),
	}
}

func (f *fixture) park(evt relay.DecodedEvent, consumer string, cause string) {
	f.t.Helper()
	if err := f.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: consumer, Err: errors.New(cause), Attempts: 5}); err != nil {
		f.t.Fatalf("park: %v", err)
	}
}

func (f *fixture) mirrorName(userID uuid.UUID) string {
	f.t.Helper()
	var name string
	f.pg.DB.Raw(`SELECT COALESCE(max(full_name), '') FROM instructor_mirror WHERE user_id = ?`, userID).Row().Scan(&name)
	return name
}

func (f *fixture) markers(consumer string, eventID uuid.UUID) int {
	f.t.Helper()
	var n int
	f.pg.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?`, consumer, eventID).Row().Scan(&n)
	return n
}

func (f *fixture) record(eventID uuid.UUID, consumer string) *mongostore.FailedEvent {
	f.t.Helper()
	rec, err := f.store.GetFailedEvent(context.Background(), eventID.String(), consumer)
	if err != nil {
		f.t.Fatalf("get failed event: %v", err)
	}
	return rec
}

func TestSink_ParksWithHistory_IdempotentPerEventAndConsumer(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 7)
	f.park(evt, mirrorConsumer, "first failure")
	f.park(evt, mirrorConsumer, "second failure")
	f.park(evt, "other-consumer", "other failure")

	page, err := f.store.ListFailedEvents(context.Background(), mongostore.FailedFilter{EventID: evt.EventID.String()}, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("records for the event = %d, want 2 (one per consumer, not per Park call)", len(page.Items))
	}
	rec := f.record(evt.EventID, mirrorConsumer)
	if len(rec.Errors) != 2 || rec.Errors[0].Message != "first failure" || rec.Errors[1].Message != "second failure" {
		t.Errorf("error history = %+v, want both failures in order", rec.Errors)
	}
	if rec.Attempts != 10 || rec.Status != mongostore.StatusParked {
		t.Errorf("attempts %d status %s, want 10 and parked", rec.Attempts, rec.Status)
	}
	if rec.OutboxID != 7 || rec.EventType != evt.EventType || rec.SchemaVersion != 1 || !rec.OccurredAt.Equal(evt.OccurredAt) {
		t.Errorf("replay metadata lost: %+v", rec)
	}
	if !json.Valid(rec.Payload) || !strings.Contains(string(rec.Payload), "full_name") {
		t.Errorf("payload not stored: %q", rec.Payload)
	}
}

func TestSink_ConcurrentParksYieldOneRecord(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: mirrorConsumer, Err: fmt.Errorf("boom %d", i), Attempts: 1}); err != nil {
				t.Errorf("park: %v", err)
			}
		}()
	}
	wg.Wait()
	page, _ := f.store.ListFailedEvents(context.Background(), mongostore.FailedFilter{EventID: evt.EventID.String()}, "", 50)
	if len(page.Items) != 1 {
		t.Fatalf("records = %d, want 1", len(page.Items))
	}
	if rec := f.record(evt.EventID, mirrorConsumer); len(rec.Errors) != 12 || rec.Attempts != 12 {
		t.Errorf("history %d attempts %d, want 12 and 12", len(rec.Errors), rec.Attempts)
	}
}

func TestSink_RefusesBadInputWithoutParking(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	if err := f.sink.Park(context.Background(), deadletter.Parked{Event: evt, Err: errors.New("x")}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Errorf("no consumer: want ErrInvalid, got %v", err)
	}
	evt.Payload = make([]byte, mongostore.MaxPayloadBytes+1)
	if err := f.sink.Park(context.Background(), deadletter.Parked{Event: evt, Consumer: "c", Err: errors.New("x")}); !errors.Is(err, mongostore.ErrPayloadTooLarge) {
		t.Errorf("oversized payload: want ErrPayloadTooLarge, got %v", err)
	}
	var nilSink *deadletter.Sink
	if err := nilSink.Park(context.Background(), deadletter.Parked{Consumer: "c"}); !errors.Is(err, deadletter.ErrNoStore) {
		t.Errorf("nil sink: want ErrNoStore, got %v", err)
	}
}

// With MongoDB gone, Park returns an error within its bound: the caller keeps
// the event; the stream is never held up.
func TestSink_MongoDown_FailsFastWithError(t *testing.T) {
	f := newFixture(t)
	f.mongo.Stop(t)
	start := time.Now()
	err := f.sink.Park(context.Background(), deadletter.Parked{Event: profileEvent(uuid.New(), "N", 1), Consumer: "c", Err: errors.New("x")})
	if err == nil {
		t.Fatal("Park succeeded with MongoDB stopped")
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Errorf("Park took %s with MongoDB down, want it bounded by its timeout", d)
	}
}

func TestReplay_ReappliesOnce_NoOpTheSecondTime(t *testing.T) {
	f := newFixture(t)
	user := uuid.New()
	f.pg.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'Old Name', 0)`, user)
	evt := profileEvent(user, "Replayed Name", 5)
	f.park(evt, mirrorConsumer, "database was down")
	rp := f.replayer(course.Handlers()...)

	res, err := rp.Replay(context.Background(), evt.EventID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Outcomes) != 1 || res.Outcomes[0].Status != deadletter.StatusReplayed || !res.Outcomes[0].Applied || !res.Replayed() {
		t.Fatalf("first replay = %+v, want replayed and applied", res)
	}
	if got := f.mirrorName(user); got != "Replayed Name" {
		t.Fatalf("mirror name = %q, want the replayed event applied", got)
	}
	if f.markers(mirrorConsumer, evt.EventID) != 1 {
		t.Error("the processed_events marker was not written with the effect")
	}
	rec := f.record(evt.EventID, mirrorConsumer)
	if rec.Status != mongostore.StatusReplayed || rec.ReplayedAt == nil {
		t.Errorf("record after replay: %+v", rec)
	}

	// Someone edits the row. A second replay must not re-apply the event.
	f.pg.DB.Exec(`UPDATE instructor_mirror SET full_name = 'Edited Since', last_event_id = 0 WHERE user_id = ?`, user)
	res, err = rp.Replay(context.Background(), evt.EventID.String())
	if err != nil {
		t.Fatal(err)
	}
	if o := res.Outcomes[0]; o.Status != deadletter.StatusAlreadyReplayed || o.Applied || res.Replayed() {
		t.Errorf("second replay = %+v, want already_replayed and not applied", res)
	}
	if got := f.mirrorName(user); got != "Edited Since" {
		t.Errorf("second replay re-applied the event: mirror name = %q", got)
	}
}

func TestReplay_ConcurrentDoubleReplayAppliesOnce(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 3)
	f.park(evt, "counting", "boom")
	var applied atomic.Int32
	h := relay.NewHandler("counting", []string{evt.EventType}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		applied.Add(1)
		time.Sleep(150 * time.Millisecond) // widen the race window
		return nil
	})
	rp := f.replayer(h)

	const callers = 8
	results := make(chan *deadletter.Result, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rp.Replay(context.Background(), evt.EventID.String())
			if err != nil {
				t.Errorf("replay: %v", err)
				return
			}
			results <- res
		}()
	}
	wg.Wait()
	close(results)
	transitions := 0
	for res := range results {
		if res.Replayed() {
			transitions++
		}
		if res.Failed() {
			t.Errorf("a concurrent replay failed: %+v", res)
		}
	}
	if n := applied.Load(); n != 1 {
		t.Errorf("handler ran %d times under %d concurrent replays, want exactly 1", n, callers)
	}
	if transitions != 1 {
		t.Errorf("%d replays reported the transition, want exactly 1", transitions)
	}
	if f.markers("counting", evt.EventID) != 1 {
		t.Error("want exactly one processed_events marker")
	}
}

func TestReplay_UnknownAndMalformedIDs(t *testing.T) {
	f := newFixture(t)
	rp := f.replayer(course.Handlers()...)
	if _, err := rp.Replay(context.Background(), uuid.NewString()); !errors.Is(err, deadletter.ErrNotFound) {
		t.Errorf("unknown id: want ErrNotFound, got %v", err)
	}
	if _, err := rp.Replay(context.Background(), "not-a-uuid"); !errors.Is(err, deadletter.ErrInvalidID) {
		t.Errorf("malformed id: want ErrInvalidID, got %v", err)
	}
}

func TestReplay_HandlerFailsAgain_StaysParkedWithNewError(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 9)
	f.park(evt, "flaky", "original failure")
	var failNow atomic.Bool
	failNow.Store(true)
	h := relay.NewHandler("flaky", []string{evt.EventType}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		if failNow.Load() {
			return errors.New("still broken")
		}
		return nil
	})
	rp := f.replayer(h)

	res, err := rp.Replay(context.Background(), evt.EventID.String())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Failed() || res.Outcomes[0].Applied || !strings.Contains(res.Outcomes[0].Error, "still broken") {
		t.Fatalf("replay with a failing handler = %+v", res)
	}
	rec := f.record(evt.EventID, "flaky")
	if rec.Status != mongostore.StatusParked || len(rec.Errors) != 2 || !strings.Contains(rec.Errors[1].Message, "still broken") {
		t.Errorf("record = status %s, errors %+v; want parked with the new error appended", rec.Status, rec.Errors)
	}
	if f.markers("flaky", evt.EventID) != 0 {
		t.Error("a failed replay left a processed_events marker")
	}

	failNow.Store(false)
	res, err = rp.Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Replayed() {
		t.Fatalf("replay after the fix = %+v, %v", res, err)
	}
}

func TestReplay_HandlerPanicIsAFailedReplay(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	f.park(evt, "panicky", "x")
	rp := f.replayer(relay.NewHandler("panicky", []string{evt.EventType}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		panic("kaboom")
	}))
	res, err := rp.Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Failed() || !strings.Contains(res.Outcomes[0].Error, "kaboom") {
		t.Fatalf("replay with a panicking handler = %+v, %v", res, err)
	}
	if f.record(evt.EventID, "panicky").Status != mongostore.StatusParked {
		t.Error("record should stay parked")
	}
}

func TestReplay_AlreadyProcessedByTheNormalPath_OnlyMarksTheRecord(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	f.park(evt, "counting", "x")
	f.pg.DB.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)`, "counting", evt.EventID)
	var applied atomic.Int32
	rp := f.replayer(relay.NewHandler("counting", []string{evt.EventType}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		applied.Add(1)
		return nil
	}))
	res, err := rp.Replay(context.Background(), evt.EventID.String())
	if err != nil {
		t.Fatal(err)
	}
	if o := res.Outcomes[0]; o.Status != deadletter.StatusReplayed || o.Applied || applied.Load() != 0 {
		t.Errorf("outcome %+v, handler runs %d; want marked replayed without running the handler", o, applied.Load())
	}
}

func TestReplay_NoHandlerForConsumer_FailsCleanlyAndKeepsRecord(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	f.park(evt, "ghost-consumer", "x")
	res, err := f.replayer(course.Handlers()...).Replay(context.Background(), evt.EventID.String())
	if err != nil || !res.Failed() || !strings.Contains(res.Outcomes[0].Error, "no handler") {
		t.Fatalf("replay without a handler = %+v, %v", res, err)
	}
	if rec := f.record(evt.EventID, "ghost-consumer"); rec.Status != mongostore.StatusParked || len(rec.Errors) != 1 {
		t.Errorf("record changed: %+v", rec)
	}
}

func TestReplay_EventParkedByTwoConsumers_ReplaysEach(t *testing.T) {
	f := newFixture(t)
	evt := profileEvent(uuid.New(), "N", 1)
	f.park(evt, "a", "x")
	f.park(evt, "b", "x")
	ok := func(name string) relay.Handler {
		return relay.NewHandler(name, []string{evt.EventType}, func(context.Context, *gorm.DB, relay.DecodedEvent) error { return nil })
	}
	res, err := f.replayer(ok("a"), ok("b")).Replay(context.Background(), evt.EventID.String())
	if err != nil || len(res.Outcomes) != 2 || res.Failed() {
		t.Fatalf("replay = %+v, %v", res, err)
	}
	for _, c := range []string{"a", "b"} {
		if f.record(evt.EventID, c).Status != mongostore.StatusReplayed {
			t.Errorf("consumer %s not replayed", c)
		}
	}
}

func TestReplay_MongoDown_ReturnsErrorNotPanic(t *testing.T) {
	f := newFixture(t)
	f.mongo.Stop(t)
	if _, err := f.replayer(course.Handlers()...).Replay(context.Background(), uuid.NewString()); err == nil || errors.Is(err, deadletter.ErrNotFound) {
		t.Fatalf("Replay with MongoDB down: want a store error, got %v", err)
	}
}
