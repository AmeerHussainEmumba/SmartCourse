package failedevents

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

type fakeStore struct {
	sum *mongostore.FailedSummary
	err error
}

func (f fakeStore) SummarizeFailed(context.Context) (*mongostore.FailedSummary, error) {
	return f.sum, f.err
}

func TestSource_MapsSummaryAndComputesAge(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-90 * time.Minute)
	s := NewSource(fakeStore{sum: &mongostore.FailedSummary{
		Total: 7, Parked: 4, Replayed: 3,
		ByConsumer: map[string]int64{"analytics": 4}, ByEventType: map[string]int64{"x": 4}, OldestParkedAt: &oldest,
	}})
	s.now = func() time.Time { return now }
	got, err := s.SummarizeFailedEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 7 || got.Unresolved != 4 || got.Replayed != 3 || got.ByConsumer["analytics"] != 4 || got.ByEventType["x"] != 4 {
		t.Errorf("summary = %+v", got)
	}
	if got.OldestUnresolvedAgeSeconds == nil || *got.OldestUnresolvedAgeSeconds != 5400 {
		t.Errorf("age = %v, want 5400", got.OldestUnresolvedAgeSeconds)
	}

	s = NewSource(fakeStore{sum: &mongostore.FailedSummary{}})
	if got, _ := s.SummarizeFailedEvents(context.Background()); got.OldestUnresolvedAgeSeconds != nil {
		t.Error("no parked events: the age must be absent, not zero")
	}
}

func TestSource_StoreErrorIsSourceUnavailable_NeverZeros(t *testing.T) {
	s := NewSource(fakeStore{err: errors.New("dial tcp 10.0.0.5:27017: refused")})
	got, err := s.SummarizeFailedEvents(context.Background())
	if got != nil || !errors.Is(err, analytics.ErrSourceUnavailable) {
		t.Fatalf("want (nil, ErrSourceUnavailable), got %+v, %v", got, err)
	}
	var nilSource *Source
	if _, err := nilSource.SummarizeFailedEvents(context.Background()); !errors.Is(err, analytics.ErrSourceUnavailable) {
		t.Errorf("nil source: got %v", err)
	}
}

// An unreachable MongoDB costs one connect attempt per RetryEvery, not one per call.
func TestLazyStore_UnreachableMongo_RetriesAreSpaced(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	lazy := NewLazyStore(mongostore.Config{URI: "mongodb://" + addr, ConnectTimeout: time.Second, ServerSelectionTimeout: 300 * time.Millisecond})
	lazy.RetryEvery = time.Hour
	defer func() { _ = lazy.Close(context.Background()) }()

	if _, err := lazy.SummarizeFailed(context.Background()); err == nil {
		t.Fatal("want an error with MongoDB unreachable")
	}
	start := time.Now()
	for range 20 {
		if _, err := lazy.SummarizeFailed(context.Background()); err == nil {
			t.Fatal("want an error")
		}
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("20 calls inside the retry gap took %s; they must fail without redialling", d)
	}
	if err := lazy.Close(context.Background()); err != nil {
		t.Errorf("close of a never-connected store: %v", err)
	}
	if _, err := lazy.SummarizeFailed(context.Background()); !errors.Is(err, mongostore.ErrClosed) {
		t.Errorf("after Close: want ErrClosed, got %v", err)
	}
}
