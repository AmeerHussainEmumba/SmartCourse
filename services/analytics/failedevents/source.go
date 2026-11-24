// Package failedevents serves the Failed Events metric (FR-5, RFC §9.1) from
// the MongoDB failed_events store (libs/mongostore, ADR-0031). It implements
// analytics.FailedEventsSource for analytics-api, which is the one HTTP
// service allowed to link libs/mongostore (scripts/check-link-graph.sh).
package failedevents

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// Store is the part of *mongostore.Store the source uses.
type Store interface {
	SummarizeFailed(ctx context.Context) (*mongostore.FailedSummary, error)
}

// Source implements analytics.FailedEventsSource over a Store.
type Source struct {
	store Store
	now   func() time.Time
}

// NewSource builds a Source over store.
func NewSource(store Store) *Source {
	return &Source{store: store, now: time.Now}
}

// SummarizeFailedEvents counts parked and replayed events, the parked backlog
// by consumer and event type, and the age of the oldest parked event. Any
// store failure is reported as analytics.ErrSourceUnavailable, never as zeros.
func (s *Source) SummarizeFailedEvents(ctx context.Context) (*analytics.FailedEventsSummary, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("%w: no store", analytics.ErrSourceUnavailable)
	}
	sum, err := s.store.SummarizeFailed(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", analytics.ErrSourceUnavailable, err)
	}
	out := &analytics.FailedEventsSummary{
		Total:       sum.Total,
		Unresolved:  sum.Parked,
		Replayed:    sum.Replayed,
		ByEventType: sum.ByEventType,
		ByConsumer:  sum.ByConsumer,
	}
	if sum.OldestParkedAt != nil {
		age := max(int64(s.now().Sub(*sum.OldestParkedAt).Seconds()), 0)
		out.OldestUnresolvedAgeSeconds = &age
	}
	return out, nil
}

// LazyStore is a Store that connects to MongoDB on first use, and again after a
// failure, instead of at process start: analytics-api must boot and serve
// every other route with MongoDB absent. Attempts are spaced by RetryEvery so
// a MongoDB outage costs one connect timeout per interval, not per request.
type LazyStore struct {
	cfg mongostore.Config
	// RetryEvery is the minimum gap between connection attempts (default 5s).
	RetryEvery time.Duration

	mu      sync.Mutex
	store   *mongostore.Store
	lastTry time.Time
	lastErr error
}

// NewLazyStore returns a LazyStore for cfg. Nothing is dialled until the first call.
func NewLazyStore(cfg mongostore.Config) *LazyStore {
	return &LazyStore{cfg: cfg, RetryEvery: 5 * time.Second}
}

func (l *LazyStore) get(ctx context.Context) (*mongostore.Store, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store != nil {
		return l.store, nil
	}
	if !l.lastTry.IsZero() && time.Since(l.lastTry) < l.RetryEvery {
		return nil, l.lastErr
	}
	l.lastTry = time.Now()
	s, err := mongostore.Open(ctx, l.cfg)
	if err != nil {
		l.lastErr = err // never contains the URI
		return nil, err
	}
	l.store, l.lastErr = s, nil
	return s, nil
}

// SummarizeFailed implements Store.
func (l *LazyStore) SummarizeFailed(ctx context.Context) (*mongostore.FailedSummary, error) {
	s, err := l.get(ctx)
	if err != nil {
		return nil, err
	}
	return s.SummarizeFailed(ctx)
}

// Close disconnects if connected.
func (l *LazyStore) Close(ctx context.Context) error {
	l.mu.Lock()
	s := l.store
	l.store, l.lastErr, l.lastTry = nil, mongostore.ErrClosed, time.Now().Add(24*time.Hour)
	l.mu.Unlock()
	return s.Close(ctx) // nil-safe
}
