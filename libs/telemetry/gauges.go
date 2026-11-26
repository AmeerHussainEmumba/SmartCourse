package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// GaugeCollector refreshes gauge-style metrics from a query (an outbox count,
// a broker admin call, an Asynq inspector). Collect is called serially by a
// Poller; it returns an error to leave the previous values in place.
type GaugeCollector interface {
	Collect(ctx context.Context) error
}

// GaugeCollectorFunc adapts a function to GaugeCollector.
type GaugeCollectorFunc func(ctx context.Context) error

// Collect calls f.
func (f GaugeCollectorFunc) Collect(ctx context.Context) error { return f(ctx) }

// Poll runs collectors immediately and then every interval until ctx is
// cancelled, giving each pass a timeout of one interval. A failing collector
// is logged and leaves its gauges untouched (stale, not zero: a broken query
// must not look like a drained queue). It blocks; run it in a goroutine the
// service owns, or use StartPoller.
func Poll(ctx context.Context, interval time.Duration, collectors ...GaugeCollector) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	run := func() {
		pctx, cancel := context.WithTimeout(ctx, interval)
		defer cancel()
		for _, c := range collectors {
			if err := c.Collect(pctx); err != nil && ctx.Err() == nil {
				slog.Warn("gauge collection failed", slog.String("error", err.Error()))
			}
		}
	}
	run()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

// StartPoller runs Poll in a goroutine and returns a stop function that
// cancels it and waits for it to exit.
func StartPoller(ctx context.Context, interval time.Duration, collectors ...GaugeCollector) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Poll(ctx, interval, collectors...)
	}()
	return func() {
		cancel()
		<-done
	}
}

// OutboxStats is the result of the outbox backlog query.
type OutboxStats struct {
	// Backlog is the number of unpublished rows.
	Backlog int64
	// OldestUnpublished is the age of the oldest unpublished row; zero when
	// Backlog is zero.
	OldestUnpublished time.Duration
}

// OutboxGauges feeds outbox_backlog and outbox_oldest_unpublished_seconds from
// query, typically one SELECT count(*), now() - min(created_at) over the
// unpublished rows.
func OutboxGauges(m *Metrics, query func(ctx context.Context) (OutboxStats, error)) GaugeCollector {
	return GaugeCollectorFunc(func(ctx context.Context) error {
		s, err := query(ctx)
		if err != nil {
			return err
		}
		m.OutboxBacklog.Set(float64(s.Backlog))
		m.OutboxOldestUnpublish.Set(s.OldestUnpublished.Seconds())
		return nil
	})
}

// ConsumerLag is one consumer group's lag on one topic.
type ConsumerLag struct {
	Group, Topic string
	Lag          int64
}

// ConsumerLagGauges feeds kafka_consumer_lag from query. Series for a
// group/topic absent from the latest result are removed.
func ConsumerLagGauges(m *Metrics, query func(ctx context.Context) ([]ConsumerLag, error)) GaugeCollector {
	set := newLabelSet()
	return GaugeCollectorFunc(func(ctx context.Context) error {
		rows, err := query(ctx)
		if err != nil {
			return err
		}
		set.begin()
		for _, r := range rows {
			m.KafkaConsumerLag.WithLabelValues(r.Group, r.Topic).Set(float64(r.Lag))
			set.seen(r.Group, r.Topic)
		}
		set.end(m.KafkaConsumerLag)
		return nil
	})
}

// QueueStats is one Asynq queue's depth and retry backlog.
type QueueStats struct {
	Queue   string
	Pending int64
	Retry   int64
}

// QueueGauges feeds asynq_queue_depth and asynq_retry_count from query.
// Series for a queue absent from the latest result are removed.
func QueueGauges(m *Metrics, query func(ctx context.Context) ([]QueueStats, error)) GaugeCollector {
	depth, retry := newLabelSet(), newLabelSet()
	return GaugeCollectorFunc(func(ctx context.Context) error {
		rows, err := query(ctx)
		if err != nil {
			return err
		}
		depth.begin()
		retry.begin()
		for _, r := range rows {
			m.AsynqQueueDepth.WithLabelValues(r.Queue).Set(float64(r.Pending))
			m.AsynqRetryCount.WithLabelValues(r.Queue).Set(float64(r.Retry))
			depth.seen(r.Queue)
			retry.seen(r.Queue)
		}
		depth.end(m.AsynqQueueDepth)
		retry.end(m.AsynqRetryCount)
		return nil
	})
}

// labelSet tracks which label combinations a collector has set so combinations
// that disappear from the source can be deleted instead of left stale.
type labelSet struct {
	mu       sync.Mutex
	previous map[string][]string
	current  map[string][]string
}

func newLabelSet() *labelSet { return &labelSet{previous: map[string][]string{}} }

func (l *labelSet) begin() {
	l.mu.Lock()
	l.current = map[string][]string{}
}

func (l *labelSet) seen(values ...string) {
	l.current[joinKey(values)] = values
}

func (l *labelSet) end(vec *prometheus.GaugeVec) {
	for k, v := range l.previous {
		if _, ok := l.current[k]; !ok {
			vec.DeleteLabelValues(v...)
		}
	}
	l.previous, l.current = l.current, nil
	l.mu.Unlock()
}

func joinKey(values []string) string {
	k := ""
	for _, v := range values {
		k += v + "\x00"
	}
	return k
}
