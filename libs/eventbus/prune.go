package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// defaultPruneBatch bounds one DELETE so a large backlog of old markers does not
// hold a long lock or make one huge transaction; PruneProcessed loops until none
// are left.
const defaultPruneBatch = 1000

// PruneOption tunes PruneProcessed.
type PruneOption func(*pruneConfig)

type pruneConfig struct{ batch int }

// WithPruneBatch sets how many rows one DELETE removes (default 1000). Tests use
// a small value to cross batch boundaries.
func WithPruneBatch(n int) PruneOption { return func(c *pruneConfig) { c.batch = n } }

// PruneProcessed deletes processed_events rows written more than retention ago
// and returns how many it removed. Each service calls it on its own
// processed_events table (the search_path of its connection selects the table),
// with its own retention, from a scheduled job: the table grows by one row per
// event per consumer, forever, otherwise.
//
// It never deletes a row newer than retention. The marker is what makes a
// redelivery a no-op, so retention is the window in which a duplicate is
// absorbed: it must be longer than anything that can redeliver an event, that is
// the Kafka topic retention (an offset reset replays the topic) and the longest
// time a parked event can wait before it is replayed from the retry topic. A
// duplicate that arrives after its marker was pruned is applied again.
//
// retention must be positive: zero would delete every marker.
func PruneProcessed(ctx context.Context, db *gorm.DB, retention time.Duration, opts ...PruneOption) (int64, error) {
	if retention <= 0 {
		return 0, errors.New("eventbus: PruneProcessed retention must be positive")
	}
	cfg := pruneConfig{batch: defaultPruneBatch}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.batch <= 0 {
		return 0, errors.New("eventbus: prune batch must be positive")
	}
	cutoff := time.Now().UTC().Add(-retention)
	var total int64
	for {
		// processed_events has no surrogate key: its primary key is
		// (consumer_name, event_id), so the batch is picked by physical row.
		res := db.WithContext(ctx).Exec(`
			DELETE FROM processed_events WHERE ctid IN (
				SELECT ctid FROM processed_events WHERE processed_at < ? LIMIT ?
			)`, cutoff, cfg.batch)
		if res.Error != nil {
			return total, fmt.Errorf("prune processed_events: %w", res.Error)
		}
		total += res.RowsAffected
		if res.RowsAffected < int64(cfg.batch) {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}
