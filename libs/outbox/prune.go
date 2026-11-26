package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// pruneBatch bounds one DELETE so a large backlog of old rows does not hold a
// long lock or produce one huge transaction; Prune loops until none are left.
const pruneBatch = 1000

// Prune deletes outbox rows the relay finished with more than olderThan ago and
// returns how many it removed (RFC §9.5 scheduled job; each producing service's worker runs it).
//
// It never deletes a row the relay has not published (published_at IS NULL),
// whatever its age: that row is still owed to its consumers. It also keeps any
// row with a last_error. The relay marks a row it gave up on as published with
// its last_error set, and until a dead-letter store exists (relay.recordFailure)
// that row is the only record of the failure, so those stay. A row that failed
// and later succeeded does not keep a last_error: the relay clears it on
// success (B-09), so such a row is pruned like any other.
//
// olderThan must be positive: zero would delete every published row.
func Prune(ctx context.Context, db *gorm.DB, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, errors.New("outbox: Prune retention must be positive")
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	var total int64
	for {
		res := db.WithContext(ctx).Exec(`
			DELETE FROM outbox WHERE id IN (
				SELECT id FROM outbox
				WHERE published_at IS NOT NULL AND published_at < ? AND last_error IS NULL
				ORDER BY id LIMIT ?
			)`, cutoff, pruneBatch)
		if res.Error != nil {
			return total, fmt.Errorf("prune outbox: %w", res.Error)
		}
		total += res.RowsAffected
		if res.RowsAffected < pruneBatch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}
