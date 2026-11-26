package outbox

import (
	"context"
	"testing"
	"time"
)

// A zero or negative retention would delete every published row, so it is
// refused before the database is touched (a nil db would panic otherwise).
func TestPrune_NonPositiveRetentionIsRejected(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Hour} {
		if _, err := Prune(context.Background(), nil, d); err == nil {
			t.Errorf("Prune(%v) succeeded", d)
		}
	}
}
