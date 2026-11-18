package config

import "fmt"

// RelayConfig sizes the outbox relay's worker pool (libs/relay). Only the
// worker binary runs a relay, but every binary reads the same environment, so
// the consistency check below runs in Load for all of them.
type RelayConfig struct {
	// Workers is RELAY_WORKERS (default 4, the relay's own default).
	Workers int
}

const (
	defaultRelayWorkers = 4
	maxRelayWorkers     = 64
)

// relayPoolError reports why maxOpenConns cannot support workers, or "".
//
// ADR-0027: every relay worker keeps its claim transaction (and so one
// connection) open while the handlers run, and the handlers open their own
// transactions, so N workers need up to 2N connections. With fewer, all
// connections can be held by claims while every handler waits for one: the
// handlers' contexts expire, attempts are burned and events are eventually
// given up on (ADR-0021), so this is rejected at startup (RFC §11.8,
// fail-fast) rather than warned about. 2N is the floor, not a recommendation:
// the worker's Temporal activities share the same pool.
func relayPoolError(maxOpenConns, workers int) string {
	if workers < 1 || workers > maxRelayWorkers || maxOpenConns < 1 {
		return "" // range errors are reported on their own
	}
	if maxOpenConns < 2*workers {
		return fmt.Sprintf("POSTGRES_MAX_OPEN_CONNS (%d) must be at least 2 x RELAY_WORKERS (%d = %d): each relay worker holds its claim connection while its handlers need another (ADR-0027)",
			maxOpenConns, workers, 2*workers)
	}
	return ""
}
