package auth

import (
	"context"
	"errors"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

// ErrArgonBusy is returned when no hashing slot freed up within the queue
// timeout. Callers map it to 503 (apperrors.ErrUnavailable).
var ErrArgonBusy = errors.New("auth: password hashing is saturated")

// ArgonGate bounds how many argon2 computations run at once (F-26 residue:
// every hash allocates MemoryKiB, so 16 concurrent logins pinned about 1 GiB).
// Peak argon2 memory is therefore at most max x MemoryKiB however many
// requests arrive. Waiters give up when their context ends or after the queue
// timeout, so a flood is refused quickly instead of piling up goroutines.
//
// A slot is taken only for the duration of one argon2 call and always
// released by defer: a cancelled waiter never holds one, and a cancelled
// request that already holds one finishes its (uninterruptible) hash and then
// frees it, which is what keeps the memory bound honest.
type ArgonGate struct {
	slots chan struct{}
	wait  time.Duration
}

// NewArgonGate builds a gate from the validated config; zero values fall back
// to the config defaults so hand-built configs (tests) keep working.
func NewArgonGate(cfg config.Argon2Config) *ArgonGate {
	n := cfg.MaxConcurrent
	if n <= 0 {
		n = config.DefaultArgon2MaxConcurrent(2, int(cfg.Parallelism))
	}
	w := cfg.QueueTimeout
	if w <= 0 {
		w = config.DefaultArgon2QueueTimeout
	}
	return &ArgonGate{slots: make(chan struct{}, n), wait: w}
}

// Acquire reserves a slot. The returned release must be called exactly once
// (extra calls are no-ops). On error no slot is held: ctx.Err() if the caller
// went away, ErrArgonBusy if the queue timeout elapsed.
func (g *ArgonGate) Acquire(ctx context.Context) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case g.slots <- struct{}{}:
		return g.releaser(), nil
	default:
	}
	t := time.NewTimer(g.wait)
	defer t.Stop()
	select {
	case g.slots <- struct{}{}:
		return g.releaser(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return nil, ErrArgonBusy
	}
}

func (g *ArgonGate) releaser() func() {
	done := false
	return func() {
		if !done {
			done = true
			<-g.slots
		}
	}
}

// InFlight reports slots currently held (for tests and metrics).
func (g *ArgonGate) InFlight() int { return len(g.slots) }

// Hash is HashPassword under the gate.
func (g *ArgonGate) Hash(ctx context.Context, cfg config.Argon2Config, password string) (string, error) {
	release, err := g.Acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return HashPassword(cfg, password)
}

// Verify is VerifyPassword under the gate. A gate error is returned as such,
// never as "wrong password".
func (g *ArgonGate) Verify(ctx context.Context, encoded, password string) (bool, error) {
	release, err := g.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return VerifyPassword(encoded, password)
}
