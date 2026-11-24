package asynqx

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/hibiken/asynq"
)

// BackoffConfig parameterises the retry delay: Base * 2^n, capped at Max, then
// reduced by up to Jitter (a fraction in [0,1)) so a burst of failures after a
// provider outage does not retry in lockstep. Jitter only ever shortens the
// delay, so Max remains a true upper bound.
type BackoffConfig struct {
	Base   time.Duration // delay before the first retry; default 2s
	Max    time.Duration // cap; default 10m
	Jitter float64       // 0 disables
}

// DefaultBackoff is 2s doubling to a 10 minute cap with 20% jitter.
var DefaultBackoff = BackoffConfig{Base: 2 * time.Second, Max: 10 * time.Minute, Jitter: 0.2}

// Validate rejects values Delay would otherwise silently correct. The zero
// value is valid (it means DefaultBackoff); Jitter must be in [0,1).
func (b BackoffConfig) Validate() error {
	switch {
	case b.Base < 0:
		return errors.New("asynqx: negative Backoff.Base")
	case b.Max < 0:
		return errors.New("asynqx: negative Backoff.Max")
	case b.Base > 0 && b.Max > 0 && b.Max < b.Base:
		return fmt.Errorf("asynqx: Backoff.Max (%v) below Backoff.Base (%v)", b.Max, b.Base)
	case b.Jitter < 0 || b.Jitter >= 1:
		return fmt.Errorf("asynqx: Backoff.Jitter %v outside [0,1)", b.Jitter)
	}
	return nil
}

// Delay returns the delay before retry n (n = 0 for the first retry). The
// loop stops at the cap, so large n never overflows.
func (b BackoffConfig) Delay(n int) time.Duration {
	if b.Base <= 0 {
		b.Base = DefaultBackoff.Base
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Max < b.Base {
		b.Max = b.Base
	}
	d := b.Base
	for i := 0; i < n && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}
	if b.Jitter > 0 {
		j := min(b.Jitter, 0.99)
		d -= time.Duration(rand.Float64() * j * float64(d))
	}
	return d
}

// Func adapts the config to Asynq's RetryDelayFunc.
func (b BackoffConfig) Func() RetryDelayFunc {
	return func(n int, _ error, _ *asynq.Task) time.Duration { return b.Delay(n) }
}
