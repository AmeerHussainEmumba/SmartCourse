package kafka

import (
	"math/rand/v2"
	"time"
)

// BackoffConfig parameterises a retry delay: Base * 2^n, capped at Max, then
// reduced by up to Jitter (a fraction in [0,1)) so a burst of failures after
// an outage does not retry in lockstep. Jitter only ever shortens the delay,
// so Max remains a true upper bound. A zero Base or Max takes the default from
// DefaultBackoff; Jitter 0 disables jitter.
type BackoffConfig struct {
	Base   time.Duration // delay before the first retry; default 200ms
	Max    time.Duration // cap; default 10s
	Jitter float64       // 0 disables
}

// DefaultBackoff is 200ms doubling to a 10s cap with 20% jitter.
var DefaultBackoff = BackoffConfig{Base: 200 * time.Millisecond, Max: 10 * time.Second, Jitter: 0.2}

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

func (b BackoffConfig) validate() error {
	switch {
	case b.Base < 0, b.Max < 0:
		return errNegative("Backoff durations")
	case b.Jitter < 0 || b.Jitter >= 1:
		return errInvalid("Backoff.Jitter must be in [0, 1)")
	}
	return nil
}
