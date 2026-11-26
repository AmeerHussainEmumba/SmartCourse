// Package ratelimit wires ADR-0020's GCRA design in: one atomic Redis call
// per check via go-redis/redis_rate, replacing the naive INCR+EXPIRE
// fixed-window approach that allows roughly double the intended rate for
// traffic straddling a window boundary — the flaw that made this a
// dedicated ADR rather than three lines of Redis code.
//
// This is exactly the "Shared Resource Synchronization... rate limiter
// state" case SmartCourse.md names explicitly, and it's the one that must
// NOT be a sync.Mutex/RWMutex: rate-limit state is shared across every API
// replica, not one process, so it lives in Redis (RFC §10.3's specific
// warning about the most common concurrency mistake in a horizontally
// scaled Go service).
package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	goredisrate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

type Limiter struct {
	rl *goredisrate.Limiter
}

func New(rdb *redis.Client) *Limiter {
	return &Limiter{rl: goredisrate.NewLimiter(rdb)}
}

// Allow checks key against limit, consuming one unit on every call
// regardless of outcome (GCRA semantics — a rejected call still counts,
// which is what makes it resistant to retry storms). Exposed directly
// (not only via Middleware) so service-layer code can apply a per-identity
// limit that isn't knowable until the request body is parsed — e.g.
// per-account login attempts, where the account is the email in the body,
// not something route-level middleware can see.
func (l *Limiter) Allow(ctx context.Context, key string, limit goredisrate.Limit) error {
	_, err := l.allow(ctx, key, limit)
	return err
}

// allow is Allow that also reports how long until the next call would be
// admitted (zero when this one was).
func (l *Limiter) allow(ctx context.Context, key string, limit goredisrate.Limit) (time.Duration, error) {
	res, err := l.rl.Allow(ctx, key, limit)
	if err != nil {
		// Fails open (RFC §13.4: "Redis down... Rate limiting fails open,
		// logged"). A Redis outage should degrade to "no rate limiting,"
		// never to "nobody can authenticate."
		return 0, nil
	}
	if res.Allowed == 0 {
		return res.RetryAfter, apperrors.ErrRateLimited
	}
	return 0, nil
}

// Check reports whether key still has budget under limit WITHOUT consuming
// any (GCRA with cost 0). It returns ErrRateLimited once the burst is
// exhausted. Use it with Allow to build "count failures only" limits:
// Check before the guarded operation, Allow (consume) only when it fails.
// Like Allow it fails open if Redis is unavailable.
func (l *Limiter) Check(ctx context.Context, key string, limit goredisrate.Limit) error {
	res, err := l.rl.AllowN(ctx, key, limit, 0)
	if err != nil {
		return nil
	}
	if res.Remaining < 1 {
		return apperrors.ErrRateLimited
	}
	return nil
}

// Middleware applies a per-client-IP limit to every request in the group
// it's attached to. keyPrefix scopes the limiter — RFC §5.7's
// ratelimit:{scope}:{identity} keyspace — so /auth/login and
// /auth/register don't share a budget.
func (l *Limiter) Middleware(keyPrefix string, limit goredisrate.Limit) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := "ratelimit:" + keyPrefix + ":ip:" + c.ClientIP()
		if wait, err := l.allow(c.Request.Context(), key, limit); err != nil {
			// RFC 9110 §10.2.3: tell the client when to come back, in whole seconds, at least 1.
			secs := int((wait + time.Second - 1) / time.Second)
			c.Header("Retry-After", strconv.Itoa(max(secs, 1)))
			httpx.Fail(c, err)
			return
		}
		c.Next()
	}
}

// AccountKey builds the Redis key for a per-account limit (RFC §11.3:
// "Rate limiting per IP and per account") — exported so callers stay
// consistent with Middleware's key format without duplicating the prefix
// convention.
func AccountKey(scope, identity string) string {
	return "ratelimit:" + scope + ":acct:" + identity
}

// AccountIPKey builds the key for a limit scoped to one account as seen from
// one client IP, so traffic from other IPs cannot exhaust it.
func AccountIPKey(scope, identity, ip string) string {
	return "ratelimit:" + scope + ":acctip:" + identity + ":" + ip
}

// Re-exported constructors so callers don't need a second import just to
// build a Limit value.
var (
	PerMinute = goredisrate.PerMinute
	PerHour   = goredisrate.PerHour
)
