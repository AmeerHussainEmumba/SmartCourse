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

	"github.com/gin-gonic/gin"
	goredisrate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
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
	res, err := l.rl.Allow(ctx, key, limit)
	if err != nil {
		// Fails open (RFC §13.4: "Redis down... Rate limiting fails open,
		// logged"). A Redis outage should degrade to "no rate limiting,"
		// never to "nobody can authenticate."
		return nil
	}
	if res.Allowed == 0 {
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
		if err := l.Allow(c.Request.Context(), key, limit); err != nil {
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

// Re-exported constructors so callers don't need a second import just to
// build a Limit value.
var (
	PerMinute = goredisrate.PerMinute
	PerHour   = goredisrate.PerHour
)
