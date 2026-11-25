// Three authorization layers (RFC §11.4), deliberately not conflated:
//
//  1. RequireAuth    — is there a valid token at all.
//  2. RequireRole    — coarse, route-level ("must be an instructor").
//  3. RequireFreshPrivilege — narrow, applied only to admin/privilege-mutating
//     routes: is this token's claim of privilege still current (ADR-0015
//     addendum). Ownership checks (this student owns *this* enrollment) are
//     a fourth layer, implemented in each module's service.go, because only
//     the service has loaded the resource to check against.
package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

// TokenVersionRedisKey is exported so the identity module can write to it
// (on role/status change) using the same key another package reads.
func TokenVersionRedisKey(userID string) string {
	return "user:tv:" + userID
}

func RequireAuth(cfg config.JWTConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			httpx.Fail(c, apperrors.ErrUnauthorized)
			return
		}
		raw := strings.TrimPrefix(header, prefix)

		claims, err := ParseAccessToken(cfg, raw)
		if err != nil {
			httpx.Fail(c, apperrors.ErrUnauthorized.Wrap(err))
			return
		}
		userID, err := claims.UserID()
		if err != nil {
			httpx.Fail(c, apperrors.ErrUnauthorized.Wrap(err))
			return
		}

		id := Identity{UserID: userID, Role: claims.Role, TokenVersion: claims.TokenVersion}
		c.Request = c.Request.WithContext(WithIdentity(c.Request.Context(), id))
		c.Next()
	}
}

// RequireRole is coarse, route-level (RFC §11.4). It cannot check ownership
// of a specific resource — at middleware time the resource hasn't been
// loaded yet — that's what RequireFreshPrivilege and service-layer
// ownership checks are for.
func RequireRole(roles ...string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(roles))
	for _, r := range roles {
		allowed[r] = true
	}
	return func(c *gin.Context) {
		id, ok := FromContext(c.Request.Context())
		if !ok {
			httpx.Fail(c, apperrors.ErrUnauthorized)
			return
		}
		if !allowed[id.Role] {
			httpx.Fail(c, apperrors.ErrForbidden)
			return
		}
		c.Next()
	}
}

// RequireFreshPrivilege guards the small set of routes where a stale token
// is actually dangerous — admin actions and anything that mutates another
// user's role or access (ADR-0015 addendum, RFC §11.2/§11.4). It compares
// the token's embedded token-version claim against the current value cached
// in Redis. A Redis failure fails CLOSED here — the one place in the system
// where that's correct, because the entire point of this check is that
// trust must be current, not assumed.
//
// A cache MISS, though, reads as version 1 ("never changed"): fine for the
// instructor routes (a flush must not lock every instructor out; see README),
// wrong for an admin surface, where a flush would re-accept the old token of an
// admin demoted from version 1. Admin-only routes that have no durable source
// use RequireFreshPrivilegeStrict.
func RequireFreshPrivilege(rdb *redis.Client) gin.HandlerFunc {
	return requireFresh(rdb, nil, false)
}

// RequireFreshPrivilegeStrict is RequireFreshPrivilege for admin-only routes
// of a service that cannot read the durable token version (audit-api and
// analytics-api cannot read identity's tables, ADR-0022 to 0024). A
// Redis miss on the token-version key FAILS CLOSED: 401 token_stale, never
// "version 1". The key comes back when identity's re-seeder rewrites every
// version above 1 (within one tick of its interval after a flush) or the admin
// logs in, refreshes, or calls an identity /me or admin route (any of which
// writes the key, version 1 included). The price is a bounded lockout of
// admins from these routes after a Redis flush; the gain is that a demoted
// admin's old token is never accepted because the cache forgot the demotion.
// Redis errors other than a miss still answer 503.
func RequireFreshPrivilegeStrict(rdb *redis.Client) gin.HandlerFunc {
	return requireFresh(rdb, nil, true)
}

// ErrUserNotFound is returned by a TokenVersionSource for an unknown (or
// deactivated) user; the middleware answers 401.
var ErrUserNotFound = errors.New("auth: user not found")

// TokenVersionSource is the durable source of truth for a user's current
// token_version (Postgres users.token_version). The identity service supplies
// it; libs/auth never imports identity.
type TokenVersionSource interface {
	CurrentTokenVersion(ctx context.Context, userID uuid.UUID) (int16, error)
}

// RequireFreshPrivilegeWithSource is RequireFreshPrivilege without the
// fail-open cache miss: when Redis has no entry for the user (restart, flush,
// eviction) the version is read from src and written back (SET NX, so a
// concurrent role change's newer value is never overwritten). Redis errors
// still fail closed. Use it on any route that must honour demotion or
// suspension promptly (identity applies it to /me as well as admin routes).
func RequireFreshPrivilegeWithSource(rdb *redis.Client, src TokenVersionSource) gin.HandlerFunc {
	return requireFresh(rdb, src, false)
}

func requireFresh(rdb *redis.Client, src TokenVersionSource, strict bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := FromContext(c.Request.Context())
		if !ok {
			httpx.Fail(c, apperrors.ErrUnauthorized)
			return
		}

		timeout := 500 * time.Millisecond
		if src != nil {
			timeout = 2 * time.Second // room for the Postgres fallback
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		key := TokenVersionRedisKey(id.UserID.String())
		current, err := rdb.Get(ctx, key).Int()
		if err != nil {
			switch {
			case err == redis.Nil && src == nil && strict:
				// No durable source and the route is privileged: a miss proves
				// nothing about the current version, so refuse.
				httpx.Fail(c, apperrors.ErrTokenStale)
				return
			case err == redis.Nil && src == nil:
				// Legacy behaviour of RequireFreshPrivilege: a miss means
				// "never changed" (version 1). Unsafe after a Redis flush;
				// prefer RequireFreshPrivilegeWithSource.
				current = 1
			case err == redis.Nil:
				v, serr := src.CurrentTokenVersion(ctx, id.UserID)
				if errors.Is(serr, ErrUserNotFound) {
					httpx.Fail(c, apperrors.ErrUnauthorized)
					return
				}
				if serr != nil {
					httpx.Fail(c, apperrors.ErrUnavailable.Wrap(serr))
					return
				}
				current = int(v)
				// SET NX: never overwrite a newer value written concurrently
				// by a role change. Best effort.
				_ = rdb.SetNX(ctx, key, current, 0).Err()
			default:
				httpx.Fail(c, apperrors.ErrUnavailable.Wrap(err))
				return
			}
		}

		if int16(current) != id.TokenVersion {
			httpx.Fail(c, apperrors.ErrTokenStale)
			return
		}
		c.Next()
	}
}
