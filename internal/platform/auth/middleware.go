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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
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
func RequireFreshPrivilege(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := FromContext(c.Request.Context())
		if !ok {
			httpx.Fail(c, apperrors.ErrUnauthorized)
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 500*time.Millisecond)
		defer cancel()

		current, err := rdb.Get(ctx, TokenVersionRedisKey(id.UserID.String())).Int()
		if err != nil {
			if err == redis.Nil {
				// No cached value yet — token_version defaults to 1 for
				// every user, so a cache miss means "never changed."
				current = 1
			} else {
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
