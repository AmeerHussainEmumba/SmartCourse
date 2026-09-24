// Request-scoped identity, carried on context.Context rather than
// gin.Context, so service-layer code (which takes ctx context.Context, not
// gin.Context — RFC §10.5) can read who's making the call without any
// dependency on the HTTP layer.
package auth

import (
	"context"

	"github.com/google/uuid"
)

type ctxKey struct{}

type Identity struct {
	UserID       uuid.UUID
	Role         string
	TokenVersion int16
}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the authenticated identity. ok is false for
// unauthenticated requests (public endpoints) — callers on an
// authenticated-only path can treat !ok as a programming error (the route
// should have RequireAuth), not a normal case to handle gracefully.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}
