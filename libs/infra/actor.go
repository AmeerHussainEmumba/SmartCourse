package infra

import (
	"context"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

// init installs the libs/auth identity as the outbox actor resolver, so every
// binary that links infra (all four) records the authenticated user in
// outbox.actor_id without each call site passing it (RFC §6.4). outbox does
// not import libs/auth itself: the relay links outbox, and the auth module
// brings dependencies the relay does not need. An ActorID a caller sets
// explicitly still wins (outbox.Write).
func init() {
	outbox.SetActorResolver(func(ctx context.Context) (uuid.UUID, bool) {
		id, ok := auth.FromContext(ctx)
		return id.UserID, ok
	})
}
