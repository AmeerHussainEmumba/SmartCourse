package identity

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// Re-seeding token versions after a Redis flush (F-38 residue).
//
// course-api has no durable source for a user's token_version (it cannot read
// identity.users, ADR-0024) and treats a missing Redis key as version 1, so
// after a flush every user whose version is above 1 gets 401 token_stale on
// course writes until identity next writes their key (login or refresh).
// Identity owns the data, so identity repairs the cache: a loop notices the
// flush through a sentinel key and re-writes every version above 1 with
// SET NX (a newer value written by a role change is never overwritten, the
// same rule seedTokenVersion uses). Versions equal to 1 need no key. No call
// crosses a service boundary and course-api is unchanged.
const (
	tvSeedSentinelKey = "identity:tv:seeded"
	// The sentinel expires so a pass interrupted by a crash, or keys lost to
	// eviction, are repaired within one TTL rather than never.
	tvSeedSentinelTTL = 10 * time.Minute
	tvSeedBatchSize   = 500
)

// ReseedTokenVersions writes every token_version above 1 to Redis if the
// sentinel is absent, and reports how many keys were written (not counting
// users whose key already existed). It is a no-op returning (0, nil) when the
// sentinel is present, and safe to run on several replicas at once: the
// sentinel is claimed with SET NX before the pass, so exactly one replica
// does the work. If the pass fails the sentinel is removed so the next tick
// retries; a flush during the pass removes it too, which triggers another.
func (s *Service) ReseedTokenVersions(ctx context.Context) (int, error) {
	claimed, err := s.rdb.SetNX(ctx, tvSeedSentinelKey, time.Now().UTC().Format(time.RFC3339), tvSeedSentinelTTL).Result()
	if err != nil || !claimed {
		return 0, err
	}
	written, err := s.reseedAll(ctx)
	if err != nil {
		// Detached: the failure may be the cancelled ctx itself.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = s.rdb.Del(dctx, tvSeedSentinelKey).Err()
		return written, err
	}
	return written, nil
}

func (s *Service) reseedAll(ctx context.Context) (int, error) {
	batch := s.reseedBatch
	if batch <= 0 {
		batch = tvSeedBatchSize
	}
	written := 0
	after := uuid.Nil
	for {
		rows, err := s.repo.ListTokenVersionsAbove1(ctx, after, batch)
		if err != nil {
			return written, err
		}
		if len(rows) == 0 {
			return written, nil
		}
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.BoolCmd, 0, len(rows))
		for _, r := range rows {
			cmds = append(cmds, pipe.SetNX(ctx, auth.TokenVersionRedisKey(r.ID.String()), int(r.TokenVersion), 0))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return written, err
		}
		for _, c := range cmds {
			if c.Val() {
				written++
			}
		}
		after = rows[len(rows)-1].ID
		if len(rows) < batch {
			return written, nil
		}
	}
}

// RunTokenVersionReseeder runs ReseedTokenVersions every interval (and once
// immediately) until ctx ends. Each tick costs one Redis SET NX while the
// sentinel is present. Errors are logged, never fatal: the cache is repaired
// at the next login or refresh regardless.
func (s *Service) RunTokenVersionReseeder(ctx context.Context, interval time.Duration) {
	run := func() {
		n, err := s.ReseedTokenVersions(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			slog.Warn("identity: token version re-seed failed", slog.Any("error", err))
		case n > 0:
			slog.Info("identity: re-seeded token versions into Redis", slog.Int("keys", n))
		}
	}
	run()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}
