// Package cache owns the Redis client. Redis holds cache, hot counters,
// rate-limiter state, and Asynq's queues — never a source of truth (RFC
// §5.1). Every key here must be reconstructible from PostgreSQL.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

func Open(cfg config.RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
