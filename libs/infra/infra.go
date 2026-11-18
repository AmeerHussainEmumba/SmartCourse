// Package infra opens the shared infrastructure clients (Postgres, Redis)
// every service binary needs. It deliberately imports no domain module and no
// Temporal code: each service's cmd/<binary>/wire.go does its own module wiring, so a
// binary only links the modules it actually runs (RFC §4.4; the Week 2
// review's point that every binary used to build every module through the
// old shared bootstrap package).
package infra

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
)

// Dependencies holds the shared infrastructure clients — connecting to
// Postgres/Redis is identical work for every service binary, so it lives here
// once instead of being duplicated across main.go files that would
// inevitably drift.
type Dependencies struct {
	DB  *gorm.DB
	RDB *redis.Client
}

func Connect(cfg *config.Config) (*Dependencies, error) {
	gdb, err := db.Open(cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	rdb, err := cache.Open(cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	return &Dependencies{DB: gdb, RDB: rdb}, nil
}

// HealthCheckers returns the dependency checks for /health/ready
// (libs/router). Shared because every service has the same
// dependencies, even though services/worker/cmd/worker doesn't expose an HTTP health
// endpoint today.
func (d *Dependencies) HealthCheckers() map[string]router.Checker {
	return map[string]router.Checker{
		"postgres": func(ctx context.Context) error { return db.Ping(ctx, d.DB) },
		"redis":    func(ctx context.Context) error { return d.RDB.Ping(ctx).Err() },
	}
}
