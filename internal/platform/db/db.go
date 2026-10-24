// Package db owns the PostgreSQL connection (RFC §5.1 — PostgreSQL is the
// single system of record). GORM is used here for single-entity CRUD and
// simple associations; modules needing non-trivial joins or window functions
// use db.Raw / db.Exec directly rather than GORM associations (RFC §5.9,
// differences-from-requirements.md DR-004).
package db

import (
	"context"
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

// Open connects to PostgreSQL and sizes the connection pool against the
// configured budget (RFC §13.2 — pool sizing is a hard rule, not a tuning
// afterthought: MaxOpenConns × replicas must stay under Postgres's
// max_connections, with headroom for migrations and operators).
func Open(cfg config.PostgresConfig) (*gorm.DB, error) {
	gdb, err := gorm.Open(postgres.Open(cfg.DSN), &gorm.Config{
		Logger:                                   gormlogger.Default.LogMode(gormlogger.Warn),
		DisableForeignKeyConstraintWhenMigrating: true, // schema is owned by migrations/, not AutoMigrate (ADR-0015 §19.15)
	})
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return gdb, nil
}

// Ping is used by the readiness probe (RFC §12.5) — a lightweight,
// context-bounded check, never a query against a real table.
func Ping(ctx context.Context, gdb *gorm.DB) error {
	sqlDB, err := gdb.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}
