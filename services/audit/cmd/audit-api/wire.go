package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/admin"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/replay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/store"
)

const shutdownGrace = 15 * time.Second

// wire builds the admin API. Readiness (public /health/ready and the internal
// listener): Redis is FATAL (without it no admin token can be checked, the
// fail-closed check answers 503); MongoDB and Kafka are NON-FATAL ("degraded":
// the failed-events routes answer 503 while MongoDB is down, replay answers 503
// while Kafka is down, and nothing crashes). Postgres is not used.
func wire(p *rt.Process) error {
	cfg := p.Config
	rdb, err := cache.Open(cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	p.OnShutdown("redis", func(context.Context) error { return rdb.Close() })
	redisCheck := func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
	p.Ready("redis", redisCheck)

	mongo := store.New(cfg.Mongo)
	p.Go("mongo", func(ctx context.Context) error { return mongo.Run(ctx, p.Logger) })
	p.Ready("mongodb", mongo.Ping, rt.NonFatal())
	kafkaCheck := kafka.HealthCheck(cfg.Kafka.Brokers)
	p.Ready("kafka", kafkaCheck, rt.NonFatal())

	// The producer needs no registry state: a replay frames the event with the
	// schema id stored when it was parked (eventbus.EncodeForRetry).
	pub, err := eventbus.NewPublisherFor(cfg.Kafka, eventbus.Schemas{}, p.Obs.TracingEnabled())
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	p.OnShutdown("producer", func(context.Context) error { pub.Close(); return nil })

	h, err := admin.Handler(admin.Deps{
		Config:   cfg,
		RDB:      rdb,
		Failed:   mongo,
		Replayer: replay.New(mongo, pub, replay.WithProduceTimeout(cfg.Kafka.ProduceTimeout), replay.WithLogger(p.Logger)),
		Auditor:  mongo,
		Required: map[string]router.Checker{"redis": redisCheck},
		Optional: map[string]router.Checker{"mongodb": mongo.Ping, "kafka": kafkaCheck},
		Observe:  p.Obs.Middleware(),
	})
	if err != nil {
		return fmt.Errorf("admin handler: %w", err)
	}
	srv := admin.NewServer(cfg, h)
	p.Go("http", func(ctx context.Context) error {
		errc := make(chan error, 1)
		go func() { errc <- srv.ListenAndServe() }()
		p.Logger.Info("audit-api listening", slog.String("addr", srv.Addr))
		select {
		case err := <-errc:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
			defer cancel()
			return srv.Shutdown(sctx)
		}
	}, rt.Critical())
	return nil
}

// loadConfig is audit-api's configuration: Redis (the token-version check), the JWT key, MongoDB and Kafka (the replay producer). It needs no POSTGRES_* or MINIO_*
// (config.Requirements asks only for what is listed).
func loadConfig() (*config.Config, error) {
	return config.LoadWith(config.Requirements{Redis: true, JWT: true, KafkaBrokers: true, Mongo: true})
}
