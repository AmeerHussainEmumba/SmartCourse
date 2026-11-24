package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultPingTimeout bounds a Ping whose context has no earlier deadline.
const DefaultPingTimeout = 3 * time.Second

// Ping reports whether at least one broker answers a metadata request. It
// opens a short-lived client, so it is cheap enough for a readiness probe but
// not for a hot path. It fails when ctx ends or after DefaultPingTimeout.
func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errInvalid("Brokers is required")
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultPingTimeout)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("smartcourse-healthcheck"),
		// Fail fast: a probe must report "down", not retry for the dial default.
		kgo.RequestRetries(0),
		kgo.DialTimeout(DefaultPingTimeout),
	)
	if err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	defer cl.Close()
	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}

// HealthCheck returns a readiness probe with the shape of router.Checker
// (func(context.Context) error) for Kafka. Register it as a NON-FATAL check:
// the API keeps serving when Kafka is down because the outbox buffers events
// (RFC §6.2), so an unreachable broker should degrade, not fail, readiness.
func HealthCheck(brokers []string) func(ctx context.Context) error {
	return func(ctx context.Context) error { return Ping(ctx, brokers) }
}
