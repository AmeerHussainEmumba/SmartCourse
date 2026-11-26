package main

import (
	"context"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

// lagGaugeInterval is how often kafka_consumer_lag asks the brokers, so the
// broker load does not depend on how often Prometheus scrapes.
const lagGaugeInterval = 15 * time.Second

// consumerHandlers is the one list of every consumer the course service runs:
// the instructor mirror (identity.user_registered, identity.user_profile_updated),
// enrollment archival (course.course_archived) and the certificate issuer
// (progress.course_completed). One consumer set runs them all; each consumer
// name is its own group, and every group also reads retry.<consumer-name>.
func consumerHandlers() []relay.Handler {
	var all []relay.Handler
	all = append(all, course.Handlers()...)
	all = append(all, enrollment.Handlers()...)
	all = append(all, certificate.Handlers()...)
	return all
}

// buildConsumers builds the consumer set over db, which must be the svc_course
// connection: each handler and its processed_events marker commit in one
// transaction in the course schema. sink is optional (nil logs and drops an
// event whose budget is spent). Nothing starts until Run.
func buildConsumers(k config.KafkaConfig, db *gorm.DB, sink relay.DeadLetterSink, logger *slog.Logger, obs *infra.Observability) (*eventbus.Consumers, error) {
	opts := []eventbus.ConsumerOption{
		eventbus.WithLogger(logger),
		eventbus.WithTracing(obs.TracingEnabled()),
		eventbus.WithMetrics(obs.Metrics),
	}
	if sink != nil {
		opts = append(opts, eventbus.WithSink(sink))
	}
	return eventbus.NewConsumers(k, consumerHandlers(), db, opts...)
}

// runConsumers runs the consumer groups, and the kafka_consumer_lag poller when
// metrics are on, until ctx is cancelled and the groups have drained. A lag
// reader that cannot be built is logged: the consumers run without the gauge.
func runConsumers(ctx context.Context, logger *slog.Logger, c *eventbus.Consumers, m *telemetry.Metrics) error {
	if m != nil {
		if collector, closeReader, err := c.LagGauges(m); err != nil {
			logger.Error("kafka_consumer_lag not exported", slog.Any("error", err))
		} else {
			stop := telemetry.StartPoller(ctx, lagGaugeInterval, collector)
			defer func() {
				stop()
				closeReader()
			}()
		}
	}
	logger.Info("kafka consumers starting", slog.Any("groups", c.Groups()))
	c.Run(ctx)
	logger.Info("kafka consumers stopped")
	return nil
}
