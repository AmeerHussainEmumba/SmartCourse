package main

import (
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// buildRelay is the course relay: it only PUBLISHES course.outbox rows to Kafka
// (docs/plans/microservices-split.md §4). It has no handlers registered, so it
// never runs another service's code or even this service's own consumers: those
// are fed from the topics by buildConsumers. Per-aggregate order comes from the
// relay's claim query, "published" means the broker acknowledged the record.
//
// db must be the svc_course connection, so the relay's unqualified `outbox` is
// course.outbox. cfg.Relay.Workers (RELAY_WORKERS) sizes the pool; Load has
// already checked the Postgres pool can hold it (ADR-0027). sink is optional
// (nil keeps log-and-drop for an event the relay gives up on); tracing adds the
// relay's per-row span observer.
func buildRelay(db *gorm.DB, cfg *config.Config, pub relay.Publisher, sink relay.DeadLetterSink, tracing bool) *relay.Relay {
	opts := []relay.Option{relay.WithWorkerCount(cfg.Relay.Workers)}
	if tracing {
		opts = append(opts, relay.WithObserver(eventbus.RelayObserver()))
	}
	if sink != nil { // a typed-nil sink must not reach the relay as a non-nil interface
		opts = append(opts, relay.WithDeadLetterSink(sink))
	}
	return relay.New(db, pub, opts...)
}
