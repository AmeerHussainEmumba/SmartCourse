// Package relaytest is test support for code that has handlers but wants to
// drive them from a real outbox without a broker: DeliverTo is a relay.Publisher
// that runs the handlers registered for the row's event type through
// relay.Deliver, which is what a Kafka consumer does with the record. A relay
// built on it (relay.New(db, relaytest.DeliverTo(db, handlers...))) therefore
// retries, parks, gives up and keeps per-aggregate order exactly as the
// publishing relay does; only the broker hop is skipped. Production code must
// not import this package.
package relaytest

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// DeliverTo returns a Publisher that delivers each event to every handler that
// registered for its type, on db. Every handler is attempted; the returned
// error joins the failures (nil when all succeeded or none matched).
func DeliverTo(db *gorm.DB, handlers ...relay.Handler) relay.Publisher {
	return deliverer{db: db, handlers: handlers}
}

type deliverer struct {
	db       *gorm.DB
	handlers []relay.Handler
}

func (d deliverer) Publish(ctx context.Context, evt relay.DecodedEvent) error {
	var errs []error
	for _, h := range d.handlers {
		if !h.Handles(evt.EventType) {
			continue
		}
		if err := relay.Deliver(ctx, d.db, h, evt); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", h.ConsumerName, err))
		}
	}
	return errors.Join(errs...)
}
