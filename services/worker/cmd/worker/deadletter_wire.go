package main

import (
	"context"
	"log/slog"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

// deadLetterAdapter lets the Mongo-backed sink serve the relay's and the Kafka
// consumers' DeadLetterSink contract. A non-nil error from Park means "not
// parked": the relay keeps the row unpublished and the transport keeps the
// record retryable rather than dropping the event.
type deadLetterAdapter struct{ s *deadletter.Sink }

func (a deadLetterAdapter) Park(ctx context.Context, d relay.DeadLetter) error {
	return a.s.Park(ctx, deadletter.Parked{
		Event: d.Event, Consumer: d.Consumer, Err: d.Err, History: d.History, Attempts: d.Attempts,
	})
}

// registerArchiver registers the audit-archiver on rl and returns its handlers
// so a parked archive event can be replayed too. It is called only in kafka
// mode: in-process, a failing handler fails the whole relay row, so a MongoDB
// outage would stall every other consumer. In kafka mode the archiver has its
// own consumer group and an outage only slows it. With MongoDB unconfigured it
// is not registered at all.
func registerArchiver(logger *slog.Logger, cfg *config.Config, mg *mongoWiring, rl *relay.Relay) []relay.Handler {
	if cfg.Mongo.URI == "" {
		logger.Warn("audit-archiver not registered: MONGO_URI is not set")
		return nil
	}
	hs := mg.ArchiveHandlers()
	for _, h := range hs {
		rl.Register(h)
	}
	return hs
}
