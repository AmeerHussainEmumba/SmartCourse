package deadletter

import (
	"context"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// ForRelay adapts the Sink to relay.DeadLetterSink. The consumer runner puts
// the record's relay.Source in the context it hands to Park, so the parked
// record keeps its topic and the schema id of its frame: what a replay needs to
// frame the event again (an event parked without a schema id is refused by
// replay). A non-nil error from Park means "not parked".
func ForRelay(s *Sink) relay.DeadLetterSink { return relayAdapter{s} }

type relayAdapter struct{ s *Sink }

func (a relayAdapter) Park(ctx context.Context, d relay.DeadLetter) error {
	p := Parked{Event: d.Event, Consumer: d.Consumer, Err: d.Err, History: d.History, Attempts: d.Attempts}
	if src, ok := relay.SourceFrom(ctx); ok {
		p.Topic, p.SchemaID = src.Topic, int32(src.SchemaID)
	}
	return a.s.Park(ctx, p)
}
