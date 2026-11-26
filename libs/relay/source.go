package relay

import "context"

// Source says where a delivered event came from when it arrived over a broker:
// the coordinates of the Kafka record and the registry id of the schema its
// frame was written with. A handler run outside a broker (a test calling Deliver directly) has none.
// It rides in the context given to handlers (WithSource / SourceFrom) so the
// handler signature stays as it is.
type Source struct {
	Topic     string
	Partition int32
	Offset    int64
	// Key is the record key as delivered (the aggregate id for the producers here).
	Key string
	// SchemaID is the Schema Registry id in the frame header (the envelope
	// schema of the topic); 0 when the frame carried none.
	SchemaID uint32
}

type sourceKey struct{}

// WithSource returns ctx carrying s.
func WithSource(ctx context.Context, s Source) context.Context {
	return context.WithValue(ctx, sourceKey{}, s)
}

// SourceFrom returns the Source a transport attached to ctx, and false when none was
// attached (no broker, so no topic, offset or registry id).
func SourceFrom(ctx context.Context) (Source, bool) {
	s, ok := ctx.Value(sourceKey{}).(Source)
	return s, ok
}
