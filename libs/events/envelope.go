package events

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/events/v1"
)

// Envelope is the generated envelope message (RFC §6.4).
type Envelope = eventsv1.Envelope

// NewEnvelope wraps payload in an Envelope. The caller sets the metadata
// fields (event id, aggregate, timestamps, trace, actor) on meta; Payload and
// EventType are filled in here from t.
func NewEnvelope(t Type, meta *Envelope, payload proto.Message) (*Envelope, error) {
	if payload.ProtoReflect().Descriptor().FullName() != t.Message().FullName() {
		return nil, fmt.Errorf("events: payload is %s, event type %s wants %s",
			payload.ProtoReflect().Descriptor().FullName(), t.EventType, t.Message().FullName())
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	env := proto.Clone(meta).(*Envelope)
	env.EventType = t.EventType
	env.Payload = body
	return env, nil
}

// OpenPayload decodes env.Payload into the message type env.EventType names.
func OpenPayload(env *Envelope) (proto.Message, error) {
	t, ok := Lookup(env.GetEventType())
	if !ok {
		return nil, fmt.Errorf("events: unknown event type %q", env.GetEventType())
	}
	msg := t.New()
	if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", t.EventType, err)
	}
	return msg, nil
}
