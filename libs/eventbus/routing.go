// Package eventbus is the Kafka side of the event transport (ADR-0029), shared
// by every service (docs/plans/microservices-split.md §4): it encodes an outbox
// row into the framed Envelope the topics carry and produces it (the relay's
// Publisher), decodes a consumed record back into the relay.DecodedEvent the
// handlers take, and runs one consumer group per consumer name (Consumers),
// each also reading its own retry topic. libs/relay stays free of any Kafka
// client; this package is the only place the two meet.
//
// The relay remains the only path from an outbox to Kafka (RFC §6.2): nothing
// here reads the outbox, and the Publisher is only ever called by the relay. A
// service wires two independent things: relay.New(db, p)
// (it takes no handler), and NewConsumers(cfg, handlers, db) for the events
// it consumes.
package eventbus

import (
	"fmt"
	"slices"
	"strings"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// topicByPrefix is RFC §6.5: the topic follows the event type's prefix (the
// text before the first dot), not the outbox aggregate_type, because enrollment
// events are keyed by a course aggregate and progress events by an enrollment.
var topicByPrefix = map[string]string{
	"identity":   topics.Identity,
	"course":     topics.Course,
	"enrollment": topics.Enrollment,
	"progress":   topics.Progress,
}

// TopicFor returns the topic an event type is published to.
func TopicFor(eventType string) (string, error) {
	prefix, _, ok := strings.Cut(eventType, ".")
	if !ok {
		return "", fmt.Errorf("eventbus: event type %q has no <domain>. prefix", eventType)
	}
	topic, ok := topicByPrefix[prefix]
	if !ok {
		return "", fmt.Errorf("eventbus: event type %q has no topic (unknown prefix %q)", eventType, prefix)
	}
	return topic, nil
}

// TopicsFor returns the sorted, de-duplicated topics a handler's event types
// live on: what its consumer group subscribes to.
func TopicsFor(h relay.Handler) ([]string, error) {
	var out []string
	for et := range h.EventTypes {
		t, err := TopicFor(et)
		if err != nil {
			return nil, fmt.Errorf("handler %s: %w", h.ConsumerName, err)
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("handler %s registers no event types", h.ConsumerName)
	}
	slices.Sort(out)
	return out, nil
}
