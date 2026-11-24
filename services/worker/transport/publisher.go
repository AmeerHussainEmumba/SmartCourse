package transport

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// Publisher is the relay's Kafka publish step (relay.Publisher). Publish
// returns nil only after the broker acknowledged the record (acks=all), which is
// the moment the relay may mark the outbox row published.
type Publisher struct {
	codec    *Codec
	producer *kafka.Producer
}

// PublisherConfig configures NewPublisher.
type PublisherConfig struct {
	Brokers []string
	// ProduceTimeout bounds one publish, retries included. Keep it below the
	// relay's 30 s claim timeout so a failed publish can still be recorded.
	ProduceTimeout time.Duration
	Schemas        Schemas
}

// NewPublisher builds the producer. It does not contact the brokers.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	p, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:         cfg.Brokers,
		ClientID:        "smartcourse-relay",
		DeliveryTimeout: cfg.ProduceTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Publisher{codec: NewCodec(cfg.Schemas), producer: p}, nil
}

// Publish encodes evt and produces it, keyed by aggregate id.
//
// An unencodable row returns its error unmarked, so the relay spends the row's
// attempt budget on it and then dead-letters it. A broker problem is marked
// relay.Transient: an outage must not use up the budget of good events. The
// exceptions are the broker rejecting the record itself (too large, corrupt),
// which no retry can fix.
func (p *Publisher) Publish(ctx context.Context, evt relay.DecodedEvent) error {
	msg, err := p.codec.Encode(evt)
	if err != nil {
		return err
	}
	if _, err := p.producer.Produce(ctx, msg); err != nil {
		return classifyProduceError(err)
	}
	return nil
}

// Close releases the producer.
func (p *Publisher) Close() { p.producer.Close() }

// rejectedByBroker are the broker's non-retriable verdicts on the record itself.
// Everything else (timeouts, leader elections, an unreachable broker, even an
// authorisation error that an operator can fix) is treated as transient, so it
// cannot dead-letter good events.
var rejectedByBroker = []*kerr.Error{
	kerr.MessageTooLarge, kerr.RecordListTooLarge, kerr.CorruptMessage, kerr.InvalidRecord, kerr.InvalidTopicException,
}

func classifyProduceError(err error) error {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		for _, r := range rejectedByBroker {
			if ke == r {
				return fmt.Errorf("broker rejected the record: %w", err)
			}
		}
	}
	return relay.Transient(err)
}
