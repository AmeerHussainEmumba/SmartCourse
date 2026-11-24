package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer defaults.
const (
	DefaultMaxRetries      = 10
	DefaultDeliveryTimeout = 30 * time.Second
)

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// Brokers are the seed broker addresses (host:port). Required.
	Brokers []string
	// ClientID is reported to the broker, for its logs and quotas. Optional.
	ClientID string

	// MaxRetries bounds how many times one record is re-sent after a
	// retriable failure (a leader election, a broker restart). Zero means
	// DefaultMaxRetries; negative is an error.
	MaxRetries int
	// Backoff is the delay between those retries. Zero value means
	// DefaultBackoff.
	Backoff BackoffConfig
	// DeliveryTimeout bounds the total time Produce may spend on one record,
	// retries included, once it is accepted into the client. Zero means
	// DefaultDeliveryTimeout. Size it to the longest broker outage the caller
	// wants to ride out inside one Produce call.
	DeliveryTimeout time.Duration
}

func (c ProducerConfig) validate() error {
	if len(c.Brokers) == 0 {
		return errInvalid("Brokers is required")
	}
	for _, b := range c.Brokers {
		if b == "" {
			return errInvalid("Brokers contains an empty address")
		}
	}
	if c.MaxRetries < 0 {
		return errNegative("MaxRetries")
	}
	if c.DeliveryTimeout < 0 {
		return errNegative("DeliveryTimeout")
	}
	return c.Backoff.validate()
}

// Producer sends messages and waits for the broker's acknowledgement. It is
// safe for concurrent use.
//
// Delivery guarantees: acks=all with the idempotent producer, so a record
// re-sent by the client after a lost ack or a broker restart is stored once.
// Ordering holds per key (hence per partition) for the records of ONE caller
// that calls Produce one after another; two goroutines producing the same key
// concurrently have no defined order. Idempotence does not cover a caller that
// retries Produce itself after an error: an error return after the delivery
// timeout, or after the context was cancelled, means the record may or may not
// have been stored, and a caller retry can then produce a duplicate. Consumers
// must stay idempotent (RFC §6.6).
type Producer struct {
	cl *kgo.Client
}

// NewProducer validates cfg and builds a client. It does not contact the
// brokers: connection problems surface from Produce.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	retries := cfg.MaxRetries
	if retries == 0 {
		retries = DefaultMaxRetries
	}
	timeout := cfg.DeliveryTimeout
	if timeout == 0 {
		timeout = DefaultDeliveryTimeout
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// The idempotent producer is franz-go's default (there is no option
		// to enable it, only DisableIdempotentWrite), and it requires acks
		// from all in-sync replicas.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// A broker that is down or restarting behind a port proxy or a load
		// balancer accepts the connection and closes it at once. franz-go
		// reads a first-read EOF as a TLS or SASL misconfiguration and fails
		// the record without retrying; found by the broker-restart test. This
		// client is plaintext today, so a restart is by far the likelier
		// cause, and the retry budget and delivery timeout still bound it.
		kgo.AlwaysRetryEOF(),
		kgo.RecordRetries(retries),
		kgo.RetryBackoffFn(func(fails int) time.Duration { return cfg.Backoff.Delay(fails - 1) }),
		kgo.RecordDeliveryTimeout(timeout),
		// The request timeout must not exceed the delivery timeout, or one
		// slow request could outlive the budget the caller set.
		kgo.ProduceRequestTimeout(min(timeout, 10*time.Second)),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer client: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce sends one message and returns after the broker has acknowledged it
// (or the retry budget, the delivery timeout or ctx ended first, in which case
// the error says so and the record's fate is unknown, see Producer).
func (p *Producer) Produce(ctx context.Context, m Message) (Result, error) {
	if m.Topic == "" {
		return Result{}, errInvalid("Message.Topic is required")
	}
	rec := &kgo.Record{Topic: m.Topic, Key: m.Key, Value: m.Value}
	if len(m.Headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, len(m.Headers))
		for i, h := range m.Headers {
			rec.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}
	got, err := p.cl.ProduceSync(ctx, rec).First()
	if err != nil {
		if errors.Is(err, kgo.ErrClientClosed) {
			return Result{}, ErrClosed
		}
		return Result{}, fmt.Errorf("kafka: produce to %s: %w", m.Topic, err)
	}
	return Result{Partition: got.Partition, Offset: got.Offset}, nil
}

// Close releases the client. Produce calls still in flight fail; call it after
// the callers have stopped. It is safe to call more than once.
func (p *Producer) Close() {
	p.cl.Close()
}
