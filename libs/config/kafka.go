package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Event transports (ADR-0029, EVENT_TRANSPORT).
const (
	// TransportInProcess is the relay running the registered handlers itself
	// (ADR-0021). It is the default.
	TransportInProcess = "inprocess"
	// TransportKafka is the relay publishing to Kafka and the worker's
	// consumers running the same handlers from the topics.
	TransportKafka = "kafka"
)

// KafkaConfig selects the event transport and tunes the Kafka side (libs/kafka,
// ADR-0029, ADR-0030). Every binary reads the same environment, so Load checks
// it for all of them; the brokers and the registry are only required, and only
// validated, when the transport is "kafka".
type KafkaConfig struct {
	// Transport is EVENT_TRANSPORT: "inprocess" (default) or "kafka".
	Transport string
	// Brokers are KAFKA_BROKERS, comma-separated host:port seeds.
	Brokers []string
	// SchemaRegistryURL is SCHEMA_REGISTRY_URL, without a trailing slash.
	SchemaRegistryURL string

	// ConsumerMaxInFlight is KAFKA_CONSUMER_MAX_IN_FLIGHT: how many handlers one
	// consumer group runs at once. Each holds a Postgres connection for its
	// transaction, and the worker runs one group per handler, so the worst case
	// is groups x this value; the pool queues the excess, it does not fail it.
	ConsumerMaxInFlight int
	// ConsumerMaxAttempts is KAFKA_CONSUMER_MAX_ATTEMPTS: tries per record
	// (first included) before it is dead-lettered and the offset advances.
	ConsumerMaxAttempts int
	// ConsumerBackoffBase and ConsumerBackoffMax shape the delay between
	// attempts, Base * 2^n capped at Max.
	ConsumerBackoffBase time.Duration
	ConsumerBackoffMax  time.Duration
	// ConsumerShutdownTimeout bounds the drain of in-flight handlers.
	ConsumerShutdownTimeout time.Duration
	// ProduceTimeout is KAFKA_PRODUCE_TIMEOUT (default 20s, at most 25s): how long the relay rides out a
	// broker outage inside one publish before the row is retried (its attempt
	// count then rises, see libs/relay).
	ProduceTimeout time.Duration
}

// KafkaEnabled reports whether the Kafka transport is selected.
func (k KafkaConfig) KafkaEnabled() bool { return k.Transport == TransportKafka }

const (
	defaultKafkaMaxInFlight = 4
	maxKafkaMaxInFlight     = 256
	defaultKafkaMaxAttempts = 5
	maxKafkaMaxAttempts     = 100
	maxKafkaProduceTimeout  = 25 * time.Second
)

// loadKafka reads and validates the transport settings, adding to errs. It takes
// Load's own parsers so the error wording is uniform.
func loadKafka(
	getenv func(string) string,
	optInt func(string, int) int,
	optDuration func(string, time.Duration) time.Duration,
	errs *[]string,
) KafkaConfig {
	bad := func(format string, args ...any) { *errs = append(*errs, fmt.Sprintf(format, args...)) }
	inRange := func(key string, v, lo, hi int) {
		if v < lo || v > hi {
			bad("%s must be between %d and %d", key, lo, hi)
		}
	}

	k := KafkaConfig{
		Transport:               TransportInProcess,
		ConsumerMaxInFlight:     optInt("KAFKA_CONSUMER_MAX_IN_FLIGHT", defaultKafkaMaxInFlight),
		ConsumerMaxAttempts:     optInt("KAFKA_CONSUMER_MAX_ATTEMPTS", defaultKafkaMaxAttempts),
		ConsumerBackoffBase:     optDuration("KAFKA_CONSUMER_BACKOFF_BASE", 200*time.Millisecond),
		ConsumerBackoffMax:      optDuration("KAFKA_CONSUMER_BACKOFF_MAX", 10*time.Second),
		ConsumerShutdownTimeout: optDuration("KAFKA_CONSUMER_SHUTDOWN_TIMEOUT", 30*time.Second),
		ProduceTimeout:          optDuration("KAFKA_PRODUCE_TIMEOUT", 20*time.Second),
	}
	// An unrecognised value must fail startup: a typo such as "Kafka" or
	// "in-process" silently falling back to in-process would hide that events
	// are not flowing through the broker.
	switch v := getenv("EVENT_TRANSPORT"); v {
	case "", TransportInProcess:
	case TransportKafka:
		k.Transport = TransportKafka
	default:
		bad("EVENT_TRANSPORT must be %q or %q, got %q", TransportInProcess, TransportKafka, v)
	}

	inRange("KAFKA_CONSUMER_MAX_IN_FLIGHT", k.ConsumerMaxInFlight, 1, maxKafkaMaxInFlight)
	inRange("KAFKA_CONSUMER_MAX_ATTEMPTS", k.ConsumerMaxAttempts, 1, maxKafkaMaxAttempts)
	if k.ConsumerBackoffBase <= 0 {
		bad("KAFKA_CONSUMER_BACKOFF_BASE must be greater than zero")
	}
	if k.ConsumerBackoffMax <= 0 {
		bad("KAFKA_CONSUMER_BACKOFF_MAX must be greater than zero")
	} else if k.ConsumerBackoffBase > k.ConsumerBackoffMax {
		bad("KAFKA_CONSUMER_BACKOFF_BASE (%v) must not exceed KAFKA_CONSUMER_BACKOFF_MAX (%v)", k.ConsumerBackoffBase, k.ConsumerBackoffMax)
	}
	if k.ConsumerShutdownTimeout <= 0 {
		bad("KAFKA_CONSUMER_SHUTDOWN_TIMEOUT must be greater than zero")
	}
	if k.ProduceTimeout <= 0 || k.ProduceTimeout > maxKafkaProduceTimeout {
		// The relay gives one row 30 s in all (claim, publish, recording the
		// result); a longer publish could not record its own failure.
		bad("KAFKA_PRODUCE_TIMEOUT must be greater than zero and at most %v", maxKafkaProduceTimeout)
	}

	if !k.KafkaEnabled() {
		return k
	}
	k.Brokers = splitAndTrim(getenv("KAFKA_BROKERS"))
	if len(k.Brokers) == 0 {
		bad("KAFKA_BROKERS is required when EVENT_TRANSPORT=kafka")
	}
	for _, b := range k.Brokers {
		if err := checkHostPort(b); err != nil {
			bad("KAFKA_BROKERS entry %q is not a host:port address: %v", b, err)
		}
	}
	reg := strings.TrimRight(getenv("SCHEMA_REGISTRY_URL"), "/")
	if reg == "" {
		bad("SCHEMA_REGISTRY_URL is required when EVENT_TRANSPORT=kafka")
	} else if u, err := url.Parse(reg); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		bad("SCHEMA_REGISTRY_URL must be an http(s) URL, got %q", reg)
	}
	k.SchemaRegistryURL = reg
	return k
}

func checkHostPort(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("empty host")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port %q", port)
	}
	return nil
}
