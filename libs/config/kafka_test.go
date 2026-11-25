package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoad_Kafka_TuningDefaultsWithoutBrokers(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	k := cfg.Kafka
	// The default Load does not ask for Kafka: brokers are neither required nor read.
	if len(k.Brokers) != 0 || k.SchemaRegistryURL != "" {
		t.Errorf("unrequested Kafka group was read: %+v", k)
	}
	if k.ConsumerMaxInFlight != 4 || k.ConsumerMaxAttempts != 8 ||
		k.ConsumerBackoffBase != 500*time.Millisecond || k.ConsumerBackoffMax != 30*time.Second ||
		k.ConsumerShutdownTimeout != 30*time.Second || k.ProduceTimeout != 20*time.Second {
		t.Errorf("defaults = %+v", k)
	}
}

func TestLoad_Kafka_UnrequestedIgnoresBrokers(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_BROKERS", "not an address")
	t.Setenv("SCHEMA_REGISTRY_URL", "nope")
	if _, err := Load(); err != nil {
		t.Fatalf("an unrequested Kafka group must not be validated: %v", err)
	}
}

func TestLoadWith_Kafka_RequiresBrokersAndRegistry(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("SCHEMA_REGISTRY_URL", "")
	_, err := LoadWith(Requirements{Kafka: true})
	if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS is required") || !strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL is required") {
		t.Fatalf("want both required errors, got %v", err)
	}

	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081/")
	cfg, err := LoadWith(Requirements{Kafka: true})
	if err != nil {
		t.Fatalf("LoadWith: %v", err)
	}
	if !reflect.DeepEqual(cfg.Kafka.Brokers, []string{"kafka-1:9092", "kafka-2:9092"}) {
		t.Errorf("Kafka = %+v", cfg.Kafka)
	}
	if cfg.Kafka.SchemaRegistryURL != "http://schema-registry:8081" {
		t.Errorf("SchemaRegistryURL = %q, want the trailing slash trimmed", cfg.Kafka.SchemaRegistryURL)
	}
}

func TestLoad_EventTransport_IsGone(t *testing.T) {
	setValidEnv(t)
	t.Setenv("EVENT_TRANSPORT", "rabbitmq")
	t.Setenv("ADMIN_HTTP_ADDR", "not an addr")
	if _, err := Load(); err != nil {
		t.Fatalf("EVENT_TRANSPORT and ADMIN_HTTP_ADDR are no longer read: %v", err)
	}
}

func TestLoad_Kafka_RejectsBadTuning(t *testing.T) {
	cases := []struct{ key, val string }{
		{"KAFKA_CONSUMER_MAX_IN_FLIGHT", "0"},
		{"KAFKA_CONSUMER_MAX_IN_FLIGHT", "257"},
		{"KAFKA_CONSUMER_MAX_ATTEMPTS", "0"},
		{"KAFKA_CONSUMER_MAX_ATTEMPTS", "101"},
		{"KAFKA_CONSUMER_BACKOFF_BASE", "0s"},
		{"KAFKA_CONSUMER_BACKOFF_BASE", "1m"}, // above the 10s default max
		{"KAFKA_CONSUMER_BACKOFF_MAX", "-1s"},
		{"KAFKA_CONSUMER_SHUTDOWN_TIMEOUT", "0s"},
		{"KAFKA_PRODUCE_TIMEOUT", "0s"},
		{"KAFKA_PRODUCE_TIMEOUT", "26s"},
		{"KAFKA_CONSUMER_MAX_IN_FLIGHT", "many"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.val, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(c.key, c.val)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), c.key) {
				t.Fatalf("want an error naming %s, got %v", c.key, err)
			}
		})
	}
}

func TestLoad_Kafka_BadBrokerAddress(t *testing.T) {
	for _, v := range []string{"kafka", "kafka:abc", ":9092", "kafka:0", "http://kafka:9092"} {
		t.Run(v, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("SCHEMA_REGISTRY_URL", "http://r:8081")
			t.Setenv("KAFKA_BROKERS", v)
			_, err := LoadWith(Requirements{Kafka: true})
			if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
				t.Fatalf("want a KAFKA_BROKERS error for %q, got %v", v, err)
			}
		})
	}
}

func TestLoad_Kafka_BadRegistryURL(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_BROKERS", "k:9092")
	t.Setenv("SCHEMA_REGISTRY_URL", "schema-registry:8081")
	if _, err := LoadWith(Requirements{Kafka: true}); err == nil || !strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL") {
		t.Fatalf("want a SCHEMA_REGISTRY_URL error, got %v", err)
	}
}
