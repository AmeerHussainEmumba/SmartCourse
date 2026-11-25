// What this checks: LoadWith lets a binary declare the groups it needs. A group
// that is not requested is not required and not validated; Load is unchanged
// (it equals LoadWith(AllRequirements())).
package config

import (
	"strings"
	"testing"
)

func clearRequiredEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "REDIS_ADDR", "MINIO_ENDPOINT",
		"MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "JWT_SIGNING_KEY",
		"KAFKA_BROKERS", "SCHEMA_REGISTRY_URL", "MONGO_URI",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadWith_NoRequirements_NeedsNothing(t *testing.T) {
	clearRequiredEnv(t)
	t.Setenv("TEMPORAL_HOST_PORT", "localhost:7233")
	cfg, err := LoadWith(Requirements{})
	if err != nil {
		t.Fatalf("LoadWith(none) with no required vars: %v", err)
	}
	if cfg.Temporal.HostPort != "localhost:7233" || cfg.Temporal.TaskQueue != "publish-workflows" {
		t.Errorf("temporal config = %+v", cfg.Temporal)
	}
	if cfg.Telemetry.MetricsAddr != ":9100" {
		t.Errorf("MetricsAddr = %q", cfg.Telemetry.MetricsAddr)
	}
	if cfg.Argon.MaxConcurrent < 1 || cfg.Argon.MemoryKiB != 64*1024 {
		t.Errorf("unrequested argon group lost its defaults: %+v", cfg.Argon)
	}
}

func TestLoad_StillRequiresEverything(t *testing.T) {
	clearRequiredEnv(t)
	_, err := Load()
	if err == nil {
		t.Fatal("Load with nothing set must fail")
	}
	for _, k := range []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "REDIS_ADDR", "MINIO_ENDPOINT", "JWT_SIGNING_KEY"} {
		if !strings.Contains(err.Error(), k+" is required") {
			t.Errorf("Load error does not name %s: %v", k, err)
		}
	}
}

func TestLoadWith_RequestedGroupStillFails(t *testing.T) {
	cases := map[string]struct {
		need Requirements
		want string
	}{
		"postgres": {Requirements{Postgres: true}, "POSTGRES_DSN is required"},
		"redis":    {Requirements{Redis: true}, "REDIS_ADDR is required"},
		"minio":    {Requirements{MinIO: true}, "MINIO_ENDPOINT is required"},
		"jwt":      {Requirements{JWT: true}, "JWT_SIGNING_KEY is required"},
		"kafka":    {Requirements{Kafka: true}, "KAFKA_BROKERS is required"},
		"mongo":    {Requirements{Mongo: true}, "MONGO_URI is required"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			clearRequiredEnv(t)
			_, err := LoadWith(c.need)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
			if name != "postgres" && strings.Contains(err.Error(), "POSTGRES_DSN") {
				t.Errorf("unrequested group reported: %v", err)
			}
		})
	}
}

func TestLoadWith_UnrequestedGroupIsNotValidated(t *testing.T) {
	clearRequiredEnv(t)
	t.Setenv("JWT_SIGNING_KEY", "short")
	t.Setenv("ARGON2_ITERATIONS", "0")
	t.Setenv("POSTGRES_MAX_OPEN_CONNS", "1")
	if _, err := LoadWith(Requirements{}); err != nil {
		t.Fatalf("unrequested groups must not be validated: %v", err)
	}
	t.Setenv("POSTGRES_DSN", "x")
	t.Setenv("POSTGRES_SEARCH_PATH", "x")
	for want, need := range map[string]Requirements{
		"JWT_SIGNING_KEY must be at least 32 bytes": {JWT: true},
		"ARGON2_ITERATIONS must be between":         {Argon: true},
		"POSTGRES_MAX_OPEN_CONNS":                   {Postgres: true},
	} {
		_, err := LoadWith(need)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("requesting %+v: want %q, got %v", need, want, err)
		}
	}
}

func TestLoadWith_OptionalGroupsStillShapeValidated(t *testing.T) {
	clearRequiredEnv(t)
	t.Setenv("METRICS_HTTP_ADDR", "not-an-addr")
	if _, err := LoadWith(Requirements{}); err == nil {
		t.Fatal("a bad METRICS_HTTP_ADDR must fail every binary")
	}
}

// Each group requested alone reports only its own variables.
func TestLoadWith_KafkaAndMongoOnlyWhenRequested(t *testing.T) {
	clearRequiredEnv(t)
	if _, err := LoadWith(Requirements{}); err != nil {
		t.Fatalf("no groups: %v", err)
	}
	_, err := LoadWith(Requirements{Kafka: true})
	if err == nil || strings.Contains(err.Error(), "MONGO_URI") || !strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL is required") {
		t.Fatalf("Kafka only: %v", err)
	}
	_, err = LoadWith(Requirements{Mongo: true})
	if err == nil || strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("Mongo only: %v", err)
	}
	// The default Load asks for neither.
	setValidEnv(t)
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("SCHEMA_REGISTRY_URL", "")
	t.Setenv("MONGO_URI", "")
	if _, err := Load(); err != nil {
		t.Fatalf("Load must not require Kafka or Mongo: %v", err)
	}
}

// KafkaBrokers asks for the brokers only: a binary that never talks to the
// Schema Registry (the audit service) must not need a registry URL.
func TestLoadWith_KafkaBrokersGroupDoesNotNeedTheRegistry(t *testing.T) {
	clearRequiredEnv(t)
	_, err := LoadWith(Requirements{KafkaBrokers: true})
	if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS is required") || strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL") {
		t.Fatalf("brokers group, nothing set: %v", err)
	}
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	cfg, err := LoadWith(Requirements{KafkaBrokers: true})
	if err != nil {
		t.Fatalf("brokers group with only KAFKA_BROKERS set: %v", err)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.SchemaRegistryURL != "" {
		t.Fatalf("brokers=%v registry=%q", cfg.Kafka.Brokers, cfg.Kafka.SchemaRegistryURL)
	}
	t.Setenv("KAFKA_BROKERS", "not-an-address")
	if _, err := LoadWith(Requirements{KafkaBrokers: true}); err == nil {
		t.Fatal("a malformed broker address must be refused")
	}
	// The full Kafka group still needs the registry.
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	if _, err := LoadWith(Requirements{Kafka: true}); err == nil || !strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL is required") {
		t.Fatalf("Kafka group without a registry: %v", err)
	}
}
