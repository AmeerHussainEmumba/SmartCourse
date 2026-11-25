package main

import (
	"strings"
	"testing"
)

// audit-api needs Redis, the JWT key, MongoDB and Kafka and nothing else: no
// POSTGRES_*, MINIO_* or SCHEMA_REGISTRY_URL has to be set.
func TestLoadConfig_NeedsNoPostgresOrMinIO(t *testing.T) {
	for _, k := range []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY_ID",
		"MINIO_SECRET_ACCESS_KEY", "SCHEMA_REGISTRY_URL", "EVENT_TRANSPORT"} {
		t.Setenv(k, "")
	}
	t.Setenv("REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("JWT_SIGNING_KEY", "test-signing-key-0123456789-abcdefghij")
	t.Setenv("MONGO_URI", "mongodb://127.0.0.1:27017")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	if _, err := loadConfig(); err != nil {
		t.Fatalf("loadConfig without POSTGRES_*, MINIO_* and SCHEMA_REGISTRY_URL: %v", err)
	}

	t.Setenv("REDIS_ADDR", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "REDIS_ADDR") {
		t.Errorf("without REDIS_ADDR: %v, want an error naming it", err)
	}
	t.Setenv("REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("JWT_SIGNING_KEY", "short")
	if _, err := loadConfig(); err == nil {
		t.Error("a weak JWT_SIGNING_KEY must be refused")
	}
}
