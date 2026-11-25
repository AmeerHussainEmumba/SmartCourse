package main

import (
	"strings"
	"testing"
)

// audit-worker needs MongoDB and Kafka and nothing else: no POSTGRES_*, MINIO_*,
// REDIS_ADDR, JWT_SIGNING_KEY or SCHEMA_REGISTRY_URL has to be set.
func TestLoadConfig_NeedsOnlyMongoAndKafka(t *testing.T) {
	for _, k := range []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY_ID",
		"MINIO_SECRET_ACCESS_KEY", "REDIS_ADDR", "JWT_SIGNING_KEY", "SCHEMA_REGISTRY_URL", "EVENT_TRANSPORT"} {
		t.Setenv(k, "")
	}
	t.Setenv("MONGO_URI", "mongodb://u:topsecret@127.0.0.1:27017")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig with only Mongo and Kafka set: %v", err)
	}
	if len(cfg.Kafka.Brokers) != 1 || !cfg.Mongo.Configured() {
		t.Errorf("brokers %v, mongo configured %v", cfg.Kafka.Brokers, cfg.Mongo.Configured())
	}

	t.Setenv("MONGO_URI", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "MONGO_URI") {
		t.Errorf("without MONGO_URI: %v, want an error naming it", err)
	}
	t.Setenv("MONGO_URI", "mongodb://u:topsecret@127.0.0.1:27017")
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Errorf("without KAFKA_BROKERS: %v, want an error naming it", err)
	}
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("MONGO_URI", "http://u:topsecret@127.0.0.1")
	if _, err := loadConfig(); err == nil || strings.Contains(err.Error(), "topsecret") {
		t.Errorf("a malformed MONGO_URI must be refused without echoing the URI: %v", err)
	}
}
