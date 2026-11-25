// Package auditconfig loads the audit binaries' configuration.
//
// STOPGAP. libs/config.Load still demands settings the audit service does not
// have (POSTGRES_DSN, POSTGRES_SEARCH_PATH, MINIO_*), reads the Kafka brokers
// only when EVENT_TRANSPORT=kafka and then also demands SCHEMA_REGISTRY_URL.
// Until libs/config grows a per-service loader (owned by another agent), these
// functions fill the unused settings with inert placeholders in the process
// environment before calling config.Load, and check the settings the binary
// really needs. Nothing connects to Postgres, MinIO or the registry: the
// placeholders are never dialled. Replace with the real loader when it lands.
package auditconfig

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

const unusedURL = "http://unused.invalid"

// placeholders are set only when the variable is empty.
var placeholders = map[string]string{
	"POSTGRES_DSN":            "postgres://unused:unused@unused.invalid:5432/unused",
	"POSTGRES_SEARCH_PATH":    "public",
	"MINIO_ENDPOINT":          "unused.invalid:9000",
	"MINIO_ACCESS_KEY_ID":     "unused",
	"MINIO_SECRET_ACCESS_KEY": "unused",
	"SCHEMA_REGISTRY_URL":     unusedURL,
}

func fill(extra map[string]string) {
	for k, v := range placeholders {
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	for k, v := range extra {
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	// The Kafka brokers are only parsed in kafka mode, and Kafka is the only
	// transport of this service.
	_ = os.Setenv("EVENT_TRANSPORT", config.TransportKafka)
}

func need(cfg *config.Config, err error, vars ...string) (*config.Config, error) {
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, v := range vars {
		if strings.TrimSpace(os.Getenv(v)) == "" {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s is required", strings.Join(missing, ", "))
	}
	if !cfg.Mongo.Configured() {
		return nil, errors.New("MONGO_URI is required")
	}
	return cfg, nil
}

// LoadAPI is the configuration of audit-api: MongoDB, Redis (the token-version
// check), the JWT key and Kafka (the replay producer).
func LoadAPI() (*config.Config, error) {
	fill(nil)
	cfg, err := config.Load()
	return need(cfg, err, "REDIS_ADDR", "JWT_SIGNING_KEY", "KAFKA_BROKERS")
}

// LoadWorker is the configuration of audit-worker: MongoDB and Kafka only. Redis
// and the JWT key are not used; placeholders satisfy the shared loader.
func LoadWorker() (*config.Config, error) {
	fill(map[string]string{
		"REDIS_ADDR":     "unused.invalid:6379",
		"JWT_SIGNING_KEY": "unused-by-audit-worker-0123456789abcdef",
	})
	cfg, err := config.Load()
	return need(cfg, err, "KAFKA_BROKERS")
}
