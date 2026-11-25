// What this checks: analytics-api loads its configuration with only the variables of
// the groups it needs (requirements), and names the missing variable of each of
// them at startup; the variables of the groups it does not need are never
// required.
package main

import (
	"strings"
	"testing"
)

// ownEnv is the minimal environment of this binary.
var ownEnv = map[string]string{
	"POSTGRES_DSN":         "postgres://u:p@127.0.0.1:1/db",
	"POSTGRES_SEARCH_PATH": "x,public",
	"REDIS_ADDR":           "127.0.0.1:1",
	"JWT_SIGNING_KEY":      "0123456789abcdef0123456789abcdef-test",
}

// allGroupVars are the variables of every group some binary requires.
var allGroupVars = []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "REDIS_ADDR", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "JWT_SIGNING_KEY", "KAFKA_BROKERS", "SCHEMA_REGISTRY_URL", "MONGO_URI"}

func setOnly(t *testing.T, env map[string]string, skip string) {
	t.Helper()
	for _, k := range allGroupVars {
		t.Setenv(k, "")
	}
	for k, v := range env {
		if k != skip {
			t.Setenv(k, v)
		}
	}
}

func TestLoadConfig_OnlyOwnVariables(t *testing.T) {
	setOnly(t, ownEnv, "")
	if _, err := loadConfig(); err != nil {
		t.Fatalf("analytics-api must start with only its own variables set: %v", err)
	}
}

func TestLoadConfig_MissingVariableIsNamed(t *testing.T) {
	for k := range ownEnv {
		t.Run(k, func(t *testing.T) {
			setOnly(t, ownEnv, k)
			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), k+" is required") {
				t.Fatalf("without %s want %q, got %v", k, k+" is required", err)
			}
		})
	}
}
