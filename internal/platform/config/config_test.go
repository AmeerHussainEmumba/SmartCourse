// What this checks: Load()'s fail-fast promise (package doc comment,
// RFC §11.8) actually holds — every required var is validated, malformed
// optional vars are rejected rather than silently falling back to their
// default, and multiple problems are aggregated into one error rather than
// stopping at the first. No infra — pure env-var logic.
package config

import (
	"strings"
	"testing"
)

// setValidEnv sets every required var, plus a couple of optional ones, to
// values Load() accepts — the baseline every test in this file starts from
// before overriding the one thing it's actually testing.
func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")
	t.Setenv("POSTGRES_SEARCH_PATH", "course,public")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("MINIO_ENDPOINT", "http://localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "minioadmin")
	t.Setenv("JWT_SIGNING_KEY", strings.Repeat("x", 32))
}

func TestLoad_ValidEnv_Succeeds(t *testing.T) {
	setValidEnv(t)
	t.Setenv("POSTGRES_MAX_OPEN_CONNS", "50")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: unexpected error: %v", err)
	}

	if cfg.Postgres.DSN != "postgres://user:pass@localhost:5432/db" {
		t.Errorf("Postgres.DSN = %q", cfg.Postgres.DSN)
	}
	if cfg.Postgres.SearchPath != "course,public" {
		t.Errorf("Postgres.SearchPath = %q", cfg.Postgres.SearchPath)
	}
	// Explicitly set optional var must override its default.
	if cfg.Postgres.MaxOpenConns != 50 {
		t.Errorf("Postgres.MaxOpenConns = %d, want 50 (explicitly set)", cfg.Postgres.MaxOpenConns)
	}
	// Unset optional var must fall back to its documented default.
	if cfg.Postgres.MaxIdleConns != 10 {
		t.Errorf("Postgres.MaxIdleConns = %d, want 10 (default, unset)", cfg.Postgres.MaxIdleConns)
	}
	if cfg.Env != "development" {
		t.Errorf("Env = %q, want default %q", cfg.Env, "development")
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want default 8080", cfg.HTTPPort)
	}
}

func TestLoad_MissingRequiredVar_NamesItInError(t *testing.T) {
	requiredVars := []string{
		"POSTGRES_DSN",
		"POSTGRES_SEARCH_PATH",
		"REDIS_ADDR",
		"MINIO_ENDPOINT",
		"MINIO_ACCESS_KEY_ID",
		"MINIO_SECRET_ACCESS_KEY",
		"JWT_SIGNING_KEY",
	}
	for _, missing := range requiredVars {
		t.Run(missing, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(missing, "") // Getenv("") and Getenv(unset) are indistinguishable to req() — this IS "missing"

			_, err := Load()
			if err == nil {
				t.Fatalf("Load: expected an error with %s missing, got nil", missing)
			}
			if !strings.Contains(err.Error(), missing) {
				t.Errorf("Load error does not name %s: %v", missing, err)
			}
		})
	}
}

func TestLoad_MultipleRequiredVarsMissing_AggregatesAllInOneError(t *testing.T) {
	setValidEnv(t)
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("MINIO_ENDPOINT", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load: expected an error with three required vars missing, got nil")
	}
	for _, missing := range []string{"POSTGRES_DSN", "REDIS_ADDR", "MINIO_ENDPOINT"} {
		if !strings.Contains(err.Error(), missing) {
			t.Errorf("Load error does not name %s — aggregation lost it: %v", missing, err)
		}
	}
}

func TestLoad_JWTSigningKeyLength(t *testing.T) {
	t.Run("under 32 bytes is rejected", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("JWT_SIGNING_KEY", "too-short")

		_, err := Load()
		if err == nil {
			t.Fatal("Load: expected an error for a JWT_SIGNING_KEY under 32 bytes, got nil")
		}
		if !strings.Contains(err.Error(), "JWT_SIGNING_KEY must be at least 32 bytes") {
			t.Errorf("Load error does not mention the length requirement: %v", err)
		}
	})

	t.Run("exactly 32 bytes is accepted", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("JWT_SIGNING_KEY", strings.Repeat("y", 32))

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: unexpected error with a 32-byte key: %v", err)
		}
		if len(cfg.JWT.SigningKey) != 32 {
			t.Errorf("JWT.SigningKey length = %d, want 32", len(cfg.JWT.SigningKey))
		}
	})

	t.Run("missing key reports only the presence error, not also a length error", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("JWT_SIGNING_KEY", "")

		_, err := Load()
		if err == nil {
			t.Fatal("Load: expected an error for a missing JWT_SIGNING_KEY, got nil")
		}
		if strings.Contains(err.Error(), "at least 32 bytes") {
			t.Errorf("Load error mentions the length check for an absent key — config.go's own comment says that check only applies once the key is present: %v", err)
		}
	})
}

func TestLoad_MalformedOptionalVar_ReturnsErrorRatherThanSilentFallback(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"int", "POSTGRES_MAX_OPEN_CONNS", "not-a-number"},
		{"bool", "MINIO_USE_SSL", "not-a-bool"},
		{"duration", "JWT_ACCESS_TTL", "not-a-duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(tt.key, tt.val)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load: expected an error for %s=%q, got nil (the default must not be silently substituted)", tt.key, tt.val)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("Load error does not name %s: %v", tt.key, err)
			}
		})
	}
}
