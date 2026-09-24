// Package config loads SmartCourse's runtime configuration from the environment.
//
// Fail-fast by design (RFC §11.8): every required variable is validated at
// startup, and no secret has a default value. A missing JWT signing key must
// prevent boot, not silently fall back to a development constant.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env  string // "development" | "test" | "production"
	Mode string // "api" | "worker" — set by CLI flag, not env; see cmd/smartcourse

	HTTPPort int

	Postgres PostgresConfig
	Redis    RedisConfig
	MinIO    MinIOConfig

	JWT   JWTConfig
	Argon Argon2Config

	CORSAllowedOrigins []string
}

type PostgresConfig struct {
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketAssets    string
}

type JWTConfig struct {
	SigningKey      string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
	Issuer          string
}

type Argon2Config struct {
	MemoryKiB   uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// Load reads and validates configuration from the environment. It returns an
// error rather than panicking so callers (including tests) can decide how to
// fail; cmd/smartcourse treats a non-nil error as fatal at boot.
func Load() (*Config, error) {
	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, fmt.Sprintf("%s is required", key))
		}
		return v
	}
	optInt := func(key string, def int) int {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be an integer: %v", key, err))
			return def
		}
		return n
	}
	optBool := func(key string, def bool) bool {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a boolean: %v", key, err))
			return def
		}
		return b
	}
	optDuration := func(key string, def time.Duration) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a duration (e.g. 15m): %v", key, err))
			return def
		}
		return d
	}

	cfg := &Config{
		Env:      envOr("APP_ENV", "development"),
		HTTPPort: optInt("HTTP_PORT", 8080),
		Postgres: PostgresConfig{
			DSN:          req("POSTGRES_DSN"),
			MaxOpenConns: optInt("POSTGRES_MAX_OPEN_CONNS", 20),
			MaxIdleConns: optInt("POSTGRES_MAX_IDLE_CONNS", 10),
		},
		Redis: RedisConfig{
			Addr:     req("REDIS_ADDR"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       optInt("REDIS_DB", 0),
		},
		MinIO: MinIOConfig{
			Endpoint:        req("MINIO_ENDPOINT"),
			AccessKeyID:     req("MINIO_ACCESS_KEY_ID"),
			SecretAccessKey: req("MINIO_SECRET_ACCESS_KEY"),
			UseSSL:          optBool("MINIO_USE_SSL", false),
			BucketAssets:    envOr("MINIO_BUCKET_ASSETS", "smartcourse-assets"),
		},
		JWT: JWTConfig{
			SigningKey:      req("JWT_SIGNING_KEY"),
			AccessTokenTTL:  optDuration("JWT_ACCESS_TTL", 15*time.Minute),
			RefreshTokenTTL: optDuration("JWT_REFRESH_TTL", 7*24*time.Hour),
			Issuer:          envOr("JWT_ISSUER", "smartcourse"),
		},
		Argon: Argon2Config{
			MemoryKiB:   uint32(optInt("ARGON2_MEMORY_KIB", 64*1024)),
			Iterations:  uint32(optInt("ARGON2_ITERATIONS", 3)),
			Parallelism: uint8(optInt("ARGON2_PARALLELISM", 2)),
			SaltLength:  16,
			KeyLength:   32,
		},
		CORSAllowedOrigins: splitAndTrim(os.Getenv("CORS_ALLOWED_ORIGINS")),
	}

	if len(cfg.JWT.SigningKey) < 32 {
		// Only enforced once the key is present; the presence check above
		// already reported a missing key.
		if cfg.JWT.SigningKey != "" {
			errs = append(errs, "JWT_SIGNING_KEY must be at least 32 bytes")
		}
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitAndTrim(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
