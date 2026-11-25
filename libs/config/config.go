// Package config loads SmartCourse's runtime configuration from the environment.
//
// Fail-fast by design (RFC §11.8): every required variable is validated at
// startup, and no secret has a default value. A missing JWT signing key must
// prevent boot, not silently fall back to a development constant.
package config

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env string // "development" | "test" | "production"

	HTTPPort int

	Postgres PostgresConfig
	Redis    RedisConfig
	MinIO    MinIOConfig
	Temporal TemporalConfig
	Relay    RelayConfig
	SMTP     SMTPConfig
	Asynq    AsynqConfig
	Jobs     JobsConfig
	Mongo    MongoConfig
	Kafka    KafkaConfig

	Telemetry TelemetryConfig

	JWT   JWTConfig
	Argon Argon2Config

	CORSAllowedOrigins []string

	// TrustedProxies lists the CIDRs/IPs of reverse proxies whose
	// X-Forwarded-For / X-Real-IP headers may be believed (TRUSTED_PROXIES,
	// comma-separated). Default none: the TCP peer address is the client IP.
	TrustedProxies []string

	// MaxBodyBytes caps request bodies (MAX_BODY_BYTES, default 1 MiB).
	MaxBodyBytes int64
	// HTTP server timeouts (HTTP_READ_TIMEOUT, HTTP_WRITE_TIMEOUT,
	// HTTP_IDLE_TIMEOUT).
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	HTTPIdleTimeout  time.Duration
	// EnableHSTS sends Strict-Transport-Security (HSTS_ENABLED); set it only
	// when the service is served over TLS (directly or via a TLS terminator).
	EnableHSTS bool
}

type PostgresConfig struct {
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	// SearchPath sets the Postgres connection's search_path (schema-per-
	// service, ADR-0024) — e.g. "course,public". Differs per binary (set
	// in docker-compose.yml / Makefile run-* targets), which is why it's
	// not hardcoded here the way table names aren't schema-qualified in
	// any repository.go: the connection resolves bare table names against
	// whichever schema(s) this binary was started with.
	SearchPath string
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
	// PublicEndpoint is the endpoint written into pre-signed URLs handed to
	// browsers. Endpoint is what the service itself dials (inside Compose,
	// http://minio:9000, unreachable from a client); the two differ whenever
	// the service reaches MinIO over an internal name. Empty means "same as
	// Endpoint". Optional (MINIO_PUBLIC_ENDPOINT).
	PublicEndpoint string
}

// TemporalConfig is deliberately all-optional, unlike Postgres/Redis above:
// Temporal backs course publishing only (ADR-0009, ADR-0025) — identity-api
// and analytics-api never touch it, and even course-api/worker only need a
// reachable server at the moment a publish actually runs, not at boot
// (temporal.Connect uses client.NewLazyClient for exactly this
// reason). Requiring these at startup the way POSTGRES_DSN is required
// would make every binary's boot depend on a service only one workflow
// uses.
type TemporalConfig struct {
	HostPort  string
	Namespace string
	// TaskQueue is TEMPORAL_TASK_QUEUE, the WORKFLOW queue (default
	// "publish-workflows"): the queue the publish workflow is started on and
	// the orchestrating worker polls.
	TaskQueue string
	// ActivityQueue is TEMPORAL_ACTIVITY_QUEUE (default "course-activities"): the
	// queue the workflow schedules its activities on and the activity worker
	// polls. Both defaults equal libs/contracts/publishing's constants.
	ActivityQueue string
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

	// MaxConcurrent bounds how many argon2 hashes run at once (F-26 residue:
	// 16 concurrent logins pinned about 1 GiB). Peak argon2 memory is about
	// MaxConcurrent x MemoryKiB. Zero means "use DefaultArgon2MaxConcurrent"
	// (consumers apply it), so hand-built configs in tests keep working.
	MaxConcurrent int
	// QueueTimeout is how long a request waits for a free slot before the
	// caller answers 503. Zero means DefaultArgon2QueueTimeout.
	QueueTimeout time.Duration
}

const (
	// DefaultArgon2QueueTimeout is deliberately shorter than the default
	// HTTP write timeout (30s) so a queued request is refused cleanly rather
	// than cut off mid-wait.
	DefaultArgon2QueueTimeout = 5 * time.Second
	maxArgonConcurrent        = 1024
	maxArgonQueueTimeout      = time.Minute
)

// DefaultArgon2MaxConcurrent derives a slot count from the CPU budget the Go
// runtime sees (cgroup-aware): one hash uses `parallelism` threads, so
// GOMAXPROCS/parallelism hashes saturate the CPUs. Clamped to [2, 8] so a
// one-CPU container still overlaps a hash with request handling and a big host
// does not pin gigabytes (8 x 64 MiB = 512 MiB with the default memory cost).
func DefaultArgon2MaxConcurrent(gomaxprocs, parallelism int) int {
	if parallelism < 1 {
		parallelism = 1
	}
	n := gomaxprocs / parallelism
	if n < 2 {
		n = 2
	}
	if n > 8 {
		n = 8
	}
	return n
}

// Load reads and validates configuration from the environment. It returns an
// error rather than panicking so callers (including tests) can decide how to
// fail; every cmd/* binary treats a non-nil error as fatal at boot.
func Load() (*Config, error) { return LoadWith(AllRequirements()) }

// Requirements declares which required-variable groups a binary needs. A group
// that is not requested is neither required nor validated: its fields keep
// their zero value (or documented default) and a bad value in it cannot stop
// the binary. Optional settings (Temporal, Telemetry, SMTP, Asynq, Jobs, the
// Kafka and Mongo tuning, HTTP limits) are always read and shape-validated, as
// in Load; Kafka and Mongo only add the "required" rule when requested.
type Requirements struct {
	// Postgres requires POSTGRES_DSN and POSTGRES_SEARCH_PATH and checks the
	// pool sizes (including the relay pool rule).
	Postgres bool
	// Redis requires REDIS_ADDR.
	Redis bool
	// MinIO requires MINIO_ENDPOINT, MINIO_ACCESS_KEY_ID and MINIO_SECRET_ACCESS_KEY.
	MinIO bool
	// JWT requires JWT_SIGNING_KEY (length and strength rules) and validates the TTLs.
	JWT bool
	// Argon validates the ARGON2_* settings.
	Argon bool
	// Kafka requires KAFKA_BROKERS (host:port seeds) and SCHEMA_REGISTRY_URL
	// (http(s) URL) and validates them. The consumer/producer tuning is not part
	// of the group: it always keeps its defaults.
	Kafka bool
	// Mongo requires MONGO_URI. Its shape and the timeouts are validated whenever set.
	Mongo bool
}

// AllRequirements is what Load asks for: Postgres, Redis, MinIO, JWT and Argon.
// Kafka and Mongo stay optional there, as before (a binary that needs them asks
// through LoadWith).
func AllRequirements() Requirements {
	return Requirements{Postgres: true, Redis: true, MinIO: true, JWT: true, Argon: true}
}

// LoadWith is Load for a binary that needs only some groups, e.g. the
// publishing worker, which needs none: LoadWith(config.Requirements{}). Load is
// LoadWith(AllRequirements()) and behaves exactly as before.
func LoadWith(need Requirements) (*Config, error) {
	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, fmt.Sprintf("%s is required", key))
		}
		return v
	}
	reqIf := func(on bool, key string) string {
		if on {
			return req(key)
		}
		return os.Getenv(key)
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

	// Range-validate before narrowing: uint32(-1) wraps to ~4 TiB and a zero
	// value panics inside argon2 (F-26). Skipped when the binary does not hash.
	argonMem, argonIter, argonPar := 64*1024, 3, 2
	argonConc, argonQueue := DefaultArgon2MaxConcurrent(runtime.GOMAXPROCS(0), argonPar), DefaultArgon2QueueTimeout
	inRange := func(key string, v, lo, hi int) {
		if v < lo || v > hi {
			errs = append(errs, fmt.Sprintf("%s must be between %d and %d", key, lo, hi))
		}
	}
	if need.Argon {
		argonMem = optInt("ARGON2_MEMORY_KIB", 64*1024)
		argonIter = optInt("ARGON2_ITERATIONS", 3)
		argonPar = optInt("ARGON2_PARALLELISM", 2)
		inRange("ARGON2_MEMORY_KIB", argonMem, 8, maxArgonMemoryKiB)
		inRange("ARGON2_ITERATIONS", argonIter, 1, maxArgonIterations)
		inRange("ARGON2_PARALLELISM", argonPar, 1, maxArgonParallelism)
		parForDefault := argonPar
		if parForDefault < 1 || parForDefault > maxArgonParallelism {
			parForDefault = 1
		}
		argonConc = optInt("ARGON2_MAX_CONCURRENT", DefaultArgon2MaxConcurrent(runtime.GOMAXPROCS(0), parForDefault))
		inRange("ARGON2_MAX_CONCURRENT", argonConc, 1, maxArgonConcurrent)
		argonQueue = optDuration("ARGON2_QUEUE_TIMEOUT", DefaultArgon2QueueTimeout)
		if argonQueue <= 0 || argonQueue > maxArgonQueueTimeout {
			errs = append(errs, "ARGON2_QUEUE_TIMEOUT must be greater than zero and at most 1m")
		}
		if argonPar >= 1 && argonPar <= maxArgonParallelism && argonMem < 8*argonPar {
			errs = append(errs, "ARGON2_MEMORY_KIB must be at least 8 * ARGON2_PARALLELISM")
		}
	}

	cfg := &Config{
		Env:      envOr("APP_ENV", "development"),
		HTTPPort: optInt("HTTP_PORT", 8080),
		Postgres: PostgresConfig{
			DSN:          reqIf(need.Postgres, "POSTGRES_DSN"),
			MaxOpenConns: optInt("POSTGRES_MAX_OPEN_CONNS", 20),
			MaxIdleConns: optInt("POSTGRES_MAX_IDLE_CONNS", 10),
			SearchPath:   reqIf(need.Postgres, "POSTGRES_SEARCH_PATH"),
		},
		Redis: RedisConfig{
			Addr:     reqIf(need.Redis, "REDIS_ADDR"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       optInt("REDIS_DB", 0),
		},
		MinIO: MinIOConfig{
			Endpoint:        reqIf(need.MinIO, "MINIO_ENDPOINT"),
			AccessKeyID:     reqIf(need.MinIO, "MINIO_ACCESS_KEY_ID"),
			SecretAccessKey: reqIf(need.MinIO, "MINIO_SECRET_ACCESS_KEY"),
			UseSSL:          optBool("MINIO_USE_SSL", false),
			BucketAssets:    envOr("MINIO_BUCKET_ASSETS", "smartcourse-assets"),
			PublicEndpoint:  os.Getenv("MINIO_PUBLIC_ENDPOINT"),
		},
		Temporal: TemporalConfig{
			HostPort:      envOr("TEMPORAL_HOST_PORT", "temporal:7233"),
			Namespace:     envOr("TEMPORAL_NAMESPACE", "default"),
			TaskQueue:     envOr("TEMPORAL_TASK_QUEUE", "publish-workflows"),
			ActivityQueue: envOr("TEMPORAL_ACTIVITY_QUEUE", "course-activities"),
		},
		JWT: JWTConfig{
			SigningKey:      reqIf(need.JWT, "JWT_SIGNING_KEY"),
			AccessTokenTTL:  optDuration("JWT_ACCESS_TTL", 15*time.Minute),
			RefreshTokenTTL: optDuration("JWT_REFRESH_TTL", 7*24*time.Hour),
			Issuer:          envOr("JWT_ISSUER", "smartcourse"),
		},
		Argon: Argon2Config{
			MemoryKiB:   uint32(argonMem),
			Iterations:  uint32(argonIter),
			Parallelism: uint8(argonPar),
			SaltLength:  16,
			KeyLength:   32,

			MaxConcurrent: argonConc,
			QueueTimeout:  argonQueue,
		},
		CORSAllowedOrigins: splitAndTrim(os.Getenv("CORS_ALLOWED_ORIGINS")),
		TrustedProxies:     splitAndTrim(os.Getenv("TRUSTED_PROXIES")),
		MaxBodyBytes:       int64(optInt("MAX_BODY_BYTES", 1<<20)),
		HTTPReadTimeout:    optDuration("HTTP_READ_TIMEOUT", 15*time.Second),
		HTTPWriteTimeout:   optDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		HTTPIdleTimeout:    optDuration("HTTP_IDLE_TIMEOUT", 120*time.Second),
		EnableHSTS:         optBool("HSTS_ENABLED", false),
	}

	cfg.Relay.Workers = optInt("RELAY_WORKERS", defaultRelayWorkers)
	inRange("RELAY_WORKERS", cfg.Relay.Workers, 1, maxRelayWorkers)
	if msg := relayPoolError(cfg.Postgres.MaxOpenConns, cfg.Relay.Workers); need.Postgres && msg != "" {
		errs = append(errs, msg)
	}

	var smtpErrs []string
	cfg.SMTP, smtpErrs = loadSMTP()
	errs = append(errs, smtpErrs...)
	cfg.Asynq, cfg.Jobs = loadAsynq(optInt, optDuration, &errs)
	var mongoErrs []string
	cfg.Mongo, mongoErrs = loadMongo(need.Mongo, os.Getenv, optDuration)
	var telemetryErrs []string
	cfg.Telemetry, telemetryErrs = loadTelemetry(os.Getenv, optBool)
	errs = append(errs, telemetryErrs...)
	errs = append(errs, mongoErrs...)
	cfg.Kafka = loadKafka(need.Kafka, os.Getenv, optInt, optDuration, &errs)

	if need.JWT {
		if len(cfg.JWT.SigningKey) < 32 {
			// Only enforced once the key is present; the presence check above
			// already reported a missing key.
			if cfg.JWT.SigningKey != "" {
				errs = append(errs, "JWT_SIGNING_KEY must be at least 32 bytes")
			}
		}

		if cfg.JWT.AccessTokenTTL <= 0 {
			errs = append(errs, "JWT_ACCESS_TTL must be greater than zero")
		}
		if cfg.JWT.RefreshTokenTTL <= 0 {
			errs = append(errs, "JWT_REFRESH_TTL must be greater than zero")
		}
	}

	if cfg.MaxBodyBytes <= 0 {
		errs = append(errs, "MAX_BODY_BYTES must be greater than zero")
	}
	for key, d := range map[string]time.Duration{
		"HTTP_READ_TIMEOUT": cfg.HTTPReadTimeout, "HTTP_WRITE_TIMEOUT": cfg.HTTPWriteTimeout, "HTTP_IDLE_TIMEOUT": cfg.HTTPIdleTimeout,
	} {
		if d <= 0 {
			errs = append(errs, key+" must be greater than zero")
		}
	}
	if need.JWT && cfg.Env != "development" && len(cfg.JWT.SigningKey) >= 32 {
		if reason := weakSigningKey(cfg.JWT.SigningKey); reason != "" {
			errs = append(errs, "JWT_SIGNING_KEY is not acceptable outside APP_ENV=development: "+reason)
		}
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

const (
	maxArgonMemoryKiB   = 1 << 20 // 1 GiB per hash
	maxArgonIterations  = 100
	maxArgonParallelism = 64
)

// knownPlaceholderKeyMarkers are fragments of the signing keys published in
// env.example and docker-compose.yml (and typical placeholders); a key
// containing any of them is a public value.
var knownPlaceholderKeyMarkers = []string{
	"replace-with", "local-dev-only", "do-not-use", "changeme", "change-me",
	"your-secret", "your_secret", "example", "placeholder",
}

// weakSigningKey returns a non-empty reason when key is a known placeholder
// or obviously low-entropy. It never echoes the key.
func weakSigningKey(key string) string {
	lower := strings.ToLower(key)
	for _, m := range knownPlaceholderKeyMarkers {
		if strings.Contains(lower, m) {
			return "it looks like a published placeholder/default key"
		}
	}
	freq := map[rune]int{}
	n := 0
	for _, r := range key {
		freq[r]++
		n++
	}
	if len(freq) < 10 {
		return "it has too few distinct characters"
	}
	var h float64
	for _, c := range freq {
		p := float64(c) / float64(n)
		h -= p * math.Log2(p)
	}
	if h < 3.0 {
		return "its entropy is too low"
	}
	return ""
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
