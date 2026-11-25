package harness

import (
	"fmt"
	"strings"
)

// The ten service binaries (docs/plans/microservices-split.md section 2).
// identity-admin is a CLI, not a process of the stack: Start runs it once.
const (
	IdentityAPI        = "identity-api"
	IdentityWorker     = "identity-worker"
	CourseAPI          = "course-api"
	CourseWorker       = "course-worker"
	AnalyticsAPI       = "analytics-api"
	AnalyticsWorker    = "analytics-worker"
	NotificationWorker = "notification-worker"
	PublishingWorker   = "publishing-worker"
	AuditAPI           = "audit-api"
	AuditWorker        = "audit-worker"
)

// Binaries lists the ten binaries in start order.
var Binaries = []string{
	IdentityAPI, IdentityWorker, CourseAPI, CourseWorker, AnalyticsAPI, AnalyticsWorker,
	NotificationWorker, PublishingWorker, AuditAPI, AuditWorker,
}

// spec says what one binary reads. The environment a process gets is built
// from these flags and nothing else, so it is exactly the configuration table
// of that binary (env.example section 2 and the cmd README): a variable a
// binary does not read is never set, and a binary that lost a required one in
// a refactor fails its start here as it would in production.
type spec struct {
	// schema is the Postgres schema and role ("identity" -> svc_identity,
	// search path "identity,public"); "" for a binary with no Postgres.
	schema string
	api    bool // serves the public API on HTTP_PORT
	redis  bool
	jwt    bool // JWT_SIGNING_KEY and JWT_ISSUER
	argon  bool // cheap ARGON2_* so a scenario can register dozens of users
	minio  bool // MINIO_* (course): required config, never reached by a scenario
	kafka  bool // KAFKA_BROKERS
	// registry adds SCHEMA_REGISTRY_URL (the audit binaries never talk to it).
	registry bool
	mongo    bool
	smtp     bool
	// temporal adds the Temporal host, namespace and workflow queue;
	// activityQueue adds the activity queue (course-api starts the workflow and
	// names it, course-worker serves it; the publishing worker takes it from the
	// workflow input).
	temporal      bool
	activityQueue bool
	// groups is how many Kafka consumer groups the process starts: its
	// readiness waits for that many "kafka consumer started" log lines.
	groups int
	// strictReady: /health/ready must say "ready", not "degraded".
	strictReady bool
	// pgConns is POSTGRES_MAX_OPEN_CONNS (relay workers hold two connections
	// each; every consumer group runs up to four handlers).
	pgConns int
}

var specs = map[string]spec{
	IdentityAPI:        {schema: "identity", api: true, redis: true, jwt: true, argon: true, pgConns: 8},
	IdentityWorker:     {schema: "identity", redis: true, kafka: true, registry: true, pgConns: 12},
	CourseAPI:          {schema: "course", api: true, redis: true, jwt: true, minio: true, temporal: true, activityQueue: true, pgConns: 8},
	CourseWorker:       {schema: "course", redis: true, minio: true, kafka: true, registry: true, mongo: true, temporal: true, activityQueue: true, groups: 3, pgConns: 30},
	AnalyticsAPI:       {schema: "analytics", api: true, redis: true, jwt: true, mongo: true, pgConns: 8},
	AnalyticsWorker:    {schema: "analytics", redis: true, kafka: true, registry: true, mongo: true, groups: 1, pgConns: 20},
	NotificationWorker: {schema: "notification", redis: true, kafka: true, registry: true, mongo: true, smtp: true, groups: 1, pgConns: 20},
	PublishingWorker:   {temporal: true, strictReady: true},
	AuditAPI:           {api: true, redis: true, jwt: true, kafka: true, mongo: true},
	AuditWorker:        {kafka: true, mongo: true, groups: 1},
}

// ConsumerGroups are the Kafka consumer groups of the system, one per
// consumer name (RFC 6.5), each owned by one binary's process.
var ConsumerGroups = []string{
	"analytics-aggregator", "audit-archiver", "certificate-issuer",
	"course-instructor-mirror", "enrollment-course-archival", "notification-dispatcher",
}

// ConsumerOwner is the binary whose process runs a consumer group.
var ConsumerOwner = map[string]string{
	"analytics-aggregator":       AnalyticsWorker,
	"audit-archiver":             AuditWorker,
	"certificate-issuer":         CourseWorker,
	"course-instructor-mirror":   CourseWorker,
	"enrollment-course-archival": CourseWorker,
	"notification-dispatcher":    NotificationWorker,
}

// RetryTopic is the topic a consumer reads replayed events from.
func RetryTopic(consumer string) string { return "retry." + consumer }

// processedSchema is the schema whose processed_events table guards a
// consumer's effects. The audit archiver has none: it is idempotent by event
// id in MongoDB (ProcessedCount asks the archive instead).
var processedSchema = map[string]string{
	"analytics-aggregator":       "analytics",
	"certificate-issuer":         "course",
	"course-instructor-mirror":   "course",
	"enrollment-course-archival": "course",
	"notification-dispatcher":    "notification",
}

// envFor builds the environment of one binary. httpPort is the API port (0 for
// a binary with no API), metricsPort its internal listener.
func (s *Stack) envFor(name string, httpPort, metricsPort int) []string {
	sp := specs[name]
	env := []string{
		"APP_ENV=development",
		"LOG_LEVEL=info",
		fmt.Sprintf("METRICS_HTTP_ADDR=127.0.0.1:%d", metricsPort),
	}
	if s.Jaeger != nil {
		env = append(env, "OTEL_EXPORTER_OTLP_ENDPOINT="+s.Jaeger.OTLPEndpoint, "OTEL_TRACES_SAMPLER_ARG=1")
	}
	if sp.schema != "" {
		// The binary's own role: it can reach its own schema and nothing else.
		env = append(env,
			"POSTGRES_DSN="+s.serviceDSN[sp.schema],
			"POSTGRES_SEARCH_PATH="+sp.schema+",public",
			fmt.Sprintf("POSTGRES_MAX_OPEN_CONNS=%d", sp.pgConns),
		)
	}
	if sp.redis {
		env = append(env, "REDIS_ADDR="+s.redisAddr)
	}
	if sp.jwt {
		env = append(env, "JWT_SIGNING_KEY="+s.jwtKey, "JWT_ISSUER=smartcourse-e2e")
	}
	if sp.argon {
		env = append(env, "ARGON2_MEMORY_KIB=8192", "ARGON2_ITERATIONS=1", "ARGON2_PARALLELISM=1")
	}
	if sp.minio {
		// Object storage is never reached by these scenarios (no lesson has an
		// asset); the clients are lazy, so any endpoint boots.
		env = append(env, "MINIO_ENDPOINT=http://127.0.0.1:1", "MINIO_ACCESS_KEY_ID=e2e", "MINIO_SECRET_ACCESS_KEY=e2e-secret-key")
	}
	if sp.kafka {
		env = append(env, "KAFKA_BROKERS="+strings.Join(s.Kafka.Brokers, ","))
	}
	if sp.registry {
		env = append(env, "SCHEMA_REGISTRY_URL="+s.RegistryURL)
	}
	if sp.mongo {
		env = append(env, "MONGO_URI="+s.MongoURI)
	}
	if sp.smtp {
		host, port, _ := strings.Cut(s.Mailpit.SMTPAddr, ":")
		env = append(env, "SMTP_HOST="+host, "SMTP_PORT="+port)
	}
	if sp.temporal {
		env = append(env, "TEMPORAL_HOST_PORT="+s.TemporalAddr, "TEMPORAL_NAMESPACE=default", "TEMPORAL_TASK_QUEUE="+s.workflowQ)
	}
	if sp.activityQueue {
		env = append(env, "TEMPORAL_ACTIVITY_QUEUE="+s.activityQ)
	}
	if sp.api {
		env = append(env, fmt.Sprintf("HTTP_PORT=%d", httpPort),
			// The services see the test as a loopback proxy and believe the
			// X-Forwarded-For each request carries (see ipSeq in api.go), so the
			// per-IP rate limits on register and login do not throttle the suite.
			"TRUSTED_PROXIES=127.0.0.1")
	}
	// Options, most general first: a binary's own override wins.
	if strings.HasSuffix(name, "-worker") {
		env = mergeEnv(env, s.opts.WorkerEnv)
	}
	if strings.HasSuffix(name, "-api") {
		env = mergeEnv(env, s.opts.APIEnv)
	}
	return mergeEnv(env, s.opts.BinaryEnv[name])
}

// mergeEnv appends over, so a later KEY=value wins (the last duplicate is the
// one a process sees).
func mergeEnv(env []string, over map[string]string) []string {
	for k, v := range over {
		env = append(env, k+"="+v)
	}
	return env
}
