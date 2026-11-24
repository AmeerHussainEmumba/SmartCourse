package harness

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// Options tunes a Stack. The zero value is the stack of E01: the Kafka
// transport, every consumer, mail to Mailpit, MongoDB for the archive.
type Options struct {
	// EventTransport is the worker's EVENT_TRANSPORT: "kafka" (default) or
	// "inprocess".
	EventTransport string
	// WorkerEnv adds to (and overrides) the worker's environment, for example
	// {"KAFKA_CONSUMER_MAX_ATTEMPTS": "3"}.
	WorkerEnv map[string]string
	// APIEnv adds to the environment of identity-api, course-api and
	// analytics-api.
	APIEnv map[string]string
	// Tracing starts a Jaeger container and points every binary's
	// OTEL_EXPORTER_OTLP_ENDPOINT at it with sampling 1 (E02). Off by
	// default: a scenario that does not read traces does not pay for it.
	Tracing bool
}

// Stack is a running system: containers, binaries, and the handles a scenario
// asserts through. Start builds one; its cleanup stops everything, and on a
// failed test dumps each process's output into the test log.
type Stack struct {
	// Postgres and Redis come from libs/testenv: DB sees every schema.
	Env *testenv.Env
	// Kafka is the scenario's broker (StopKafka and StartKafka drive it).
	Kafka *testenv.KafkaEnv
	// MongoURI is the connection string; Mongo queries it.
	MongoURI string
	Mongo    *Mongo
	Mailpit  *Mailpit
	// Jaeger is set when Options.Tracing is on.
	Jaeger *Jaeger

	RegistryURL  string
	TemporalAddr string

	// Process handles. Worker is replaced by StartWorker after a kill.
	Worker, Identity, Course, Analytics *Proc

	IdentityURL, CourseURL, AnalyticsURL string
	WorkerAdminURL, WorkerMetricsURL     string

	opts       Options
	mongoC     *mongoContainer
	postgresDS string
	redisAddr  string
	jwtKey     string
	taskQueue  string
	adminEmail string
}

// Start brings up a full stack for one scenario and returns when every
// process answers ready. It uses t.Cleanup for teardown.
//
// Order: shared infrastructure (Schema Registry, Temporal: once per test
// binary), this scenario's own Postgres (all migrations), Redis, Kafka,
// MongoDB and Mailpit, then worker, identity-api, course-api, analytics-api,
// then a bootstrapped admin account.
func Start(t *testing.T, opts Options) *Stack {
	t.Helper()
	if opts.EventTransport == "" {
		opts.EventTransport = "kafka"
	}
	began := time.Now()
	si := Shared(t)

	s := &Stack{opts: opts, RegistryURL: si.RegistryURL, TemporalAddr: si.TemporalAddr}
	s.Env = testenv.Setup(t)
	s.Kafka = testenv.SetupKafka(t)
	s.mongoC = startMongo(t)
	s.MongoURI = s.mongoC.URI
	s.Mongo = connectMongo(t, s.MongoURI)
	s.Mailpit = startMailpit(t)
	if opts.Tracing {
		s.Jaeger = startJaeger(t)
	}
	t.Logf("stack: containers ready after %s", time.Since(began).Round(time.Second))

	d, ok := s.Env.DB.Dialector.(*postgres.Dialector)
	if !ok {
		t.Fatalf("unexpected dialector %T", s.Env.DB.Dialector)
	}
	s.postgresDS = d.DSN
	s.redisAddr = s.Env.RDB.Options().Addr
	s.jwtKey = "e2e-signing-key-" + strings.ReplaceAll(uuid.NewString(), "-", "") // 48 bytes
	s.taskQueue = "e2e-publish-" + uuid.NewString()[:8]

	identityPort, coursePort, analyticsPort := mustPort(t), mustPort(t), mustPort(t)
	adminPort, metricsPort := mustPort(t), mustPort(t)
	s.IdentityURL = fmt.Sprintf("http://127.0.0.1:%d", identityPort)
	s.CourseURL = fmt.Sprintf("http://127.0.0.1:%d", coursePort)
	s.AnalyticsURL = fmt.Sprintf("http://127.0.0.1:%d", analyticsPort)
	s.WorkerAdminURL = fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	s.WorkerMetricsURL = fmt.Sprintf("http://127.0.0.1:%d", metricsPort)

	s.Worker = newProc("worker", Binary(t, "worker"), s.workerEnv(adminPort, metricsPort))
	s.Identity = newProc("identity-api", Binary(t, "identity-api"), s.apiEnv("identity,platform,public", identityPort))
	s.Course = newProc("course-api", Binary(t, "course-api"), s.apiEnv("course,platform,public", coursePort))
	s.Analytics = newProc("analytics-api", Binary(t, "analytics-api"), s.apiEnv("analytics,public", analyticsPort))

	t.Cleanup(func() {
		if t.Failed() {
			for _, p := range []*Proc{s.Worker, s.Identity, s.Course, s.Analytics} {
				p.dumpLogs(t)
			}
		}
		for _, p := range []*Proc{s.Worker, s.Analytics, s.Course, s.Identity} {
			p.Stop(15 * time.Second)
		}
	})

	for _, p := range []*Proc{s.Worker, s.Identity, s.Course, s.Analytics} {
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
	}
	s.WaitWorkerReady(t)
	for name, url := range map[string]string{"identity-api": s.IdentityURL, "course-api": s.CourseURL, "analytics-api": s.AnalyticsURL} {
		Eventually(t, time.Minute, name+" ready", func() error { return CheckReady(url) })
	}
	s.bootstrapAdmin(t)
	t.Logf("stack: ready after %s", time.Since(began).Round(time.Second))
	return s
}

func mustPort(t testing.TB) int {
	t.Helper()
	p, err := freePort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	return p
}

// baseEnv is what every binary reads: the same variables docker-compose.yml
// sets, pointed at the containers.
func (s *Stack) baseEnv(searchPath string) []string {
	env := s.staticEnv(searchPath)
	if s.Jaeger != nil {
		env = append(env, "OTEL_EXPORTER_OTLP_ENDPOINT="+s.Jaeger.OTLPEndpoint, "OTEL_TRACES_SAMPLER_ARG=1")
	}
	return env
}

func (s *Stack) staticEnv(searchPath string) []string {
	return []string{
		"APP_ENV=development",
		"LOG_LEVEL=info",
		"POSTGRES_DSN=" + s.postgresDS,
		"POSTGRES_SEARCH_PATH=" + searchPath,
		"POSTGRES_MAX_OPEN_CONNS=12",
		"REDIS_ADDR=" + s.redisAddr,
		// Object storage is never reached by these scenarios (no lesson has an
		// asset); the clients are lazy, so any endpoint boots.
		"MINIO_ENDPOINT=http://127.0.0.1:1",
		"MINIO_ACCESS_KEY_ID=e2e",
		"MINIO_SECRET_ACCESS_KEY=e2e-secret-key",
		"JWT_SIGNING_KEY=" + s.jwtKey,
		"JWT_ISSUER=smartcourse-e2e",
		// Cheap hashing: a scenario registers dozens of users.
		"ARGON2_MEMORY_KIB=8192", "ARGON2_ITERATIONS=1", "ARGON2_PARALLELISM=1",
		"TEMPORAL_HOST_PORT=" + s.TemporalAddr,
		"TEMPORAL_NAMESPACE=default",
		"TEMPORAL_TASK_QUEUE=" + s.taskQueue,
		"MONGO_URI=" + s.MongoURI,
		"METRICS_ENABLED=true",
		// The services see the test as a loopback proxy and believe the
		// X-Forwarded-For each request carries (see ipSeq in api.go), so the
		// per-IP rate limits on register and login do not throttle the suite.
		"TRUSTED_PROXIES=127.0.0.1",
	}
}

func (s *Stack) apiEnv(searchPath string, port int) []string {
	env := append(s.baseEnv(searchPath), fmt.Sprintf("HTTP_PORT=%d", port), "POSTGRES_MAX_OPEN_CONNS=8")
	return mergeEnv(env, s.opts.APIEnv)
}

func (s *Stack) workerEnv(adminPort, metricsPort int) []string {
	host, port, _ := strings.Cut(s.Mailpit.SMTPAddr, ":")
	env := append(s.baseEnv("platform,course,analytics,public"),
		"EVENT_TRANSPORT="+s.opts.EventTransport,
		"KAFKA_BROKERS="+strings.Join(s.Kafka.Brokers, ","),
		"SCHEMA_REGISTRY_URL="+s.RegistryURL,
		"SMTP_HOST="+host, "SMTP_PORT="+port,
		fmt.Sprintf("ADMIN_HTTP_ADDR=127.0.0.1:%d", adminPort),
		fmt.Sprintf("METRICS_HTTP_ADDR=127.0.0.1:%d", metricsPort),
		"POSTGRES_MAX_OPEN_CONNS=30",
	)
	return mergeEnv(env, s.opts.WorkerEnv)
}

// mergeEnv appends over, so a later KEY=value wins (the last duplicate is the
// one a process sees).
func mergeEnv(env []string, over map[string]string) []string {
	for k, v := range over {
		env = append(env, k+"="+v)
	}
	return env
}

// WaitWorkerReady waits until the worker's admin API is healthy and its
// Temporal publish worker, Asynq servers and (in kafka mode) consumer groups
// have started.
func (s *Stack) WaitWorkerReady(t testing.TB) {
	t.Helper()
	Eventually(t, 2*time.Minute, "worker ready", func() error {
		if !s.Worker.Running() {
			t.Fatalf("worker exited during start-up; see its output")
		}
		if code, body, err := s.WorkerHealth(); err != nil || code != 200 {
			return Errf("admin health: %d %s (err %v)", code, body, err)
		}
		for _, want := range []string{"publish worker started", "asynq started"} {
			if !s.Worker.LogContains(want) {
				return Errf("worker has not logged %q yet", want)
			}
		}
		if s.opts.EventTransport == "kafka" && s.Worker.LogCount("kafka consumer started") < 6 {
			return Errf("only %d of 6 kafka consumer groups started", s.Worker.LogCount("kafka consumer started"))
		}
		return nil
	})
}

// KillWorker sends SIGKILL to the worker (kill -9): no drain, no offset
// commit, no transaction commit of anything in flight.
func (s *Stack) KillWorker() { s.Worker.Kill() }

// StartWorker starts the worker again after KillWorker, with the same
// environment (so the same ports and the same consumer groups), and waits for
// it to be ready. Its output is appended to the same log.
func (s *Stack) StartWorker(t testing.TB) {
	t.Helper()
	before := s.Worker.LogCount("publish worker started")
	if err := s.Worker.Start(); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 2*time.Minute, "restarted worker ready", func() error {
		if code, body, err := s.WorkerHealth(); err != nil || code != 200 {
			return Errf("admin health: %d %s (err %v)", code, body, err)
		}
		if s.Worker.LogCount("publish worker started") <= before {
			return Errf("restarted worker has not started its publish worker yet")
		}
		return nil
	})
}

// NewWorkerProc builds, without starting, a second worker process on the
// stack's infrastructure with fresh admin and metrics ports and over added to
// its environment (a second Start of a different configuration: E11 points it
// at a registry that lacks a subject and expects it to exit). Its output is
// dumped on failure; Stop is registered as cleanup.
func (s *Stack) NewWorkerProc(t testing.TB, over map[string]string) *Proc {
	t.Helper()
	p := newProc("worker-extra", Binary(t, "worker"), mergeEnv(s.workerEnv(mustPort(t), mustPort(t)), over))
	t.Cleanup(func() {
		if t.Failed() {
			p.dumpLogs(t)
		}
		p.Stop(10 * time.Second)
	})
	return p
}

// Exited waits up to timeout for the process to exit and reports whether it did.
func (p *Proc) Exited(timeout time.Duration) bool {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// StopKafka stops the broker (data kept). StartKafka brings it back on the
// same address.
func (s *Stack) StopKafka(t *testing.T) { s.Kafka.Stop(t) }

// StartKafka restarts the broker stopped by StopKafka.
func (s *Stack) StartKafka(t *testing.T) { s.Kafka.Start(t) }

// StopSMTP stops Mailpit: the mail provider is down, connections refused.
func (s *Stack) StopSMTP(t testing.TB) { s.Mailpit.StopSMTP(t) }

// StartSMTP starts Mailpit again on the same ports (its inbox is empty).
func (s *Stack) StartSMTP(t testing.TB) { s.Mailpit.StartSMTP(t) }

// bootstrapAdmin creates the one account the API cannot create: an admin. It
// registers a user through the API (so the account is real in every table and
// event) and then sets its role in the database, the equivalent of the seed
// tool's first admin.
func (s *Stack) bootstrapAdmin(t testing.TB) {
	t.Helper()
	s.adminEmail = uniqueEmail("admin")
	s.Register(t, s.adminEmail, "Admin E2E")
	if err := s.Env.DB.Exec(`UPDATE identity.users SET role = 'admin' WHERE email = ?`, s.adminEmail).Error; err != nil {
		t.Fatalf("promote bootstrap admin: %v", err)
	}
}

// ---- SQL helpers (the scenarios assert on tables directly) ----

// DB is the connection that sees every schema.
func (s *Stack) DB() *gorm.DB { return s.Env.DB }

// Count runs a `SELECT count(*) ...` style query and returns the number.
func (s *Stack) Count(query string, args ...any) (int64, error) {
	var n int64
	if err := s.Env.DB.Raw(query, args...).Row().Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// MustCount is Count failing the test on a SQL error.
func (s *Stack) MustCount(t testing.TB, query string, args ...any) int64 {
	t.Helper()
	n, err := s.Count(query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// OutboxUnpublished counts outbox rows not yet published.
func (s *Stack) OutboxUnpublished() (int64, error) {
	return s.Count(`SELECT count(*) FROM platform.outbox WHERE published_at IS NULL`)
}

// WaitOutboxDrained waits until every outbox row is published.
func (s *Stack) WaitOutboxDrained(t testing.TB, timeout time.Duration) {
	t.Helper()
	Eventually(t, timeout, "the outbox to drain", func() error {
		n, err := s.OutboxUnpublished()
		if err != nil {
			return err
		}
		if n != 0 {
			return Errf("%d outbox rows unpublished", n)
		}
		return nil
	})
}

// ProcessedCount counts processed_events rows for a consumer and event id.
func (s *Stack) ProcessedCount(consumer, eventID string) (int64, error) {
	return s.Count(`SELECT count(*) FROM platform.processed_events WHERE consumer_name = ? AND event_id = ?`, consumer, eventID)
}

// EventIDFor returns the outbox event id of the (single) event of eventType
// whose payload contains the JSON key/value, for example
// EventIDFor("enrollment.student_enrolled", "enrollment_id", id).
func (s *Stack) EventIDFor(eventType, key, value string) (string, error) {
	var id string
	err := s.Env.DB.Raw(`SELECT event_id::text FROM platform.outbox WHERE event_type = ? AND convert_from(payload, 'UTF8')::jsonb ->> ? = ? ORDER BY id LIMIT 1`,
		eventType, key, value).Row().Scan(&id)
	return id, err
}

// ConsumerGroups are the worker's Kafka consumer groups (one per consumer
// name, RFC 6.5).
var ConsumerGroups = []string{
	"analytics-aggregator", "audit-archiver", "certificate-issuer",
	"course-instructor-mirror", "enrollment-course-archival", "notification-dispatcher",
}

// WaitSettled waits until the outbox is drained and every consumer group has
// no lag: every event written so far has been published and handled. Take
// baselines (analytics counters, mail counts) after it, and assert after it,
// so a "no change" check cannot pass just because an event is still in flight.
func (s *Stack) WaitSettled(t testing.TB, timeout time.Duration) {
	t.Helper()
	s.WaitOutboxDrained(t, timeout)
	Eventually(t, timeout, "every consumer group to catch up", func() error {
		for _, g := range ConsumerGroups {
			lag, err := s.ConsumerLag(g)
			if err != nil {
				return Errf("group %s: %v", g, err)
			}
			if lag != 0 {
				return Errf("group %s lag %d", g, lag)
			}
		}
		return nil
	})
}

// AdminEmail is the bootstrapped admin account's address (its password is
// Password); log in with Stack.Login or take a token with Stack.AdminToken.
func (s *Stack) AdminEmail() string { return s.adminEmail }
