package harness

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// Options tunes a Stack. The zero value is the stack of E01: every binary,
// mail to Mailpit, MongoDB for dead letters and the archive.
type Options struct {
	// WorkerEnv adds to (and overrides) the environment of every *-worker
	// binary, for example {"KAFKA_CONSUMER_MAX_ATTEMPTS": "3"}.
	WorkerEnv map[string]string
	// APIEnv adds to the environment of every *-api binary.
	APIEnv map[string]string
	// BinaryEnv adds to the environment of one binary and wins over WorkerEnv
	// and APIEnv, for example {NotificationWorker: {"ASYNQ_BACKOFF_BASE": "1s"}}.
	BinaryEnv map[string]map[string]string
	// Tracing starts a Jaeger container and points every binary's
	// OTEL_EXPORTER_OTLP_ENDPOINT at it with sampling 1 (E02). Off by
	// default: a scenario that does not read traces does not pay for it.
	Tracing bool
}

// Stack is a running system: containers, the ten binaries, and the handles a
// scenario asserts through. Start builds one; its cleanup stops everything,
// and on a failed test dumps each process's output into the test log.
type Stack struct {
	// Postgres and Redis come from libs/testenv. Env.DB is the OWNER
	// connection (search path "public"): it can read every schema, which the
	// service binaries cannot, so a scenario qualifies its tables
	// (course.enrollments, analytics.course_stats, ...).
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

	// Public API base URLs.
	IdentityURL, CourseURL, AnalyticsURL, AuditURL string

	opts       Options
	mongoC     *mongoContainer
	serviceDSN map[string]string // schema -> DSN of that service's role
	redisAddr  string
	jwtKey     string
	// Temporal queues are per stack, so scenarios running one after another
	// against the shared Temporal never see each other's tasks.
	workflowQ   string
	activityQ   string
	adminEmail  string
	procs       map[string]*Proc
	httpPorts   map[string]int
	metricPorts map[string]int
}

// Start brings up a full stack for one scenario and returns when every
// process answers ready. It uses t.Cleanup for teardown.
//
// Order: shared infrastructure (Schema Registry with the schemas registered by
// the built register-schemas tool, and Temporal: once per test binary), this
// scenario's own Postgres (real roles script and every migration), Redis,
// Kafka, MongoDB and Mailpit, the first admin through the identity-admin CLI,
// then the ten binaries, each as a subprocess with exactly the environment of
// its configuration table.
func Start(t *testing.T, opts Options) *Stack {
	t.Helper()
	began := time.Now()
	si := Shared(t)

	s := &Stack{
		opts: opts, RegistryURL: si.RegistryURL, TemporalAddr: si.TemporalAddr,
		procs: map[string]*Proc{}, httpPorts: map[string]int{}, metricPorts: map[string]int{},
		serviceDSN: map[string]string{},
	}
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

	for _, svc := range testenv.Services {
		s.serviceDSN[svc] = s.Env.ServiceDSN(t, svc)
	}
	s.redisAddr = s.Env.RDB.Options().Addr
	s.jwtKey = "e2e-signing-key-" + strings.ReplaceAll(uuid.NewString(), "-", "") // 48 bytes
	suffix := uuid.NewString()[:8]
	s.workflowQ, s.activityQ = "e2e-publish-"+suffix, "e2e-activities-"+suffix

	for _, name := range Binaries {
		s.metricPorts[name] = mustPort(t)
		if specs[name].api {
			s.httpPorts[name] = mustPort(t)
		}
	}
	s.IdentityURL = s.apiURL(IdentityAPI)
	s.CourseURL = s.apiURL(CourseAPI)
	s.AnalyticsURL = s.apiURL(AnalyticsAPI)
	s.AuditURL = s.apiURL(AuditAPI)
	for _, name := range Binaries {
		s.procs[name] = newProc(name, Binary(t, name), s.envFor(name, s.httpPorts[name], s.metricPorts[name]))
	}

	t.Cleanup(func() {
		if t.Failed() {
			for _, name := range Binaries {
				s.procs[name].dumpLogs(t)
			}
		}
		var wg sync.WaitGroup
		for _, p := range s.procs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.Stop(15 * time.Second)
			}()
		}
		wg.Wait()
	})

	s.createFirstAdmin(t)
	for _, name := range Binaries {
		if err := s.procs[name].Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range Binaries {
		s.waitReady(t, name, 0)
	}
	t.Logf("stack: ready after %s", time.Since(began).Round(time.Second))
	return s
}

func (s *Stack) apiURL(name string) string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.httpPorts[name])
}

func mustPort(t testing.TB) int {
	t.Helper()
	p, err := freePort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	return p
}

// createFirstAdmin runs the built identity-admin CLI, the only way to make the
// first admin (the HTTP API grants admin only to an existing admin). It uses
// the identity role and Redis like the identity-api does.
func (s *Stack) createFirstAdmin(t testing.TB) {
	t.Helper()
	s.adminEmail = uniqueEmail("admin")
	cmd := exec.Command(Binary(t, "identity-admin"))
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")},
		"APP_ENV=development",
		"POSTGRES_DSN="+s.serviceDSN["identity"],
		"POSTGRES_SEARCH_PATH=identity,public",
		"POSTGRES_MAX_OPEN_CONNS=10",
		"REDIS_ADDR="+s.redisAddr,
		"ARGON2_MEMORY_KIB=8192", "ARGON2_ITERATIONS=1", "ARGON2_PARALLELISM=1",
		"IDENTITY_ADMIN_EMAIL="+s.adminEmail,
		"IDENTITY_ADMIN_PASSWORD="+Password,
		"IDENTITY_ADMIN_NAME=Admin E2E",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("identity-admin: %v\n%s", err, out)
	}
}

// AdminEmail is the first admin's address (its password is Password); log in
// with Stack.Login or take a token with Stack.AdminToken.
func (s *Stack) AdminEmail() string { return s.adminEmail }

// ---- Process control: per binary ----

// Proc is the process handle of a binary (valid names: Binaries).
func (s *Stack) Proc(name string) *Proc {
	p, ok := s.procs[name]
	if !ok {
		panic("harness: unknown binary " + name)
	}
	return p
}

// MetricsURL is the base URL of a binary's internal listener (/metrics,
// /health/live, /health/ready).
func (s *Stack) MetricsURL(name string) string {
	s.Proc(name) // validates the name
	return fmt.Sprintf("http://127.0.0.1:%d", s.metricPorts[name])
}

// Metrics scrapes a binary's /metrics.
func (s *Stack) Metrics(name string) (Metrics, error) {
	return ScrapeMetrics(s.MetricsURL(name) + "/metrics")
}

// Health is GET /health/ready on a binary's internal listener.
func (s *Stack) Health(name string) (int, string, error) {
	code, raw, err := Do("GET", s.MetricsURL(name)+"/health/ready", "", nil)
	if err != nil {
		return 0, "", err
	}
	return code, string(raw), nil
}

// Kill sends SIGKILL (kill -9) to a binary: no drain, no offset commit, no
// transaction commit of anything in flight.
func (s *Stack) Kill(name string) { s.Proc(name).Kill() }

// Stop shuts a binary down gracefully (SIGTERM, killed after 30 s).
func (s *Stack) Stop(name string) { s.Proc(name).Stop(30 * time.Second) }

// Start starts a binary again after Stop or Kill, with the same environment
// (so the same ports and the same consumer groups), and waits for it to be
// ready. Its output is appended to the same log.
func (s *Stack) Start(t testing.TB, name string) {
	t.Helper()
	p := s.Proc(name)
	before := p.LogCount("kafka consumer started")
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	s.waitReady(t, name, before)
}

// waitReady waits until the binary's internal /health/ready answers 200 and,
// for a binary with consumers, every consumer group has logged that it
// started (consumersBefore is the count from earlier runs of the same
// process).
func (s *Stack) waitReady(t testing.TB, name string, consumersBefore int) {
	t.Helper()
	sp, p := specs[name], s.Proc(name)
	Eventually(t, 2*time.Minute, name+" ready", func() error {
		if !p.Running() {
			t.Fatalf("%s exited during start-up; see its output", name)
		}
		code, body, err := s.Health(name)
		if err != nil || code != 200 {
			return Errf("/health/ready: %d %s (err %v)", code, body, err)
		}
		if sp.strictReady && !strings.Contains(body, `"ready"`) {
			return Errf("/health/ready: %s", body)
		}
		if got := p.LogCount("kafka consumer started") - consumersBefore; got < sp.groups {
			return Errf("%d of %d kafka consumer groups started", got, sp.groups)
		}
		if sp.api {
			return CheckReady(s.apiURL(name))
		}
		return nil
	})
}

// NewProc builds, without starting, a second process of a binary on the
// stack's infrastructure with fresh ports and over added to its environment
// (a second Start of a different configuration: E11 points a worker at a
// registry that lacks a subject and expects it to exit). Its output is dumped
// on failure; Stop is registered as cleanup.
func (s *Stack) NewProc(t testing.TB, name string, over map[string]string) *Proc {
	t.Helper()
	port := 0
	if specs[name].api {
		port = mustPort(t)
	}
	p := newProc(name+"-extra", Binary(t, name), mergeEnv(s.envFor(name, port, mustPort(t)), over))
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

// ---- Process control: dependencies ----

// StopKafka stops the broker (data kept). StartKafka brings it back on the
// same address.
func (s *Stack) StopKafka(t *testing.T) { s.Kafka.Stop(t) }

// StartKafka restarts the broker stopped by StopKafka.
func (s *Stack) StartKafka(t *testing.T) { s.Kafka.Start(t) }

// StopSMTP stops Mailpit: the mail provider is down, connections refused.
func (s *Stack) StopSMTP(t testing.TB) { s.Mailpit.StopSMTP(t) }

// StartSMTP starts Mailpit again on the same ports (its inbox is empty).
func (s *Stack) StartSMTP(t testing.TB) { s.Mailpit.StartSMTP(t) }

// FlushRedis is a Redis restart that lost its data: FLUSHALL (token versions,
// rate-limit counters, Asynq queues and the popular-courses set are gone).
func (s *Stack) FlushRedis(t testing.TB) {
	t.Helper()
	if err := s.Env.RDB.FlushAll(t.Context()).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
}

// ---- SQL helpers (the scenarios assert on tables directly, as the owner) ----

// DB is the owner connection: it sees every schema, so qualify tables.
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

// OutboxRow is a row of identity.outbox or course.outbox.
type OutboxRow struct {
	Schema      string // "identity" or "course": whose outbox holds it
	ID          int64
	EventID     string
	EventType   string
	AggregateID string
	Published   bool
}

// OutboxRows lists every event of the system, from both outboxes, oldest
// first by occurrence. ID is the row's id in its own outbox.
func (s *Stack) OutboxRows() ([]OutboxRow, error) {
	var rows []OutboxRow
	err := s.Env.DB.Raw(`SELECT * FROM (
		SELECT 'identity' AS schema, id, event_id::text AS event_id, event_type, aggregate_id::text AS aggregate_id, published_at IS NOT NULL AS published, occurred_at FROM identity.outbox
		UNION ALL
		SELECT 'course', id, event_id::text, event_type, aggregate_id::text, published_at IS NOT NULL, occurred_at FROM course.outbox
	) o ORDER BY occurred_at, schema, id`).Scan(&rows).Error
	return rows, err
}

// OutboxUnpublished counts outbox rows (both services) not yet published.
func (s *Stack) OutboxUnpublished() (int64, error) {
	return s.Count(`SELECT (SELECT count(*) FROM identity.outbox WHERE published_at IS NULL) + (SELECT count(*) FROM course.outbox WHERE published_at IS NULL)`)
}

// WaitOutboxDrained waits until every outbox row is published.
func (s *Stack) WaitOutboxDrained(t testing.TB, timeout time.Duration) {
	t.Helper()
	Eventually(t, timeout, "the outboxes to drain", func() error {
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

// ProcessedCount counts the marker that proves a consumer applied an event
// once: the row in its service's processed_events table (course, analytics or
// notification). The audit archiver has no such table, it is idempotent by
// event id in MongoDB, so for "audit-archiver" it is the number of archive
// documents for the event.
func (s *Stack) ProcessedCount(consumer, eventID string) (int64, error) {
	if consumer == "audit-archiver" {
		return s.Mongo.ArchivedEvents(eventID)
	}
	schema, ok := processedSchema[consumer]
	if !ok {
		return 0, fmt.Errorf("unknown consumer %q", consumer)
	}
	return s.Count(`SELECT count(*) FROM `+testenv.Qualify(schema, "processed_events")+` WHERE consumer_name = ? AND event_id = ?`, consumer, eventID)
}

// EventIDFor returns the outbox event id of the (single) event of eventType
// whose payload contains the JSON key/value, for example
// EventIDFor("enrollment.student_enrolled", "enrollment_id", id).
func (s *Stack) EventIDFor(eventType, key, value string) (string, error) {
	var id string
	err := s.Env.DB.Raw(`SELECT event_id FROM (
		SELECT id, event_id::text AS event_id, event_type, payload FROM identity.outbox
		UNION ALL SELECT id, event_id::text, event_type, payload FROM course.outbox
	) o WHERE event_type = ? AND convert_from(payload, 'UTF8')::jsonb ->> ? = ? ORDER BY id LIMIT 1`,
		eventType, key, value).Row().Scan(&id)
	return id, err
}

// WaitSettled waits until the outboxes are drained and every consumer group
// has no lag: every event written so far has been published and handled. Take
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
