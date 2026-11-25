//go:build integration

// analytics-worker under libs/runtime over real Postgres (as svc_analytics),
// Redis, Kafka and, where stated, MongoDB. The Schema Registry is the
// libs/eventbus/transporttest stand-in (the real registry is covered by
// libs/events), so production's prepareKafka runs unchanged.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type stack struct {
	t        *testing.T
	env      *testenv.Env
	kafka    *testenv.KafkaEnv
	registry *transporttest.Registry
	internal string
	stopFn   func() error
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs the worker's production wiring (wireWith with prepareKafka). mongoURI
// is MONGO_URI ("" leaves it unset).
func start(t *testing.T, mongoURI string) *stack {
	t.Helper()
	env := setup(t)
	kb := testenv.SetupKafka(t)
	reg := transporttest.NewRegistry(t)
	s := &stack{t: t, env: env, kafka: kb, registry: reg, internal: freeAddr(t)}

	for k, v := range map[string]string{
		"POSTGRES_DSN": env.ServiceDSN(t, "analytics"), "POSTGRES_SEARCH_PATH": "analytics,public",
		"REDIS_ADDR": env.RDB.Options().Addr, "JWT_SIGNING_KEY": "test-signing-key-0123456789-abcdefghij",
		"MINIO_ENDPOINT": "127.0.0.1:1", "MINIO_ACCESS_KEY_ID": "x", "MINIO_SECRET_ACCESS_KEY": "y",
		"KAFKA_BROKERS": strings.Join(kb.Brokers, ","), "SCHEMA_REGISTRY_URL": reg.URL,
		"METRICS_HTTP_ADDR": s.internal, "MONGO_URI": mongoURI,
		"KAFKA_CONSUMER_BACKOFF_BASE": "20ms", "KAFKA_CONSUMER_BACKOFF_MAX": "100ms",
	} {
		t.Setenv(k, v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "analytics-worker", wire, runtime.WithConfig(loadConfig), runtime.WithDependencies(infra.Connect), runtime.WithLogger(quiet))
	}()
	var once sync.Once
	var runErr error
	s.stopFn = func() error {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(30 * time.Second):
				runErr = errors.New("analytics-worker did not shut down")
			}
		})
		return runErr
	}
	t.Cleanup(func() { _ = s.stopFn() })
	return s
}

func (s *stack) get(path string) (int, string) {
	resp, err := http.Get("http://" + s.internal + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

type readyBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func (s *stack) ready() (int, readyBody) {
	code, body := s.get("/health/ready")
	var r readyBody
	_ = json.Unmarshal([]byte(body), &r)
	return code, r
}

func (s *stack) eventually(d time.Duration, what string, fn func() bool) {
	s.t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if fn() {
			return
		}
	}
	s.t.Fatalf("timed out waiting for %s", what)
}

func (s *stack) count(q string, args ...any) int64 {
	s.t.Helper()
	var n int64
	if err := s.env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		s.t.Fatal(err)
	}
	return n
}

// Components registered, readiness, the internal listener.
func TestAnalyticsWorker_ReadyWithKafkaNonFatal_MetricsOnInternalListener(t *testing.T) {
	s := start(t, "")
	s.eventually(60*time.Second, "ready", func() bool {
		code, r := s.ready()
		return code == 200 && r.Status == "ready"
	})
	_, r := s.ready()
	for _, dep := range []string{"postgres", "redis", "kafka"} {
		if r.Checks[dep] != "ok" {
			t.Errorf("check %s = %q, want ok (all: %v)", dep, r.Checks[dep], r.Checks)
		}
	}
	if _, ok := r.Checks["mongodb"]; ok {
		t.Error("a mongodb check is registered although MONGO_URI is unset")
	}
	for name, state := range r.Checks {
		if strings.HasPrefix(name, "component:") {
			t.Errorf("component failed or stopped: %s=%s", name, state)
		}
	}

	code, body := s.get("/metrics")
	if code != 200 || !strings.Contains(body, "db_connections_in_use") {
		t.Fatalf("internal /metrics = %d, lacks db_connections_in_use", code)
	}
	if strings.Contains(body, "outbox_backlog") {
		t.Error("analytics-worker exports outbox_backlog although it has no outbox")
	}
	if code, _ := s.get("/health/live"); code != 200 {
		t.Errorf("/health/live = %d", code)
	}

	// No outbox in the analytics schema: the role cannot even name one.
	if err := s.env.DB.Exec(`SELECT 1 FROM outbox`).Error; err == nil {
		t.Error("svc_analytics can read an outbox table")
	}
	if err := s.stopFn(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// The consumers really consume: a record on a source topic and one on the
// group's retry topic are both applied, through the idempotent path
// (analytics.processed_events), and a retry replay of an applied event is absorbed.
func TestAnalyticsWorker_ConsumesSourceAndRetryTopics(t *testing.T) {
	s := start(t, "")
	ctx := context.Background()
	schemas, err := eventbus.LoadSchemas(ctx, registry.New(s.registry.URL))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := eventbus.NewPublisher(eventbus.PublisherConfig{Brokers: s.kafka.Brokers, ProduceTimeout: 10 * time.Second, Schemas: schemas})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	s.eventually(60*time.Second, "ready", func() bool { code, _ := s.ready(); return code == 200 })

	mk := func(role string) relay.DecodedEvent {
		u := uuid.New()
		raw, _ := json.Marshal(map[string]any{"user_id": u, "email": u.String() + "@example.com", "role": role, "full_name": "N"})
		return relay.DecodedEvent{OutboxID: 1, EventID: uuid.New(), AggregateType: "user", AggregateID: u,
			EventType: "identity.user_registered", SchemaVersion: 1, Payload: raw, OccurredAt: time.Now().UTC()}
	}
	mirrored := func(e relay.DecodedEvent) bool {
		return s.count(`SELECT count(*) FROM user_role_mirror WHERE user_id = ?`, e.AggregateID) == 1
	}

	viaTopic := mk("student")
	if err := pub.Publish(ctx, viaTopic); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s.eventually(90*time.Second, "the source-topic event to be applied", func() bool { return mirrored(viaTopic) })

	viaRetry := mk("instructor")
	if err := pub.PublishRetry(ctx, "analytics-aggregator", schemas.EnvelopeIDs[topics.Identity], viaRetry); err != nil {
		t.Fatalf("PublishRetry: %v", err)
	}
	s.eventually(90*time.Second, "the retry-topic event to be applied", func() bool { return mirrored(viaRetry) })

	for _, e := range []relay.DecodedEvent{viaTopic, viaRetry} {
		if n := s.count(`SELECT count(*) FROM processed_events WHERE consumer_name = 'analytics-aggregator' AND event_id = ?`, e.EventID); n != 1 {
			t.Errorf("processed_events rows for %s = %d, want 1", e.EventID, n)
		}
	}
	// A replay of the applied event through the retry topic is a duplicate.
	if err := pub.PublishRetry(ctx, "analytics-aggregator", schemas.EnvelopeIDs[topics.Identity], viaTopic); err != nil {
		t.Fatal(err)
	}
	marker := mk("student")
	if err := pub.PublishRetry(ctx, "analytics-aggregator", schemas.EnvelopeIDs[topics.Identity], marker); err != nil {
		t.Fatal(err)
	}
	s.eventually(90*time.Second, "the event behind the replay to be applied", func() bool { return mirrored(marker) })
	if n := s.count(`SELECT count(*) FROM processed_events WHERE consumer_name = 'analytics-aggregator' AND event_id = ?`, viaTopic.EventID); n != 1 {
		t.Errorf("the replayed event has %d markers, want 1", n)
	}
}

// The reconciler job is registered with the Asynq servers on the low queue and
// runs the real reconciler: a task of its type, enqueued as the scheduler's
// tick would, rebuilds wiped aggregates from the mirrors.
func TestAnalyticsWorker_ReconcileTaskRunsTheRealReconciler(t *testing.T) {
	s := start(t, "")
	course, enr, stu := uuid.New(), uuid.New(), uuid.New()
	feed := func(typ string, payload map[string]any) {
		raw, _ := json.Marshal(payload)
		for _, h := range analytics.Handlers() {
			if !h.Handles(typ) {
				continue
			}
			evt := relay.DecodedEvent{OutboxID: 1, EventID: uuid.New(), EventType: typ, Payload: raw, OccurredAt: time.Now().UTC()}
			if err := relay.Deliver(context.Background(), s.env.DB, h, evt); err != nil {
				t.Fatal(err)
			}
		}
	}
	feed("course.course_published", map[string]any{"course_id": course})
	feed("enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": time.Now().UTC()})
	if n := s.count(`SELECT total_enrollments FROM course_stats WHERE course_id = ?`, course); n != 1 {
		t.Fatalf("precondition: total_enrollments = %d", n)
	}
	if err := s.env.Owner.Exec(`TRUNCATE analytics.course_stats, analytics.daily_enrollments, analytics.platform_snapshot`).Error; err != nil {
		t.Fatal(err)
	}

	rc := asynqx.NewClient(asynqx.RedisClient(s.env.RDB.Options().Addr, "", 0), asynqx.WithNamespace("analytics"))
	t.Cleanup(func() { _ = rc.Close() })
	s.eventually(60*time.Second, "ready", func() bool { code, _ := s.ready(); return code == 200 })
	if _, err := rc.Enqueue(context.Background(), asynqx.Job{
		Type: TypeReconcile, ID: asynqx.DeterministicID("test", uuid.NewString()), Queue: asynqx.QueueLow, MaxRetry: 1, Timeout: 30 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	s.eventually(60*time.Second, "the reconcile task to rebuild course_stats", func() bool {
		return s.count(`SELECT count(*) FROM course_stats WHERE course_id = ? AND total_enrollments = 1`, course) == 1
	})
}

// A broker outage degrades readiness (200 "degraded") instead of failing it: the
// consumers wait, the process stays up.
func TestAnalyticsWorker_KafkaDownIsDegradedNotUnready(t *testing.T) {
	s := start(t, "")
	s.eventually(60*time.Second, "ready", func() bool { code, r := s.ready(); return code == 200 && r.Status == "ready" })
	s.kafka.Stop(t)
	s.eventually(30*time.Second, "degraded", func() bool {
		code, r := s.ready()
		return code == 200 && r.Status == "degraded" && r.Checks["kafka"] == "failed"
	})
	if r := func() readyBody { _, r := s.ready(); return r }(); r.Checks["postgres"] != "ok" || r.Checks["redis"] != "ok" {
		t.Errorf("postgres/redis checks = %v", r.Checks)
	}
}

// With MONGO_URI set, MongoDB is a non-fatal readiness check and the dead-letter
// sink parks into failed_events through libs/mongostore, idempotently.
func TestAnalyticsWorker_MongoConfigured_ParksDeadLetters(t *testing.T) {
	mg := testenv.SetupMongo(t)
	s := start(t, mg.URI)
	s.eventually(60*time.Second, "mongodb ok", func() bool {
		code, r := s.ready()
		return code == 200 && r.Checks["mongodb"] == "ok"
	})

	d := newDeadLetters(config.MongoConfig{URI: mg.URI, Database: "analytics_dl_test", ConnectTimeout: 5 * time.Second,
		ServerSelectionTimeout: 5 * time.Second, OpTimeout: 5 * time.Second}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = d.run(ctx, quiet); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	s.eventually(30*time.Second, "sink connected", func() bool { return d.Ping(context.Background()) == nil })

	dl := deadLetter()
	for i := 0; i < 2; i++ { // twice: idempotent per (event_id, consumer)
		if err := d.Park(ctx, dl); err != nil {
			t.Fatalf("Park #%d: %v", i+1, err)
		}
	}
	st, err := mongostore.Open(ctx, mongostore.Config{URI: mg.URI, Database: "analytics_dl_test", ConnectTimeout: 5 * time.Second,
		ServerSelectionTimeout: 5 * time.Second, OpTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	fe, err := st.GetFailedEvent(ctx, dl.Event.EventID.String(), "analytics-aggregator")
	if err != nil || fe == nil {
		t.Fatalf("parked event not found: %v", err)
	}
	if fe.EventType != "course.course_published" {
		t.Errorf("parked event type = %q", fe.EventType)
	}
}

// MongoDB configured but unreachable: the worker still starts and is ready
// ("degraded"), the aggregates are unaffected, and nothing is reported parked.
func TestAnalyticsWorker_MongoUnreachable_IsDegradedNotUnready(t *testing.T) {
	s := start(t, "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=300")
	s.eventually(60*time.Second, "degraded", func() bool {
		code, r := s.ready()
		return code == 200 && r.Status == "degraded" && r.Checks["mongodb"] == "failed" && r.Checks["postgres"] == "ok" && r.Checks["kafka"] == "ok"
	})
}
