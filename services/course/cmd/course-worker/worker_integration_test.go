//go:build integration

// What this file checks, against real Postgres (as svc_course), Redis, Kafka and
// MongoDB: the relay is publish-only over course.outbox; course-worker boots
// through libs/runtime with every component and the readiness checks it should
// have; an event the course service itself wrote comes back through Kafka into
// its OWN consumer (enrollment archival) with the marker in course.processed_events;
// the dead-letter connection parks into MongoDB.
package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func get(url string) (int, string) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// seedArchivedCourse inserts a course with one active enrollment and writes the
// course.course_archived event the way ArchiveCourse does (state change and
// outbox row in one transaction, in the course schema). It returns the ids.
func seedArchivedCourse(t *testing.T, db *gorm.DB) (courseID, enrollmentID uuid.UUID) {
	t.Helper()
	courseID, enrollmentID = uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO courses (id, instructor_id, slug, enrollment_count) VALUES (?, ?, ?, 1)`, courseID, uuid.New(), courseID.String()).Error; err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`, enrollmentID, uuid.New(), courseID).Error; err != nil {
			return err
		}
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "course", AggregateID: courseID, EventType: "course.course_archived", SchemaVersion: 1,
			Payload: map[string]any{"course_id": courseID},
		})
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return courseID, enrollmentID
}

type recordingPublisher struct {
	mu   sync.Mutex
	seen []relay.DecodedEvent
}

func (p *recordingPublisher) Publish(_ context.Context, evt relay.DecodedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, evt)
	return nil
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen)
}

// The relay publishes course.outbox and runs no handler: not even the course
// service's own enrollment-archival handler, which only the consumers run.
func TestBuildRelay_PublishesCourseOutboxAndRunsNoHandlers(t *testing.T) {
	env := testenv.Setup(t)
	db := env.ServiceDB(t, "course")
	_, enrollmentID := seedArchivedCourse(t, db)

	pub := &recordingPublisher{}
	rl := buildRelay(db, &config.Config{Relay: config.RelayConfig{Workers: 2}}, pub, nil, false)
	if n := len(rl.Handlers()); n != 0 {
		t.Fatalf("the relay has %d handlers registered, want a publish-only relay", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	eventually(t, "the row to be published", func() bool {
		var n int64
		db.Raw(`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`).Scan(&n)
		return n == 1 && pub.count() == 1
	})
	if got := pub.seen[0].EventType; got != "course.course_archived" {
		t.Errorf("published %s", got)
	}
	var status string
	db.Raw(`SELECT status::text FROM enrollments WHERE id = ?`, enrollmentID).Scan(&status)
	if status != "active" {
		t.Errorf("enrollment is %q: the relay ran the archival handler", status)
	}
	// The relay used the course schema's tables, not any other service's.
	var inOwner int64
	env.Owner.Raw(`SELECT count(*) FROM course.outbox WHERE published_at IS NOT NULL`).Scan(&inOwner)
	if inOwner != 1 {
		t.Errorf("course.outbox has %d published rows, want 1", inOwner)
	}
}

func workerConfig(t *testing.T, env *testenv.Env, brokers []string, registryURL, internal, mongoURI string) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Env:      "test",
		Postgres: config.PostgresConfig{DSN: env.ServiceDSN(t, "course"), MaxOpenConns: 12, MaxIdleConns: 4, SearchPath: "course,public"},
		Redis:    config.RedisConfig{Addr: env.RDB.Options().Addr},
		Relay:    config.RelayConfig{Workers: 2},
		Kafka: config.KafkaConfig{
			Brokers: brokers, SchemaRegistryURL: registryURL, ProduceTimeout: 10 * time.Second,
			ConsumerMaxAttempts: 3, ConsumerBackoffBase: 100 * time.Millisecond, ConsumerBackoffMax: time.Second,
			ConsumerShutdownTimeout: 5 * time.Second,
		},
		Jobs: config.JobsConfig{
			ReconcilePublishesInterval: time.Minute, EnrollmentCountInterval: time.Hour,
			IdempotencyPruneInterval: time.Hour, OutboxPruneInterval: time.Hour, OutboxRetention: 168 * time.Hour, ProcessedEventsRetention: 336 * time.Hour,
		},
		Asynq: config.AsynqConfig{
			LowConcurrency: 2, MaxRetry: 3, BackoffBase: time.Second, BackoffMax: 5 * time.Second, ShutdownTimeout: 2 * time.Second,
		},
		// An unreachable Temporal: the activities worker keeps retrying, the rest runs.
		Temporal:  config.TemporalConfig{HostPort: "127.0.0.1:1", Namespace: "default", TaskQueue: "publish-workflows", ActivityQueue: "course-activities"},
		Telemetry: config.TelemetryConfig{MetricsAddr: internal, MetricsEnabled: true, SampleRatio: 1},
	}
	if mongoURI != "" {
		cfg.Mongo = config.MongoConfig{URI: mongoURI, Database: "course_worker_test", ConnectTimeout: time.Second, ServerSelectionTimeout: 500 * time.Millisecond, OpTimeout: time.Second}
	}
	return cfg
}

// The whole worker through libs/runtime: ready with Kafka reported, an event the
// course wrote reaches Kafka via the publish-only relay and returns through the
// worker's own consumer, whose marker lands in course.processed_events.
func TestBoot_RelayPublishesAndTheWorkersOwnConsumerApplies(t *testing.T) {
	env := testenv.Setup(t)
	kb := testenv.SetupKafka(t)
	reg := transporttest.NewRegistry(t)
	internal := freeAddr(t)
	cfg := workerConfig(t, env, kb.Brokers, reg.URL, internal, "")
	db := env.ServiceDB(t, "course")
	courseID, enrollmentID := seedArchivedCourse(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "course-worker", wire,
			runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }), runtime.WithOutbox(),
			runtime.WithLogger(quiet()))
	}()
	t.Cleanup(cancel)

	eventually(t, "readiness with postgres, redis and kafka", func() bool {
		code, body := get("http://" + internal + "/health/ready")
		return code == 200 && strings.Contains(body, `"postgres":"ok"`) && strings.Contains(body, `"redis":"ok"`) && strings.Contains(body, `"kafka":"ok"`)
	})
	eventually(t, "the enrollment to be withdrawn by the worker's own consumer", func() bool {
		var status string
		db.Raw(`SELECT status::text FROM enrollments WHERE id = ?`, enrollmentID).Scan(&status)
		return status == "withdrawn"
	})
	var published, markers int64
	db.Raw(`SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL`, courseID).Scan(&published)
	db.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = 'enrollment-course-archival'`).Scan(&markers)
	if published != 1 || markers != 1 {
		t.Errorf("published outbox rows = %d, course.processed_events markers = %d; want 1 and 1", published, markers)
	}
	code, body := get("http://" + internal + "/metrics")
	if code != 200 || !strings.Contains(body, "outbox_backlog") {
		t.Errorf("internal /metrics = %d without the outbox gauges", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("course-worker did not shut down")
	}
}

// With a Kafka broker that is not there, start-up fails with a message naming
// Kafka instead of running half a worker.
func TestBoot_UnreachableKafkaFailsStartUp(t *testing.T) {
	env := testenv.Setup(t)
	cfg := workerConfig(t, env, []string{freeAddr(t)}, "http://"+freeAddr(t), freeAddr(t), "")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := runtime.Run(ctx, "course-worker", wire,
		runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }), runtime.WithLogger(quiet()))
	if err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("Run = %v, want a start-up error naming kafka", err)
	}
}

func mongoCfg(uri string) config.MongoConfig {
	return config.MongoConfig{URI: uri, Database: "wire_test", ConnectTimeout: time.Second, ServerSelectionTimeout: 500 * time.Millisecond, OpTimeout: time.Second}
}

// MongoDB configured but unreachable: the connection keeps retrying without
// blocking anything, reports "not parked", and stops promptly when told to.
func TestMongoConn_Unreachable_RetriesAndStopsOnCancel(t *testing.T) {
	mc := &mongoConn{cfg: mongoCfg("mongodb://" + freeAddr(t))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = mc.run(ctx, quiet()); close(done) }()
	time.Sleep(1500 * time.Millisecond)
	if _, err := mc.get(); !errors.Is(err, errMongoNotReady) {
		t.Errorf("get with MongoDB unreachable: %v, want errMongoNotReady", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

// MongoDB reachable: the sink parks the event, idempotently per (event, consumer).
func TestParkSink_ParksIntoMongoDB(t *testing.T) {
	mongoEnv := testenv.SetupMongo(t)
	mc := &mongoConn{cfg: mongoCfg(mongoEnv.URI)}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = mc.run(ctx, quiet()) }()
	defer func() { cancel(); wg.Wait() }()
	eventually(t, "the MongoDB connection", func() bool { return mc.Ping(context.Background()) == nil })

	evt := sampleEvent()
	sink := newParkSink(mc, nil)
	for range 2 {
		if err := sink.Park(ctx, relay.DeadLetter{Event: evt, Consumer: "certificate-issuer", Err: errors.New("x"), Attempts: 5}); err != nil {
			t.Fatalf("park: %v", err)
		}
	}
	s, _ := mc.get()
	fe, err := s.GetFailedEvent(ctx, evt.EventID.String(), "certificate-issuer")
	if err != nil {
		t.Fatalf("parked event not readable: %v", err)
	}
	if fe.Consumer != "certificate-issuer" {
		t.Errorf("parked %+v", fe)
	}
}
