//go:build integration

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/auditconfig"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func get(url string) (int, string) {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

type running struct {
	internal string
	done     chan error
	cancel   context.CancelFunc
}

func start(t *testing.T, mongoURI string, brokers []string) *running {
	t.Helper()
	internal := freeAddr(t)
	t.Setenv("METRICS_HTTP_ADDR", internal)
	t.Setenv("MONGO_URI", mongoURI)
	t.Setenv("MONGO_SERVER_SELECTION_TIMEOUT", "500ms")
	t.Setenv("KAFKA_BROKERS", strings.Join(brokers, ","))
	t.Setenv("KAFKA_CONSUMER_BACKOFF_BASE", "20ms")
	t.Setenv("KAFKA_CONSUMER_BACKOFF_MAX", "100ms")
	t.Setenv("KAFKA_CONSUMER_MAX_ATTEMPTS", "2")
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{internal: internal, done: make(chan error, 1), cancel: cancel}
	go func() {
		r.done <- rt.Run(ctx, "audit-worker", wire, rt.WithoutDependencies(), rt.WithConfig(auditconfig.LoadWorker))
	}()
	t.Cleanup(cancel)
	return r
}

func (r *running) waitReady(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body := get("http://" + r.internal + "/health/ready")
		if code == 200 {
			return body
		}
		select {
		case err := <-r.done:
			t.Fatalf("audit-worker exited: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("never ready: %d %s", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("shutdown error: %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("audit-worker did not shut down")
	}
}

// With MongoDB unreachable the worker starts and stays up, readiness is
// degraded (the events wait in Kafka), live is unaffected, and it shuts down
// cleanly. Kafka is real here.
func TestAuditWorker_MongoUnreachable_DegradedNoCrash(t *testing.T) {
	kb := testenv.SetupKafka(t)
	r := start(t, "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500", kb.Brokers)
	body := r.waitReady(t)
	if !strings.Contains(body, `"degraded"`) || !strings.Contains(body, `"mongodb":"failed"`) || !strings.Contains(body, `"kafka":"ok"`) {
		t.Errorf("ready = %s, want degraded with mongodb failed and kafka ok", body)
	}
	if code, _ := get("http://" + r.internal + "/health/live"); code != 200 {
		t.Errorf("live = %d", code)
	}
	time.Sleep(2 * time.Second) // the consumer and the mongo retry loop keep running
	select {
	case err := <-r.done:
		t.Fatalf("audit-worker exited with MongoDB down: %v", err)
	default:
	}
	r.stop(t)
}

// The assembled binary wiring end to end: an event published to Kafka is
// archived in MongoDB with its Kafka source by the real wire().
func TestAuditWorker_Wired_ArchivesAnEvent(t *testing.T) {
	kb := testenv.SetupKafka(t)
	mg := testenv.SetupMongo(t)
	r := start(t, mg.URI, kb.Brokers)
	if body := r.waitReady(t); !strings.Contains(body, `"mongodb":"ok"`) {
		// MongoDB connects in the background; wait for it.
		deadline := time.Now().Add(20 * time.Second)
		for !strings.Contains(body, `"mongodb":"ok"`) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			_, body = get("http://" + r.internal + "/health/ready")
		}
	}

	reg := transporttest.NewRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schemas, err := eventbus.LoadSchemas(ctx, registry.New(reg.URL))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := eventbus.NewPublisher(eventbus.PublisherConfig{Brokers: kb.Brokers, ProduceTimeout: 15 * time.Second, Schemas: schemas})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	agg := uuid.New()
	evt := relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "user", AggregateID: agg, EventType: "identity.user_profile_updated",
		Payload: []byte(`{"user_id":"` + agg.String() + `","full_name":"Ada"}`), OccurredAt: time.Now().UTC(),
	}
	if err := pub.Publish(ctx, evt); err != nil {
		t.Fatal(err)
	}

	st, err := mongostore.Open(ctx, mongostore.Config{URI: mg.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close(context.Background()) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if doc, err := st.GetEvent(ctx, evt.EventID.String()); err == nil {
			if doc.Topic == "" || doc.SchemaID == 0 || doc.Offset == nil {
				t.Errorf("archived without its Kafka source: %+v", doc)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the event was not archived by the wired worker")
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.stop(t)
}
