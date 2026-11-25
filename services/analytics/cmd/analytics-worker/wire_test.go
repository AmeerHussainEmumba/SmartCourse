package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

func kafkaCfg() config.KafkaConfig {
	return config.KafkaConfig{
		Transport: config.TransportKafka, Brokers: []string{"127.0.0.1:1"}, ConsumerMaxInFlight: 2, ConsumerMaxAttempts: 3,
		ConsumerBackoffBase: time.Millisecond, ConsumerBackoffMax: time.Millisecond, ConsumerShutdownTimeout: time.Second,
	}
}

// The consumers are one group named by the handlers' consumer name, subscribed
// to every source topic the analytics handlers need and to the group's retry
// topic (replay by republish lands there).
func TestBuildConsumers_OneGroupOnSourceTopicsAndRetryTopic(t *testing.T) {
	c, err := buildConsumers(kafkaCfg(), &gorm.DB{}, nil, slog.Default(), &infra.Observability{})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Groups(); !slices.Equal(got, []string{"analytics-aggregator"}) {
		t.Fatalf("groups = %v, want [analytics-aggregator]", got)
	}
	got := c.Topics("analytics-aggregator")
	for _, want := range []string{
		topics.Retry("analytics-aggregator"),
		"identity.user_registered", "identity.user_role_changed", "course.course_published",
		"enrollment.student_enrolled", "enrollment.student_withdrawn", "progress.course_completed", "course.publish_failed",
	} {
		if !slices.Contains(got, want) && !slices.ContainsFunc(got, func(topic string) bool { return topicCarries(topic, want) }) {
			t.Errorf("group is not subscribed to a topic carrying %q (subscribed: %v)", want, got)
		}
	}
	retry := 0
	for _, tp := range got {
		if topics.IsRetry(tp) {
			retry++
		}
	}
	if retry != 1 {
		t.Errorf("want exactly one retry topic, subscribed: %v", got)
	}
}

// topicCarries: topics follow the event type's prefix (libs/eventbus routing),
// so "enrollment.student_enrolled" travels on the enrollment topic.
func topicCarries(topic, eventType string) bool {
	prefix, _, _ := strings.Cut(eventType, ".")
	return strings.Contains(topic, prefix)
}

func TestBuildConsumers_NeedsBrokers(t *testing.T) {
	k := kafkaCfg()
	k.Brokers = nil
	if _, err := buildConsumers(k, &gorm.DB{}, nil, slog.Default(), &infra.Observability{}); err == nil {
		t.Fatal("consumers without brokers must not build")
	}
}

func jobsCfg() *config.Config {
	return &config.Config{Jobs: config.JobsConfig{AnalyticsReconcileInterval: time.Minute}}
}

// The reconciler runs every minute and processed_events is pruned; nothing
// else (analytics owns no outbox or idempotency keys).
func TestBuildJobs_ReconcilerEveryMinuteAndProcessedPrune(t *testing.T) {
	t.Setenv(ProcessedRetentionEnv, "")
	js, err := buildJobs(&gorm.DB{}, nil, jobsCfg())
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]string{}
	for _, j := range js {
		byName[j.Name] = j.Spec
		if j.Run == nil || j.Timeout <= 0 {
			t.Errorf("job %s has no Run or timeout", j.Name)
		}
	}
	if byName[TypeReconcile] != "* * * * *" {
		t.Errorf("reconciler spec = %q, want every minute (all jobs: %v)", byName[TypeReconcile], byName)
	}
	if _, ok := byName["maintenance:processed-events-prune"]; !ok || len(js) != 2 {
		t.Errorf("jobs = %v, want the reconciler and the processed_events prune only", byName)
	}
}

func TestBuildJobs_ReconcileIntervalComesFromConfig(t *testing.T) {
	cfg := jobsCfg()
	cfg.Jobs.AnalyticsReconcileInterval = 5 * time.Minute
	js, err := buildJobs(&gorm.DB{}, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if js[0].Name != TypeReconcile || js[0].Spec != "*/5 * * * *" {
		t.Fatalf("reconciler job = %s %q", js[0].Name, js[0].Spec)
	}
}

func TestProcessedRetention(t *testing.T) {
	env := func(v string) func(string) string { return func(string) string { return v } }
	if d, err := processedRetention(env("")); err != nil || d != 14*24*time.Hour {
		t.Fatalf("default = %v %v, want 14 days", d, err)
	}
	if d, err := processedRetention(env("48h")); err != nil || d != 48*time.Hour {
		t.Fatalf("48h = %v %v", d, err)
	}
	for _, bad := range []string{"0", "-1h", "soon", "14d"} {
		if _, err := processedRetention(env(bad)); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
	t.Setenv(ProcessedRetentionEnv, "nonsense")
	if _, err := buildJobs(&gorm.DB{}, nil, jobsCfg()); err == nil {
		t.Error("buildJobs accepted an invalid retention")
	}
}

// Analytics produces no events: this binary must have no outbox and no relay.
// A source scan, so a later edit that adds either fails loudly.
func TestWorkerHasNoOutboxOrRelay(t *testing.T) {
	var offenders []string
	for _, dir := range []string{".", "../../analytics", "../../failedevents"} {
		files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, bad := range []string{"relay.New(", "outbox.Write", "libs/outbox\"", "NewPublisher", "WithOutbox"} {
				if strings.Contains(string(b), bad) {
					offenders = append(offenders, f+": "+bad)
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("analytics must not publish events: %v", offenders)
	}
}

type fakeStore struct {
	got []mongostore.ParkInput
	err error
}

func (f *fakeStore) ParkFailedEvent(_ context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.got = append(f.got, in)
	return &mongostore.FailedEvent{}, nil
}

func deadLetter() relay.DeadLetter {
	actor := uuid.New()
	return relay.DeadLetter{
		Event: relay.DecodedEvent{
			OutboxID: 7, EventID: uuid.New(), AggregateType: "course", AggregateID: uuid.New(),
			EventType: "course.course_published", SchemaVersion: 1, Payload: []byte(`{"a":1}`),
			OccurredAt: time.Now(), ActorID: &actor,
		},
		Consumer: "analytics-aggregator", Err: errors.New("boom"), History: []error{errors.New("e1"), errors.New("e2")}, Attempts: 2,
	}
}

func TestDeadLetters_ParkMapsTheEventAndCountsOnlyWhatWasParked(t *testing.T) {
	reg := telemetry.NewRegistry()
	m, err := telemetry.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	deadLettered := func() float64 {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		var n float64
		for _, mf := range mfs {
			if mf.GetName() == "dead_letter_total" {
				for _, mm := range mf.GetMetric() {
					n += mm.GetCounter().GetValue()
				}
			}
		}
		return n
	}
	st := &fakeStore{}
	d := newDeadLetters(config.MongoConfig{}, m)
	d.override = st
	dl := deadLetter()
	if err := d.Park(context.Background(), dl); err != nil {
		t.Fatal(err)
	}
	if len(st.got) != 1 {
		t.Fatalf("parked %d, want 1", len(st.got))
	}
	if n := deadLettered(); n != 1 {
		t.Errorf("dead_letter_total = %v after one park, want 1", n)
	}
	in := st.got[0]
	if in.EventID != dl.Event.EventID.String() || in.Consumer != "analytics-aggregator" || in.EventType != "course.course_published" ||
		in.Attempts != 2 || len(in.History) != 2 || in.History[1] != "e2" || in.Err != "boom" || in.ActorID == "" || string(in.Payload) != `{"a":1}` {
		t.Errorf("park input = %+v", in)
	}
	st.err = errors.New("mongo down")
	if err := d.Park(context.Background(), dl); err == nil {
		t.Error("a failed park must return an error (not parked)")
	}
	if n := deadLettered(); n != 1 {
		t.Errorf("dead_letter_total = %v: a failed park must not count", n)
	}
	if err := d.Park(context.Background(), relay.DeadLetter{Event: dl.Event}); err == nil {
		t.Error("a park without a consumer must be rejected")
	}
}

// Before MongoDB has connected, Park reports "not parked" so the consumers keep
// the event loud instead of treating it as parked.
func TestDeadLetters_NotConnectedIsNotParked(t *testing.T) {
	d := newDeadLetters(config.MongoConfig{URI: "mongodb://127.0.0.1:1"}, nil)
	if err := d.Park(context.Background(), deadLetter()); !errors.Is(err, errMongoNotReady) {
		t.Fatalf("Park before connect = %v, want errMongoNotReady", err)
	}
	if err := d.Ping(context.Background()); err == nil {
		t.Fatal("Ping before connect must fail")
	}
}
