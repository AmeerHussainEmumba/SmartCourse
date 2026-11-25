package main

import (
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/activities"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// One consumer set carries the three handler families, and every group reads its
// source topics plus its own retry topic (replay is republish to retry.<name>).
func TestBuildConsumers_OneSetWithAllThreeGroupsAndTheirRetryTopics(t *testing.T) {
	c, err := buildConsumers(config.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}, &gorm.DB{}, nil, quiet(), &infra.Observability{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		topics.ConsumerCourseInstructorMirror:   {topics.Identity},
		topics.ConsumerEnrollmentCourseArchival: {topics.Course},
		topics.ConsumerCertificateIssuer:        {topics.Progress},
	}
	got := c.Groups()
	slices.Sort(got)
	if !reflect.DeepEqual(got, []string{"certificate-issuer", "course-instructor-mirror", "enrollment-course-archival"}) {
		t.Fatalf("groups = %v", got)
	}
	for group, source := range want {
		wantTopics := append(slices.Clone(source), topics.Retry(group))
		slices.Sort(wantTopics)
		gotTopics := c.Topics(group)
		slices.Sort(gotTopics)
		if !reflect.DeepEqual(gotTopics, wantTopics) {
			t.Errorf("%s topics = %v, want %v", group, gotTopics, wantTopics)
		}
	}
}

// The activities answer to the contract's names, all of them and no others.
func TestRegisterActivities_UsesTheContractNames(t *testing.T) {
	rec := &recorder{}
	registerActivities(rec, activities.NewActivities(nil))
	if !reflect.DeepEqual(rec.names, publishing.ActivityNames()) {
		t.Fatalf("registered %v, want the contract's %v", rec.names, publishing.ActivityNames())
	}
	for i, fn := range rec.fns {
		if reflect.TypeOf(fn).Kind() != reflect.Func {
			t.Errorf("%s: registered a %T, want a function", rec.names[i], fn)
		}
	}
}

type recorder struct {
	names []string
	fns   []any
}

func (r *recorder) RegisterActivityWithOptions(a any, o activity.RegisterOptions) {
	r.names = append(r.names, o.Name)
	r.fns = append(r.fns, a)
}

func TestProcessedEventsRetention(t *testing.T) {
	env := func(v string) func(string) string { return func(string) string { return v } }
	if d, err := processedEventsRetention(env("")); err != nil || d != 14*24*time.Hour {
		t.Errorf("default = %v, %v; want 14 days", d, err)
	}
	if d, err := processedEventsRetention(env("72h")); err != nil || d != 72*time.Hour {
		t.Errorf("72h = %v, %v", d, err)
	}
	for _, bad := range []string{"soon", "0s", "-1h"} {
		if _, err := processedEventsRetention(env(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func jobsConfig() *config.Config {
	return &config.Config{
		Jobs: config.JobsConfig{
			ReconcilePublishesInterval: time.Minute, EnrollmentCountInterval: time.Hour,
			IdempotencyPruneInterval: time.Hour, OutboxPruneInterval: time.Hour, OutboxRetention: 168 * time.Hour,
		},
		Asynq: config.AsynqConfig{
			LowConcurrency: 3, MaxRetry: 5, BackoffBase: time.Second, BackoffMax: 10 * time.Second, ShutdownTimeout: time.Second,
		},
		Redis: config.RedisConfig{Addr: "127.0.0.1:1"},
	}
}

// Every job the course worker owns is scheduled, the stale-publish check every
// minute, and a bad retention is a start-up error.
func TestBuildJobs_SchedulesTheFiveCourseJobs(t *testing.T) {
	js, err := buildJobs(&gorm.DB{}, jobsConfig(), nil, 14*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	spec := map[string]string{}
	for _, j := range js {
		spec[j.Name] = j.Spec
	}
	for _, name := range []string{
		TypeReconcileStalePublishes, TypeEnrollmentCounts, jobs.TypeIdempotencyPrune, jobs.TypeOutboxPrune, jobs.TypeProcessedPrune,
	} {
		if _, ok := spec[name]; !ok {
			t.Errorf("job %s is not scheduled (have %v)", name, spec)
		}
	}
	if len(js) != 5 {
		t.Errorf("%d jobs, want 5", len(js))
	}
	if spec[TypeReconcileStalePublishes] != "* * * * *" {
		t.Errorf("stale-publish spec = %q, want every minute", spec[TypeReconcileStalePublishes])
	}
	if _, err := buildJobs(&gorm.DB{}, jobsConfig(), nil, 0); err == nil || !strings.Contains(err.Error(), "processed_events") {
		t.Errorf("zero processed_events retention accepted: %v", err)
	}
}

// The jobs register on the scheduler and the low-queue pool without Redis.
func TestBuildAsynq_RegistersEveryJobOnTheLowQueue(t *testing.T) {
	js, err := buildJobs(&gorm.DB{}, jobsConfig(), nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	servers, scheduler, err := buildAsynq(jobsConfig(), quiet(), js)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()
	if q := servers.Queues(); len(q) != 1 || q[0] != "low" {
		t.Errorf("queues = %v, want only the maintenance (low) queue", q)
	}
}
