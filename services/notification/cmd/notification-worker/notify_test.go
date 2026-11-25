package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/services/notification/notification"
)

func TestDeliveryTaskID_SameWithinABucketNewAfterIt(t *testing.T) {
	payload := []byte(`{"delivery_id":"11111111-1111-1111-1111-111111111111"}`)
	start := time.Unix(1_700_000_100, 0) // inside a bucket
	same := deliveryTaskID("notification:deliver", payload, start.Add(time.Minute))
	if deliveryTaskID("notification:deliver", payload, start) != same {
		t.Error("two enqueues in one bucket must share a task ID so the second is a duplicate")
	}
	if deliveryTaskID("notification:deliver", payload, start.Add(taskIDBucket)) == same {
		t.Error("the next bucket must get a new task ID so a row whose task was archived can be enqueued again")
	}
	other := []byte(`{"delivery_id":"22222222-2222-2222-2222-222222222222"}`)
	if deliveryTaskID("notification:deliver", other, start) == deliveryTaskID("notification:deliver", payload, start) {
		t.Error("different deliveries must never share a task ID")
	}
}

func testServers(t *testing.T) *asynqx.Servers {
	t.Helper()
	cfg := &config.Config{Asynq: config.AsynqConfig{
		CriticalConcurrency: 2, DefaultConcurrency: 1, LowConcurrency: 1,
		BackoffBase: time.Second, BackoffMax: time.Second, ShutdownTimeout: time.Second,
	}}
	servers, scheduler, err := buildAsynq(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Shutdown)
	return servers
}

// The delivery task belongs on the critical queue (RFC §6.7): registering it
// again there reports the queue it already holds, so this fails if it was put
// on another queue.
func TestRegisterDelivery_RegistersOnTheCriticalQueue(t *testing.T) {
	servers := testServers(t)
	prov := notification.ProviderFunc(func(_ context.Context, _ notification.Message) error { return nil })
	ok, err := registerDelivery(slog.New(slog.NewTextHandler(io.Discard, nil)), servers, nil, prov)
	if err != nil || !ok {
		t.Fatalf("registerDelivery = %v, %v", ok, err)
	}
	err = servers.Register(notification.TaskTypeDeliver, asynqx.QueueCritical, func(context.Context, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), `on queue "critical"`) {
		t.Fatalf("delivery task is not registered on the critical queue: %v", err)
	}
}

// With no provider nothing is registered and the log says deliveries stay pending.
func TestRegisterDelivery_NoProviderRegistersNothingAndWarns(t *testing.T) {
	servers := testServers(t)
	var buf bytes.Buffer
	ok, err := registerDelivery(slog.New(slog.NewTextHandler(&buf, nil)), servers, nil, nil)
	if err != nil || ok {
		t.Fatalf("registerDelivery = %v, %v; want false, nil", ok, err)
	}
	if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), "stay pending") {
		t.Fatalf("no warning that deliveries stay pending: %q", buf.String())
	}
	// The task type is still free: nothing was registered.
	if err := servers.Register(notification.TaskTypeDeliver, asynqx.QueueCritical, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("a delivery task was registered without a provider: %v", err)
	}
}

func TestNewProvider_NilWithoutSMTPHost(t *testing.T) {
	if p := newProvider(slog.New(slog.NewTextHandler(io.Discard, nil)), &config.Config{}); p != nil {
		t.Fatalf("provider built without SMTP_HOST: %v", p)
	}
}

func TestBuildJobs_SchedulesTheProcessedEventsPrune(t *testing.T) {
	cfg := &config.Config{Jobs: config.JobsConfig{OutboxPruneInterval: time.Hour, ProcessedEventsRetention: time.Hour}}
	js, err := buildJobs(nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(js) != 1 || !strings.Contains(js[0].Name, "processed") {
		t.Fatalf("jobs = %+v, want exactly the processed_events prune", js)
	}
	cfg.Jobs.ProcessedEventsRetention = 0
	if _, err := buildJobs(nil, cfg); err == nil {
		t.Error("a zero retention was accepted")
	}
}
