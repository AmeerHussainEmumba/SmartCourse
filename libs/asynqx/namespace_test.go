package asynqx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestQueueName(t *testing.T) {
	if got := QueueName("", QueueLow); got != "low" {
		t.Errorf("empty namespace: %q, want low (no prefix)", got)
	}
	if got := QueueName("course", QueueLow); got != "course:low" {
		t.Errorf("QueueName = %q, want course:low", got)
	}
}

func TestValidateNamespace(t *testing.T) {
	for _, ok := range []string{"", "course", "identity", "a", "svc-1", "svc_1"} {
		if err := ValidateNamespace(ok); err != nil {
			t.Errorf("ValidateNamespace(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"Course", "a:b", "1a", "-a", "a b", strings.Repeat("a", 33), "course:low"} {
		if err := ValidateNamespace(bad); err == nil {
			t.Errorf("ValidateNamespace(%q) = nil, want an error", bad)
		}
	}
}

func TestJobOptionsIn_PrefixesQueueAndRejectsPrefixedNames(t *testing.T) {
	_, q, err := Job{Type: "t", ID: "i", Queue: QueueCritical}.optionsIn("notification")
	if err != nil || q != "notification:critical" {
		t.Fatalf("queue = %q, err = %v; want notification:critical", q, err)
	}
	_, q, err = Job{Type: "t", ID: "i"}.optionsIn("notification")
	if err != nil || q != "notification:default" {
		t.Fatalf("default queue = %q, err = %v; want notification:default", q, err)
	}
	// A prefixed name is not a logical queue: another service's queue cannot be
	// named, not even by repeating the prefix.
	for _, name := range []string{"course:low", "notification:low"} {
		_, _, err = Job{Type: "t", ID: "i", Queue: name}.optionsIn("notification")
		if !errors.Is(err, ErrInvalidJob) {
			t.Errorf("Queue %q: err = %v, want ErrInvalidJob", name, err)
		}
	}
}

func TestTickIDIn_IsNamespaced(t *testing.T) {
	slot := time.Unix(1_700_000_040, 0)
	const name = "maintenance:outbox-prune"
	if TickIDIn("", name, slot) != TickID(name, slot) {
		t.Error("TickID must equal TickIDIn with the empty namespace (unchanged behaviour)")
	}
	ids := map[string]string{}
	for _, ns := range []string{"", "identity", "course", "analytics", "notification"} {
		id := TickIDIn(ns, name, slot)
		if prev, dup := ids[id]; dup {
			t.Errorf("namespaces %q and %q share tick ID %s", prev, ns, id)
		}
		ids[id] = ns
	}
}

func TestNewServers_NamespacePrefixesQueuesAndRejectsPrefixedRegister(t *testing.T) {
	ss, err := NewServers(ServersConfig{Redis: RedisClient("127.0.0.1:1", "", 0), Namespace: "course", Pools: map[string]int{QueueLow: 1, QueueCritical: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if got := ss.Queues(); len(got) != 2 || got[0] != "course:critical" || got[1] != "course:low" {
		t.Fatalf("Queues = %v, want [course:critical course:low]", got)
	}
	if ss.Namespace() != "course" {
		t.Errorf("Namespace = %q", ss.Namespace())
	}
	fn := func(context.Context, []byte) error { return nil }
	for _, q := range []string{"course:low", "notification:low"} {
		if err := ss.Register("x", q, fn); !errors.Is(err, ErrUnknownQueue) {
			t.Errorf("Register on %q: %v, want ErrUnknownQueue", q, err)
		}
	}
	if err := ss.Register("x", QueueLow, fn); err != nil {
		t.Errorf("Register on the logical queue: %v", err)
	}
}

func TestNewServers_RejectsInvalidNamespace(t *testing.T) {
	if _, err := NewServers(ServersConfig{Redis: RedisClient("127.0.0.1:1", "", 0), Namespace: "a:b"}); err == nil {
		t.Error("NewServers accepted namespace a:b")
	}
	if _, err := NewScheduler(SchedulerConfig{Redis: RedisClient("127.0.0.1:1", "", 0), Namespace: "A"}); err == nil {
		t.Error("NewScheduler accepted namespace A")
	}
}
