//go:build integration

package topics_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// partitions describes every topic of topics.All on the broker.
func partitions(t *testing.T, brokers []string) map[string]int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	details, err := kadm.NewClient(cl).ListTopics(ctx, topics.Names()...)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	out := map[string]int{}
	for name, d := range details {
		if d.Err == nil {
			out[name] = len(d.Partitions)
		}
	}
	return out
}

func TestEnsure_CreatesIdempotentlyAndNeverAlters(t *testing.T) {
	env := testenv.SetupKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := topics.Ensure(ctx, env.Brokers); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	got := partitions(t, env.Brokers)
	if len(got) != len(topics.All()) {
		t.Fatalf("after Ensure the broker has %v, want all of %v", got, topics.Names())
	}
	for name, n := range got {
		if n != int(topics.DefaultPartitions) {
			t.Errorf("%s has %d partitions, want %d", name, n, topics.DefaultPartitions)
		}
	}

	if err := topics.Ensure(ctx, env.Brokers); err != nil {
		t.Fatalf("second Ensure must be a no-op, got: %v", err)
	}
	// Like kafka-init's --if-not-exists, a later call with other numbers does
	// not touch a topic that exists.
	if err := topics.EnsureWith(ctx, env.Brokers, topics.Options{Partitions: 7}); err != nil {
		t.Fatalf("EnsureWith on existing topics: %v", err)
	}
	for name, n := range partitions(t, env.Brokers) {
		if n != int(topics.DefaultPartitions) {
			t.Errorf("%s changed to %d partitions; Ensure must never alter an existing topic", name, n)
		}
	}
}
