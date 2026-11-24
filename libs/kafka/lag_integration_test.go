//go:build integration

package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// LagReader reports a group's lag per topic as a total over partitions: a
// group the broker does not know yields nothing, records the handler has not
// finished are lag, and the lag returns to 0 once they are handled.
func TestLagReader(t *testing.T) {
	env := testenv.SetupKafka(t)
	topic, group := uniq("lag"), uniq("lag-group")
	createTopic(t, env.Brokers, topic, 3)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{ClientID: "lag-producer"})

	lr, err := kafka.NewLagReader(env.Brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer lr.Close()
	ctx := context.Background()

	// A fresh broker answers COORDINATOR_NOT_AVAILABLE until its offsets topic
	// exists; that is an error (the caller keeps its old values), not a zero.
	waitFor(t, 30*time.Second, "an answer about a group the broker has never seen", func() bool {
		got, err := lr.Lag(ctx, group)
		if err == nil && len(got) != 0 {
			t.Fatalf("a group the broker has never seen: got %v; want no entries", got)
		}
		return err == nil
	})

	for i := 0; i < 6; i++ {
		produce(t, p, topic, string(rune('a'+i)), "v")
	}
	release := make(chan struct{})
	started := make(chan struct{}, 6)
	startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic}, ClientID: "lag-consumer",
		Handler: func(ctx context.Context, _ kafka.Record) error {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	})
	recv(t, started, "the first record to reach the handler")

	waitFor(t, 20*time.Second, "lag of 6 while the handlers are blocked", func() bool {
		l, err := lr.Lag(ctx, group)
		return err == nil && len(l) == 1 && l[0].Group == group && l[0].Topic == topic && l[0].Lag == 6
	})
	close(release)
	waitFor(t, 30*time.Second, "lag back to 0 once handled", func() bool {
		l, err := lr.Lag(ctx, group)
		return err == nil && len(l) == 1 && l[0].Lag == 0
	})
}
