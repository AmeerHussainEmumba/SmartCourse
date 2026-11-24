//go:build integration

package kafka_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// The relay's usage, which TestBrokerRestart (one long delivery timeout, no
// caller retries) does not cover: a SHORT delivery timeout, callers that retry a
// failed Produce themselves, and a broker that goes away and comes back. A
// version of Produce that waited on franz-go alone was seen to hang for ever in
// this shape (a record neither delivered nor failed; found through the worker's
// transport tests, about one run in four). Produce must therefore return within
// DeliveryTimeout plus its grace every time, and the producer must work again
// after the restart.
//
// The hang is not deterministic, so a pass is evidence, not proof.
func TestProducer_ShortDeliveryTimeoutAcrossRestart_NeverHangs(t *testing.T) {
	const timeout = 2 * time.Second
	const bound = timeout + 3*time.Second /* the producer's grace */ + 4*time.Second /* scheduling slack */
	const producers = 3

	env := testenv.SetupKafka(t)
	topic := uniq("stuck")
	createTopic(t, env.Brokers, topic, 3)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{DeliveryTimeout: timeout})

	var acked atomic.Int64
	var slowest atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for k := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "key" + strconv.Itoa(k)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				_, err := p.Produce(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(key + ":" + strconv.Itoa(i))})
				cancel()
				if d := time.Since(start).Nanoseconds(); d > slowest.Load() {
					slowest.Store(d)
				}
				if err == nil {
					acked.Add(1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	waitFor(t, 60*time.Second, "records acked before the outage", func() bool { return acked.Load() >= 10 })
	env.Stop(t)
	time.Sleep(8 * time.Second) // bounded: several failed produces against the dead broker
	env.Start(t)
	before := acked.Load()
	waitFor(t, 90*time.Second, "the producer to deliver again after the restart", func() bool { return acked.Load() >= before+10 })
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(bound + 10*time.Second):
		t.Fatal("a Produce call did not return: the producer hung")
	}
	if d := time.Duration(slowest.Load()); d > bound {
		t.Fatalf("the slowest Produce took %s, want at most %s (DeliveryTimeout plus grace)", d, bound)
	}
}
