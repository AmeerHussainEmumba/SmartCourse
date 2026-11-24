//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

// series returns the value of every series of metric name, keyed by its label
// values as "k=v,k=v" (empty for an unlabelled metric).
func series(t *testing.T, g prometheus.Gatherer, name string) map[string]float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			key := ""
			for _, l := range m.GetLabel() {
				if key != "" {
					key += ","
				}
				key += l.GetName() + "=" + l.GetValue()
			}
			switch {
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			}
		}
	}
	return out
}

func scalarSeries(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	return series(t, g, name)[""]
}

func allZeroAndPresent(s map[string]float64) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return len(s) > 0
}

// enrollAs enrolls a fresh student through the HTTP route.
func (p *pipeline) enrollAs(student uuid.UUID) {
	p.t.Helper()
	token, err := auth.IssueAccessToken(p.cfg.JWT, student, "student", 1)
	if err != nil {
		p.t.Fatal(err)
	}
	old := p.token
	p.token = token
	defer func() { p.token = old }()
	p.enroll("")
}

// E5 with the numbers (RFC §12.3): with Kafka down the API keeps accepting
// writes, outbox_backlog rises and outbox_oldest_unpublished_seconds grows;
// when the broker is back both fall to zero and kafka_consumer_lag returns to
// 0. Real Postgres, Redis and Kafka. The same registry passes the cardinality
// guard, and event_processed_total counted the consumed events.
func TestWorker_Metrics_KafkaOutageAndRecovery(t *testing.T) {
	p := startPipeline(t, config.TransportKafka, false)
	g := p.obs.Gatherer()

	// Healthy: one event flows end to end; the lag series exist and read 0.
	p.enroll("")
	p.waitSent()
	pollUntil(t, 60*time.Second, func() []string {
		var problems []string
		if v := scalarSeries(t, g, telemetry.MetricOutboxBacklog); v != 0 {
			problems = append(problems, "outbox_backlog is not 0 after the event was delivered")
		}
		if !allZeroAndPresent(series(t, g, telemetry.MetricKafkaConsumerLag)) {
			problems = append(problems, "kafka_consumer_lag has no series or a non-zero one: "+formatSeries(series(t, g, telemetry.MetricKafkaConsumerLag)))
		}
		return problems
	})
	lagBefore := series(t, g, telemetry.MetricKafkaConsumerLag)
	if _, ok := lagBefore["group=notification-dispatcher,topic=enrollment.events"]; !ok {
		t.Errorf("no kafka_consumer_lag series for the notification group on enrollment.events: %s", formatSeries(lagBefore))
	}
	processed := series(t, g, telemetry.MetricEventProcessedTotal)
	if processed["consumer=notification-dispatcher,outcome=processed"] < 1 {
		t.Errorf("event_processed_total{notification-dispatcher, processed} not counted: %s", formatSeries(processed))
	}

	// Kafka goes away. Writes still succeed (the outbox buffers them).
	p.kafka.Stop(t)
	for i := 0; i < 3; i++ {
		p.enrollAs(uuid.New())
	}
	pollUntil(t, 30*time.Second, func() []string {
		if v := scalarSeries(t, g, telemetry.MetricOutboxBacklog); v < 3 {
			return []string{"outbox_backlog is below 3 while the broker is down"}
		}
		return nil
	})
	first := scalarSeries(t, g, telemetry.MetricOutboxOldestUnpublish)
	pollUntil(t, 30*time.Second, func() []string { // grows with time, not a one-off sample
		if v := scalarSeries(t, g, telemetry.MetricOutboxOldestUnpublish); v <= first+0.5 {
			return []string{"outbox_oldest_unpublished_seconds is not growing"}
		}
		return nil
	})
	if first <= 0 {
		t.Errorf("outbox_oldest_unpublished_seconds = %v with unpublished rows", first)
	}

	// Kafka is back: everything drains, the gauges fall, the lag is zero.
	p.kafka.Start(t)
	pollUntil(t, 120*time.Second, func() []string {
		var problems []string
		if v := scalarSeries(t, g, telemetry.MetricOutboxBacklog); v != 0 {
			problems = append(problems, "outbox_backlog has not returned to 0")
		}
		if v := scalarSeries(t, g, telemetry.MetricOutboxOldestUnpublish); v != 0 {
			problems = append(problems, "outbox_oldest_unpublished_seconds has not returned to 0")
		}
		lag := series(t, g, telemetry.MetricKafkaConsumerLag)
		if !allZeroAndPresent(lag) {
			problems = append(problems, "kafka_consumer_lag has not returned to 0: "+formatSeries(lag))
		}
		return problems
	})
	if n := scalarOf(p.env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Errorf("%d outbox rows are still unpublished", n)
	}

	if err := telemetry.CheckLabels(g); err != nil {
		t.Errorf("the worker's registry fails the cardinality guard: %v", err)
	}
	for name := range series(t, g, telemetry.MetricKafkaConsumerLag) {
		// The labels are group and topic only: no partition, no offset.
		for _, bad := range []string{"partition", "offset"} {
			if containsLabel(name, bad) {
				t.Errorf("kafka_consumer_lag carries a %s label: %s", bad, name)
			}
		}
	}
}

func containsLabel(key, label string) bool {
	return strings.HasPrefix(key, label+"=") || strings.Contains(key, ","+label+"=")
}

func formatSeries(s map[string]float64) string {
	out := ""
	for k, v := range s {
		out += fmt.Sprintf("{%s}=%v ", k, v)
	}
	return out
}

// countingStore is a ParkStore that records parks, standing in for MongoDB: the
// counter under test is in countParked, which wraps whatever store sits behind.
type countingStore struct {
	mu    sync.Mutex
	parks []mongostore.ParkInput
	err   error
}

func (c *countingStore) ParkFailedEvent(_ context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.parks = append(c.parks, in)
	return &mongostore.FailedEvent{}, nil
}

func (c *countingStore) consumers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, p := range c.parks {
		out = append(out, p.Consumer)
	}
	return out
}

// alwaysFails is a consumer whose handler never succeeds on identity.user_registered.
func alwaysFails(name string) relay.Handler {
	return relay.NewHandler(name, []string{"identity.user_registered"},
		func(context.Context, *gorm.DB, relay.DecodedEvent) error { return errors.New("poison") })
}

func writeUserRegistered(t *testing.T, p *pipeline) {
	t.Helper()
	id := uuid.New()
	if err := p.env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "user", AggregateID: id, EventType: "identity.user_registered", SchemaVersion: 1,
			Payload: map[string]any{"user_id": id, "email": "poison@example.com", "role": "student", "full_name": "Poison"},
		})
	}); err != nil {
		t.Fatal(err)
	}
}

// Parking an event through the worker's dead-letter sink increments
// dead_letter_total{consumer}, whichever path gave up on it: the in-process
// relay running out of attempts on a handler, or (kafka) a consumer group using
// up its budget. A park the store refuses is not a dead letter and is not
// counted.
func TestWorker_DeadLetterTotal_CountsParkedEventsOnBothTransports(t *testing.T) {
	for _, mode := range transportModes {
		t.Run(mode, func(t *testing.T) {
			store := &countingStore{}
			p := startPipelineWith(t, mode, false, parkingSink(store), alwaysFails("poison-test"))
			writeUserRegistered(t, p)

			pollUntil(t, 90*time.Second, func() []string {
				if got := series(t, p.obs.Gatherer(), telemetry.MetricDeadLetterTotal)["consumer=poison-test"]; got != 1 {
					return []string{"dead_letter_total{consumer=poison-test} is not 1"}
				}
				return nil
			})
			if got := store.consumers(); len(got) != 1 || got[0] != "poison-test" {
				t.Errorf("the store parked %v, want exactly [poison-test]", got)
			}
			if err := telemetry.CheckLabels(p.obs.Gatherer()); err != nil {
				t.Error(err)
			}
			if mode == config.TransportKafka {
				ev := series(t, p.obs.Gatherer(), telemetry.MetricEventProcessedTotal)
				if ev["consumer=poison-test,outcome=dead_lettered"] != 1 || ev["consumer=poison-test,outcome=failed"] < 1 {
					t.Errorf("event_processed_total for the poison consumer: %s, want dead_lettered=1 and failed>=1", formatSeries(ev))
				}
			}
		})
	}
}

// The relay's own give-up path: a row that cannot be encoded for Kafka is
// parked with consumer "relay" after its attempts, and counted.
func TestWorker_DeadLetterTotal_RelayGiveUpIsCounted(t *testing.T) {
	store := &countingStore{}
	p := startPipelineWith(t, config.TransportKafka, false, parkingSink(store))
	if err := p.env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "x", AggregateID: uuid.New(), EventType: "identity.no_such_event", SchemaVersion: 1, Payload: map[string]any{"a": 1},
		})
	}); err != nil {
		t.Fatal(err)
	}
	pollUntil(t, 60*time.Second, func() []string {
		if got := series(t, p.obs.Gatherer(), telemetry.MetricDeadLetterTotal)["consumer="+relay.RelayConsumer]; got != 1 {
			return []string{"dead_letter_total{consumer=relay} is not 1"}
		}
		return nil
	})
	if _, _, g := p.rl.Stats(); g != 1 {
		t.Errorf("relay given up = %d, want 1", g)
	}
}

// A store that refuses the park (MongoDB down) leaves the counter alone.
func TestWorker_DeadLetterTotal_RefusedParkIsNotCounted(t *testing.T) {
	reg := telemetry.NewRegistry()
	m, err := telemetry.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{err: errors.New("mongo down")}
	sink := deadLetterAdapter{deadletter.NewSink(countParked(store, m))}
	d := relay.DeadLetter{Event: relay.DecodedEvent{EventID: uuid.New(), EventType: "identity.user_registered"}, Consumer: "c", Err: errors.New("x"), Attempts: 5}
	if err := sink.Park(context.Background(), d); err == nil {
		t.Fatal("the refused park reported success")
	}
	if got := series(t, reg, telemetry.MetricDeadLetterTotal); len(got) != 0 {
		t.Errorf("a refused park was counted: %v", got)
	}
}

// parkingSink builds the dead-letter sink the way main does (deadletter.NewSink
// over countParked, behind the relay adapter) on store.
func parkingSink(store parkStore) func(*telemetry.Metrics) relay.DeadLetterSink {
	return func(m *telemetry.Metrics) relay.DeadLetterSink {
		return deadLetterAdapter{deadletter.NewSink(countParked(store, m))}
	}
}
