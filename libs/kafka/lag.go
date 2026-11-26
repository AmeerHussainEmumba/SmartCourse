package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicLag is how far one consumer group is behind the log end of one topic,
// summed over the topic's partitions.
type TopicLag struct {
	Group string
	Topic string
	// Lag is the number of records between the group's committed offsets and
	// the partitions' end offsets. Records the group has fetched but whose
	// handler has not finished are still lag: offsets commit after the handler.
	Lag int64
}

// LagReader reads consumer group lag from the brokers. It holds one client, so
// build it once and call Lag on an interval; it is safe for concurrent use.
type LagReader struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// NewLagReader builds the reader. It does not contact the brokers.
func NewLagReader(brokers []string) (*LagReader, error) {
	if len(brokers) == 0 {
		return nil, errInvalid("Brokers is required")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("smartcourse-lag"), kgo.AlwaysRetryEOF())
	if err != nil {
		return nil, fmt.Errorf("kafka: lag reader: %w", err)
	}
	return &LagReader{cl: cl, adm: kadm.NewClient(cl)}, nil
}

// Close releases the client.
func (r *LagReader) Close() { r.cl.Close() }

// Lag returns one entry per (group, topic) the groups have committed offsets
// for or are assigned, sorted by group then topic. A group the broker does not
// know (not started yet) contributes nothing. The partition is deliberately not
// part of the result: it would make metric label values depend on topic
// configuration and rebalances, and a total per topic is what an alert needs.
//
// It fails when the brokers cannot be reached or any group's lag cannot be
// computed, so a caller keeps its previous values instead of reporting a false
// zero.
func (r *LagReader) Lag(ctx context.Context, groups ...string) ([]TopicLag, error) {
	lags, err := r.adm.Lag(ctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("kafka: group lag: %w", err)
	}
	var out []TopicLag
	for _, g := range groups {
		l, ok := lags[g]
		if !ok {
			continue
		}
		if err := l.Error(); err != nil {
			return nil, fmt.Errorf("kafka: group %s lag: %w", g, err)
		}
		totals := map[string]int64{}
		for topic, parts := range l.Lag {
			for p, pl := range parts {
				if pl.Err != nil {
					return nil, fmt.Errorf("kafka: group %s lag on %s[%d]: %w", g, topic, p, pl.Err)
				}
				totals[topic] += max(pl.Lag, 0)
			}
		}
		for topic, n := range totals {
			out = append(out, TopicLag{Group: g, Topic: topic, Lag: n})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Topic < out[j].Topic
	})
	return out, nil
}
