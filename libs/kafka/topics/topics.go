// Package topics holds the Kafka topic list of RFC §6.5 as code, and creates
// the topics idempotently. It is the in-process twin of docker-compose's
// kafka-init service, which creates the same four topics with the kafka CLI.
//
// One topic per aggregate, not per event type (RFC §6.5): per-type topics
// would multiply partitions and lose ordering between related facts. The
// producer's key (the aggregate id) picks the partition, so ordering holds per
// aggregate and nowhere else.
package topics

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic names (RFC §6.5).
const (
	Identity   = "identity.events"
	Course     = "course.events"
	Enrollment = "enrollment.events"
	Progress   = "progress.events"
)

// Defaults match docker-compose's kafka-init (KAFKA_TOPIC_PARTITIONS and
// KAFKA_TOPIC_REPLICATION_FACTOR): three partitions, one replica, which is a
// single-broker development setting. RFC §20 (D5) defers the partition count
// to consumer lag under representative load, so 3 is a placeholder, not a
// tuned value.
const (
	DefaultPartitions        int32 = 3
	DefaultReplicationFactor int16 = 1
)

// Topic describes one topic.
type Topic struct {
	Name string
	// Key documents what the producer keys records on (RFC §6.5); the
	// producer, not this package, sets it.
	Key string
	// Partitions is how many partitions the topic is created with. Zero in
	// All() means "use Options.Partitions".
	Partitions int32
}

// All returns the RFC §6.5 topics. The partition count is left at zero so that
// Ensure applies Options.Partitions uniformly, as kafka-init does; a topic that
// later needs its own count sets Partitions here.
func All() []Topic {
	return []Topic{
		{Name: Identity, Key: "user_id"},
		{Name: Course, Key: "course_id"},
		{Name: Enrollment, Key: "course_id"},
		{Name: Progress, Key: "enrollment_id"},
	}
}

// Names returns the topic names of All.
func Names() []string {
	all := All()
	names := make([]string, len(all))
	for i, t := range all {
		names[i] = t.Name
	}
	return names
}

// Options sets the creation parameters. Zero fields take the defaults.
type Options struct {
	Partitions        int32
	ReplicationFactor int16
}

func (o Options) withDefaults() Options {
	if o.Partitions == 0 {
		o.Partitions = DefaultPartitions
	}
	if o.ReplicationFactor == 0 {
		o.ReplicationFactor = DefaultReplicationFactor
	}
	return o
}

// Ensure creates every topic of All with the default partition count and
// replication factor, skipping those that already exist. See EnsureWith.
func Ensure(ctx context.Context, brokers []string) error {
	return EnsureWith(ctx, brokers, Options{})
}

// EnsureWith is Ensure with explicit creation parameters. It is idempotent and
// safe to run concurrently from several processes: a topic that exists (or is
// created by someone else between the check and the create) is left alone.
// Like `kafka-topics.sh --create --if-not-exists`, it never changes an existing
// topic: partitions are not grown, and a different replication factor is not
// applied. Use kafka-topics.sh --alter to grow a topic.
func EnsureWith(ctx context.Context, brokers []string, opts Options) error {
	if len(brokers) == 0 {
		return errors.New("topics: brokers is required")
	}
	if opts.Partitions < 0 || opts.ReplicationFactor < 0 {
		return errors.New("topics: partitions and replication factor must not be negative")
	}
	opts = opts.withDefaults()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("smartcourse-topics"))
	if err != nil {
		return fmt.Errorf("topics: new client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for _, t := range All() {
		parts := t.Partitions
		if parts == 0 {
			parts = opts.Partitions
		}
		_, err := adm.CreateTopic(ctx, parts, opts.ReplicationFactor, nil, t.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("topics: create %s: %w", t.Name, err)
		}
	}
	return nil
}
