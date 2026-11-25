// Package topics holds the Kafka topic list of RFC §6.5 as code, and creates
// the topics idempotently. It is the in-process twin of docker-compose's
// kafka-init service, which creates the same topics with the kafka CLI: the
// four event topics and one retry topic per consumer (design §4.1).
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
	"strings"

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

// RetryPrefix starts the name of every retry topic: a consumer named N reads
// "retry.N" next to its source topics. The audit service republishes a parked
// event there, and the consuming service's own consumer group handles it
// through the normal idempotent path (docs/plans/microservices-split.md §4.1).
const RetryPrefix = "retry."

// Consumer names, the Kafka consumer group ids of the handlers that exist
// today (the ConsumerName of each relay.Handler). A service that adds a consumer
// adds its name here, which creates its retry topic; a test fails when the
// compose file's list differs.
const (
	ConsumerCertificateIssuer        = "certificate-issuer"
	ConsumerAnalyticsAggregator      = "analytics-aggregator"
	ConsumerCourseInstructorMirror   = "course-instructor-mirror"
	ConsumerEnrollmentCourseArchival = "enrollment-course-archival"
	ConsumerNotificationDispatcher   = "notification-dispatcher"
	ConsumerAuditArchiver            = "audit-archiver"
)

// Retry returns the retry topic of a consumer.
func Retry(consumer string) string { return RetryPrefix + consumer }

// IsRetry reports whether name is a retry topic.
func IsRetry(name string) bool { return strings.HasPrefix(name, RetryPrefix) }

// Consumers returns every known consumer name.
func Consumers() []string {
	return []string{
		ConsumerCertificateIssuer, ConsumerAnalyticsAggregator, ConsumerCourseInstructorMirror,
		ConsumerEnrollmentCourseArchival, ConsumerNotificationDispatcher, ConsumerAuditArchiver,
	}
}

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

// RetryTopics returns the retry topic of every known consumer. Like All, the
// partition count is left at zero (Options.Partitions applies).
func RetryTopics() []Topic {
	cs := Consumers()
	out := make([]Topic, len(cs))
	for i, c := range cs {
		out[i] = Topic{Name: Retry(c), Key: "aggregate_id"}
	}
	return out
}

// Provisioned is every topic Ensure creates: the event topics, then the retry
// topics. It is what docker-compose's kafka-init must list too.
func Provisioned() []Topic { return append(All(), RetryTopics()...) }

// ProvisionedNames returns the names of Provisioned.
func ProvisionedNames() []string {
	var names []string
	for _, t := range Provisioned() {
		names = append(names, t.Name)
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

// Ensure creates every topic of Provisioned with the default partition count and
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

	for _, t := range Provisioned() {
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
