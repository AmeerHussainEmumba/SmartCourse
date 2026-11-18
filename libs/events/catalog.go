package events

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	coursev1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/course/v1"
	enrollmentv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/enrollment/v1"
	eventsv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/events/v1"
	identityv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/identity/v1"
	progressv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/progress/v1"
	protos "github.com/AmeerHussainEmumba/SmartCourse/libs/events/proto"
)

// Topics (RFC §6.5): one per aggregate, not per event type.
const (
	TopicIdentity   = "identity.events"
	TopicCourse     = "course.events"
	TopicEnrollment = "enrollment.events"
	TopicProgress   = "progress.events"
)

// Type describes one event the services write to the outbox: its name, the
// topic it is published to, and the payload message.
type Type struct {
	// EventType is the outbox event_type string, e.g. "identity.user_registered".
	EventType string
	Topic     string
	// New returns an empty payload message.
	New func() proto.Message
}

// Message returns the payload's descriptor.
func (t Type) Message() protoreflect.MessageDescriptor { return t.New().ProtoReflect().Descriptor() }

// Subject is the registry subject of the payload schema: the payload message's
// full name (the registry's record-name strategy). Envelope.schema_version is
// the registry version of this subject.
func (t Type) Subject() string { return string(t.Message().FullName()) }

// ProtoFile is the path of the .proto source declaring the payload.
func (t Type) ProtoFile() string { return t.Message().ParentFile().Path() }

// Types lists every event the services emit today. Events RFC §6.5 names but
// no service emits yet (CourseCreated, CertificateIssued, ...) are not here;
// they get a message when a service starts writing them.
func Types() []Type {
	return []Type{
		{"identity.user_registered", TopicIdentity, func() proto.Message { return new(identityv1.UserRegistered) }},
		{"identity.user_profile_updated", TopicIdentity, func() proto.Message { return new(identityv1.UserProfileUpdated) }},
		{"identity.user_role_changed", TopicIdentity, func() proto.Message { return new(identityv1.UserRoleChanged) }},
		{"course.course_published", TopicCourse, func() proto.Message { return new(coursev1.CoursePublished) }},
		{"course.course_archived", TopicCourse, func() proto.Message { return new(coursev1.CourseArchived) }},
		{"enrollment.student_enrolled", TopicEnrollment, func() proto.Message { return new(enrollmentv1.StudentEnrolled) }},
		{"enrollment.student_withdrawn", TopicEnrollment, func() proto.Message { return new(enrollmentv1.StudentWithdrawn) }},
		{"progress.course_completed", TopicProgress, func() proto.Message { return new(progressv1.CourseCompleted) }},
	}
}

// Lookup finds a Type by its event_type string.
func Lookup(eventType string) (Type, bool) {
	for _, t := range Types() {
		if t.EventType == eventType {
			return t, true
		}
	}
	return Type{}, false
}

// Topics lists the four topics in a stable order.
func Topics() []string {
	return []string{TopicIdentity, TopicCourse, TopicEnrollment, TopicProgress}
}

// EnvelopeSubject is the registry subject for the envelope on topic (Kafka's
// topic-name strategy: "<topic>-value").
func EnvelopeSubject(topic string) string { return topic + "-value" }

// Subject pairs a registry subject with the schema text to register under it.
type Subject struct {
	Name   string
	Schema string
}

// Subjects returns every subject to register: the envelope once per topic and
// each payload record once. Order is stable.
func Subjects() ([]Subject, error) {
	env, err := protos.FS.ReadFile((&eventsv1.Envelope{}).ProtoReflect().Descriptor().ParentFile().Path())
	if err != nil {
		return nil, fmt.Errorf("read envelope proto: %w", err)
	}
	var out []Subject
	for _, topic := range Topics() {
		out = append(out, Subject{Name: EnvelopeSubject(topic), Schema: string(env)})
	}
	for _, t := range Types() {
		src, err := protos.FS.ReadFile(t.ProtoFile())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", t.ProtoFile(), err)
		}
		out = append(out, Subject{Name: t.Subject(), Schema: string(src)})
	}
	return out, nil
}
