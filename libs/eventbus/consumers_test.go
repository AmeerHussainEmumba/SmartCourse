package eventbus_test

import (
	"testing"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

func TestNewConsumers_Validation(t *testing.T) {
	db := &gorm.DB{}
	cfg := config.KafkaConfig{Brokers: []string{"localhost:9092"}}
	h := func(name string, types ...string) relay.Handler { return relay.NewHandler(name, types, nil) }

	// The relay allows several handlers under one consumer name (analytics does);
	// they become one consumer group.
	if _, err := eventbus.NewConsumers(cfg, []relay.Handler{
		h("analytics-aggregator", "identity.user_registered"),
		h("analytics-aggregator", "enrollment.student_enrolled"),
		h("other", "course.course_published"),
	}, db); err != nil {
		t.Fatalf("handlers sharing a consumer name must be accepted: %v", err)
	}
	if _, err := eventbus.NewConsumers(cfg, []relay.Handler{h("c", "unknown.event")}, db); err == nil {
		t.Error("a handler whose event type has no topic must be rejected at construction")
	}
	if _, err := eventbus.NewConsumers(config.KafkaConfig{}, nil, db); err == nil {
		t.Error("no brokers must be rejected")
	}
	if _, err := eventbus.NewConsumers(cfg, nil, nil); err == nil {
		t.Error("no database must be rejected")
	}
}
