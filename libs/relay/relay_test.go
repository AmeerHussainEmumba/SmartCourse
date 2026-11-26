package relay_test

import (
	"context"
	"testing"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type nilPub struct{}

func (nilPub) Publish(context.Context, relay.DecodedEvent) error { return nil }

// There is no in-process mode any more: a relay without a Publisher must fail at
// construction, loudly, not run and do nothing.
func TestNew_RequiresAPublisher(t *testing.T) {
	defer func() {
		if p := recover(); p == nil {
			t.Error("relay.New with a nil Publisher did not panic")
		}
	}()
	relay.New(&gorm.DB{}, nil)
}

func TestNew_WithAPublisherDoesNotPanic(t *testing.T) {
	if relay.New(&gorm.DB{}, nilPub{}) == nil {
		t.Fatal("relay.New returned nil")
	}
}
