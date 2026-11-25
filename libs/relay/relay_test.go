package relay_test

import (
	"context"
	"testing"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type nilPub struct{}

func (nilPub) Publish(context.Context, relay.DecodedEvent) error { return nil }

// There is no in-process mode any more: a relay without a Publisher (or without
// a database) must fail at construction, loudly, not run and do nothing.
func TestNew_RequiresAPublisherAndADatabase(t *testing.T) {
	for name, fn := range map[string]func(){
		"nil publisher": func() { relay.New(&gorm.DB{}, nil) },
		"nil db":        func() { relay.New(nil, nilPub{}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: relay.New did not panic", name)
				}
			}()
			fn()
		}()
	}
}
