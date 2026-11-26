package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
)

// prepareTimeout bounds startup provisioning and the registry check.
const prepareTimeout = 60 * time.Second

// Prepare is a service's Kafka start-up: create the topics that are
// missing (idempotent, RFC §6.5) and read the schema ids and versions the
// producer frames messages with. It fails with a message that names the
// problem when the broker or the registry is unreachable or a subject has not
// been registered, rather than letting the relay discover it row by row.
//
// It does not register schemas: that is libs/events/cmd/register-schemas, run
// by the deploy (docs/plans/week-3.md). Registering here would let a worker
// with a stale binary change the registry.
func Prepare(ctx context.Context, brokers []string, registryURL string) (Schemas, error) {
	ctx, cancel := context.WithTimeout(ctx, prepareTimeout)
	defer cancel()
	if err := topics.Ensure(ctx, brokers); err != nil {
		return Schemas{}, fmt.Errorf("provision topics on %s: %w", strings.Join(brokers, ","), err)
	}
	return LoadSchemas(ctx, registry.New(registryURL))
}

// LoadSchemas reads, for every subject libs/events defines, the latest id and
// version from the registry. A missing subject lists every missing one and says
// how to register them.
func LoadSchemas(ctx context.Context, c *registry.Client) (Schemas, error) {
	s := Schemas{EnvelopeIDs: map[string]uint32{}, PayloadVersions: map[string]int32{}}
	var missing []string
	fetch := func(subject string) (registry.Latest, bool, error) {
		l, err := c.LatestSchema(ctx, subject)
		if errors.Is(err, registry.ErrSubjectNotFound) {
			missing = append(missing, subject)
			return l, false, nil
		}
		if err != nil {
			return l, false, fmt.Errorf("schema registry %s: reading subject %s: %w", c.BaseURL, subject, err)
		}
		return l, true, nil
	}
	for _, topic := range events.Topics() {
		l, ok, err := fetch(events.EnvelopeSubject(topic))
		if err != nil {
			return Schemas{}, err
		}
		if ok {
			s.EnvelopeIDs[topic] = uint32(l.ID)
		}
	}
	for _, t := range events.Types() {
		l, ok, err := fetch(t.Subject())
		if err != nil {
			return Schemas{}, err
		}
		if ok {
			s.PayloadVersions[t.EventType] = int32(l.Version)
		}
	}
	if len(missing) > 0 {
		return Schemas{}, fmt.Errorf("schema registry %s is missing %d subject(s): %s; register them with `go run ./cmd/register-schemas -registry %s` from libs/events",
			c.BaseURL, len(missing), strings.Join(missing, ", "), c.BaseURL)
	}
	return s, nil
}
