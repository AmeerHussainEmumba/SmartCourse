//go:build integration

package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// The runner hands each handler the record's coordinates and the frame's
// registry schema id in the context (relay.SourceFrom). E13: the archiver
// stores them; before this, schema_id was 0 and topic/partition/offset/key
// were not recorded.
func TestRunner_HandlerSeesTheRecordSource(t *testing.T) {
	s := newStack(t)
	got := make(chan relay.Source, 1)
	h := relay.NewHandler("source-consumer", []string{effectType}, func(ctx context.Context, _ *gorm.DB, _ relay.DecodedEvent) error {
		src, ok := relay.SourceFrom(ctx)
		if !ok {
			t.Error("the handler's context carries no relay.Source")
		}
		select {
		case got <- src:
		default:
		}
		return nil
	})
	s.startRelay(s.publisher(10*time.Second), 1)
	s.startRunner(runnerCfg{}, h)

	agg := uuid.New()
	s.write(agg, 1)
	select {
	case src := <-got:
		if src.Topic != topics.Identity {
			t.Errorf("topic = %q, want %q", src.Topic, topics.Identity)
		}
		if src.Key != agg.String() {
			t.Errorf("key = %q, want the aggregate id %s", src.Key, agg)
		}
		if want := uint32(s.schemas.EnvelopeIDs[topics.Identity]); src.SchemaID == 0 || src.SchemaID != want {
			t.Errorf("schema id = %d, want the identity envelope's registry id %d", src.SchemaID, want)
		}
		if src.Partition < 0 || src.Offset < 0 {
			t.Errorf("coordinates %d/%d are not valid", src.Partition, src.Offset)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the handler was not called")
	}
}
