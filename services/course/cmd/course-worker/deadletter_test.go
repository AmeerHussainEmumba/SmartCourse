package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

type fakeParker struct {
	got []mongostore.ParkInput
	err error
	do  func()
}

func (f *fakeParker) ParkFailedEvent(_ context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	if f.do != nil {
		f.do()
	}
	if f.err != nil {
		return nil, f.err
	}
	f.got = append(f.got, in)
	return &mongostore.FailedEvent{}, nil
}

func sampleEvent() relay.DecodedEvent {
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateID: uuid.New(), AggregateType: "course", EventType: "course.course_archived",
		Payload: []byte(`{}`), OccurredAt: time.Now(), OutboxID: 7, SchemaVersion: 1,
	}
}

func TestParkSink_MapsTheEventAndCountsOnlyWhatWasParked(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := telemetry.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeParker{}
	s := newParkSink(store, m)
	evt := sampleEvent()
	err = s.Park(context.Background(), relay.DeadLetter{
		Event: evt, Consumer: "certificate-issuer", Err: errors.New("boom"), Attempts: 8,
		History: []error{errors.New("a"), errors.New("b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := store.got[0]
	if in.EventID != evt.EventID.String() || in.Consumer != "certificate-issuer" || in.EventType != evt.EventType ||
		in.Attempts != 8 || in.OutboxID != 7 || in.Err != "boom" || len(in.History) != 2 {
		t.Fatalf("parked %+v", in)
	}
	if got := testutil.ToFloat64(m.DeadLetterTotal.WithLabelValues("certificate-issuer")); got != 1 {
		t.Errorf("dead_letter_total = %v, want 1", got)
	}

	// A failed park is an error ("not parked": the transport keeps the event) and
	// is not counted as a dead letter.
	store.err = errors.New("mongo down")
	if err := s.Park(context.Background(), relay.DeadLetter{Event: evt, Consumer: "certificate-issuer", Err: errors.New("x")}); err == nil {
		t.Fatal("a failed park returned nil: the event would be dropped unparked")
	}
	if got := testutil.ToFloat64(m.DeadLetterTotal.WithLabelValues("certificate-issuer")); got != 1 {
		t.Errorf("dead_letter_total = %v after a failed park, want still 1", got)
	}
}

func TestParkSink_NeverPanicsAndRejectsBadInput(t *testing.T) {
	s := newParkSink(&fakeParker{do: func() { panic("driver bug") }}, nil)
	if err := s.Park(context.Background(), relay.DeadLetter{Event: sampleEvent(), Consumer: "c", Err: errors.New("x")}); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Errorf("panic in the store: %v", err)
	}
	if err := newParkSink(&fakeParker{}, nil).Park(context.Background(), relay.DeadLetter{Event: sampleEvent()}); err == nil {
		t.Error("an empty consumer name was accepted")
	}
	if err := (*parkSink)(nil).Park(context.Background(), relay.DeadLetter{}); !errors.Is(err, errMongoNotReady) {
		t.Errorf("nil sink: %v", err)
	}
	// A long and non-UTF-8 error is bounded and valid.
	store := &fakeParker{}
	long := errors.New(strings.Repeat("x", 5000) + "\xff")
	if err := newParkSink(store, nil).Park(context.Background(), relay.DeadLetter{Event: sampleEvent(), Consumer: "c", Err: long}); err != nil {
		t.Fatal(err)
	}
	if n := len(store.got[0].Err); n > maxErrorLen {
		t.Errorf("stored error is %d bytes, cap %d", n, maxErrorLen)
	}
}

// Until MongoDB is connected the sink reports "not parked", so the transport
// keeps the event retryable.
func TestMongoConn_NotConnectedMeansNotParked(t *testing.T) {
	mc := &mongoConn{}
	err := newParkSink(mc, nil).Park(context.Background(), relay.DeadLetter{Event: sampleEvent(), Consumer: "c", Err: errors.New("x")})
	if !errors.Is(err, errMongoNotReady) {
		t.Errorf("Park before connect = %v, want errMongoNotReady", err)
	}
	if mc.Ping(context.Background()) == nil {
		t.Error("Ping succeeded without a connection")
	}
}
