package main

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

type fakeParkStore struct{ err error }

func (f fakeParkStore) ParkFailedEvent(context.Context, mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	return &mongostore.FailedEvent{}, f.err
}

func TestCountParked_CountsOnlySuccessfulParks(t *testing.T) {
	m, err := telemetry.NewMetrics(telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ok := countParked(fakeParkStore{}, m)
	for i := 0; i < 3; i++ {
		if _, err := ok.ParkFailedEvent(context.Background(), mongostore.ParkInput{Consumer: "analytics"}); err != nil {
			t.Fatal(err)
		}
	}
	bad := countParked(fakeParkStore{err: errors.New("mongo down")}, m)
	if _, err := bad.ParkFailedEvent(context.Background(), mongostore.ParkInput{Consumer: "analytics"}); err == nil {
		t.Fatal("the store's error was swallowed")
	}
	if got := testutil.ToFloat64(m.DeadLetterTotal.WithLabelValues("analytics")); got != 3 {
		t.Errorf("dead_letter_total{consumer=analytics} = %v, want 3 (a failed park is not a dead letter)", got)
	}
}

func TestCountParked_NilMetricsReturnsStoreUnchanged(t *testing.T) {
	inner := fakeParkStore{}
	if got := countParked(inner, nil); got != any(inner) {
		t.Errorf("with metrics disabled the store must not be wrapped, got %T", got)
	}
}
