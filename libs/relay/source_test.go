package relay

import (
	"context"
	"testing"
)

func TestSourceRoundTrip(t *testing.T) {
	if _, ok := SourceFrom(context.Background()); ok {
		t.Fatal("a bare context must carry no source")
	}
	want := Source{Topic: "course.events", Partition: 2, Offset: 41, Key: "k", SchemaID: 7}
	got, ok := SourceFrom(WithSource(context.Background(), want))
	if !ok || got != want {
		t.Fatalf("SourceFrom = %+v, %v; want %+v", got, ok, want)
	}
}
