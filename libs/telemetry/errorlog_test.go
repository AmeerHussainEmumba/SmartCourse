package telemetry

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestRateLimitedHandlerLogsRepeatsOncePerWindow(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	clock := time.Unix(1000, 0)
	h := newRateLimitedHandler(func() time.Time { return clock }, time.Minute)

	down := errors.New("dial tcp: lookup jaeger: no such host")
	for i := 0; i < 50; i++ {
		h(down)
		clock = clock.Add(time.Second)
	}
	if n := strings.Count(buf.String(), "no such host"); n != 1 {
		t.Fatalf("a failing collector logged %d lines in under a minute, want 1:\n%s", n, buf.String())
	}

	h(errors.New("a different problem"))
	if !strings.Contains(buf.String(), "a different problem") {
		t.Error("a distinct error must be logged immediately")
	}

	clock = clock.Add(2 * time.Minute)
	h(down)
	if n := strings.Count(buf.String(), "no such host"); n != 2 || !strings.Contains(buf.String(), "suppressed_repeats=") {
		t.Errorf("the error was not logged again after the window, with the suppressed count:\n%s", buf.String())
	}
}
