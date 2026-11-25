package runtime

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Check is a readiness probe: nil means healthy.
type Check func(ctx context.Context) error

// readyTimeout bounds one /health/ready request, every check included.
const readyTimeout = 2 * time.Second

// Health is the readiness state of a process: a set of named checks and the
// state of its components. /health/live never reads it (a dependency outage must
// not restart the process); /health/ready does.
//
// A fatal check that fails makes readiness 503. A non-fatal one (Kafka, whose
// outage the outbox buffers, RFC §6.2) is reported as "failed" in the body and
// turns the status to "degraded", but readiness stays 200.
type Health struct {
	mu       sync.Mutex
	checks   []namedCheck
	comps    map[string]string // component name -> "failed" | "stopped"
	starting bool
	stopping bool
}

type namedCheck struct {
	name     string
	check    Check
	nonFatal bool
}

// CheckOption tunes Health.Add.
type CheckOption func(*namedCheck)

// NonFatal makes a failing check degrade readiness instead of failing it.
func NonFatal() CheckOption { return func(c *namedCheck) { c.nonFatal = true } }

// NewHealth returns a Health that reports "starting" (503) until Started is
// called, so a probe does not see a process whose components are not up yet.
func NewHealth() *Health { return &Health{comps: map[string]string{}, starting: true} }

// Add registers a check. Names are the keys of the response body: use plain
// dependency names ("postgres"); the body never carries the error text.
func (h *Health) Add(name string, check Check, opts ...CheckOption) {
	c := namedCheck{name: name, check: check}
	for _, o := range opts {
		o(&c)
	}
	h.mu.Lock()
	h.checks = append(h.checks, c)
	h.mu.Unlock()
}

// Started marks the process ready to be probed.
func (h *Health) Started() {
	h.mu.Lock()
	h.starting = false
	h.mu.Unlock()
}

// Stopping makes readiness 503 ("shutting_down") so traffic drains while the
// process finishes its work; liveness is unaffected.
func (h *Health) Stopping() {
	h.mu.Lock()
	h.stopping = true
	h.mu.Unlock()
}

// setComponent records a component that failed or ended early, as a non-fatal
// entry of the body.
func (h *Health) setComponent(name, state string) {
	h.mu.Lock()
	h.comps[name] = state
	h.mu.Unlock()
}

// report is the readiness result: status for the body, code for HTTP.
type report struct {
	status string
	code   int
	checks map[string]string
}

// run evaluates the checks concurrently under one deadline. Raw errors carry
// hosts, ports and database names and /health/ready is unauthenticated, so the
// body holds a bare ok/failed per name and the detail goes to the log.
func (h *Health) run(ctx context.Context) report {
	h.mu.Lock()
	starting, stopping := h.starting, h.stopping
	checks := append([]namedCheck(nil), h.checks...)
	comps := make(map[string]string, len(h.comps))
	for k, v := range h.comps {
		comps[k] = v
	}
	h.mu.Unlock()

	switch {
	case stopping:
		return report{status: "shutting_down", code: http.StatusServiceUnavailable}
	case starting:
		return report{status: "starting", code: http.StatusServiceUnavailable}
	}

	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	results := make([]error, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					results[i] = errPanic(p)
				}
			}()
			results[i] = c.check(ctx)
		}()
	}
	wg.Wait()

	r := report{status: "ready", code: http.StatusOK, checks: map[string]string{}}
	for i, c := range checks {
		if results[i] == nil {
			r.checks[c.name] = "ok"
			continue
		}
		r.checks[c.name] = "failed"
		slog.Error("readiness check failed", slog.String("dependency", c.name), slog.Bool("fatal", !c.nonFatal), slog.Any("error", results[i]))
		if c.nonFatal {
			if r.status == "ready" {
				r.status = "degraded"
			}
		} else {
			r.status, r.code = "not_ready", http.StatusServiceUnavailable
		}
	}
	names := make([]string, 0, len(comps))
	for n := range comps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r.checks["component:"+n] = comps[n] // failed or stopped: reported, never fatal
		if r.status == "ready" {
			r.status = "degraded"
		}
	}
	return r
}

type panicError struct{ v any }

func (e panicError) Error() string { return "check panicked" }

func errPanic(v any) error { return panicError{v} }
