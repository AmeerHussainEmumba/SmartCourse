//go:build integration

package infra_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// A metric a binary never feeds is absent from its /metrics, not a constant 0
// (an analytics-api outbox_backlog of 0 would read as "nothing is waiting").
func TestObserve_MetricsTheBinaryDoesNotFeedAreAbsent(t *testing.T) {
	env := testenv.Setup(t)
	deps := depsOf(env)
	scrapeWith := func(o infra.ObserveOptions) string {
		obs := deps.Observe(context.Background(), telemetryCfg("", true), o)
		defer obs.Shutdown(context.Background())
		r := serve(t, deps, obs)
		get(r, "/things/1") // a labelled series exists only after its first request
		rec := get(r, "/metrics")
		b, _ := io.ReadAll(rec.Body)
		return string(b)
	}

	plain := scrapeWith(infra.ObserveOptions{Service: "analytics-api"})
	for _, name := range []string{"outbox_backlog", "outbox_oldest_unpublished_seconds", "workflow_started_total", "workflow_completed_total", "workflow_failed_total"} {
		if strings.Contains(plain, "# HELP "+name+" ") {
			t.Errorf("%s is exported by a binary that never feeds it", name)
		}
	}
	if !strings.Contains(plain, "http_requests_total") {
		t.Error("the RED metrics must remain")
	}

	full := scrapeWith(infra.ObserveOptions{Service: "worker", Outbox: true, Workflows: true})
	for _, name := range []string{"outbox_backlog", "workflow_started_total", "workflow_failed_total"} {
		if !strings.Contains(full, "# HELP "+name+" ") {
			t.Errorf("%s missing from the worker's metrics", name)
		}
	}
}
