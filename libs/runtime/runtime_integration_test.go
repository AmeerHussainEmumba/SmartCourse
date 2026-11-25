//go:build integration

package runtime_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/postgres"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// With dependencies: Run connects Postgres and Redis itself, adds their
// readiness checks (fatal), exposes db_* and, on request, the outbox gauges on the
// internal listener, and closes the connections at shutdown.
func TestRun_ConnectsDependenciesAndReportsThemOnTheInternalListener(t *testing.T) {
	env := testenv.Setup(t)
	dsn := env.DB.Dialector.(*postgres.Dialector).Config.DSN
	addr := freeAddr(t)
	cfg := &config.Config{
		Env:       "test",
		Postgres:  config.PostgresConfig{DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2, SearchPath: "platform,public"},
		Redis:     config.RedisConfig{Addr: env.RDB.Options().Addr},
		Telemetry: config.TelemetryConfig{MetricsAddr: addr, MetricsEnabled: true, SampleRatio: 1},
	}
	h := &harness{t: t, addr: addr, done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	var sawDeps bool
	go func() {
		h.err = runtime.Run(ctx, "it-svc", func(p *runtime.Process) error {
			sawDeps = p.Deps != nil && p.Deps.DB != nil && p.Deps.RDB != nil && p.Obs != nil
			p.Go("idle", func(ctx context.Context) error { <-ctx.Done(); return nil })
			return nil
		}, runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }), runtime.WithOutbox(), runtime.WithLogger(quiet))
		close(h.done)
	}()
	t.Cleanup(func() { cancel(); <-h.done })

	eventually(t, "ready with both dependencies", func() bool {
		code, m := h.ready()
		checks, _ := m["checks"].(map[string]any)
		return code == 200 && checks["postgres"] == "ok" && checks["redis"] == "ok"
	})
	if !sawDeps {
		t.Fatal("the wiring function did not receive connected dependencies")
	}
	var body string
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, body = h.get("/metrics")
		if strings.Contains(body, "db_connections_in_use") && strings.Contains(body, "outbox_backlog") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/metrics lacks db_connections_in_use or outbox_backlog:\n%.600s", body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if strings.Contains(body, "workflow_started_total") {
		t.Error("workflow counters exported without WithWorkflows")
	}
	h.cancel()
	if !h.wait(10*time.Second) || h.err != nil {
		t.Fatalf("Run: %v", h.err)
	}
}
