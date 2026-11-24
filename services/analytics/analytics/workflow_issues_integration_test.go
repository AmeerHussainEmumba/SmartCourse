//go:build integration

// Workflow Issues (DR-026), against real Postgres: course.publish_failed,
// through the real handler, fills the publish_failures mirror and the
// workflow_issue_stats count; a redelivery counts once; concurrent failures
// are all counted; the reconciler heals a corrupted count from the mirror; the
// real source reports it; and migration 000004 goes down and up again.
package analytics_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

func publishFailedPayload(reason string) map[string]any {
	return map[string]any{
		"course_id": uuid.New(), "version_id": uuid.New(), "version_number": 2, "title": "T",
		"instructor_id": uuid.New(), "failed_activity": "BuildOutline", "reason": reason,
		"failed_at": "2026-11-24T10:00:00.123456Z",
	}
}

func workflowFailed(t *testing.T, env *testenv.Env) int64 {
	t.Helper()
	var n int64
	if err := env.DB.Raw(`SELECT coalesce(sum(failed), 0) FROM workflow_issue_stats WHERE workflow = ?`, analytics.PublishWorkflow).Row().Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPublishFailed_MirrorsTheEventAndCountsOnce(t *testing.T) {
	env := testenv.Setup(t)
	e := ev(1, "course.publish_failed", publishFailedPayload("asset missing"))
	feed(t, env, e)
	feed(t, env, e) // redelivery with the same event id: processed_events skips it
	if n := workflowFailed(t, env); n != 1 {
		t.Fatalf("failed after one event delivered twice: %d, want 1", n)
	}
	// Even without the idempotency marker (pruned), the mirror's key holds.
	if err := env.DB.Exec(`DELETE FROM processed_events`).Error; err != nil {
		t.Fatal(err)
	}
	feed(t, env, e)
	if n := workflowFailed(t, env); n != 1 {
		t.Fatalf("failed after a replay without the marker: %d, want 1", n)
	}
	var rows int64
	var activity, reason string
	if err := env.DB.Raw(`SELECT count(*), max(failed_activity), max(reason) FROM publish_failures`).Row().Scan(&rows, &activity, &reason); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || activity != "BuildOutline" || reason != "asset missing" {
		t.Errorf("mirror: rows=%d activity=%q reason=%q", rows, activity, reason)
	}
	feed(t, env, ev(2, "course.publish_failed", publishFailedPayload("another")))
	if n := workflowFailed(t, env); n != 2 {
		t.Fatalf("failed after a second distinct event: %d, want 2", n)
	}
}

func TestPublishFailed_MalformedPayloadIsAnError(t *testing.T) {
	env := testenv.Setup(t)
	for name, payload := range map[string]map[string]any{
		"not an object field": {"course_id": 5},
		"missing ids":         {"reason": "x"},
	} {
		if err := deliver(env, ev(1, "course.publish_failed", payload)); err == nil {
			t.Errorf("%s: want an error, got nil", name)
		}
	}
	if n := workflowFailed(t, env); n != 0 {
		t.Errorf("a rejected event changed the count: %d", n)
	}
}

func TestPublishFailed_ConcurrentFailuresAreAllCounted(t *testing.T) {
	env := testenv.Setup(t)
	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := deliver(env, ev(int64(i+1), "course.publish_failed", publishFailedPayload(fmt.Sprint("r", i)))); err != nil {
				t.Errorf("deliver %d: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := workflowFailed(t, env); got != n {
		t.Fatalf("failed after %d concurrent failures: %d (a stale recompute overwrote a newer total)", n, got)
	}
}

func TestPublishFailed_ReconcilerHealsTheCountFromTheMirror(t *testing.T) {
	env := testenv.Setup(t)
	for i := range 3 {
		feed(t, env, ev(int64(i+1), "course.publish_failed", publishFailedPayload("x")))
	}
	if err := env.DB.Exec(`UPDATE workflow_issue_stats SET failed = 40`).Error; err != nil {
		t.Fatal(err)
	}
	if err := analytics.NewReconciler(env.DB, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := workflowFailed(t, env); n != 3 {
		t.Fatalf("after reconcile: %d, want 3", n)
	}
	// A lost count row is recreated from the mirror.
	if err := env.DB.Exec(`DELETE FROM workflow_issue_stats`).Error; err != nil {
		t.Fatal(err)
	}
	if err := analytics.NewReconciler(env.DB, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := workflowFailed(t, env); n != 3 {
		t.Fatalf("after reconcile with the count row deleted: %d, want 3", n)
	}
}

func TestWorkflowIssuesSource_ReportsTheCounts(t *testing.T) {
	env := testenv.Setup(t)
	src := analytics.NewWorkflowIssuesSource(env.DB)
	empty, err := src.SummarizeWorkflowIssues(context.Background())
	if err != nil || empty.Failed != 0 || empty.ByWorkflow == nil || len(empty.ByWorkflow) != 0 {
		t.Fatalf("empty: %+v err %v", empty, err)
	}
	feed(t, env, ev(1, "course.publish_failed", publishFailedPayload("a")), ev(2, "course.publish_failed", publishFailedPayload("b")))
	got, err := src.SummarizeWorkflowIssues(context.Background())
	if err != nil || got.Failed != 2 || got.ByWorkflow[analytics.PublishWorkflow] != 2 {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestMigration000004_DownDropsTheTables_UpRestoresThemEmpty(t *testing.T) {
	s := newSecEnv(t)
	feed(t, s.env, ev(1, "course.publish_failed", publishFailedPayload("a")))
	tables := func() int64 {
		var n int64
		if err := s.env.DB.Raw(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name IN ('publish_failures', 'workflow_issue_stats')`).Row().Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	apply := func(name string) {
		t.Helper()
		sqlDB, err := s.env.DB.DB()
		if err != nil {
			t.Fatal(err)
		}
		conn, err := sqlDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = conn.ExecContext(t.Context(), `SET search_path TO identity,course,analytics,platform,public`)
			_ = conn.Close()
		}()
		for _, st := range migrationStatements(t, name) {
			if _, err := conn.ExecContext(t.Context(), st); err != nil {
				t.Fatalf("%s: %v\n%s", name, err, st)
			}
		}
	}
	if tables() != 2 {
		t.Fatal("precondition: both tables should exist after the up migration")
	}
	apply("000004_workflow_issues.down.sql")
	if tables() != 0 {
		t.Fatal("down migration left a table behind")
	}
	apply("000004_workflow_issues.down.sql") // idempotent
	apply("000004_workflow_issues.up.sql")
	if tables() != 2 {
		t.Fatal("up migration after down did not restore both tables")
	}
	if n := workflowFailed(t, s.env); n != 0 {
		t.Fatalf("restored tables must start empty, got %d", n)
	}
}
