//go:build e2e

package e2e_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func redisZ(score float64, member string) redis.Z { return redis.Z{Score: score, Member: member} }

func keysOf[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// maintenanceJobs are the scheduler entry names (jobs.Type*).
var maintenanceJobs = []string{
	"maintenance:reconcile-stale-publishes", "maintenance:analytics-reconcile",
	"maintenance:enrollment-counts", "maintenance:idempotency-prune", "maintenance:outbox-prune",
}

// completedTicks counts, per maintenance job type, the scheduler ticks that
// Asynq ran to completion and still retains (the scheduler keeps a finished
// tick for five minutes so a late replica cannot enqueue it again): the proof
// that a cron tick enqueued the task and a server ran it. Entries are in
// `asynq:{low}:completed`, each task's message in the hash `asynq:{low}:t:<id>`.
func completedTicks(t testing.TB, s *harness.Stack) map[string]int {
	t.Helper()
	ctx := context.Background()
	rdb := s.Env.RDB
	ids, err := rdb.ZRange(ctx, "asynq:{low}:completed", 0, -1).Result()
	if err != nil {
		t.Fatalf("read the completed set of the low queue: %v", err)
	}
	out := map[string]int{}
	for _, id := range ids {
		msg, err := rdb.HGet(ctx, "asynq:{low}:t:"+id, "msg").Result()
		if err != nil {
			continue // pruned between the two reads
		}
		for _, job := range maintenanceJobs {
			if strings.Contains(msg, job) {
				out[job]++
			}
		}
	}
	return out
}
