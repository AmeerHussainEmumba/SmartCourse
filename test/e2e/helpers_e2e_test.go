//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// decodeAccessToken reads access_token from a login or refresh response.
func decodeAccessToken(t *testing.T, raw []byte) string {
	t.Helper()
	var r struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.AccessToken == "" {
		t.Fatalf("no access_token in %s (err %v)", raw, err)
	}
	return r.AccessToken
}

// countEvent is how many of the records are the event with this id (a retry
// topic or a source topic can hold copies of one event).
func countEvent(recs []harness.KRecord, eventID string) int {
	n := 0
	for _, r := range recs {
		if r.EventID() == eventID {
			n++
		}
	}
	return n
}

func keysOf[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func redisZ(score float64, member string) redis.Z { return redis.Z{Score: score, Member: member} }

// maintenanceJobs are the scheduler entry names (jobs.Type*).
var maintenanceJobs = []string{
	"maintenance:reconcile-stale-publishes", "maintenance:analytics-reconcile",
	"maintenance:enrollment-counts", "maintenance:idempotency-prune", "maintenance:outbox-prune",
}

// completedTicks counts, per maintenance job type, the scheduler ticks that
// Asynq ran to completion and still retains (a finished tick is kept for five
// minutes so a late replica cannot enqueue it again): the proof that a cron
// tick enqueued the task and a server ran it. Each service namespaces its
// queues ("course:low", "analytics:low", ...): entries are in
// `asynq:{<ns>:low}:completed`, each task's message in the hash
// `asynq:{<ns>:low}:t:<id>`.
func completedTicks(t testing.TB, s *harness.Stack) map[string]int {
	t.Helper()
	ctx := context.Background()
	rdb := s.Env.RDB
	out := map[string]int{}
	for _, ns := range []string{"identity", "course", "analytics", "notification"} {
		q := "asynq:{" + ns + ":low}:"
		ids, err := rdb.ZRange(ctx, q+"completed", 0, -1).Result()
		if err != nil {
			t.Fatalf("read the completed set of the %s low queue: %v", ns, err)
		}
		for _, id := range ids {
			msg, err := rdb.HGet(ctx, q+"t:"+id, "msg").Result()
			if err != nil {
				continue // pruned between the two reads
			}
			for _, job := range maintenanceJobs {
				if strings.Contains(msg, job) {
					out[job]++
				}
			}
		}
	}
	return out
}
