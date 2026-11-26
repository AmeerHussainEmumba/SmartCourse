package asynqx

import (
	"context"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// Gauges returns the collector that feeds asynq_queue_depth and
// asynq_retry_count (libs/telemetry) from the three standard queues; run it
// with telemetry.StartPoller. asynq_queue_depth is the tasks waiting to start
// (pending plus scheduled first runs), asynq_retry_count the tasks waiting for
// their next attempt; running tasks and archived (exhausted) tasks are in
// neither. A failed Redis read leaves the previous values (stale, never a
// false zero).
func (i *Inspector) Gauges(m *telemetry.Metrics) telemetry.GaugeCollector {
	return telemetry.QueueGauges(m, func(context.Context) ([]telemetry.QueueStats, error) {
		stats, err := i.Stats()
		if err != nil {
			return nil, err
		}
		out := make([]telemetry.QueueStats, 0, len(stats))
		for _, s := range stats {
			out = append(out, telemetry.QueueStats{Queue: s.Queue, Pending: int64(s.Pending + s.Scheduled), Retry: int64(s.Retry)})
		}
		return out, nil
	})
}
