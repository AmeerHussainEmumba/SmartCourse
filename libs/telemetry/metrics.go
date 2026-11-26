package telemetry

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric names. Dashboards and alert rules are provisioned against these
// (ADR-0032, RFC §12.3); renaming one is a breaking change.
const (
	MetricHTTPRequestsTotal      = "http_requests_total"
	MetricHTTPRequestDuration    = "http_request_duration_seconds"
	MetricOutboxBacklog          = "outbox_backlog"
	MetricOutboxOldestUnpublish  = "outbox_oldest_unpublished_seconds"
	MetricKafkaConsumerLag       = "kafka_consumer_lag"
	MetricAsynqQueueDepth        = "asynq_queue_depth"
	MetricAsynqRetryCount        = "asynq_retry_count"
	MetricDeadLetterTotal        = "dead_letter_total"
	MetricWorkflowStartedTotal   = "workflow_started_total"
	MetricWorkflowCompletedTotal = "workflow_completed_total"
	MetricWorkflowFailedTotal    = "workflow_failed_total"
	MetricDBConnectionsInUse     = "db_connections_in_use"
	MetricDBConnectionsMax       = "db_connections_max"
	MetricEventProcessedTotal    = "event_processed_total"
	MetricDeadLetterHeld         = "dead_letter_held_records"
	MetricDeadLetterParkFailures = "dead_letter_park_failures_total"
)

// Outcome is the bounded value set of the event_processed_total outcome label.
type Outcome string

// Outcomes of handling one event. Any other value is recorded as OutcomeOther
// so a caller bug cannot create unbounded series.
const (
	OutcomeProcessed  Outcome = "processed"
	OutcomeDuplicate  Outcome = "duplicate"
	OutcomeFailed     Outcome = "failed"
	OutcomeDeadLetter Outcome = "dead_lettered"
	OutcomeOther      Outcome = "other"
)

// Metrics holds every application metric. Create it once per process with
// NewMetrics; the fields are safe for concurrent use.
//
// db_connections_in_use and db_connections_max are not fields: they come from
// RegisterDBStats, which reads them from the pool at scrape time.
type Metrics struct {
	HTTPRequests *prometheus.CounterVec   // route, method, status
	HTTPDuration *prometheus.HistogramVec // route, method

	OutboxBacklog          prometheus.Gauge
	OutboxOldestUnpublish  prometheus.Gauge
	KafkaConsumerLag       *prometheus.GaugeVec   // group, topic
	AsynqQueueDepth        *prometheus.GaugeVec   // queue
	AsynqRetryCount        *prometheus.GaugeVec   // queue
	DeadLetterTotal        *prometheus.CounterVec // consumer
	WorkflowStartedTotal   prometheus.Counter
	WorkflowCompletedTotal prometheus.Counter
	WorkflowFailedTotal    prometheus.Counter
	EventProcessedTotal    *prometheus.CounterVec // consumer, outcome
	DeadLetterHeld         *prometheus.GaugeVec   // consumer
	DeadLetterParkFailures *prometheus.CounterVec // consumer

}

// NewRegistry returns a registry with the Go runtime and process collectors,
// which every service wants beside its own metrics. A dedicated registry (not
// the global default) keeps tests and multiple servers independent.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// NewMetrics creates and registers every application metric on reg. It is
// idempotent per registry: calling it again returns metrics backed by the
// already-registered collectors instead of panicking.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{}
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error

	m.HTTPRequests, err = registerOrGet(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricHTTPRequestsTotal,
		Help: "HTTP requests by route template, method and status code.",
	}, []string{"route", "method", "status"}))
	add(err)
	m.HTTPDuration, err = registerOrGet(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    MetricHTTPRequestDuration,
		Help:    "HTTP request duration in seconds by route template and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"}))
	add(err)
	m.OutboxBacklog, err = registerOrGet(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: MetricOutboxBacklog,
		Help: "Outbox rows not yet published.",
	}))
	add(err)
	m.OutboxOldestUnpublish, err = registerOrGet(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: MetricOutboxOldestUnpublish,
		Help: "Age in seconds of the oldest unpublished outbox row; 0 when the outbox is empty.",
	}))
	add(err)
	m.KafkaConsumerLag, err = registerOrGet(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricKafkaConsumerLag,
		Help: "Messages behind the log end per consumer group and topic.",
	}, []string{"group", "topic"}))
	add(err)
	m.AsynqQueueDepth, err = registerOrGet(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricAsynqQueueDepth,
		Help: "Pending Asynq tasks per queue.",
	}, []string{"queue"}))
	add(err)
	m.AsynqRetryCount, err = registerOrGet(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricAsynqRetryCount,
		Help: "Asynq tasks waiting for retry per queue.",
	}, []string{"queue"}))
	add(err)
	m.DeadLetterTotal, err = registerOrGet(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricDeadLetterTotal,
		Help: "Events sent to the dead-letter topic per consumer.",
	}, []string{"consumer"}))
	add(err)
	m.WorkflowStartedTotal, err = registerOrGet(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: MetricWorkflowStartedTotal,
		Help: "Workflows started.",
	}))
	add(err)
	m.WorkflowCompletedTotal, err = registerOrGet(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: MetricWorkflowCompletedTotal,
		Help: "Workflows completed successfully.",
	}))
	add(err)
	m.WorkflowFailedTotal, err = registerOrGet(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: MetricWorkflowFailedTotal,
		Help: "Workflows that ended in failure.",
	}))
	add(err)
	m.EventProcessedTotal, err = registerOrGet(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricEventProcessedTotal,
		Help: "Events handled per consumer and outcome.",
	}, []string{"consumer", "outcome"}))
	add(err)
	m.DeadLetterHeld, err = registerOrGet(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricDeadLetterHeld,
		Help: "Records a consumer is holding right now because the dead-letter sink refused them; non-zero means a partition is stalled.",
	}, []string{"consumer"}))
	add(err)
	m.DeadLetterParkFailures, err = registerOrGet(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricDeadLetterParkFailures,
		Help: "Times the dead-letter sink refused a record, per consumer.",
	}, []string{"consumer"}))
	add(err)

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

// EventProcessed counts one handled event. outcome outside the known set is
// recorded as OutcomeOther.
func (m *Metrics) EventProcessed(consumer string, outcome Outcome) {
	switch outcome {
	case OutcomeProcessed, OutcomeDuplicate, OutcomeFailed, OutcomeDeadLetter:
	default:
		outcome = OutcomeOther
	}
	m.EventProcessedTotal.WithLabelValues(consumer, string(outcome)).Inc()
}

// DeadLettered counts one event sent to a dead-letter topic by consumer.
func (m *Metrics) DeadLettered(consumer string) {
	m.DeadLetterTotal.WithLabelValues(consumer).Inc()
}

// HeldDelta adds delta (+1 when a record starts being held, -1 when the
// hold ends) to the consumer's held-record gauge.
func (m *Metrics) HeldDelta(consumer string, delta float64) {
	m.DeadLetterHeld.WithLabelValues(consumer).Add(delta)
}

// ParkFailed counts one refusal by the dead-letter sink.
func (m *Metrics) ParkFailed(consumer string) {
	m.DeadLetterParkFailures.WithLabelValues(consumer).Inc()
}

// registerOrGet registers c, or returns the collector already registered under
// the same descriptor, so a second registration never panics.
func registerOrGet[C prometheus.Collector](reg prometheus.Registerer, c C) (C, error) {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(C); ok {
				return existing, nil
			}
		}
		var zero C
		return zero, fmt.Errorf("telemetry: register metric: %w", err)
	}
	return c, nil
}

// Handler serves g (normally the registry from NewRegistry) in the Prometheus
// text format. It never fails a scrape because one collector errored.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// GinHandler is Handler as a Gin handler: r.GET("/metrics", telemetry.GinHandler(reg)).
// /metrics is unauthenticated; expose it on the internal network only.
func GinHandler(g prometheus.Gatherer) gin.HandlerFunc {
	return gin.WrapH(Handler(g))
}

// DBStatser is satisfied by *sql.DB (and by GORM's underlying pool).
type DBStatser interface {
	Stats() sql.DBStats
}

// RegisterDBStats registers db_connections_in_use and db_connections_max,
// read from db.Stats() at scrape time. Registering twice on one registry is a
// no-op. db_connections_max is 0 when the pool has no limit
// (SetMaxOpenConns(0)).
func RegisterDBStats(reg prometheus.Registerer, db DBStatser) error {
	_, err := registerOrGet(reg, &dbCollector{
		db:    db,
		inUse: prometheus.NewDesc(MetricDBConnectionsInUse, "Open database connections currently in use.", nil, nil),
		max:   prometheus.NewDesc(MetricDBConnectionsMax, "Configured maximum open database connections (0 means unlimited).", nil, nil),
	})
	return err
}

type dbCollector struct {
	db         DBStatser
	inUse, max *prometheus.Desc
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inUse
	ch <- c.max
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(s.InUse))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(s.MaxOpenConnections))
}
