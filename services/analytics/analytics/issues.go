// Failed Events / Workflow Issues (SmartCourse.md FR-5, the ninth metric).
// Its data is the dead-letter store (MongoDB failed_events, RFC §9.1) and
// the workflow engine's failure counts. This package owns neither, so it
// defines only the narrow interfaces it needs; whichever binary assembles the
// service supplies implementations through Service.WithIssueSources.
// analytics-api supplies the failed-events one over libs/mongostore
// (services/analytics/failedevents; ADR-0031 deviation note). The workflow
// source is not built: the failure counts live in the course schema, which
// this module may not read. Until failed events are attached the route
// answers 503 rather than reporting a made-up zero.
package analytics

import (
	"context"
	"errors"
)

// ErrSourceUnavailable is wrapped by a source whose backing store is
// unreachable. GET /analytics/issues answers 503 for it (a degraded
// dependency), not 500.
var ErrSourceUnavailable = errors.New("analytics: issue source unavailable")

// FailedEventsSummary is the dead-letter backlog: events the relay or a
// consumer gave up on. Unresolved excludes those already replayed.
type FailedEventsSummary struct {
	Total       int64
	Unresolved  int64
	Replayed    int64
	ByEventType map[string]int64
	// ByConsumer counts unresolved events per consumer.
	ByConsumer map[string]int64
	// OldestUnresolvedAgeSeconds is how long the longest-waiting unresolved
	// event has been failing; nil when there is none.
	OldestUnresolvedAgeSeconds *int64
}

// WorkflowIssuesSummary counts workflow runs that ended in failure, by
// workflow name (for example the course publishing workflow).
type WorkflowIssuesSummary struct {
	Failed     int64
	ByWorkflow map[string]int64
}

// FailedEventsSource reads the dead-letter store.
type FailedEventsSource interface {
	SummarizeFailedEvents(ctx context.Context) (*FailedEventsSummary, error)
}

// WorkflowIssuesSource reads failed-workflow counts.
type WorkflowIssuesSource interface {
	SummarizeWorkflowIssues(ctx context.Context) (*WorkflowIssuesSummary, error)
}

// IssuesReport is what GET /analytics/issues returns. WorkflowIssues is nil
// when no workflow source is attached (reported as unavailable, not zero).
type IssuesReport struct {
	FailedEvents   *FailedEventsSummary
	WorkflowIssues *WorkflowIssuesSummary
}
