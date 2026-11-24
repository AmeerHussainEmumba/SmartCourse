// Failed Events / Workflow Issues (SmartCourse.md FR-5, the ninth metric).
// Its data is the dead-letter store (MongoDB failed_events, RFC §9.1) and
// the workflow engine's failure counts, neither of which this module owns or
// may link (the link-graph guard keeps libs/mongostore out of analytics-api).
// So the module defines only the narrow interfaces it needs; whichever
// binary assembles the service supplies implementations through
// Service.WithIssueSources. Until it does, the route answers 503 rather
// than reporting a made-up zero.
package analytics

import "context"

// FailedEventsSummary is the dead-letter backlog: events the relay or a
// consumer gave up on. Unresolved excludes those already replayed.
type FailedEventsSummary struct {
	Total       int64
	Unresolved  int64
	ByEventType map[string]int64
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

// IssuesReport is what GET /analytics/issues returns.
type IssuesReport struct {
	FailedEvents   *FailedEventsSummary
	WorkflowIssues *WorkflowIssuesSummary
}
