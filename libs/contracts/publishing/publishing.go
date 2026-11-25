// Package publishing is the contract between the service that starts the course
// publish workflow, the service that orchestrates it and the service that runs
// its activities (docs/plans/microservices-split.md §5): the workflow name, the
// two task queues, the activity names and the DTOs the activities exchange.
//
// It is plain Go with no service and no Temporal import, so the orchestrating
// service can call every activity by name without linking the code behind it.
// Names and JSON shapes here are wire format: Temporal stores them in workflow
// history, so changing one breaks workflows already running.
package publishing

import "github.com/google/uuid"

// WorkflowName is the registered Temporal workflow type of the publish workflow.
// The starter passes it to client.ExecuteWorkflow and the orchestrating worker
// registers the workflow function under it.
const WorkflowName = "PublishCourseWorkflow"

// The two task queues. The workflow runs on WorkflowQueue, served by the
// orchestrating worker. Every activity is scheduled on ActivityQueue, served by
// the service that owns the data. TEMPORAL_TASK_QUEUE and TEMPORAL_ACTIVITY_QUEUE
// (libs/config) override them per deployment.
const (
	WorkflowQueue = "publish-workflows"
	ActivityQueue = "course-activities"
)

// The activity names, as registered by the activity worker and as the workflow
// calls them. They equal the method names of the activity implementation, so a
// struct registered with RegisterActivity answers to them unchanged.
const (
	ActivityValidateMetadata  = "ValidateMetadata"
	ActivityVerifyAssets      = "VerifyAssets"
	ActivityComputeMetadata   = "ComputeMetadata"
	ActivityBuildOutline      = "BuildOutline"
	ActivityIndexSearch       = "IndexSearch"
	ActivityWarmCache         = "WarmCache"
	ActivityPromoteAndPublish = "PromoteAndPublish"
	ActivityMarkPublishFailed = "MarkPublishFailed"
)

// ActivityNames lists every activity in the order of the activity graph, for a
// worker that registers them one by one and for tests that check both sides
// agree.
func ActivityNames() []string {
	return []string{
		ActivityValidateMetadata, ActivityVerifyAssets, ActivityComputeMetadata, ActivityBuildOutline,
		ActivityIndexSearch, ActivityWarmCache, ActivityPromoteAndPublish, ActivityMarkPublishFailed,
	}
}

// FailureType is the Temporal ApplicationError type an activity uses for a
// failure whose message is already a bounded, fixed-vocabulary reason. The
// workflow trusts the message of an error of this type, and only that one, when
// it records why a publish failed.
const FailureType = "PublishFailure"

// PublishWorkflowInput is the workflow's sole parameter: plain,
// JSON-serializable data only (Temporal marshals workflow arguments).
type PublishWorkflowInput struct {
	CourseID     uuid.UUID
	VersionID    uuid.UUID
	OldVersionID *uuid.UUID
	Title        string
	// ActivityQueue is the task queue the workflow schedules its activities on.
	// Empty means the ActivityQueue constant; a starter that runs with a
	// different TEMPORAL_ACTIVITY_QUEUE sets it.
	ActivityQueue string `json:",omitempty"`
}

// ActivityQueueOrDefault is the queue the activities run on for this input.
func (in PublishWorkflowInput) ActivityQueueOrDefault() string {
	if in.ActivityQueue != "" {
		return in.ActivityQueue
	}
	return ActivityQueue
}

// PublishOutline is BuildOutline's result and IndexSearch/WarmCache's input (RFC
// §7.4): everything about a draft needed to index it for search and warm its
// detail cache, assembled once rather than twice. DetailJSON is the
// pre-marshaled course detail response WarmCache writes verbatim into Redis, so
// a cache hit never re-serializes anything.
type PublishOutline struct {
	CourseID        uuid.UUID
	VersionID       uuid.UUID
	VersionNumber   int
	Slug            string
	Title           string
	Description     string
	InstructorName  string
	Category        *string
	Level           *string
	Tags            []string
	EnrollmentCount int
	DetailJSON      []byte
}
