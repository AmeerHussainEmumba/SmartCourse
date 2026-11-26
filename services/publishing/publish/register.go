package publish

import (
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

// Registry is the part of a Temporal worker Register needs (worker.Worker
// satisfies it).
type Registry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

// Register registers PublishCourseWorkflow under publishing.WorkflowName,
// explicitly rather than by the Go function's name: the same string the starter
// (course.Service.Publish) passes to ExecuteWorkflow.
func Register(r Registry) {
	r.RegisterWorkflowWithOptions(PublishCourseWorkflow, workflow.RegisterOptions{Name: publishing.WorkflowName})
}
