// Command publishing-worker runs the publish workflow's Temporal worker
// (docs/plans/microservices-split.md §5): PublishCourseWorkflow on the workflow
// queue, and nothing else. It has no database, no Redis and no Kafka; the
// activities it schedules run in the course worker.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	runtime "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("publishing-worker", wire,
		runtime.WithoutDependencies(),
		runtime.WithWorkflows(),
		runtime.WithConfig(func() (*config.Config, error) { return config.LoadWith(config.Requirements{}) }),
	)
}
