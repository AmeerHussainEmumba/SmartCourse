package main

import (
	"github.com/gin-gonic/gin"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
)

// registerRoutes wires internal/course, internal/enrollment, and
// internal/progress's HTTP routes onto r. The three stay together, not split
// into three more binaries, because enrollment locks the `courses` row in
// the SAME database transaction it writes an enrollment row in (ADR-0012's
// seat-limit guarantee) — that requires both to run in one process against
// one connection; splitting them would force that lock into a network call.
// See ADR-0023.
// temporal is passed in explicitly, not read off deps — see
// libs/temporal for why identity-api/analytics-api never
// acquire one.
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config, temporal temporalclient.Client) {
	api := r.Group("/api/v1")

	courseSvc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue)
	course.RegisterRoutes(api, courseSvc, cfg.JWT)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(deps.DB))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT)

	progressSvc := progress.NewService(progress.NewRepository(deps.DB))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)
}
