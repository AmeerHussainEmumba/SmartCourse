package main

import (
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

// registerRoutes wires services/course/course, services/course/enrollment, and
// services/course/progress's HTTP routes onto r. The three stay together, not split
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

	// Idempotency-Key (RFC §15.1) on the non-idempotent POST routes.
	idem := idempotency.Middleware(deps.DB, idempotency.Options{
		UserID: func(c *gin.Context) (uuid.UUID, bool) {
			id, ok := auth.FromContext(c.Request.Context())
			return id.UserID, ok
		},
	})

	courseSvc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue)
	// Object storage backs POST /assets/upload-url. If it cannot be built
	// (bad MinIO settings), upload-url answers 503; nothing else is affected.
	if store, err := assets.New(cfg.MinIO); err != nil {
		slog.Error("asset storage unavailable; POST /assets/upload-url will return 503", slog.Any("error", err))
	} else {
		courseSvc.WithAssetStore(store)
	}
	course.RegisterRoutes(api, courseSvc, cfg.JWT, idem)

	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(deps.DB))
	enrollment.RegisterRoutes(api, enrollmentSvc, cfg.JWT, idem)

	progressSvc := progress.NewService(progress.NewRepository(deps.DB))
	progress.RegisterRoutes(api, progressSvc, cfg.JWT)

	// The public verification route (RFC §8.4) takes a per-IP limit, 30 a
	// minute like the identity module's other token-guessing surfaces.
	certificateSvc := certificate.NewService(certificate.NewRepository(deps.DB))
	limiter := ratelimit.New(deps.RDB)
	certificate.RegisterRoutes(api, certificateSvc, cfg.JWT,
		limiter.Middleware("certificate-verify", ratelimit.PerMinute(30)))
}
