// HTTP — read-only for now (RFC §4.3). This module's real work happens in
// handlers.go, reacting to events; this file just exposes what's been
// computed so far. Goes through Service, like every other module's
// handler — see service.go's doc comment for why that wasn't true before.
package analytics

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
)

type Handler struct{ svc *Service }

func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig) {
	h := &Handler{svc: svc}
	g := r.Group("/analytics")
	g.Use(auth.RequireAuth(jwtCfg))
	g.GET("/platform", h.getPlatformSnapshot)
	g.GET("/courses/:id", h.getCourseStats)
	g.GET("/courses/:id/daily-enrollments", h.listDailyEnrollments)
}

func (h *Handler) getPlatformSnapshot(c *gin.Context) {
	snap, err := h.svc.GetPlatformSnapshot(c.Request.Context())
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"total_students":          snap.TotalStudents,
		"total_instructors":       snap.TotalInstructors,
		"total_courses_published": snap.TotalCoursesPublished,
		"avg_courses_per_student": snap.AvgCoursesPerStudent,
	})
}

// getCourseStats backs GET /analytics/courses/{id} (FR-5: total/active
// enrollments, completions, completion rate, average time to complete).
// Same auth as the platform snapshot — RequireAuth only, no ownership
// check — matching this module's existing read-only, any-authenticated-
// caller exposure policy; it doesn't carry anything a course's catalogue
// listing doesn't already surface in aggregate.
func (h *Handler) getCourseStats(c *gin.Context) {
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	stats, err := h.svc.GetCourseStats(c.Request.Context(), courseID)
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"total_enrollments":  stats.TotalEnrollments,
		"active_enrollments": stats.ActiveEnrollments,
		"completions":        stats.Completions,
		"completion_rate":    stats.CompletionRate,
		"avg_completion_s":   stats.AvgCompletionS,
	})
}

// listDailyEnrollments backs GET /analytics/courses/{id}/daily-enrollments
// (FR-5: "New Enrollments Over Time").
func (h *Handler) listDailyEnrollments(c *gin.Context) {
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	rows, err := h.svc.ListDailyEnrollments(c.Request.Context(), courseID)
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		items = append(items, gin.H{"day": row.Day.Format("2006-01-02"), "count": row.Count})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
