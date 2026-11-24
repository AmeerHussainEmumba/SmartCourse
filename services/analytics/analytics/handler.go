// HTTP — read-only for now (RFC §4.3). This module's real work happens in
// handlers.go, reacting to events; this file just exposes what's been
// computed so far. Goes through Service, like every other module's
// handler — see service.go's doc comment for why that wasn't true before.
package analytics

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

type Handler struct{ svc *Service }

func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig) {
	h := &Handler{svc: svc}
	g := r.Group("/analytics")
	g.Use(auth.RequireAuth(jwtCfg))
	g.GET("/platform", h.getPlatformSnapshot)
	g.GET("/popular-courses", h.listPopularCourses)
	g.GET("/issues", auth.RequireRole("admin"), h.getIssues)
	g.GET("/courses/:id", h.authorizeCourse, h.getCourseStats)
	g.GET("/courses/:id/daily-enrollments", h.authorizeCourse, h.listDailyEnrollments)
}

// authorizeCourse gates the per-course routes (Service.CanReadCourse).
// Denial is a 404, not 403, so a caller cannot probe which courses exist.
func (h *Handler) authorizeCourse(c *gin.Context) {
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	id, ok := auth.FromContext(c.Request.Context())
	if !ok {
		httpx.Fail(c, apperrors.ErrUnauthorized)
		return
	}
	allowed, err := h.svc.CanReadCourse(c.Request.Context(), id, courseID)
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	if !allowed {
		httpx.Fail(c, apperrors.ErrNotFound)
		return
	}
	c.Next()
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

// listPopularCourses backs GET /analytics/popular-courses (FR-5: "Most
// Popular Courses"). Platform-level like /platform: ids and enrollment
// counts only, so any authenticated user may read it.
func (h *Handler) listPopularCourses(c *gin.Context) {
	limit := 0 // 0 = the service default
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxPopularLimit {
			httpx.Fail(c, apperrors.ErrValidation)
			return
		}
		limit = n
	}
	rows, err := h.svc.GetPopularCourses(c.Request.Context(), limit)
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		items = append(items, gin.H{"course_id": row.CourseID, "enrollments": row.Enrollments})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// getIssues backs GET /analytics/issues (FR-5: "Failed Events / Workflow
// Issues"), admin only: dead-letter contents are operational detail. 503
// when no failed-events source is attached or its store is unreachable
// (issues.go); the workflow section is null when no workflow source is
// attached (unavailable, not zero).
func (h *Handler) getIssues(c *gin.Context) {
	report, err := h.svc.GetIssues(c.Request.Context())
	if errors.Is(err, ErrIssuesNotConfigured) || errors.Is(err, ErrSourceUnavailable) {
		httpx.Fail(c, apperrors.ErrUnavailable.Wrap(err))
		return
	}
	if err != nil {
		httpx.Fail(c, apperrors.ErrInternal.Wrap(err))
		return
	}
	var workflow any // nil serialises as null
	if wf := report.WorkflowIssues; wf != nil {
		workflow = gin.H{"failed": wf.Failed, "by_workflow": nonNilCounts(wf.ByWorkflow)}
	}
	fe := report.FailedEvents
	c.JSON(http.StatusOK, gin.H{
		"failed_events": gin.H{
			"total":                         fe.Total,
			"unresolved":                    fe.Unresolved,
			"replayed":                      fe.Replayed,
			"by_event_type":                 nonNilCounts(fe.ByEventType),
			"by_consumer":                   nonNilCounts(fe.ByConsumer),
			"oldest_unresolved_age_seconds": fe.OldestUnresolvedAgeSeconds,
		},
		"workflow_issues": workflow,
	})
}

// nonNilCounts keeps an absent map serialising as {} rather than null.
func nonNilCounts(m map[string]int64) map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return m
}

// getCourseStats backs GET /analytics/courses/{id} (FR-5: total/active
// enrollments, completions, completion rate, average time to complete).
// Gated by authorizeCourse: admin any course, an instructor only a course
// they own, a student only an enrolled course, else 404.
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
