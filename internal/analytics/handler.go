// HTTP — read-only for now (RFC §4.3). This module's real work happens in
// handlers.go, reacting to events; this file just exposes what's been
// computed so far.
package analytics

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
)

type Handler struct{ db *gorm.DB }

func RegisterRoutes(r *gin.RouterGroup, db *gorm.DB, jwtCfg config.JWTConfig) {
	h := &Handler{db: db}
	g := r.Group("/analytics")
	g.Use(auth.RequireAuth(jwtCfg))
	g.GET("/platform", h.getPlatformSnapshot)
}

func (h *Handler) getPlatformSnapshot(c *gin.Context) {
	snap, err := GetPlatformSnapshot(c.Request.Context(), h.db)
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
