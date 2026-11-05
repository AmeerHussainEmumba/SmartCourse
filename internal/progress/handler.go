// HTTP — DTO binding, validation, status codes (RFC §4.3).
package progress

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

	g := r.Group("/enrollments/:id")
	g.Use(auth.RequireAuth(jwtCfg))
	g.GET("/progress", h.getProgress)
	g.PUT("/lessons/:lessonKey/complete", h.markComplete)
}

func (h *Handler) markComplete(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	enrollmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	lessonKey, err := uuid.Parse(c.Param("lessonKey"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	if err := h.svc.MarkComplete(c.Request.Context(), id.UserID, enrollmentID, lessonKey); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) getProgress(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	enrollmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	items, err := h.svc.GetProgress(c.Request.Context(), id.UserID, enrollmentID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
