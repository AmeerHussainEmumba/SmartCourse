// HTTP — DTO binding, validation, status codes (RFC §4.3).
package course

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	identityauth "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
)

type Handler struct{ svc *Service }

func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig) {
	h := &Handler{svc: svc}

	// Discovery — public.
	r.GET("/courses", h.listCatalogue)
	r.GET("/courses/:slug", h.getDetail)

	// Authoring — instructor-owned, ownership itself checked in service.go.
	instructor := r.Group("/courses")
	instructor.Use(identityauth.RequireAuth(jwtCfg), identityauth.RequireRole("instructor"))
	instructor.POST("", h.createCourse)
	instructor.PATCH("/:id", h.updateDraft)
	instructor.POST("/:id/modules", h.addModule)
	instructor.POST("/:id/publish", h.publish)

	modules := r.Group("/modules")
	modules.Use(identityauth.RequireAuth(jwtCfg), identityauth.RequireRole("instructor"))
	modules.POST("/:id/lessons", h.addLesson)
	modules.DELETE("/:id", h.deleteModule)

	lessons := r.Group("/lessons")
	lessons.Use(identityauth.RequireAuth(jwtCfg), identityauth.RequireRole("instructor"))
	lessons.DELETE("/:id", h.deleteLesson)
}

func (h *Handler) createCourse(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	var req CreateCourseRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	course, version, err := h.svc.CreateCourse(c.Request.Context(), id.UserID, req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"course": course, "version": version})
}

func (h *Handler) updateDraft(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	var req UpdateDraftRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.UpdateDraft(c.Request.Context(), courseID, id.UserID, req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) addModule(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	var req CreateModuleRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.AddModule(c.Request.Context(), courseID, id.UserID, req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (h *Handler) deleteModule(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	moduleID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	courseID, err := h.svc.ResolveModuleCourseID(c.Request.Context(), moduleID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	if err := h.svc.DeleteModule(c.Request.Context(), courseID, id.UserID, moduleID); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) addLesson(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	moduleID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	courseID, err := h.svc.ResolveModuleCourseID(c.Request.Context(), moduleID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	var req CreateLessonRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.AddLesson(c.Request.Context(), courseID, id.UserID, moduleID, req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (h *Handler) deleteLesson(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	lessonID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	courseID, err := h.svc.ResolveLessonCourseID(c.Request.Context(), lessonID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	if err := h.svc.DeleteLesson(c.Request.Context(), courseID, id.UserID, lessonID); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) publish(c *gin.Context) {
	id, _ := identityauth.FromContext(c.Request.Context())
	courseID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	resp, err := h.svc.Publish(c.Request.Context(), courseID, id.UserID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) listCatalogue(c *gin.Context) {
	limit := httpx.ClampLimit(atoiOr(c.Query("limit"), 0))
	resp, err := h.svc.ListCatalogue(c.Request.Context(), limit, c.Query("cursor"))
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) getDetail(c *gin.Context) {
	resp, err := h.svc.GetPublishedDetail(c.Request.Context(), c.Param("slug"))
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func atoiOr(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
