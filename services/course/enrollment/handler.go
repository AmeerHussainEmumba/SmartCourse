// HTTP — DTO binding, validation, status codes (RFC §4.3).
package enrollment

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

type Handler struct{ svc *Service }

// postMW runs after authentication on POST /enrollments (the Idempotency-Key
// middleware, RFC §8.1 step 2); withdraw is idempotent by construction.
func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig, postMW ...gin.HandlerFunc) {
	h := &Handler{svc: svc}

	g := r.Group("/enrollments")
	g.Use(auth.RequireAuth(jwtCfg))
	g.POST("", append(append([]gin.HandlerFunc{}, postMW...), h.enroll)...)
	g.GET("", h.listMine)
	g.GET("/:id", h.getByID)
	g.DELETE("/:id", h.withdraw)
}

func (h *Handler) enroll(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	var req EnrollRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	courseID, err := uuid.Parse(req.CourseID)
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	e, err := h.svc.Enroll(c.Request.Context(), id.UserID, courseID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, toEnrollmentResponse(e))
}

func (h *Handler) withdraw(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	enrollmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	if err := h.svc.Withdraw(c.Request.Context(), id.UserID, enrollmentID); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) getByID(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	enrollmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	e, err := h.svc.GetByID(c.Request.Context(), id.UserID, enrollmentID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, toEnrollmentResponse(e))
}

func (h *Handler) listMine(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	list, err := h.svc.ListMine(c.Request.Context(), id.UserID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	resp := make([]EnrollmentResponse, 0, len(list))
	for i := range list {
		resp = append(resp, toEnrollmentResponse(&list[i]))
	}
	c.JSON(http.StatusOK, gin.H{"items": resp})
}
