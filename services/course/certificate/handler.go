// HTTP — DTO binding, validation, status codes (RFC §4.3, §8.4).
package certificate

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

// publicMW runs before GET /certificates/verify/{code}, the one route that
// needs no token. The caller passes a rate limiter (it holds the Redis
// client), as the identity module's public routes do.
func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig, publicMW ...gin.HandlerFunc) {
	h := &Handler{svc: svc}
	requireAuth := auth.RequireAuth(jwtCfg)

	g := r.Group("/certificates")
	g.GET("", requireAuth, h.listMine)
	g.GET("/:id", requireAuth, h.getByID)
	g.GET("/verify/:code", append(append([]gin.HandlerFunc{}, publicMW...), h.verify)...)
}

func (h *Handler) listMine(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	list, err := h.svc.ListMine(c.Request.Context(), id.UserID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	resp := make([]CertificateResponse, 0, len(list))
	for i := range list {
		resp = append(resp, toCertificateResponse(&list[i]))
	}
	c.JSON(http.StatusOK, gin.H{"items": resp})
}

func (h *Handler) getByID(c *gin.Context) {
	id, _ := auth.FromContext(c.Request.Context())
	certID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	cert, err := h.svc.GetByID(c.Request.Context(), id.UserID, certID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, toCertificateResponse(cert))
}

func (h *Handler) verify(c *gin.Context) {
	cert, err := h.svc.Verify(c.Request.Context(), c.Param("code"))
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, toVerificationResponse(cert))
}
