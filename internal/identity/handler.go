// HTTP — DTO binding, validation, status codes (RFC §4.3). No business
// logic lives here; every branch delegates to service.go.
package identity

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/ratelimit"
)

type Handler struct {
	svc *Service
}

// RegisterRoutes wires every identity endpoint (RFC §15.2) onto r. jwtCfg
// and rdb are needed here (not just in Service) because RequireAuth and
// RequireFreshPrivilege are per-route middleware, not service calls.
func RegisterRoutes(r *gin.RouterGroup, svc *Service, jwtCfg config.JWTConfig, rdb *redis.Client) {
	h := &Handler{svc: svc}
	limiter := ratelimit.New(rdb)

	authGroup := r.Group("/auth")
	// Per-IP limits (RFC §11.3), on top of Login's own per-account limit
	// (service.go) — the two guard against different attack shapes: many
	// guesses against one account from one IP (this), versus one attacker
	// spreading guesses for one account across many IPs (service.go).
	authGroup.POST("/register", limiter.Middleware("register", ratelimit.PerMinute(5)), h.register)
	authGroup.POST("/login", limiter.Middleware("login", ratelimit.PerMinute(20)), h.login)
	authGroup.POST("/refresh", h.refresh)
	authGroup.POST("/logout", h.logout)
	authGroup.POST("/password-reset/request", h.requestPasswordReset)
	authGroup.POST("/password-reset/confirm", h.confirmPasswordReset)
	authGroup.POST("/verify-email/resend", h.resendVerification)
	authGroup.GET("/verify-email/confirm", h.confirmVerification)

	me := r.Group("/me")
	me.Use(auth.RequireAuth(jwtCfg))
	me.GET("", h.getMe)
	me.PATCH("", h.updateMe)

	adminUsers := r.Group("/admin/users")
	adminUsers.Use(auth.RequireAuth(jwtCfg), auth.RequireRole(string(RoleAdmin)), auth.RequireFreshPrivilege(rdb))
	adminUsers.PATCH("/:id/role", h.updateUserRole)
}

func (h *Handler) register(c *gin.Context) {
	var req RegisterRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.Register(c.Request.Context(), req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (h *Handler) login(c *gin.Context) {
	var req LoginRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.Login(c.Request.Context(), req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) refresh(c *gin.Context) {
	var req RefreshRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.Refresh(c.Request.Context(), req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) logout(c *gin.Context) {
	var req LogoutRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	if err := h.svc.Logout(c.Request.Context(), req); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) getMe(c *gin.Context) {
	id, ok := auth.FromContext(c.Request.Context())
	if !ok {
		httpx.Fail(c, apperrors.ErrUnauthorized)
		return
	}
	resp, err := h.svc.GetMe(c.Request.Context(), id.UserID)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) updateMe(c *gin.Context) {
	id, ok := auth.FromContext(c.Request.Context())
	if !ok {
		httpx.Fail(c, apperrors.ErrUnauthorized)
		return
	}
	var req UpdateProfileRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.UpdateMe(c.Request.Context(), id.UserID, req)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

type updateRoleRequest struct {
	Role   Role       `json:"role" binding:"required,oneof=student instructor admin"`
	Status UserStatus `json:"status" binding:"required,oneof=active suspended deleted"`
}

func (h *Handler) updateUserRole(c *gin.Context) {
	targetID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
		return
	}
	var req updateRoleRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	resp, err := h.svc.UpdateUserRole(c.Request.Context(), targetID, req.Role, req.Status)
	if err != nil {
		httpx.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) requestPasswordReset(c *gin.Context) {
	var req PasswordResetRequestDTO
	if !httpx.BindJSON(c, &req) {
		return
	}
	if err := h.svc.RequestPasswordReset(c.Request.Context(), req); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusAccepted)
}

func (h *Handler) confirmPasswordReset(c *gin.Context) {
	var req PasswordResetConfirmRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	if err := h.svc.ConfirmPasswordReset(c.Request.Context(), req); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) resendVerification(c *gin.Context) {
	var req ResendVerificationRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	if err := h.svc.ResendEmailVerification(c.Request.Context(), req); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusAccepted)
}

func (h *Handler) confirmVerification(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		httpx.Fail(c, apperrors.ErrValidation.Wrap(errors.New("token query parameter is required")))
		return
	}
	if err := h.svc.ConfirmEmailVerification(c.Request.Context(), token); err != nil {
		httpx.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
