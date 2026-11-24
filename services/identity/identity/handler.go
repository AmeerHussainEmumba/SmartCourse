// HTTP — DTO binding, validation, status codes (RFC §4.3). No business
// logic lives here; every branch delegates to service.go.
package identity

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
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
	// Remaining anonymous routes (F-15). Reset/resend are tight because each
	// accepted request can send an e-mail; the service adds a per-address
	// budget on top. Confirm tokens are 256-bit, so those limits only bound
	// abuse volume.
	authGroup.POST("/refresh", limiter.Middleware("refresh", ratelimit.PerMinute(60)), h.refresh)
	authGroup.POST("/logout", limiter.Middleware("logout", ratelimit.PerMinute(60)), h.logout)
	authGroup.POST("/password-reset/request", limiter.Middleware("pwreset-request", ratelimit.PerMinute(5)), h.requestPasswordReset)
	authGroup.POST("/password-reset/confirm", limiter.Middleware("pwreset-confirm", ratelimit.PerMinute(30)), h.confirmPasswordReset)
	authGroup.POST("/verify-email/resend", limiter.Middleware("verify-resend", ratelimit.PerMinute(5)), h.resendVerification)
	authGroup.GET("/verify-email/confirm", limiter.Middleware("verify-confirm", ratelimit.PerMinute(30)), h.confirmVerification)

	me := r.Group("/me")
	// Token-version check on /me too (F-11): a suspended, deleted or demoted
	// user's old access token stops working immediately, not after its TTL.
	// A Redis miss falls back to Postgres (F-03).
	versions := svc.repo
	me.Use(auth.RequireAuth(jwtCfg), auth.RequireFreshPrivilegeWithSource(rdb, versions))
	me.GET("", h.getMe)
	me.PATCH("", h.updateMe)

	adminUsers := r.Group("/admin/users")
	adminUsers.Use(auth.RequireAuth(jwtCfg), auth.RequireRole(string(RoleAdmin)), auth.RequireFreshPrivilegeWithSource(rdb, versions))
	adminUsers.PATCH("/:id/role", h.updateUserRole)
}

func (h *Handler) register(c *gin.Context) {
	var req RegisterRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	if err := h.svc.RegisterPublic(c.Request.Context(), req); err != nil {
		httpx.Fail(c, err)
		return
	}
	// Identical for new and existing addresses (F-32); no session is issued.
	c.JSON(http.StatusAccepted, gin.H{"message": "If this address can be registered, an email with next steps has been sent. Sign in with your password to continue."})
}

func (h *Handler) login(c *gin.Context) {
	var req LoginRequest
	if !httpx.BindJSON(c, &req) {
		return
	}
	req.ClientIP = c.ClientIP()
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
