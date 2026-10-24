// Package httpx holds the conventions every module's handler.go follows:
// how a request ID is attached, how errors become JSON, how a request body
// is bound and validated. Centralising this is what keeps eight modules'
// handlers looking like one system instead of eight.
//
// RFC §11.5: request DTOs are explicit structs with validation tags. ORM
// models are never bound to requests and never returned in responses — that
// rule is enforced by convention (every module's dto.go), not by this
// package, but this package is what makes following the convention easy.
package httpx

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
)

const RequestIDHeader = "X-Request-Id"

// RequestID assigns a request ID (reusing an inbound one if a trusted proxy
// already set it) and makes it available via gin.Context and response
// header. trace_id/span_id propagation via OpenTelemetry is layered on top
// of this in M4 (RFC §12.2); this is the minimum needed to correlate log
// lines today.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(RequestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Writer.Header().Set(RequestIDHeader, id)
		c.Next()
	}
}

// errorResponse is the wire shape for every error (RFC §15.1):
// { "code", "message", "details" }.
type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// Fail writes err as a stable JSON error response. Non-*AppError values are
// treated as internal errors — the original error is never serialized, only
// logged by the caller.
func Fail(c *gin.Context, err error) {
	var appErr *apperrors.AppError
	if errors.As(err, &appErr) {
		c.AbortWithStatusJSON(appErr.HTTPStatus, errorResponse{
			Code:    appErr.Code,
			Message: appErr.Message,
			Details: appErr.Details,
		})
		return
	}
	c.AbortWithStatusJSON(http.StatusInternalServerError, errorResponse{
		Code:    apperrors.ErrInternal.Code,
		Message: apperrors.ErrInternal.Message,
	})
}

// BindJSON decodes and validates a request DTO. Validation failures map to
// ErrValidation with per-field details rather than leaking the raw
// validator error.
func BindJSON(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		Fail(c, apperrors.ErrValidation.Wrap(err))
		return false
	}
	return true
}

// RequestIDFromContext reads the request ID set by RequestID(), for
// handlers that need it outside the gin.Context (e.g. to pass into a
// service call for logging).
func RequestIDFromContext(c *gin.Context) string {
	if v, ok := c.Get("request_id"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
