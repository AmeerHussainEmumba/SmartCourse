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

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
)

const RequestIDHeader = "X-Request-Id"

// maxRequestIDLen bounds an accepted inbound request ID.
const maxRequestIDLen = 64

// validRequestID reports whether id is safe to echo in a response header and
// write to logs: 1..64 characters of [A-Za-z0-9._-]. Anything else (control
// characters, JSON, very long values) is replaced by a generated UUID so a
// client cannot forge log lines or bloat headers (F-29).
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// RequestID assigns a request ID (reusing an inbound one only if it matches
// the bounded charset/length of validRequestID) and makes it available via
// gin.Context and response header. trace_id/span_id propagation via
// OpenTelemetry is layered on top of this in M4 (RFC §12.2); this is the
// minimum needed to correlate log lines today.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(RequestIDHeader)
		if !validRequestID(id) {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Writer.Header().Set(RequestIDHeader, id)
		c.Next()
	}
}

// SecurityHeaders sets conservative response headers for a JSON API:
// nosniff, no framing, no referrer, a CSP that forbids everything, and
// Cache-Control: no-store (token responses must never be cached). HSTS is
// sent only when hsts is true or the request itself arrived over TLS, so
// plain-HTTP local development is unaffected (F-29).
func SecurityHeaders(hsts bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Cache-Control", "no-store")
		if hsts || c.Request.TLS != nil {
			h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}
		c.Next()
	}
}

// DefaultMaxBodyBytes is the request-body cap applied when none is configured.
const DefaultMaxBodyBytes int64 = 1 << 20

// MaxBodyBytes caps request bodies at limit bytes (F-16). A declared
// Content-Length above the cap is rejected up front with 413; undeclared or
// understated bodies are cut off by http.MaxBytesReader when read.
func MaxBodyBytes(limit int64) gin.HandlerFunc {
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	return func(c *gin.Context) {
		if c.Request.ContentLength > limit {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, errorResponse{
				Code: "PAYLOAD_TOO_LARGE", Message: "request body too large",
			})
			return
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		}
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
