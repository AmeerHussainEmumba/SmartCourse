// Package errors defines SmartCourse's error-response contract.
//
// RFC §11.6: error responses carry a stable code and a safe message.
// Database errors, stack traces, and internal identifiers are logged with
// the trace ID and never returned. Every handler maps domain errors to an
// *AppError via the constructors below rather than returning ad-hoc JSON.
package errors

import (
	"errors"
	"fmt"
	"net/http"
)

// AppError is the only error shape a handler should ever write to a
// response body. Code is stable and machine-readable (RFC §15.1); Message is
// safe to show a client; the wrapped internal error (if any) is for logs
// only and is never serialized.
type AppError struct {
	HTTPStatus int
	Code       string
	Message    string
	Details    any
	internal   error
}

func (e *AppError) Error() string {
	if e.internal != nil {
		return fmt.Sprintf("%s: %v", e.Code, e.internal)
	}
	return e.Code
}

func (e *AppError) Unwrap() error { return e.internal }

// Wrap attaches an internal error for logging without changing what the
// client sees.
func (e *AppError) Wrap(err error) *AppError {
	cp := *e
	cp.internal = err
	return &cp
}

func New(status int, code, message string) *AppError {
	return &AppError{HTTPStatus: status, Code: code, Message: message}
}

// Common, stable error kinds. Handlers build on these rather than inventing
// new codes ad hoc, so clients get a predictable, documented vocabulary.
var (
	ErrValidation   = New(http.StatusBadRequest, "validation_failed", "The request could not be validated.")
	ErrUnauthorized = New(http.StatusUnauthorized, "unauthorized", "Authentication is required.")
	ErrTokenStale   = New(http.StatusUnauthorized, "token_stale", "Your session is outdated; please sign in again.")
	ErrForbidden    = New(http.StatusForbidden, "forbidden", "You do not have permission to perform this action.")
	ErrNotFound     = New(http.StatusNotFound, "not_found", "The requested resource was not found.")
	ErrConflict     = New(http.StatusConflict, "conflict", "The request conflicts with the current state of the resource.")
	ErrRateLimited  = New(http.StatusTooManyRequests, "rate_limited", "Too many requests. Please try again later.")
	ErrInternal     = New(http.StatusInternalServerError, "internal_error", "Something went wrong. Please try again.")
	ErrUnavailable  = New(http.StatusServiceUnavailable, "temporarily_unavailable", "This action is temporarily unavailable. Please try again shortly.")
)

// As reports whether err is an *AppError, mirroring errors.As so callers
// don't need to import both packages.
func As(err error) (*AppError, bool) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr, true
	}
	return nil, false
}
