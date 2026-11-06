# internal/platform/errors

SmartCourse's error-response contract (RFC §11.6): every handler returns an `*AppError`, never ad-hoc JSON.

## Why a stable, closed vocabulary

`AppError` carries an HTTP status, a stable machine-readable `Code`, and a client-safe `Message` — the wrapped internal error (`Wrap`) is for logs only and is never serialized. The package exports a fixed set of common kinds (`ErrValidation`, `ErrUnauthorized`, `ErrTokenStale`, `ErrForbidden`, `ErrNotFound`, `ErrConflict`, `ErrRateLimited`, `ErrInternal`, `ErrUnavailable`) that every module builds on rather than inventing new codes per endpoint — a client integrating against this API gets one documented vocabulary, not eight modules' worth of slightly different error shapes.

`As(err)` mirrors the standard library's `errors.As` so callers checking "is this an `*AppError`, and if so which kind" don't need to import both `errors` and this package under two different names — see any module's test file for the pattern (`appErr, ok := apperrors.As(err); appErr.Code == "..."`).

`ErrTokenStale` and `ErrUnavailable` are worth knowing by name if you're new to this codebase: the former is ADR-0015's addendum (`RequireFreshPrivilege` rejecting a token whose embedded privilege claim is no longer current); the latter is what a publish returns when Temporal itself can't be reached (ADR-0025) — a 503, not a 500, because the failure is the dependency's, not this service's own logic.

## Testing

No dedicated test file — `AppError`'s behavior (status/code/message shape, `Wrap`'s internal-error-stays-internal property) is exercised indirectly by every module's integration and HTTP-level tests, which assert on specific error codes throughout (`apperrors.As(err)` is this repo's standard assertion pattern for "did the right kind of failure happen").
