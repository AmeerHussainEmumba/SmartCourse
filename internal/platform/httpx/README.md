# internal/platform/httpx

The conventions every module's `handler.go` follows (RFC §11.5/§15.1), centralized so eight modules' HTTP surfaces look like one system instead of eight independently-invented ones.

## What's here

- **`RequestID()`** — middleware that assigns (or reuses an inbound) request ID, attached to `gin.Context` and the response header. The minimum needed to correlate log lines today; full trace-context propagation via OpenTelemetry is layered on top in M4.
- **`Fail(c, err)`** — the one function every handler calls on error. An `*apperrors.AppError` writes its stable `code`/safe `message`/HTTP status; anything else is treated as an internal error and never serialized — the real underlying error is for logs only, never the response body.
- **`BindJSON(c, dst)`** — decodes and validates a request DTO, mapping a validation failure to `apperrors.ErrValidation` rather than leaking the raw validator error shape to a client.
- **`pagination.go`** — keyset pagination helpers (RFC §13.2): `Cursor`, `EncodeCursor`/`DecodeCursor` (an opaque, base64-encoded `(created_at, id)` pair — the wire format is deliberately not a contract clients should depend on), and `ClampLimit` (server-side page-size cap; an uncapped `limit` query param is a trivial denial-of-service, per RFC §11.5).

## Testing

`pagination_test.go` (no build tag) covers `EncodeCursor`/`DecodeCursor`'s round-trip and malformed-input rejection, and `ClampLimit`'s boundary behavior, in isolation. The *use* of a real cursor across a real second page of results is tested at the module level instead — see `internal/course`'s `pagination_integration_test.go` — since that needs a real database with more than one page of real rows to be meaningful.

`Fail`/`BindJSON` are exercised indirectly by every module's HTTP-level test (each module's `*_http_test.go`, build tag `integration`) rather than directly here — they only do anything observable against a real `gin.Context`, which those tests already construct.
