package httpx

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// Keyset pagination only (RFC §13.2) — OFFSET is never used on a list
// endpoint. A cursor encodes the sort key of the last row seen; the next
// page's WHERE clause is (created_at, id) < (cursor.CreatedAt, cursor.ID),
// which uses the index at constant cost regardless of depth.

const DefaultPageLimit = 20
const MaxPageLimit = 100

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// EncodeCursor produces the opaque string returned to clients as
// next_cursor. Opaque deliberately — the wire format isn't a contract
// clients should depend on.
func EncodeCursor(c Cursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// DecodeCursor is the inverse of EncodeCursor. A malformed cursor is a
// client error (ErrValidation), not a 500 — it means a manipulated or stale
// query parameter, not a server fault.
func DecodeCursor(s string) (Cursor, error) {
	var c Cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	return c, nil
}

// ClampLimit enforces the server-side cap on page size (RFC §11.5 — an
// uncapped limit parameter is a trivial denial-of-service).
func ClampLimit(requested int) int {
	if requested <= 0 {
		return DefaultPageLimit
	}
	if requested > MaxPageLimit {
		return MaxPageLimit
	}
	return requested
}
