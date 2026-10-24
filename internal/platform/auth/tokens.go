// Opaque, single-use tokens (refresh, password-reset, email-verification)
// share one shape across all three flows in the identity module: a random
// value handed to the client, only its SHA-256 hash stored (ADR-0015 — a
// database disclosure yields no usable token).
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
)

// GenerateOpaqueToken returns a URL-safe random token and its SHA-256 hash.
// The raw token is returned to the client exactly once and never stored;
// the hash is what's persisted and compared against on redemption.
func GenerateOpaqueToken() (raw string, hash []byte, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("generate token: %w", err)
	}
	raw = base64.RawURLEncoding.EncodeToString(b)
	sum := sha256.Sum256([]byte(raw))
	return raw, sum[:], nil
}

// HashOpaqueToken hashes a client-supplied raw token for lookup against a
// stored hash — same algorithm as GenerateOpaqueToken, split out so
// redemption doesn't need to re-derive it inline.
func HashOpaqueToken(raw string) []byte {
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}

// ConstantTimeEqual compares two hashes without leaking timing
// information — used wherever a looked-up row's hash is compared against a
// freshly-hashed client-supplied token.
func ConstantTimeEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}
