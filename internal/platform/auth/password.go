// argon2id password hashing (ADR-0015): chosen over bcrypt, which silently
// truncates input at 72 bytes and is no longer OWASP's first
// recommendation. Parameters are configurable because appropriate cost
// changes over time — see config.Argon2Config.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

// encoded format: $argon2id$v=19$m=<kib>,t=<iter>,p=<par>$<salt>$<hash>
// Self-describing so parameters can change over time without invalidating
// hashes created under older settings.
const argon2Format = "$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s"

func HashPassword(cfg config.Argon2Config, password string) (string, error) {
	salt := make([]byte, cfg.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}

	hash := argon2.IDKey([]byte(password), salt, cfg.Iterations, cfg.MemoryKiB, cfg.Parallelism, cfg.KeyLength)

	return fmt.Sprintf(argon2Format,
		argon2.Version, cfg.MemoryKiB, cfg.Iterations, cfg.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

// VerifyPassword re-derives the hash from the encoded parameters and
// compares in constant time. It never returns an error for "wrong
// password" — only for a malformed encoded hash, which indicates data
// corruption, not user input.
//
// Parsed with strings.Split rather than fmt.Sscanf: Sscanf's %s stops only
// at whitespace, not at '$', so it silently misparses this format — a bug
// caught by this package's own tests, not by inspection.
func VerifyPassword(encoded, password string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, fmt.Errorf("malformed password hash: unexpected format")
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("malformed password hash: version segment: %w", err)
	}

	var memoryKiB, iterations uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memoryKiB, &iterations, &parallelism); err != nil {
		return false, fmt.Errorf("malformed password hash: params segment: %w", err)
	}

	saltB64, hashB64 := parts[4], parts[5]
	salt, err := base64.RawStdEncoding.DecodeString(saltB64)
	if err != nil {
		return false, fmt.Errorf("decode salt: %w", err)
	}
	want, err := base64.RawStdEncoding.DecodeString(hashB64)
	if err != nil {
		return false, fmt.Errorf("decode hash: %w", err)
	}

	got := argon2.IDKey([]byte(password), salt, iterations, memoryKiB, parallelism, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// DummyHash is compared against for unknown-email login attempts (RFC
// §11.3), so response timing for "no such account" matches "wrong
// password" and doesn't reveal account existence. Generated once at
// package init against a fixed, non-secret password — its value is
// irrelevant, only its comparison cost matters.
var DummyHash string

func init() {
	h, err := HashPassword(config.Argon2Config{MemoryKiB: 64 * 1024, Iterations: 3, Parallelism: 2, SaltLength: 16, KeyLength: 32}, "dummy-password-for-timing-safety")
	if err != nil {
		panic(fmt.Sprintf("auth: failed to precompute dummy hash: %v", err))
	}
	DummyHash = h
}
