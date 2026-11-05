// What this checks: argon2id hashing round-trips correctly, wrong passwords
// are rejected, and the dummy hash (used for timing-safe unknown-account
// responses, RFC §11.3) is itself a valid, comparable hash. No database —
// see docs/guides/testing.md, "unit" tier.
package auth

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

func testArgonCfg() config.Argon2Config {
	// Deliberately cheap parameters so the test suite stays fast; production
	// values (env.example) are far more expensive by design.
	return config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
}

func TestHashAndVerifyPassword_CorrectPassword(t *testing.T) {
	hash, err := HashPassword(testArgonCfg(), "correct-horse-battery-staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	ok, err := VerifyPassword(hash, "correct-horse-battery-staple")
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if !ok {
		t.Fatal("expected correct password to verify")
	}
}

func TestHashAndVerifyPassword_WrongPassword(t *testing.T) {
	hash, err := HashPassword(testArgonCfg(), "correct-horse-battery-staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	ok, err := VerifyPassword(hash, "wrong-password")
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if ok {
		t.Fatal("expected wrong password to fail verification")
	}
}

func TestHashPassword_UniqueSaltPerCall(t *testing.T) {
	h1, _ := HashPassword(testArgonCfg(), "same-password")
	h2, _ := HashPassword(testArgonCfg(), "same-password")
	if h1 == h2 {
		t.Fatal("expected different salts to produce different encoded hashes for the same password")
	}
}

func TestVerifyPassword_MalformedHash(t *testing.T) {
	_, err := VerifyPassword("not-a-real-hash", "anything")
	if err == nil {
		t.Fatal("expected malformed hash to return an error, not a bool result")
	}
}

func TestDummyHash_IsValidAndAlwaysFails(t *testing.T) {
	ok, err := VerifyPassword(DummyHash, "some guess an attacker might make")
	if err != nil {
		t.Fatalf("VerifyPassword against DummyHash: %v", err)
	}
	if ok {
		t.Fatal("dummy hash should never verify successfully")
	}
}
