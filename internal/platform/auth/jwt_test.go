// What this checks: a token issued by IssueAccessToken parses back to the
// same claims; expired tokens are rejected; tokens signed with a different
// key are rejected; and — the specific vulnerability class this guards
// against — a token whose header claims "alg: none" or an unexpected
// algorithm is rejected rather than trusted. No database — see
// docs/guides/testing.md, "unit" tier.
package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

func testJWTCfg() config.JWTConfig {
	return config.JWTConfig{
		SigningKey:      "test-signing-key-at-least-32-bytes-long!!",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		Issuer:          "smartcourse-test",
	}
}

func TestIssueAndParseAccessToken_RoundTrip(t *testing.T) {
	cfg := testJWTCfg()
	userID := uuid.New()

	signed, err := IssueAccessToken(cfg, userID, "instructor", 3)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	claims, err := ParseAccessToken(cfg, signed)
	if err != nil {
		t.Fatalf("ParseAccessToken: %v", err)
	}
	gotUserID, err := claims.UserID()
	if err != nil {
		t.Fatalf("claims.UserID: %v", err)
	}
	if gotUserID != userID {
		t.Errorf("UserID: got %s, want %s", gotUserID, userID)
	}
	if claims.Role != "instructor" {
		t.Errorf("Role: got %s, want instructor", claims.Role)
	}
	if claims.TokenVersion != 3 {
		t.Errorf("TokenVersion: got %d, want 3", claims.TokenVersion)
	}
}

func TestParseAccessToken_Expired(t *testing.T) {
	cfg := testJWTCfg()
	claims := Claims{
		Role:         "student",
		TokenVersion: 1,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.NewString(),
			Issuer:    cfg.Issuer,
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)), // already expired
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(cfg.SigningKey))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if _, err := ParseAccessToken(cfg, signed); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestParseAccessToken_WrongSigningKey(t *testing.T) {
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	wrongCfg := cfg
	wrongCfg.SigningKey = "a-completely-different-signing-key-32bytes"
	if _, err := ParseAccessToken(wrongCfg, signed); err == nil {
		t.Fatal("expected token signed with a different key to be rejected")
	}
}

// TestParseAccessToken_RejectsAlgNone is the specific check for the
// alg:none / alg-confusion vulnerability class (ADR-0015): a token whose
// header claims an unsigned or unexpected algorithm must never be trusted,
// even if a caller controls the token's contents entirely.
func TestParseAccessToken_RejectsAlgNone(t *testing.T) {
	cfg := testJWTCfg()
	claims := Claims{
		Role: "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.NewString(),
			Issuer:    cfg.Issuer,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	signed, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign with alg:none: %v", err)
	}

	if _, err := ParseAccessToken(cfg, signed); err == nil {
		t.Fatal("expected alg:none token to be rejected — this is the exact vulnerability the explicit method check exists to close")
	}
}

func TestParseAccessToken_WrongIssuer(t *testing.T) {
	cfg := testJWTCfg()
	signed, err := IssueAccessToken(cfg, uuid.New(), "student", 1)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	otherCfg := cfg
	otherCfg.Issuer = "someone-else"
	if _, err := ParseAccessToken(otherCfg, signed); err == nil {
		t.Fatal("expected token with mismatched issuer to be rejected")
	}
}
