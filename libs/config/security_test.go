// Security checks for Load() (area A/F): secrets must not be echoed back in
// startup errors, and the weakest accepted JWT key is pinned.
package config

import (
	"strings"
	"testing"
)

func TestLoad_ShortJWTKey_ErrorDoesNotEchoTheSecret(t *testing.T) {
	setValidEnv(t)
	const secret = "s3cr3t-short"
	t.Setenv("JWT_SIGNING_KEY", secret)
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error for a short key")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("startup error echoes the signing key: %v", err)
	}
}

func TestLoad_BadNumericEnv_ErrorDoesNotEchoCredentials(t *testing.T) {
	setValidEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://user:hunter2@db/x")
	t.Setenv("HTTP_PORT", "not-a-number")
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, s := range []string{"hunter2", "minioadmin", strings.Repeat("x", 32)} {
		if strings.Contains(err.Error(), s) {
			t.Fatalf("startup error leaks a secret value %q: %v", s, err)
		}
	}
}

func TestLoad_JWTKeyBoundary_31Rejected_32Accepted(t *testing.T) {
	setValidEnv(t)
	t.Setenv("JWT_SIGNING_KEY", strings.Repeat("k", 31))
	if _, err := Load(); err == nil {
		t.Fatal("31-byte key accepted")
	}
	t.Setenv("JWT_SIGNING_KEY", strings.Repeat("k", 32))
	if _, err := Load(); err != nil {
		t.Fatalf("32-byte key rejected: %v", err)
	}
}
