//go:build integration && bugrepro

// Reproducers for REAL config-validation gaps. Not part of the default
// suite; run with -tags='integration bugrepro'. Each test FAILS today.
package config

import "testing"

// A production process may start with the documented placeholder/dev signing
// keys from env.example and docker-compose.yml: they are >= 32 bytes so the
// only length check passes. Anyone who reads the public repo can mint admin
// tokens for such a deployment.
func TestBug_Load_ProductionAcceptsPublishedDefaultSigningKey(t *testing.T) {
	for _, key := range []string{
		"replace-with-a-real-random-secret-at-least-32-bytes-long", // env.example
		"local-dev-only-signing-key-do-not-use-elsewhere-32b",      // docker-compose.yml
	} {
		setValidEnv(t)
		t.Setenv("APP_ENV", "production")
		t.Setenv("JWT_SIGNING_KEY", key)
		if _, err := Load(); err == nil {
			t.Errorf("APP_ENV=production started with the published default signing key %q", key)
		}
	}
}

// uint32(optInt(...)) wraps negatives and accepts zero: ARGON2_MEMORY_KIB=-1
// asks argon2 for ~4 TiB on the first login (OOM kill); ARGON2_ITERATIONS=0
// makes argon2.IDKey panic on every register/login (500s, recovered).
func TestBug_Load_AcceptsNonsensicalArgon2Parameters(t *testing.T) {
	for _, kv := range [][2]string{
		{"ARGON2_MEMORY_KIB", "-1"},
		{"ARGON2_MEMORY_KIB", "0"},
		{"ARGON2_ITERATIONS", "0"},
		{"ARGON2_PARALLELISM", "0"},
		{"ARGON2_PARALLELISM", "300"}, // wraps to uint8(44)
	} {
		setValidEnv(t)
		t.Setenv(kv[0], kv[1])
		if _, err := Load(); err == nil {
			t.Errorf("Load accepted %s=%s", kv[0], kv[1])
		}
	}
}

// Zero/negative token TTLs are accepted (instantly-expired sessions, or with a
// negative refresh TTL, tokens that are born expired).
func TestBug_Load_AcceptsNonPositiveTokenTTLs(t *testing.T) {
	for _, kv := range [][2]string{{"JWT_ACCESS_TTL", "0s"}, {"JWT_ACCESS_TTL", "-5m"}, {"JWT_REFRESH_TTL", "-1h"}} {
		setValidEnv(t)
		t.Setenv(kv[0], kv[1])
		if _, err := Load(); err == nil {
			t.Errorf("Load accepted %s=%s", kv[0], kv[1])
		}
	}
}
