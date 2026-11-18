package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_SigningKeyPolicyByEnvironment(t *testing.T) {
	const strong = "q8Zr3vLk0pXw7TnB2sYd9HcJf5GmEa1U"
	cases := []struct {
		env, key string
		wantErr  bool
	}{
		{"production", strong, false},
		{"production", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", true},
		{"production", "abababababababababababababababababab", true},
		{"test", "local-dev-only-signing-key-do-not-use-elsewhere-32b", true},
		{"development", "local-dev-only-signing-key-do-not-use-elsewhere-32b", false},
		{"development", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", false},
	}
	for _, c := range cases {
		setValidEnv(t)
		t.Setenv("APP_ENV", c.env)
		t.Setenv("JWT_SIGNING_KEY", c.key)
		_, err := Load()
		if (err != nil) != c.wantErr {
			t.Errorf("env=%s key=%q: err=%v wantErr=%v", c.env, c.key, err, c.wantErr)
		}
		if err != nil && strings.Contains(err.Error(), c.key) {
			t.Errorf("error echoes the signing key")
		}
	}
}

func TestLoad_HTTPHardeningDefaultsAndTrustedProxies(t *testing.T) {
	setValidEnv(t)
	t.Setenv("TRUSTED_PROXIES", "10.0.0.0/8, 192.168.1.1")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TrustedProxies) != 2 || cfg.MaxBodyBytes != 1<<20 || cfg.HTTPReadTimeout <= 0 || cfg.HTTPWriteTimeout <= 0 || cfg.HTTPIdleTimeout <= 0 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	t.Setenv("MAX_BODY_BYTES", "0")
	if _, err := Load(); err == nil {
		t.Error("MAX_BODY_BYTES=0 accepted")
	}
}

func TestLoad_Argon2UpperBounds(t *testing.T) {
	for _, kv := range [][2]string{{"ARGON2_MEMORY_KIB", "99999999"}, {"ARGON2_ITERATIONS", "1000"}, {"ARGON2_PARALLELISM", "65"}} {
		setValidEnv(t)
		t.Setenv(kv[0], kv[1])
		if _, err := Load(); err == nil {
			t.Errorf("Load accepted %s=%s", kv[0], kv[1])
		}
	}
}

func TestLoad_Argon2ConcurrencyDefaultsAndRanges(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Argon.MaxConcurrent < 2 || cfg.Argon.MaxConcurrent > 8 {
		t.Errorf("default MaxConcurrent = %d, want within [2,8]", cfg.Argon.MaxConcurrent)
	}
	if cfg.Argon.QueueTimeout != DefaultArgon2QueueTimeout {
		t.Errorf("default QueueTimeout = %v", cfg.Argon.QueueTimeout)
	}

	setValidEnv(t)
	t.Setenv("ARGON2_MAX_CONCURRENT", "3")
	t.Setenv("ARGON2_QUEUE_TIMEOUT", "2s")
	cfg, err = Load()
	if err != nil || cfg.Argon.MaxConcurrent != 3 || cfg.Argon.QueueTimeout != 2*time.Second {
		t.Fatalf("explicit values not honoured: %+v err=%v", cfg, err)
	}

	for _, kv := range [][2]string{
		{"ARGON2_MAX_CONCURRENT", "0"}, {"ARGON2_MAX_CONCURRENT", "-1"}, {"ARGON2_MAX_CONCURRENT", "1025"},
		{"ARGON2_MAX_CONCURRENT", "many"}, {"ARGON2_QUEUE_TIMEOUT", "0s"}, {"ARGON2_QUEUE_TIMEOUT", "-1s"},
		{"ARGON2_QUEUE_TIMEOUT", "2h"},
	} {
		setValidEnv(t)
		t.Setenv(kv[0], kv[1])
		if _, err := Load(); err == nil {
			t.Errorf("Load accepted %s=%s", kv[0], kv[1])
		}
	}
}

func TestDefaultArgon2MaxConcurrent(t *testing.T) {
	for _, c := range []struct{ procs, par, want int }{
		{1, 2, 2}, {4, 2, 2}, {8, 2, 4}, {64, 2, 8}, {16, 1, 8}, {8, 0, 8},
	} {
		if got := DefaultArgon2MaxConcurrent(c.procs, c.par); got != c.want {
			t.Errorf("DefaultArgon2MaxConcurrent(%d,%d) = %d, want %d", c.procs, c.par, got, c.want)
		}
	}
}
