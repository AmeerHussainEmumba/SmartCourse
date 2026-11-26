package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Guard-bypass regression tests (security tester, Week 3, S-12). Each test
// builds a tiny scratch tree in a temp directory (never touching the repo),
// points the real scripts/check-dev-defaults.sh at it (GUARD_ROOT) and shows a
// change an attacker-minded developer could make; the guard must FAIL (or
// WARN) on it. They were failing reproducers (build tag bugrepro) until
// tools/guardcheck replaced the grep and awk parsing; the same forms are also
// cases in scripts/test-guards.sh and unit tests in tools/guardcheck.
//
//	cd test/e2e && go test -run Guard ./

const unsafeKey = "local-dev-only-signing-key-do-not-use-elsewhere-32b"

// A config.go that really rejects the published keys outside development.
const goodConfig = `package config

import "strings"

type Config struct{ Env string }

var markers = []string{"local-dev-only", "replace-with"}

func weak(k string) bool {
	for _, m := range markers {
		if strings.Contains(strings.ToLower(k), m) {
			return true
		}
	}
	return false
}

func Load(c *Config, key string) bool { return c.Env != "development" && weak(key) }
`

const goodSeed = `package main

import "os"

type cfgT struct{ Env string }

const seedPassword = "SeedPass123!"

func main() {
	cfg := cfgT{Env: os.Getenv("APP_ENV")}
	if cfg.Env != "development" {
		os.Exit(1)
	}
}
`

// cleanTree is a scratch tree every guard passes on.
func cleanTree() map[string]string {
	return map[string]string{
		"env.example":           "JWT_SIGNING_KEY=" + unsafeKey + "\n",
		"docker-compose.yml":    "services:\n  api:\n    environment:\n      APP_ENV: development\n      JWT_SIGNING_KEY: " + unsafeKey + "\n    ports: [\"127.0.0.1:8080:8080\"]\n",
		"libs/config/config.go": goodConfig,
		"tools/seed/main.go":    goodSeed,
		"Dockerfile.x":          "FROM alpine:3.20\nUSER app\n",
	}
}

// guardTree writes the clean tree plus the overrides into a temp dir and
// returns the guard's combined output and exit error.
func guardTree(t *testing.T, files map[string]string) (string, error) {
	t.Helper()
	dir := t.TempDir()
	tree := cleanTree()
	for k, v := range files {
		tree[k] = v
	}
	for name, body := range tree {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", "../../scripts/check-dev-defaults.sh")
	cmd.Env = append(os.Environ(), "GUARD_ROOT="+dir)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// The scratch tree itself is clean: the bypass tests below fail because of
// their one change, not because of noise.
func TestGuard_CleanScratchTreePasses(t *testing.T) {
	out, err := guardTree(t, nil)
	if err != nil || !strings.Contains(out, "0 FAIL, 0 WARN") {
		t.Fatalf("the clean scratch tree must pass every check (err %v):\n%s", err, out)
	}
}

// S-12a: check B was satisfied by a comment. It only grepped
// libs/config/config.go for the words "production" and "placeholder|...|weak"
// anywhere, so a config.go that still accepts the published signing key passed
// as long as a comment mentioned them.
func TestGuard_PlaceholderKeyCheckNotSatisfiedByAComment(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"libs/config/config.go": "package config\n// TODO production placeholder weak keys\nfunc check(k string) bool { return len(k) >= 32 }\n",
	})
	if !strings.Contains(out, "FAIL [B]") {
		t.Fatalf("guard B reports no failure for a config that only checks the key length:\n%s", out)
	}
}

// S-12b: check D (seed refuses production) was satisfied by any occurrence of
// "APP_ENV", "cfg.Env" or "production" in tools/seed/main.go, a comment included.
func TestGuard_SeedProductionCheckNotSatisfiedByAComment(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"tools/seed/main.go": "package main\n// not for production, see APP_ENV\nconst seedPassword = \"SeedPass123!\"\nfunc main() {}\n",
	})
	if !strings.Contains(out, "FAIL [D]") {
		t.Fatalf("guard D reports no failure for a seed with a fixed admin password and no environment check:\n%s", out)
	}
}

// S-12c: check G (ports published on all interfaces) only matched the YAML
// list form ("- 5433:5432"); the inline form the compose file uses was invisible.
func TestGuard_InlinePortsOnAllInterfacesAreReported(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"docker-compose.yml": "services:\n  postgres:\n    environment:\n      POSTGRES_PASSWORD: smartcourse\n    ports: [\"5433:5432\"]\n  redis:\n    ports: [\"0.0.0.0:6380:6379\"]\n",
	})
	if !strings.Contains(out, "WARN [G]") && !strings.Contains(out, "FAIL [G]") {
		t.Fatalf("guard G is silent about Postgres and Redis published on every interface:\n%s", out)
	}
}

// S-12d: check C (credential literals) only recognised double-quoted strings;
// a raw (backtick) string literal hid a credential from it.
func TestGuard_RawStringCredentialIsReported(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"svc/main.go": "package svc\nconst dbPassword = `hunter2hunter2hunter2`\nvar apiKey = `sk_live_0123456789abcdef`\n",
	})
	if !strings.Contains(out, "FAIL [C]") {
		t.Fatalf("guard C reports nothing for credentials in raw string literals:\n%s", out)
	}
}

// S-12e: check A (every compose service carrying a signing key is pinned to
// APP_ENV: development) only understood the map form "KEY: value"; the list
// form "- JWT_SIGNING_KEY=..." with "- APP_ENV=production" was not seen at all.
func TestGuard_ListFormEnvironmentIsChecked(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"docker-compose.yml": "services:\n  api:\n    environment:\n      - APP_ENV=production\n      - JWT_SIGNING_KEY=" + unsafeKey + "\n",
	})
	if !strings.Contains(out, "FAIL [A]") {
		t.Fatalf("guard A is silent about a production service shipping the published signing key (list-form env):\n%s", out)
	}
}

// S-12f: the guard flagged the E2E harness's own throwaway test password as a
// production credential literal, so scripts/check-dev-defaults.sh exited 1 on
// the repository itself (a guard that always fails is a guard people stop
// reading). It must exit 0 on the repository as committed.
func TestGuard_PassesOnTheRepositoryItself(t *testing.T) {
	out, err := exec.Command("bash", "../../scripts/check-dev-defaults.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("scripts/check-dev-defaults.sh exits non-zero on the repo as committed:\n%s", out)
	}
}
