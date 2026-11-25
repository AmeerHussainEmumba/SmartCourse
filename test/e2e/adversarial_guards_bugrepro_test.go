//go:build bugrepro

package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Guard-bypass reproducers (security tester, Week 3). Each test builds a tiny
// scratch tree in a temp directory (never touching the repo), copies the real
// scripts/check-dev-defaults.sh into it and shows a change an attacker-minded
// developer could make that the guard does not report. Every test asserts the
// guard FAILS (or warns) on the unsafe change, so each currently fails.
//
//	cd test/e2e && go test -tags=bugrepro -run Guard ./

const unsafeKey = "local-dev-only-signing-key-do-not-use-elsewhere-32b"

// guardTree writes the files into a scratch tree next to a copy of the script
// and returns the script's combined output and exit error.
func guardTree(t *testing.T, files map[string]string) (string, error) {
	t.Helper()
	dir := t.TempDir()
	src, err := os.ReadFile("../../scripts/check-dev-defaults.sh")
	if err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		"env.example":           "JWT_SIGNING_KEY=" + unsafeKey + "\n",
		"docker-compose.yml":    "services:\n  api:\n    environment:\n      APP_ENV: development\n      JWT_SIGNING_KEY: " + unsafeKey + "\n",
		"libs/config/config.go": "package config\n// production: reject placeholder keys\n",
		"tools/seed/main.go":    "package main\nfunc main() { _ = cfg.Env }\n",
		"Dockerfile.x":          "FROM alpine:3.20\nUSER app\n",
	}
	for k, v := range files {
		base[k] = v
	}
	base["scripts/check-dev-defaults.sh"] = string(src)
	for name, body := range base {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	out, err := exec.Command("bash", filepath.Join(dir, "scripts/check-dev-defaults.sh")).CombinedOutput()
	return string(out), err
}

// S-12a: check B is satisfied by a comment. It only greps libs/config/config.go
// for the words "production" and "placeholder|...|weak" anywhere, so config.go
// that still accepts the published signing key passes as long as a comment
// mentions them.
func TestBugrepro_Guard_PlaceholderKeyCheckSatisfiedByAComment(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"libs/config/config.go": "package config\n// TODO production placeholder weak keys\nfunc check(k string) bool { return len(k) >= 32 }\n",
	})
	if !strings.Contains(out, "FAIL [B]") {
		t.Fatalf("guard B reports no failure for a config that only checks the key length:\n%s", out)
	}
}

// S-12b: check D (seed refuses production) is satisfied by any occurrence of
// "APP_ENV", "cfg.Env" or "production" in tools/seed/main.go, including a comment.
func TestBugrepro_Guard_SeedProductionCheckSatisfiedByAComment(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"tools/seed/main.go": "package main\n// not for production, see APP_ENV\nconst seedPassword = \"SeedPass123!\"\nfunc main() {}\n",
	})
	if !strings.Contains(out, "FAIL [D]") {
		t.Fatalf("guard D reports no failure for a seed with a fixed admin password and no environment check:\n%s", out)
	}
}

// S-12c: check G (ports published on all interfaces) only matches the YAML list
// form ("- 5433:5432"); the inline form every service in docker-compose.yml
// actually uses is invisible to it.
func TestBugrepro_Guard_InlinePortsOnAllInterfacesNotReported(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"docker-compose.yml": "services:\n  postgres:\n    environment:\n      POSTGRES_PASSWORD: smartcourse\n    ports: [\"5433:5432\"]\n  redis:\n    ports: [\"0.0.0.0:6380:6379\"]\n",
	})
	if !strings.Contains(out, "WARN [G]") && !strings.Contains(out, "FAIL [G]") {
		t.Fatalf("guard G is silent about Postgres and Redis published on every interface:\n%s", out)
	}
}

// S-12d: check C (credential literals) only recognises double-quoted strings;
// a raw (backtick) string literal hides a credential from it.
func TestBugrepro_Guard_RawStringCredentialNotReported(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"svc/main.go": "package svc\nconst dbPassword = `hunter2hunter2hunter2`\nvar apiKey = `sk_live_0123456789abcdef`\n",
	})
	if !strings.Contains(out, "FAIL [C]") {
		t.Fatalf("guard C reports nothing for credentials in raw string literals:\n%s", out)
	}
}

// S-12e: check A (every compose service carrying a signing key is pinned to
// APP_ENV: development) only understands the map form "KEY: value"; the list
// form "- JWT_SIGNING_KEY=..." with "- APP_ENV=production" is not seen at all.
func TestBugrepro_Guard_ListFormEnvironmentNotChecked(t *testing.T) {
	out, _ := guardTree(t, map[string]string{
		"docker-compose.yml": "services:\n  api:\n    environment:\n      - APP_ENV=production\n      - JWT_SIGNING_KEY=" + unsafeKey + "\n",
	})
	if !strings.Contains(out, "FAIL [A]") {
		t.Fatalf("guard A is silent about a production service shipping the published signing key (list-form env):\n%s", out)
	}
}

// S-12f: the guard flags the E2E harness's own throwaway test password as a
// production credential literal, so scripts/check-dev-defaults.sh exits 1 on the
// repository today (a guard that always fails is a guard people stop reading,
// and the allowlist the developers will add can be abused the same way).
func TestBugrepro_Guard_FailsOnTheRepositoryItself(t *testing.T) {
	out, err := exec.Command("bash", "../../scripts/check-dev-defaults.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("scripts/check-dev-defaults.sh exits non-zero on the repo as committed:\n%s", out)
	}
}
