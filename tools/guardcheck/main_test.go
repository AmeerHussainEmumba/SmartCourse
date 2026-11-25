package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const publishedKey = "local-dev-only-signing-key-do-not-use-elsewhere-32b"

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func compose(t *testing.T, yml string) *composeFile {
	t.Helper()
	cf, err := loadCompose(writeTree(t, map[string]string{"docker-compose.yml": yml, "svc.env": "JWT_SIGNING_KEY=" + publishedKey + "\n"}))
	if err != nil {
		t.Fatal(err)
	}
	return cf
}

func TestCheckA(t *testing.T) {
	key := "JWT_SIGNING_KEY"
	for _, tc := range []struct {
		name string
		yml  string
		want string // "ok" or "FAIL"
	}{
		{"map form pinned", "services:\n  a:\n    environment:\n      APP_ENV: development\n      " + key + ": x\n", "ok"},
		{"list form pinned", "services:\n  a:\n    environment:\n      - APP_ENV=development\n      - " + key + "=x\n", "ok"},
		{"list form production (S-12e)", "services:\n  a:\n    environment:\n      - APP_ENV=production\n      - " + key + "=x\n", "FAIL"},
		{"map form production", "services:\n  a:\n    environment:\n      APP_ENV: production\n      " + key + ": x\n", "FAIL"},
		{"no APP_ENV at all", "services:\n  a:\n    environment:\n      " + key + ": x\n", "FAIL"},
		{"substitution default production", "services:\n  a:\n    environment:\n      APP_ENV: ${APP_ENV:-production}\n      " + key + ": x\n", "FAIL"},
		{"substitution cannot be proven development", "services:\n  a:\n    environment:\n      APP_ENV: ${APP_ENV:-development}\n      " + key + ": x\n", "FAIL"},
		{"key without value in list", "services:\n  a:\n    environment:\n      - " + key + "\n", "FAIL"},
		{"inline map", "services:\n  a:\n    environment: {APP_ENV: production, " + key + ": x}\n", "FAIL"},
		{"anchor and merge key", "x-env: &env\n  APP_ENV: production\n  " + key + ": x\nservices:\n  a:\n    environment:\n      <<: *env\n", "FAIL"},
		{"key from env_file, production in environment", "services:\n  a:\n    env_file: svc.env\n    environment:\n      APP_ENV: production\n", "FAIL"},
		{"key from env_file list form, no APP_ENV", "services:\n  a:\n    env_file: [svc.env]\n", "FAIL"},
		{"service without a signing key", "services:\n  a:\n    environment:\n      APP_ENV: production\n", "ok"},
	} {
		if got := checkA(compose(t, tc.yml)); got.Level != tc.want {
			t.Errorf("%s: %s, want %s: %s", tc.name, got.Level, tc.want, got.Msg)
		}
	}
}

func TestCheckG(t *testing.T) {
	cred := "    environment:\n      POSTGRES_PASSWORD: smartcourse\n"
	for _, tc := range []struct {
		name string
		yml  string
		want string
	}{
		{"inline loopback", "services:\n  a:\n    ports: [\"127.0.0.1:5433:5432\"]\n", "ok"},
		{"list loopback", "services:\n  a:\n    ports:\n      - \"127.0.0.1:5433:5432\"\n", "ok"},
		{"ipv6 loopback", "services:\n  a:\n    ports: [\"[::1]:5433:5432\"]\n", "ok"},
		{"inline all interfaces (S-12c)", "services:\n  a:\n    ports: [\"5433:5432\"]\n", "WARN"},
		{"inline 0.0.0.0", "services:\n  a:\n    ports: [\"0.0.0.0:6380:6379\"]\n", "WARN"},
		{"list all interfaces", "services:\n  a:\n    ports:\n      - 5433:5432\n", "WARN"},
		{"container port only", "services:\n  a:\n    ports: [5432]\n", "WARN"},
		{"with protocol", "services:\n  a:\n    ports: [\"5433:5432/tcp\"]\n", "WARN"},
		{"long syntax without host_ip", "services:\n  a:\n    ports:\n      - target: 5432\n        published: 5433\n", "WARN"},
		{"long syntax loopback", "services:\n  a:\n    ports:\n      - target: 5432\n        published: 5433\n        host_ip: 127.0.0.1\n", "ok"},
		{"substituted bind address", "services:\n  a:\n    ports: [\"${BIND}:5433:5432\"]\n", "WARN"},
		{"host network", "services:\n  a:\n    network_mode: host\n", "WARN"},
		{"all interfaces next to default credentials", "services:\n  a:\n" + cred + "    ports: [\"5433:5432\"]\n", "FAIL"},
		{"another service's credentials do not escalate", "services:\n  a:\n" + cred + "    ports: [\"127.0.0.1:5433:5432\"]\n  b:\n    ports: [\"6380:6379\"]\n", "WARN"},
	} {
		if got := checkG(compose(t, tc.yml))[0]; got.Level != tc.want {
			t.Errorf("%s: %s, want %s: %s", tc.name, got.Level, tc.want, got.Msg)
		}
	}
}

func levels(fs []Finding) string {
	var out []string
	for _, f := range fs {
		out = append(out, f.Level)
	}
	return strings.Join(out, ",")
}

func TestCheckC(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"double quoted", map[string]string{"a/a.go": "package a\nvar dbPassword = \"hunter2hunter2\"\n"}, "FAIL"},
		{"raw string (S-12d)", map[string]string{"a/a.go": "package a\nconst dbPassword = `hunter2hunter2`\n"}, "FAIL"},
		{"concatenation", map[string]string{"a/a.go": "package a\nvar apiKey = \"hunter\" + `2hunter2`\n"}, "FAIL"},
		{"byte conversion", map[string]string{"a/a.go": "package a\nvar secretBytes = []byte(`hunter2hunter2`)\n"}, "FAIL"},
		{"struct field", map[string]string{"a/a.go": "package a\ntype C struct{ Password string }\nvar c = C{Password: `hunter2hunter2`}\n"}, "FAIL"},
		{"selector assignment", map[string]string{"a/a.go": "package a\ntype C struct{ AccessKey string }\nfunc f(c *C) { c.AccessKey = \"AKIDEXAMPLE1234\" }\n"}, "FAIL"},
		{"map entry", map[string]string{"a/a.go": "package a\nvar m = map[string]string{\"password\": \"hunter2hunter2\"}\n"}, "FAIL"},
		{"typed const", map[string]string{"a/a.go": "package a\nconst signingKey string = \"0123456789abcdef\"\n"}, "FAIL"},
		{"lower case token name", map[string]string{"a/a.go": "package a\nvar authtoken = \"abcdefghij\"\n"}, "FAIL"},
		{"an unparsable file cannot be checked", map[string]string{"a/a.go": "package a\nvar x = \n"}, "FAIL"},
		{"upper-case secret under a plain name is not an env var name", map[string]string{"a/a.go": "package a\nvar secretValue = \"SUPER_SECRET_VALUE\"\n"}, "FAIL"},
		{"short value", map[string]string{"a/a.go": "package a\nvar password = \"abc\"\n"}, "ok"},
		{"env var name is not a secret", map[string]string{"a/a.go": "package a\nconst signingKeyVar = \"JWT_SIGNING_KEY\"\n"}, "ok"},
		{"struct tag is not an assignment", map[string]string{"a/a.go": "package a\ntype C struct {\n\tPassword string `json:\"password_value\"`\n}\n"}, "ok"},
		{"test file is skipped", map[string]string{"a/a_test.go": "package a\nvar dbPassword = \"hunter2hunter2\"\n"}, "ok"},
		{"test tree is skipped", map[string]string{"test/e2e/h.go": "package h\nconst Password = \"correct-horse-battery-1\"\n"}, "ok"},
		{"seed tool is checked by D", map[string]string{"tools/seed/main.go": "package main\nconst seedPassword = \"SeedPass123!\"\n"}, "ok"},
		{"test-support package is skipped", map[string]string{"internal/miniotest/m.go": "package miniotest\nconst secret = \"throwaway-secret\"\n"}, "ok"},
		{"clean code", map[string]string{"a/a.go": "package a\nfunc f(password string) string { return password }\n"}, "ok"},
	} {
		got := checkC(writeTree(t, tc.files))
		if levels(got) != tc.want {
			t.Errorf("%s: %s, want %s: %v", tc.name, levels(got), tc.want, got)
		}
	}
}

func TestCheckB(t *testing.T) {
	published := "JWT_SIGNING_KEY=" + publishedKey + "\n"
	for _, tc := range []struct {
		name string
		cfg  string
		want string
	}{
		{"guarded call with markers", "package config\nvar markers = []string{\"local-dev-only\"}\nfunc weak(k string) bool { for _, m := range markers { _ = m }; return false }\nfunc Load(env, k string) { if env != \"development\" { _ = weak(k) } }\ntype X struct{ Env string }\nfunc L2(x X, k string) { if x.Env != \"development\" { weak(k) } }\n", "ok"},
		{"guard as a && expression", "package config\ntype X struct{ Env string }\nvar markers = []string{\"local-dev-only\"}\nfunc weak(k string) bool { _ = markers; return true }\nfunc L(x X, k string) bool { return x.Env != \"development\" && weak(k) }\n", "ok"},
		{"comment only (S-12a)", "package config\n// production placeholder weak: reject local-dev-only keys\nfunc L(k string) bool { return len(k) >= 32 }\n", "FAIL"},
		{"markers never used", "package config\nvar markers = []string{\"local-dev-only\"}\nfunc weak(k string) bool { return false }\ntype X struct{ Env string }\nfunc L(x X, k string) { if x.Env != \"development\" { weak(k) } }\n", "FAIL"},
		{"check not conditional on the environment", "package config\nvar markers = []string{\"local-dev-only\"}\nfunc weak(k string) bool { _ = markers; return true }\nfunc L(k string) { weak(k) }\n", "FAIL"},
		{"markers do not cover the published key", "package config\nvar markers = []string{\"something-else\"}\nfunc weak(k string) bool { _ = markers; return true }\ntype X struct{ Env string }\nfunc L(x X, k string) { if x.Env != \"development\" { weak(k) } }\n", "FAIL"},
		{"length check only", "package config\ntype X struct{ Env string }\nfunc L(x X, k string) bool { if x.Env != \"development\" { return len(k) >= 32 }; return true }\n", "FAIL"},
	} {
		dir := writeTree(t, map[string]string{"libs/config/config.go": tc.cfg, "env.example": published})
		if got := checkB(dir, nil); got.Level != tc.want {
			t.Errorf("%s: %s, want %s: %s", tc.name, got.Level, tc.want, got.Msg)
		}
	}
	// The published key in docker-compose.yml counts too: a marker that covers
	// env.example but not the compose key leaves it unrejected.
	dir := writeTree(t, map[string]string{
		"libs/config/config.go": "package config\nvar m = []string{\"replace-with\"}\nfunc weak(k string) bool { _ = m; return true }\ntype X struct{ Env string }\nfunc L(x X, k string) { if x.Env != \"development\" { weak(k) } }\n",
		"env.example":           "JWT_SIGNING_KEY=replace-with-a-real-random-secret\n",
	})
	cf := compose(t, "services:\n  a:\n    environment:\n      JWT_SIGNING_KEY: "+publishedKey+"\n")
	if got := checkB(dir, cf); got.Level != "FAIL" || !strings.Contains(got.Msg, publishedKey) {
		t.Errorf("a compose key outside the markers must fail: %s: %s", got.Level, got.Msg)
	}
}

func TestCheckD(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed string
		want string
	}{
		{"guard with os.Exit", "package main\nimport \"os\"\ntype C struct{ Env string }\nconst seedPassword = \"SeedPass123!\"\nfunc main() { cfg := C{}; if cfg.Env != \"development\" { os.Exit(1) } }\n", "ok"},
		{"guard on APP_ENV with return", "package main\nimport \"os\"\nconst seedPassword = \"SeedPass123!\"\nfunc main() { if os.Getenv(\"APP_ENV\") != \"development\" { return } }\n", "ok"},
		{"comment only (S-12b)", "package main\n// not for production, see APP_ENV and cfg.Env\nconst seedPassword = \"SeedPass123!\"\nfunc main() {}\n", "FAIL"},
		{"environment tested but nothing refuses", "package main\ntype C struct{ Env string }\nconst seedPassword = \"SeedPass123!\"\nfunc main() { cfg := C{}; if cfg.Env != \"development\" { _ = cfg } }\n", "FAIL"},
		{"guard in a function other than main", "package main\nimport \"os\"\ntype C struct{ Env string }\nconst seedPassword = \"SeedPass123!\"\nfunc check(c C) { if c.Env != \"development\" { os.Exit(1) } }\nfunc main() {}\n", "FAIL"},
		{"no fixed password, nothing to guard", "package main\nfunc main() {}\n", "ok"},
	} {
		got := checkD(writeTree(t, map[string]string{"tools/seed/main.go": tc.seed}))
		if levels(got) != tc.want {
			t.Errorf("%s: %s, want %s: %v", tc.name, levels(got), tc.want, got)
		}
	}
}
