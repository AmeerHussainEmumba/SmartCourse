#!/usr/bin/env bash
# Scans for committed credentials and for dev defaults that could run unchanged
# outside a dev profile. Dev defaults in env.example / docker-compose.yml are
# EXPECTED (ADR-0018); what must not happen is a production process starting
# with them, or a credential living in production code.
#
#   FAIL = a defect that makes a non-dev deployment unsafe by default
#   WARN = hardening gap / informational
#
# Checks
#   A  every compose service that carries a JWT_SIGNING_KEY pins APP_ENV: development
#   B  libs/config rejects the published placeholder signing keys when APP_ENV=production
#   C  no credential-looking literals in non-test Go code (tools/seed is checked separately)
#   D  tools/seed refuses to run outside development (it creates admin@smartcourse.dev with a known password)
#   E  no committed .env files, private keys, or cloud tokens
#   F  Dockerfiles: final stage runs as a non-root USER; base images pinned
#   G  compose publishes data-store ports on all interfaces next to default credentials
#
# Usage: scripts/check-dev-defaults.sh        (exit 1 if any FAIL)
set -uo pipefail
cd "$(dirname "$0")/.."

fails=0; warns=0
fail() { echo "FAIL [$1] $2"; fails=$((fails+1)); }
warn() { echo "WARN [$1] $2"; warns=$((warns+1)); }
ok()   { echo "ok   [$1] $2"; }

# ---- A ---------------------------------------------------------------------
bad=$(awk '
  /^  [a-zA-Z0-9_-]+:[[:space:]]*$/ { if (svc != "" && key && env != "development") print svc; svc=$1; key=0; env="" }
  /JWT_SIGNING_KEY:/ { key=1 }
  /APP_ENV:/ { env=$2 }
  END { if (svc != "" && key && env != "development") print svc }' docker-compose.yml)
if [ -n "$bad" ]; then fail A "compose service(s) with a JWT_SIGNING_KEY but without APP_ENV: development: $(echo $bad)"; else ok A "every compose service with a signing key is pinned to APP_ENV=development"; fi

# ---- B ---------------------------------------------------------------------
placeholders=$( { grep -hE '^JWT_SIGNING_KEY=' env.example; grep -hE 'JWT_SIGNING_KEY:' docker-compose.yml; } | sed -E 's/^[A-Z_]+[=:] *//' | sort -u)
if grep -qE 'production' libs/config/config.go && grep -qE 'placeholder|replace-with|local-dev|known.default|denylist|weak' libs/config/config.go; then
  ok B "libs/config appears to reject placeholder signing keys in production"
else
  fail B "libs/config/config.go only checks len(JWT_SIGNING_KEY) >= 32; these published values would be accepted with APP_ENV=production:"
  echo "$placeholders" | sed 's/^/       /'
  echo "       reproducer: go test -tags='integration bugrepro' -run TestBug_Load_ProductionAcceptsPublishedDefaultSigningKey ./libs/config"
fi

# ---- C ---------------------------------------------------------------------
hits=$(grep -rnE '(Password|Passwd|Secret|SigningKey|APIKey|AccessKey|PrivateKey|Token)[A-Za-z]*[[:space:]]*(:=|=|:)[[:space:]]*"[^"]{6,}"' --include='*.go' . \
  | grep -vE '_test\.go:|/testdata/|tools/seed/' | grep -vE 'json:"|binding:"|gorm:"' || true)
if [ -n "$hits" ]; then fail C "credential-looking literals in production Go code:"; echo "$hits" | sed 's/^/       /'; else ok C "no credential literals in non-test Go code outside tools/seed"; fi

# ---- D ---------------------------------------------------------------------
if grep -qE 'seedPassword[[:space:]]*=' tools/seed/main.go; then
  if grep -qE 'cfg\.Env|APP_ENV|"production"' tools/seed/main.go; then
    ok D "tools/seed references the environment (verify it refuses production)"
  else
    pw=$(grep -nE 'seedPassword[[:space:]]*=' tools/seed/main.go | head -1)
    fail D "tools/seed/main.go has one fixed password for every seeded account incl. an admin ($pw) and never checks cfg.Env: pointing POSTGRES_DSN at a real database seeds admin@smartcourse.dev with a public password"
  fi
fi

# ---- E ---------------------------------------------------------------------
if git rev-parse --git-dir >/dev/null 2>&1; then
  envs=$(git ls-files | grep -E '(^|/)\.env($|\.)' | grep -v 'env.example' || true)
  if [ -n "$envs" ]; then fail E "committed env files: $envs"; else ok E "no committed .env files"; fi
fi
keys=$(grep -rlE -e '-----BEGIN ([A-Z ]+)?PRIVATE KEY-----|AKIA[0-9A-Z]{16}|ghp_[0-9A-Za-z]{30,}|xox[baprs]-[0-9A-Za-z-]{10,}' . --exclude-dir=.git 2>/dev/null | grep -v 'scripts/check-dev-defaults.sh' || true)
if [ -n "$keys" ]; then fail E "private keys / cloud tokens committed in: $keys"; else ok E "no private keys or cloud tokens in the tree"; fi

# ---- F ---------------------------------------------------------------------
for df in Dockerfile.*; do
  last_from=$(grep -nE '^FROM ' "$df" | tail -1 | cut -d: -f1)
  if ! tail -n +"$last_from" "$df" | grep -qE '^USER '; then
    warn F "$df: the final stage has no USER directive, so the service runs as root in the container"
  fi
  if grep -qE '^FROM golang:[0-9]*-alpine' "$df"; then
    warn F "$df: build stage uses a floating tag ($(grep -E '^FROM golang' "$df" | head -1 | awk '{print $2}')); pin a version/digest for reproducible, auditable images"
  fi
  if grep -qE '^ENV .*(KEY|SECRET|PASSWORD|TOKEN)=' "$df"; then fail F "$df bakes a credential into the image with ENV"; fi
done

# ---- G ---------------------------------------------------------------------
exposed=$(grep -nE '^[[:space:]]+- "?[0-9]+:[0-9]+"?' docker-compose.yml | grep -v '127.0.0.1' || true)
if [ -n "$exposed" ]; then
  warn G "docker-compose publishes ports on 0.0.0.0 (reachable from the LAN) next to default credentials (smartcourse/smartcourse, minio smartcourse123, redis without a password); bind to 127.0.0.1:"
  echo "$exposed" | sed 's/^/       /'
fi

echo
echo "dev-defaults check: $fails FAIL, $warns WARN"
[ "$fails" -eq 0 ]
