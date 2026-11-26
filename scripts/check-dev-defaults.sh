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
#   B  libs/config rejects the published placeholder signing keys when APP_ENV is not development
#   C  no credential-looking literals in non-test Go code (tools/seed and test/ are checked separately or not shipped)
#   D  tools/seed refuses to run outside development (it creates admin@smartcourse.dev with a known password)
#   E  no committed .env files, private keys, or cloud tokens
#   F  Dockerfiles: final stage runs as a non-root USER; base images pinned
#   G  compose publishes ports beyond 127.0.0.1 (FAIL next to default credentials in the same service)
#
# A, B, C, D and G are done by tools/guardcheck, which parses docker-compose.yml as
# YAML and the Go sources with go/parser (week-3 finding S-12: grep cannot tell code
# from a comment, "x" from `x`, a list from an inline list, or "KEY: v" from
# "- KEY=v"). Go rather than a script interpreter because the toolchain is already
# required by every other check and by CI (actions/setup-go), whereas PyYAML is not
# guaranteed on a CI image and a regex/awk YAML reader is exactly what was bypassed.
#
# Usage: scripts/check-dev-defaults.sh        (exit 1 if any FAIL)
#        GUARD_ROOT=<dir> scripts/check-dev-defaults.sh   check another tree (the guard's own tests do)
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd -P)
cd "${GUARD_ROOT:-$here}" || { echo "FAIL [tooling] cannot enter ${GUARD_ROOT:-$here}"; exit 1; }
root=$(pwd -P)

fails=0; warns=0
fail() { echo "FAIL [$1] $2"; fails=$((fails+1)); }
warn() { echo "WARN [$1] $2"; warns=$((warns+1)); }
ok()   { echo "ok   [$1] $2"; }

# ---- A, B, C, D, G (structured) ----------------------------------------------
# A guard that cannot run must fail, never pass: a missing toolchain or a build
# error is FAIL [tooling], and every FAIL/WARN line the program prints is counted.
if ! command -v go >/dev/null 2>&1; then
  fail tooling "go is not installed: checks A, B, C, D and G (tools/guardcheck) cannot run"
else
  gc_out=$(cd "$here/tools/guardcheck" && GOWORK=off go run . -root "$root" 2>&1); gc_rc=$?
  if [ -n "$gc_out" ]; then echo "$gc_out"; fi
  fails=$((fails + $(grep -c '^FAIL ' <<<"$gc_out")))
  warns=$((warns + $(grep -c '^WARN ' <<<"$gc_out")))
  if [ "$gc_rc" -ne 0 ] && ! grep -q '^FAIL ' <<<"$gc_out"; then
    fail tooling "tools/guardcheck exited $gc_rc without a result line (build or runtime error above)"
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

echo
echo "dev-defaults check: $fails FAIL, $warns WARN"
[ "$fails" -eq 0 ]
