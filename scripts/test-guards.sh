#!/usr/bin/env bash
# Mutation test for the module-boundary guards: copy the repo to a temp dir,
# inject one boundary violation at a time, and assert the right guard FAILS
# with the right message; then assert every guard PASSES on the clean tree.
# A guard that never fails is not a guard.
#
# Guards under test:
#   scripts/check-module-graph.sh       go.mod-level rules (who may require whom)
#   scripts/check-link-graph.sh         what each binary actually links
#   scripts/check-dockerfile-modules.sh Dockerfile COPY list vs replace closure
#   GOWORK=off go build ./...           a module must build on its own
#
# Needs the Go module cache to already hold the repo's dependencies (the
# injections run `go mod tidy` offline, GOPROXY=off). A case whose setup
# cannot be completed offline is reported as SKIP, never as a pass.
#
# Usage: scripts/test-guards.sh [-k]     (-k keeps the temp dir for inspection)
set -uo pipefail

root=$(cd "$(dirname "$0")/.." && pwd -P)
keep=0; [ "${1:-}" = "-k" ] && keep=1
mod=github.com/AmeerHussainEmumba/SmartCourse

work=$(mktemp -d)
[ "$keep" -eq 1 ] || trap 'rm -rf "$work"' EXIT
echo "work dir: $work"

rsync -a --exclude .git --exclude '*.test' --exclude '.idea' "$root"/ "$work/base/"

pass=0; fail=0; skip=0
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1"; fail=$((fail+1)); }
skp()  { echo "  SKIP  $1"; skip=$((skip+1)); }

fresh() { rm -rf "$work/$1"; cp -R "$work/base" "$work/$1"; echo "$work/$1"; }

# expect_fail <label> <expected-regex> <dir> <command...>
expect_fail() {
  local label=$1 re=$2 dir=$3; shift 3
  local out rc
  out=$(cd "$dir" && "$@" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then bad "$label: guard PASSED but must fail"; echo "$out" | tail -5 | sed 's/^/        /'
  elif ! grep -Eq "$re" <<<"$out"; then bad "$label: failed, but not with the expected message /$re/"; echo "$out" | tail -8 | sed 's/^/        /'
  else ok "$label"; fi
}

expect_pass() {
  local label=$1 dir=$2; shift 2
  local out rc
  out=$(cd "$dir" && "$@" 2>&1); rc=$?
  if [ $rc -ne 0 ]; then bad "$label"; echo "$out" | tail -8 | sed 's/^/        /'; else ok "$label"; fi
}

# add a require+replace for another module of this repo to <modroot>/go.mod,
# the way `go get` / `go mod edit` would (a normal require-block entry)
add_dep() { # <modroot> <depdir e.g. services/course> <relpath e.g. ../course>
  (cd "$1" && GOWORK=off go mod edit -require="$mod/$2@v0.0.0" -replace="$mod/$2=$3")
}
TIDY_ERR=""
tidy_offline() { TIDY_ERR=$(cd "$1" && GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go mod tidy 2>&1); }

echo
echo "== clean tree: every guard must pass =="
expect_pass "check-module-graph on the clean tree"      "$work/base" scripts/check-module-graph.sh
expect_pass "check-link-graph on the clean tree"        "$work/base" scripts/check-link-graph.sh
expect_pass "check-dockerfile-modules on the clean tree" "$work/base" scripts/check-dockerfile-modules.sh
for m in services/identity services/course services/analytics services/worker; do
  expect_pass "standalone build of $m (GOWORK=off)" "$work/base/$m" env GOWORK=off go build ./...
done

echo
echo "== inject: identity's go.mod requires services/course =="
d=$(fresh c1); add_dep "$d/services/identity" services/course ../course
expect_fail "module graph rejects identity -> course" "FAIL services/identity requires services/course" "$d" scripts/check-module-graph.sh

echo
echo "== inject: a library requires a service =="
d=$(fresh c2); add_dep "$d/libs/auth" services/identity ../../services/identity
expect_fail "module graph rejects libs/auth -> services/identity" "FAIL libs/auth .*references a service or tool module" "$d" scripts/check-module-graph.sh

echo
echo "== inject: analytics requires identity (service -> service) =="
d=$(fresh c3); add_dep "$d/services/analytics" services/identity ../identity
expect_fail "module graph rejects analytics -> identity" "FAIL services/analytics requires services/identity" "$d" scripts/check-module-graph.sh

echo
echo "== inject: identity-api imports the Temporal SDK =="
d=$(fresh c4)
cat >"$d/services/identity/cmd/identity-api/zz_inject.go" <<'EOF'
package main

import _ "go.temporal.io/sdk/client"
EOF
tsdk=$(awk '$1=="go.temporal.io/sdk" {print $2; exit}' "$d/services/course/go.mod")
(cd "$d/services/identity" && GOWORK=off go mod edit -require="go.temporal.io/sdk@$tsdk")
if tidy_offline "$d/services/identity"; then
  expect_fail "link graph rejects identity-api -> go.temporal.io" "FAIL identity-api links packages it must not" "$d" scripts/check-link-graph.sh
else skp "identity-api imports temporal (go mod tidy offline failed: ${TIDY_ERR##*$'\n'})"; fi

echo
echo "== inject: cmd/identity-api imports services/course =="
d=$(fresh c5)
cat >"$d/services/identity/cmd/identity-api/zz_inject.go" <<EOF
package main

import _ "$mod/services/course/course"
EOF
add_dep "$d/services/identity" services/course ../course
# course also needs libs/relay and libs/temporal; without their replaces the standalone build already
# fails (a second guard layer), so give identity the full closure to test the link-graph rule itself
add_dep "$d/services/identity" libs/relay ../../libs/relay
add_dep "$d/services/identity" libs/temporal ../../libs/temporal
if tidy_offline "$d/services/identity"; then
  expect_fail "link graph rejects identity-api -> services/course" "FAIL identity-api links packages it must not" "$d" scripts/check-link-graph.sh
  expect_fail "module graph also rejects the go.mod edge"            "FAIL services/identity requires services/course" "$d" scripts/check-module-graph.sh
else skp "identity-api imports services/course (go mod tidy offline failed: ${TIDY_ERR##*$'\n'})"; fi

echo
echo "== inject: service module loses the replace for a transitive lib =="
d=$(fresh c6)
grep -v "libs/ratelimit => " "$d/services/identity/go.mod" >"$d/services/identity/go.mod.new" && mv "$d/services/identity/go.mod.new" "$d/services/identity/go.mod"
expect_fail "standalone build of identity fails without the replace" "ratelimit|no required module|cannot find|reading|missing" "$d/services/identity" env GOWORK=off go build ./...
expect_fail "dockerfile check reports the REPLACE-GAP"          "REPLACE-GAP" "$d" scripts/check-dockerfile-modules.sh

echo
echo "== inject: Dockerfile misses a COPY / copies an extra module =="
d=$(fresh c7); grep -v "^COPY libs/ratelimit" "$d/Dockerfile.identity-api" >"$d/Dockerfile.new" && mv "$d/Dockerfile.new" "$d/Dockerfile.identity-api"
expect_fail "dockerfile check reports the MISSING copy" "MISSING from the Dockerfile" "$d" scripts/check-dockerfile-modules.sh
d=$(fresh c8); sed -i.bak 's#^COPY services/identity ./services/identity#COPY services/course ./services/course\nCOPY services/identity ./services/identity#' "$d/Dockerfile.identity-api" && rm -f "$d/Dockerfile.identity-api.bak"
expect_fail "dockerfile check reports the EXTRA copy" "EXTRA COPY" "$d" scripts/check-dockerfile-modules.sh

echo
echo "== inject: go.work lists a module whose replace target is gone =="
d=$(fresh c9); rm -rf "$d/libs/ratelimit"
expect_fail "standalone build of identity fails when a replaced lib is missing" "ratelimit|no such file|reading" "$d/services/identity" env GOWORK=off go build ./...

echo
echo "== known weakness probe (informational, not counted as pass/fail) =="
d=$(fresh c10)
printf '\nrequire %s/services/course v0.0.0\n' "$mod" >>"$d/services/identity/go.mod"
if (cd "$d" && scripts/check-module-graph.sh >/dev/null 2>&1); then
  echo "  GAP   check-module-graph.sh does not see a single-line 'require <mod> <ver>' directive (it only scans"
  echo "        'require (' blocks): identity -> services/course slipped through. go get/go mod edit write block"
  echo "        entries, so this needs a hand-edited go.mod, but the guard should still handle it."
else
  echo "  ok    single-line require directives are detected"
fi

echo
echo "summary: $pass passed, $fail failed, $skip skipped"
[ "$fail" -eq 0 ]
