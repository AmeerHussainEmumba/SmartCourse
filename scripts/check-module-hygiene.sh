#!/usr/bin/env bash
# Module hygiene across the whole monorepo:
#   1. every go.mod in the tree is listed in go.work, and every go.work entry
#      has a go.mod (an unlisted module silently escapes for-each-module.sh)
#   2. every `replace ... => <path>` target exists on disk and has a go.mod
#   3. `go mod tidy` is a no-op for every module (run on a COPY of the tree
#      with GOWORK=off; nothing in the working tree is touched)
#   4. no third-party module is required at different versions by different
#      modules (version skew; the workspace build silently picks the max, a
#      standalone/GOWORK=off build uses each module's own, so the Docker
#      images can ship different versions than `go test` in the workspace)
#
# Usage: scripts/check-module-hygiene.sh [--skip-tidy] [--report-only]
#   --skip-tidy    skip check 3 (needs the module cache; offline is fine if warm)
#   --report-only  always exit 0 (print findings only)
# Bash 3.2 compatible.
set -uo pipefail

cd "$(dirname "$0")/.."
skip_tidy=0; report_only=0
for a in "$@"; do
  case $a in --skip-tidy) skip_tidy=1 ;; --report-only) report_only=1 ;; esac
done
fail=0
note() { echo "FAIL $*"; fail=1; }

echo "== 1. go.work vs go.mod files =="
listed=$(awk '/^\t/{print $1}' go.work | sed 's#^\./##' | sort)
found=$(find . -name go.mod -not -path '*/node_modules/*' -not -path './.git/*' | sed 's#^\./##; s#/go.mod$##' | sort)
for m in $(comm -13 <(echo "$listed") <(echo "$found")); do note "$m has a go.mod but is not in go.work"; done
for m in $(comm -23 <(echo "$listed") <(echo "$found")); do note "go.work lists $m but it has no go.mod"; done
[ $fail -eq 0 ] && echo "ok   $(echo "$listed" | wc -l | tr -d ' ') modules, go.work and go.mod files agree"

echo "== 2. replace targets exist =="
before=$fail
for m in $listed; do
  [ -f "$m/go.mod" ] || continue
  for target in $(awk '/=>/ {for (i=1;i<=NF;i++) if ($i=="=>") print $(i+1)}' "$m/go.mod"); do
    if [ ! -d "$m/$target" ]; then note "$m/go.mod replaces to $target, which does not exist"
    elif [ ! -f "$m/$target/go.mod" ]; then note "$m/go.mod replaces to $target, which has no go.mod"; fi
  done
done
[ $fail -eq $before ] && echo "ok   all replace targets exist"

echo "== 3. go mod tidy is a no-op =="
if [ $skip_tidy -eq 1 ]; then
  echo "skip (--skip-tidy)"
else
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  rsync -a --exclude .git ./ "$tmp/"
  before=$fail
  for m in $listed; do
    [ -f "$m/go.mod" ] || continue
    if ! out=$(cd "$tmp/$m" && GOWORK=off GOFLAGS=-mod=mod go mod tidy 2>&1); then
      note "go mod tidy failed in $m: $(echo "$out" | tail -1)"; continue
    fi
    for f in go.mod go.sum; do
      if ! diff -q "$m/$f" "$tmp/$m/$f" >/dev/null 2>&1; then
        note "$m/$f drifts: go mod tidy would change it ($(diff "$m/$f" "$tmp/$m/$f" 2>/dev/null | grep -c '^[<>]') lines)"
        diff "$m/$f" "$tmp/$m/$f" 2>/dev/null | grep '^[<>]' | head -6 | sed 's/^/       /'
      fi
    done
  done
  [ $fail -eq $before ] && echo "ok   go mod tidy changes nothing in any module"
fi

echo "== 4. third-party version skew across modules =="
raw=$(mktemp)
for m in $listed; do
  awk -v mod="$m" '
    /^require \(/ {f=1; next}
    f && /^\)/ {f=0; next}
    (f || /^require /) {
      line=$0; sub(/^require /, "", line)
      split(line, a, " ")
      if (a[1] !~ /SmartCourse/ && a[2] ~ /^v/) print a[1], a[2], mod
    }' "$m/go.mod"
done | sort -u >"$raw"
# (portable awk only: no arrays of arrays, which BSD awk lacks)
skewed=$(awk '{print $1, $2}' "$raw" | sort -u | awk '{c[$1]++} END {for (k in c) if (c[k] > 1) print k}' | sort)
skew=""
for k in $skewed; do
  skew="$skew$k
$(awk -v k="$k" '$1==k {print "    " $2 " <- " $3}' "$raw")
"
done
rm -f "$raw"
if [ -n "$skew" ]; then
  echo "FAIL version skew (module: versions <- requiring modules):"
  echo "$skew" | sed 's/^/  /'
  fail=1
else
  echo "ok   every third-party module has a single version across all modules"
fi

[ $report_only -eq 1 ] && exit 0
exit $fail
