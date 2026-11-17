#!/usr/bin/env bash
# Fails if a Go module requires a sibling module it isn't allowed to. This is
# the module-level boundary that replaced the old depguard rules between
# services: a service can't import another service's code without adding it
# to its go.mod, so this file is where such a change becomes visible and
# reviewable. See docs/plans/module-split.md and RFC §4.4.
#
#   libs/*            may require only other libs/*
#   services/identity, services/course, services/analytics
#                     may require only libs/*  (no service imports another)
#   services/worker   may require libs/* and services/analytics, services/course
#                     (it runs their relay handlers and the publish workflow)
#   tools/seed        may require libs/* and services/identity, services/course
#
# Usage: scripts/check-module-graph.sh
set -euo pipefail

cd "$(dirname "$0")/.."
mod=github.com/AmeerHussainEmumba/SmartCourse

allowed_services() { # module dir -> space-separated allowed services/* modules
  case "$1" in
    services/worker) echo "services/analytics services/course" ;;
    tools/seed)      echo "services/identity services/course" ;;
    *)               echo "" ;;
  esac
}

fail=0
for dir in $(awk '/^\t/{print $1}' go.work | sed 's#^\./##'); do
  allowed=" $(allowed_services "$dir") "
  # Handles both `require (` block entries and a hand-written single-line
  # `require <mod> <ver>`; skips replace stanzas and the module line.
  reqs=$(awk '
    /^replace \(/ {r=1; next}
    r && /^\)/ {r=0; next}
    r {next}
    /^replace / || /^module / {next}
    /SmartCourse\/services\// { print ($1=="require" ? $2 : $1) }
  ' "$dir/go.mod" | sed "s#^$mod/##")
  for r in $reqs; do
    if [[ "$allowed" != *" $r "* ]]; then
      echo "FAIL $dir requires $r, which it may not (see scripts/check-module-graph.sh)"
      fail=1
    fi
  done
  # a libs/* module must never require anything outside libs/*
  if [[ "$dir" == libs/* ]] && grep -q "SmartCourse/services\|SmartCourse/tools" "$dir/go.mod"; then
    echo "FAIL $dir (a library) references a service or tool module"
    fail=1
  fi
done
[ "$fail" -eq 0 ] && echo "ok   module graph"
exit $fail
