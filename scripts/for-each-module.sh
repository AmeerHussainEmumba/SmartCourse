#!/usr/bin/env bash
# Runs a command in every Go module listed in go.work, each with the workspace
# switched off (GOWORK=off) so a module is built exactly as it would be on its
# own: a missing `replace` directive or an undeclared dependency fails here
# instead of being hidden by go.work. `go ./...` doesn't cross module
# boundaries, so plain `go test ./...` at the repo root no longer covers the
# libraries. See docs/plans/module-split.md.
#
# Usage: scripts/for-each-module.sh go vet ./...
set -uo pipefail

cd "$(dirname "$0")/.."
fail=0
for dir in $(awk '/^\t/{print $1}' go.work); do
  echo "=== $dir"
  (cd "$dir" && GOWORK=off "$@") || fail=1
done
exit $fail
