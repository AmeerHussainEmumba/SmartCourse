#!/usr/bin/env bash
# Fails when an end-to-end spec is still a skipped placeholder past the phase
# that owns it (docs/plans/week-3.md section 5, test/README.md).
#
# Every E1 to E14 spec is a function TestE2E_E<nn>_... in test/e2e. A spec not
# built yet starts with t.Skip("pending: Phase N - ..."). This script lists the
# specs that still do and checks them against ALLOWED below: the specs that are
# allowed to be pending today, each with the phase that owns it.
#
#   FAIL  a spec is pending and is not in the allowlist (an implemented spec
#         went back to a skip, or a new pending spec was added without an
#         owner)
#   FAIL  a spec in the allowlist is no longer pending (implemented: remove its
#         line from ALLOWED, so the list cannot rot into an excuse)
#   FAIL  a spec E01 to E14 does not exist exactly once (deleted or duplicated)
#
# The scan reads the Go source, not test output, so it needs no Docker and
# runs in seconds. A skip inside a subtest (t.Run) that does not start with
# "pending:" is a recorded known gap, not a pending spec, and is not listed.
#
# Usage: scripts/check-pending-e2e.sh [dir]     (dir defaults to test/e2e;
# scripts/test-guards.sh points it at a mutated copy)
set -euo pipefail

cd "$(dirname "$0")/.."
dir=${1:-test/e2e}

# <id> <owning phase>: the specs allowed to be pending, and who builds them.
# E02 and E08 to E14 were implemented on the old four-binary harness and are
# pending again until Wave C3b ports them to the ten-binary harness (their old
# bodies are in test/e2e/legacy/, the skipped stubs in
# test/e2e/scenarios_pending_e2e_test.go). Remove a line when its spec is ported.
ALLOWED="
E08 Wave-C3b
E09 Wave-C3b
E10 Wave-C3b
E11 Wave-C3b
E12 Wave-C3b
E13 Wave-C3b
E14 Wave-C3b
"

files=$(find "$dir" -maxdepth 1 -name '*_test.go' | sort)
[ -n "$files" ] || { echo "FAIL no *_test.go files in $dir"; exit 1; }

# One line per spec function: "<id> <pending|done> <file>".
specs=$(for f in $files; do
  awk -v file="$f" '
    function flush() { if (id != "") print id, (pending ? "pending" : "done"), file }
    /^func TestE2E_E[0-9][0-9]_/ {
      flush()
      id = $2; sub(/^TestE2E_/, "", id); sub(/_.*/, "", id)
      pending = 0
      next
    }
    /t\.Skip\("pending:/ && id != "" { pending = 1 }
    END { flush() }
  ' "$f"
done | sort)

fail=0
for n in 01 02 03 04 05 06 07 08 09 10 11 12 13 14; do
  id="E$n"
  count=$(grep -c "^$id " <<<"$specs" || true)
  if [ "$count" -ne 1 ]; then
    echo "FAIL $id: found $count spec functions (want exactly 1) in $dir"
    fail=1
  fi
done

pending=0
while read -r id state file; do
  [ -n "$id" ] || continue
  owner=$(awk -v id="$id" '$1 == id { print $2 }' <<<"$ALLOWED")
  if [ "$state" = pending ]; then
    pending=$((pending + 1))
    if [ -z "$owner" ]; then
      echo "FAIL $id is still skipped as pending ($file) and is not in the allowlist of scripts/check-pending-e2e.sh: implement it, or add it with the phase that owns it"
      fail=1
    else
      echo "ok   $id pending, owned by $owner ($file)"
    fi
  else
    if [ -n "$owner" ]; then
      echo "FAIL $id is implemented ($file) but still in the allowlist of scripts/check-pending-e2e.sh: remove its line"
      fail=1
    else
      echo "ok   $id implemented ($file)"
    fi
  fi
done <<<"$specs"

[ "$fail" -eq 0 ] && echo "ok   pending e2e specs: $pending, all allowlisted with an owning phase"
exit $fail
