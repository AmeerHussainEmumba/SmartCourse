#!/usr/bin/env bash
# Fails when test/e2e/scenario-map.json and the E2E tests disagree:
#
#   FAIL  a Test function in a file carrying the `e2e` build tag has no entry in
#         the map (CI would not know which services it exercises, so a
#         service-only PR could skip it)
#   FAIL  the map names a test function that does not exist (renamed or deleted)
#   FAIL  an entry has no services, or names a service that is not one of the
#         map's "services" list
#
# Tests in files without the `e2e` tag (the `integration` scenarios, the compose
# and guard tests) run with the module's ordinary tests and are not mapped.
#
# Usage: scripts/check-e2e-map.sh [dir]     (dir defaults to test/e2e;
# scripts/test-guards.sh points it at a mutated copy)
set -euo pipefail

cd "$(dirname "$0")/.."
dir=${1:-test/e2e}
map="$dir/scenario-map.json"
[ -f "$map" ] || { echo "FAIL $map does not exist"; exit 1; }
command -v jq >/dev/null || { echo "FAIL jq is required"; exit 1; }

# Test functions of the files whose //go:build line selects the e2e tag.
actual=$(for f in "$dir"/*_test.go; do
  awk '
    /^\/\/go:build / && !seen { seen = 1; line = $0; gsub(/!e2e/, "", line); n = split(line, w, /[ ()&|]+/); for (i = 1; i <= n; i++) if (w[i] == "e2e") tagged = 1 }
    tagged && /^func Test[A-Za-z0-9_]*\(t \*testing\.T\)/ { name = $2; sub(/\(.*/, "", name); if (name != "TestMain") print name }
  ' "$f"
done | sort -u)
mapped=$(jq -r '.tests | keys[]' "$map" | sort -u)

fail=0
missing=$(comm -23 <(echo "$actual") <(echo "$mapped") | sed '/^$/d')
extra=$(comm -13 <(echo "$actual") <(echo "$mapped") | sed '/^$/d')
for t in $missing; do echo "FAIL $t is an e2e test with no entry in $map"; fail=1; done
for t in $extra; do echo "FAIL $map names $t, which is not an e2e-tagged Test function"; fail=1; done

bad=$(jq -r '. as $m | .tests | to_entries[] | select((.value.services | length) == 0 or ((.value.services - $m.services) | length) > 0) | .key' "$map")
for t in $bad; do echo "FAIL $t: empty services, or a service not in the map's services list"; fail=1; done

[ "$fail" -eq 0 ] && echo "ok   e2e scenario map: $(echo "$actual" | grep -c .) tests, all mapped"
exit $fail
