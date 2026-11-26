#!/usr/bin/env bash
# Unit tests for scripts/ci-select.sh (what CI runs for a set of changed files)
# and scripts/ci-e2e-gate.sh (whether a skip-e2e label is honoured). No Docker,
# no network: the selector runs on the repo's own go.work, go.mod files and
# test/e2e/scenario-map.json; the gate runs on synthetic event lists.
#
# Usage: scripts/test-ci-select.sh     (make test-ci-select)
set -uo pipefail

cd "$(dirname "$0")/.."
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
pass=0; fail=0
ok()  { echo "  PASS  $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; [ -n "${2:-}" ] && echo "        $2"; fail=$((fail+1)); }

# sel <file>...: the selector's JSON for these changed files.
sel() { printf '%s\n' "$@" | scripts/ci-select.sh --files -; }
# check <label> <jq expression that must print true> <json>
check() {
  local label=$1 expr=$2 json=$3 got
  got=$(jq -r "$expr" <<<"$json" 2>&1)
  if [ "$got" = true ]; then ok "$label"; else bad "$label" "jq: $expr => $got"; fi
}

all_modules=$(awk '/^\t/ && $1 != ")" {sub(/^\.\//, "", $1); print $1}' go.work | wc -l | tr -d ' ')
n_tests=$(jq '[.tests | keys[] | select(startswith("TestE2E_"))] | length' test/e2e/scenario-map.json)

echo "== single service"
j=$(sel services/notification/notification/sender.go)
check "notification change selects only services/notification" '[.modules[].dir] == ["services/notification"]' "$j"
check "  and runs lint, govulncheck and integration for it" '.modules[0] | .lint and .govulncheck and .integration' "$j"
check "  and builds only its image" '.images == ["notification-worker"]' "$j"
check "  and is not a full run" '.full == false and .docs_only == false and .migrations == false' "$j"
check "  and E2E runs a regexp with E07 (SMTP) but not E12 (publish)" '.e2e.run | startswith("^(") and contains("TestE2E_E07_SMTPProviderDown") and (contains("E12_PublishUnderFault") | not)' "$j"
j=$(sel services/publishing/publish/workflow.go)
check "publishing change: E12 yes, E07 no" '.e2e.run | contains("TestE2E_E12_PublishUnderFault") and (contains("E07_") | not)' "$j"
check "  and only publishing is selected" '[.modules[].dir] == ["services/publishing"]' "$j"
j=$(sel services/audit/archive/a.go services/audit/store/b.go)
check "two files of one service select one module" '(.modules | length) == 1 and .modules[0].dir == "services/audit"' "$j"
j=$(sel services/identity/identity/h.go services/audit/archive/a.go)
check "two services select exactly those two" '[.modules[].dir] == ["services/audit","services/identity"]' "$j"
j=$(sel Dockerfile.audit-worker)
check "a service Dockerfile counts as that service" '[.modules[].dir] == ["services/audit"] and .images == ["audit-api","audit-worker"] and .full == false' "$j"
j=$(sel migrations/course/000010_x.up.sql)
check "a service's migration counts as that service and runs the migrations job" '[.modules[].dir] == ["services/course"] and .migrations == true and .full == false' "$j"

echo "== libs change"
j=$(sel libs/relay/relay.go)
check "libs/relay is selected" '[.modules[].dir] | index("libs/relay") != null' "$j"
want=$(grep -l 'SmartCourse/libs/relay v' */*/go.mod test/e2e/go.mod 2>/dev/null | sed 's#/go.mod$##' | sort)
got=$(jq -r '.modules[] | select(.reason == "dependent") | .dir' <<<"$j" | sort)
for m in $want; do
  [ "$m" = libs/relay ] && continue
  if grep -qxF "$m" <<<"$got"; then ok "  dependent $m (its go.mod requires libs/relay) is selected"; else bad "  dependent $m is missing"; fi
done
check "  a module that does not require libs/relay is left out (not a full run)" '[.modules[].dir] | length < '"$all_modules"'' "$j"
check "  E2E runs everything" '.e2e.run == "all"' "$j"
j=$(sel libs/contracts/publishing/c.go)
check "nested module libs/contracts/publishing resolves to itself, not libs/contracts" '[.modules[].dir] | index("libs/contracts/publishing") != null' "$j"
j=$(sel libs/events/cmd/register-schemas/main.go)
check "libs/events change rebuilds the schema-init image" '.images | index("schema-init") != null' "$j"
j=$(sel libs/errors/e.go)
check "a foundation lib selects (nearly) every module and every service image" '(.modules | length) > 10 and (.images | length) >= 10' "$j"

echo "== docs only"
j=$(sel docs/guides/ci.md README.md services/course/README.md test/README.md)
check "docs select no module, image or migration" '.modules == [] and .images == [] and .migrations == false and .docs_only == true and .full == false' "$j"
check "  and no E2E" '.e2e.run == "none"' "$j"
j=$(sel)
check "no changed files behaves like docs only" '.modules == [] and .e2e.run == "none"' "$j"

echo "== shared files select everything"
for f in go.work Makefile scripts/check-link-graph.sh .github/workflows/ci.yml docker-compose.yml Dockerfile.schema-init migrations/platform/000001_x.up.sql migrations/cutover/000001_x.up.sql .golangci.yml some/unknown/file.txt; do
  j=$(sel "$f")
  check "$f -> full run" '.full == true and (.modules | length) == '"$all_modules"' and .e2e.run == "all" and .migrations == true' "$j"
done
j=$(sel docs/a.md go.work)
check "docs plus go.work is still full" '.full == true' "$j"
j=$(scripts/ci-select.sh --full)
check "--full is a full run with every image" '.full == true and (.modules | length) == '"$all_modules"' and (.images | length) == 11' "$j"

echo "== tools and e2e"
j=$(sel tools/seed/main.go)
check "tools/seed selects only itself, no E2E" '[.modules[].dir] == ["tools/seed"] and .e2e.run == "none"' "$j"
j=$(sel test/e2e/harness/stack.go)
check "test/e2e/harness change selects test/e2e and all scenarios" '([.modules[].dir] | index("test/e2e") != null) and .e2e.run == "all"' "$j"
j=$(sel test/e2e/scenario-map.json)
check "the scenario map itself is an E2E change: all scenarios" '.e2e.run == "all"' "$j"
j=$(sel services/audit/a.go docs/x.md tools/seed/main.go)
check "service + docs + tools/seed: that service's scenarios only" '.e2e.run | startswith("^(")' "$j"

echo "== E2E regexp"
j=$(sel services/audit/a.go)
re=$(jq -r .e2e.run <<<"$j")
n=$(tr '|' '\n' <<<"$re" | wc -l | tr -d ' ')
[ "$n" -le "$n_tests" ] && ok "a subset is never wider than the $n_tests default scenarios" || bad "regexp has $n names"
case $re in *TestAdversarial_*) bad "regexp selects a test the default 'make test-e2e' does not run" ;; *) ok "only TestE2E_ tests are selected (same set as a full run)" ;; esac
mk=$(make -n test-e2e E2E_RUN="$re" 2>&1)
case $mk in *"-run '$re' ./..."*) ok "the regexp reaches go test unchanged through make (anchored, no \$ for make to eat)" ;; *) bad "make rewrote the regexp" "$mk" ;; esac
echo '{"tests":{"TestE2E_X":{"services":["identity"]}}}' >"$tmp/m.json"
j=$(CI_SELECT_MAP=$tmp/m.json sel services/audit/a.go)
check "a service no scenario exercises gets E2E none with a reason" '.e2e.run == "none" and (.e2e.reason | contains("audit"))' "$j"

echo "== github output format"
out=$(printf 'services/audit/a.go\n' | scripts/ci-select.sh --files - --format github)
if grep -q '^modules=\[{' <<<"$out" && grep -q '^has_modules=true' <<<"$out" && grep -q '^e2e_run=\^(' <<<"$out" && [ "$(wc -l <<<"$out" | tr -d ' ')" -eq 9 ]; then ok "key=value lines, one per line, compact JSON"; else bad "format github" "$out"; fi

echo "== skip-e2e gate"
ev() { # ev <file> <json>
  echo "$2" >"$tmp/$1"; echo "$tmp/$1"
}
owners="$tmp/owners"; printf '# comment\nAlice\n@bob\n' >"$owners"
co="$tmp/CODEOWNERS"; printf '# c\n*  @carol\n/x/  @carol @dave @org/team\n' >"$co"
gate() { scripts/ci-e2e-gate.sh --events "$1" --owners "$owners" --codeowners "$co"; }
L='"label":{"name":"skip-e2e"}'
out=$(gate "$(ev e1 '[{"event":"labeled",'"$L"',"actor":{"login":"alice"}}]')")
grep -q '^skip=true' <<<"$out" && grep -q '^by=alice' <<<"$out" && ok "label by a login in the owners file (case-insensitive): honoured, by recorded" || bad "owner label" "$out"
out=$(gate "$(ev e2 '[{"event":"labeled",'"$L"',"actor":{"login":"carol"}}]')")
grep -q '^skip=true' <<<"$out" && ok "label by a CODEOWNERS user: honoured" || bad "codeowner label" "$out"
out=$(gate "$(ev e3 '[{"event":"labeled",'"$L"',"actor":{"login":"mallory"}}]')")
grep -q '^skip=false' <<<"$out" && grep -q 'not a code owner' <<<"$out" && ok "label by a non-owner: ignored" || bad "non-owner label" "$out"
out=$(gate "$(ev e4 '[{"event":"labeled",'"$L"',"actor":{"login":"org"}}]')")
grep -q '^skip=false' <<<"$out" && ok "a CODEOWNERS team entry is not a user" || bad "team" "$out"
out=$(gate "$(ev e5 '[{"event":"labeled",'"$L"',"actor":{"login":"alice"}},{"event":"unlabeled",'"$L"',"actor":{"login":"alice"}}]')")
grep -q '^skip=false' <<<"$out" && ok "label removed again: ignored" || bad "unlabeled" "$out"
out=$(gate "$(ev e6 '[{"event":"labeled",'"$L"',"actor":{"login":"alice"}},{"event":"unlabeled",'"$L"',"actor":{"login":"alice"}},{"event":"labeled",'"$L"',"actor":{"login":"mallory"}}]')")
grep -q '^skip=false' <<<"$out" && ok "owner removed it, a non-owner re-applied it: ignored" || bad "relabel" "$out"
out=$(gate "$(ev e7 '[{"event":"labeled","label":{"name":"other"},"actor":{"login":"alice"}}]')")
grep -q '^skip=false' <<<"$out" && ok "another label does not count" || bad "other label" "$out"
out=$(gate "$(ev e8 '[]')")
grep -q '^skip=false' <<<"$out" && ok "no events: E2E runs" || bad "no events" "$out"
out=$(gate "$(ev e9 '[{"event":"commented"}][{"event":"labeled",'"$L"',"actor":{"login":"bob"}}]')")
grep -q '^skip=true' <<<"$out" && ok "paginated output (concatenated arrays) is read" || bad "pages" "$out"
out=$(scripts/ci-e2e-gate.sh --events "$(ev e10 '[{"event":"labeled",'"$L"',"actor":{"login":"alice"}}]')" --owners "$tmp/missing")
grep -q '^skip=false' <<<"$out" && ok "missing owner file: nobody is an owner, E2E runs" || bad "missing owners" "$out"

echo
echo "summary: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
