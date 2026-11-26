#!/usr/bin/env bash
# Decides what CI runs for a set of changed files (docs/guides/ci.md).
#
#   scripts/ci-select.sh --base origin/main            changed files = git diff --name-only origin/main...HEAD
#   scripts/ci-select.sh --files list.txt              one path per line ("-" reads stdin)
#   scripts/ci-select.sh --full                        everything (pushes to main)
#   scripts/ci-select.sh ... --format github           key=value lines for $GITHUB_OUTPUT (default: json)
#
# Policy:
#   - a shared file (go.work, Makefile, scripts/, .github/, compose files,
#     .golangci.yml, a Dockerfile that is not one service's, migrations of the
#     platform or cutover sets, any path nothing else claims) selects EVERYTHING;
#   - docs (docs/, *.md, LICENSE, ...) select nothing but the cheap guards;
#   - a file of services/<svc>/, Dockerfile.<svc>-*, migrations/<svc>/ selects
#     that service's module; a file of a libs/ or tools/ module or of test/e2e
#     selects that module;
#   - every module that requires a selected module, directly or transitively
#     (read from each go.mod, never from a list), is selected too;
#   - each selected module runs build, vet, lint, govulncheck and unit +
#     integration tests (the flags are in the output so a future policy can vary them);
#   - E2E: everything when go.work or any shared file, a libs/ module or test/e2e
#     changed; none for docs only; else the scenarios of test/e2e/scenario-map.json
#     that exercise a changed service (only tests the default `make test-e2e`
#     run, TestE2E_*, are selected, so a subset is never wider than a full run).
#
# Needs jq and awk; portable to bash 3.2 (macOS) and the runner's bash.
set -euo pipefail

cd "$(dirname "$0")/.."

PREFIX=github.com/AmeerHussainEmumba/SmartCourse/
E2E_DEFAULT_PREFIX=TestE2E_    # Makefile: E2E_RUN ?= TestE2E_
MAP=${CI_SELECT_MAP:-test/e2e/scenario-map.json}

full=0 files="" base="" format=json
while [ $# -gt 0 ]; do
  case $1 in
    --full) full=1 ;;
    --files) files=$2; shift ;;
    --base) base=$2; shift ;;
    --format) format=$2; shift ;;
    *) echo "usage: $0 [--full] [--base REF | --files FILE|-] [--format json|github]" >&2; exit 2 ;;
  esac
  shift
done

modules=$(awk '/^\t/ && $1 != ")" {sub(/^\.\//, "", $1); print $1} $1 == "use" && $2 != "(" {sub(/^\.\//, "", $2); print $2}' go.work)
[ -n "$modules" ] || { echo "ci-select: no modules in go.work" >&2; exit 1; }
services=$(echo "$modules" | sed -n 's#^services/##p')

# Changed files.
if [ "$full" -eq 0 ]; then
  if [ -n "$base" ]; then
    changed=$(git diff --name-only --no-renames "$base...HEAD")
  elif [ "$files" = "-" ]; then
    changed=$(cat)
  elif [ -n "$files" ]; then
    changed=$(cat "$files")
  else
    echo "ci-select: give --base, --files or --full" >&2; exit 2
  fi
  changed=$(echo "$changed" | sed -e 's#^\./##' -e '/^$/d' | sort -u)
fi

# Module requires: "<module> <required module>" for every sibling module.
edges=$(for m in $modules; do
  awk -v P="$PREFIX" -v M="$m" '
    /=>/ { next }
    { for (i = 1; i < NF; i++) if (index($i, P) == 1 && $(i + 1) ~ /^v[0-9]/) { d = substr($i, length(P) + 1); print M, d } }
  ' "$m/go.mod"
done | sort -u)

in_list() { case "
$2
" in *"
$1
"*) return 0 ;; esac; return 1; }

# classify <file>: prints "docs", "full <why>" or "module <dir>".
classify() {
  local f=$1 name svc m best=""
  case $f in
    .github/*) echo "full .github/ changed ($f)"; return ;;
    go.work|go.work.sum|Makefile|docker-compose*|.golangci.*|.dockerignore|scripts/*) echo "full shared file $f"; return ;;
    docs/*|*.md|LICENSE*|.gitignore|.gitattributes|.editorconfig) echo docs; return ;;
    Dockerfile.*)
      name=${f#Dockerfile.}; svc=${name%%-*}
      if in_list "$svc" "$services"; then echo "module services/$svc"; else echo "full shared Dockerfile $f"; fi
      return ;;
    migrations/*/*)
      svc=${f#migrations/}; svc=${svc%%/*}
      if in_list "$svc" "$services"; then echo "module services/$svc"; else echo "full migrations of every service ($f)"; fi
      return ;;
  esac
  for m in $modules; do
    case $f in "$m"/*) [ ${#m} -gt ${#best} ] && best=$m ;; esac
  done
  if [ -n "$best" ]; then echo "module $best"; else echo "full unrecognised path $f"; fi
}

full_reason=""
[ "$full" -eq 1 ] && full_reason="forced (--full)"
changed_modules="" docs_only=0 migrations=0 nonempty=0
if [ "$full" -eq 0 ]; then
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    nonempty=1
    case $f in migrations/*) case $f in *.md) ;; *) migrations=1 ;; esac ;; esac
    c=$(classify "$f")
    case $c in
      docs) ;;
      "full "*) [ -n "$full_reason" ] || full_reason=${c#full } ;;
      "module "*) m=${c#module }; in_list "$m" "$changed_modules" || changed_modules="$changed_modules${changed_modules:+
}$m" ;;
    esac
  done <<EOF
$changed
EOF
  [ -z "$full_reason" ] && [ -z "$changed_modules" ] && docs_only=1
fi

# Selected modules = changed + transitive dependents.
if [ -n "$full_reason" ]; then
  selected=$modules; migrations=1
else
  selected=$({ for m in $changed_modules; do echo "S $m"; done; sed 's/^/E /' <<<"$edges"; } | awk '
    $1 == "S" { sel[$2] = 1; next }
    $1 == "E" { n++; m[n] = $2; d[n] = $3 }
    END {
      do { grew = 0
        for (i = 1; i <= n; i++) if ((d[i] in sel) && !(m[i] in sel)) { sel[m[i]] = 1; grew = 1 }
      } while (grew)
      for (k in sel) print k
    }' | sort)
fi

module_json=$(for m in $selected; do
  if in_list "$m" "$changed_modules" || [ -n "$full_reason" ]; then why=changed; else why=dependent; fi
  jq -n --arg dir "$m" --arg why "$why" '{dir: $dir, name: ($dir | gsub("/"; "-")), reason: $why, lint: true, govulncheck: true, integration: true}'
done | jq -s -c 'sort_by(.dir)')
[ -n "$module_json" ] || module_json='[]'

# Images: one per Dockerfile.<name>; its module is the last `WORKDIR /src/<dir>`.
images=$(for df in Dockerfile.*; do
  img=${df#Dockerfile.}
  dir=$(awk '$1 == "WORKDIR" && $2 ~ /^\/src\/./ { d = substr($2, 6) } END { print d }' "$df")
  if [ -n "$full_reason" ] || { [ -n "$dir" ] && in_list "$dir" "$selected"; }; then echo "$img"; fi
done | jq -R -s -c 'split("\n") | map(select(. != ""))')

# E2E.
e2e_run=none e2e_reason="docs only: nothing for E2E to exercise"
if [ -n "$full_reason" ]; then
  e2e_run=all; e2e_reason="all scenarios: $full_reason"
elif [ "$docs_only" -eq 0 ]; then
  all_reason=""
  svcs=""
  for m in $changed_modules; do
    case $m in
      services/*) svcs="$svcs${svcs:+ }${m#services/}" ;;
      libs/*|test/e2e) [ -n "$all_reason" ] || all_reason="all scenarios: $m changed" ;;
    esac
  done
  if [ -n "$all_reason" ]; then
    e2e_run=all; e2e_reason=$all_reason
  elif [ -z "$svcs" ]; then
    e2e_reason="no service or shared library changed; E2E does not exercise $(echo $changed_modules)"
  else
    svc_json=$(printf '%s\n' $svcs | jq -R . | jq -s -c .)
    names=$(jq -r --argjson s "$svc_json" --arg p "$E2E_DEFAULT_PREFIX" '
      .tests | to_entries[] | select(.key | startswith($p))
      | select([.value.services[] | select(. as $x | $s | index($x))] | length > 0) | .key' "$MAP" | sort)
    if [ -z "$names" ]; then
      e2e_reason="no mapped scenario exercises $svcs"
    else
      e2e_run="^($(echo "$names" | paste -sd'|' -))\$"
      e2e_reason="$(echo "$names" | wc -l | tr -d ' ') scenarios that exercise: $svcs"
    fi
  fi
fi

has_modules=false; [ "$module_json" != "[]" ] && has_modules=true
has_images=false; [ "$images" != "[]" ] && has_images=true
is_full=false; [ -n "$full_reason" ] && is_full=true
mig=false; [ "$migrations" -eq 1 ] && mig=true

case $format in
  json)
    jq -n --argjson modules "$module_json" --argjson images "$images" --arg run "$e2e_run" --arg why "$e2e_reason" \
      --argjson full "$is_full" --arg fr "$full_reason" --argjson docs "$([ $docs_only -eq 1 ] && echo true || echo false)" \
      --argjson mig "$mig" \
      '{full: $full, full_reason: $fr, docs_only: $docs, modules: $modules, images: $images, migrations: $mig, e2e: {run: $run, reason: $why}}' ;;
  github)
    echo "full=$is_full"
    echo "full_reason=$full_reason"
    echo "modules=$module_json"
    echo "has_modules=$has_modules"
    echo "images=$images"
    echo "has_images=$has_images"
    echo "migrations=$mig"
    echo "e2e_run=$e2e_run"
    echo "e2e_reason=$e2e_reason" ;;
  *) echo "ci-select: unknown format $format" >&2; exit 2 ;;
esac
