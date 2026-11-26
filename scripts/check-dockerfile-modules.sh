#!/usr/bin/env bash
# Verifies that each Dockerfile.<binary> copies exactly the modules the
# binary's Go module needs, and nothing else.
#
# Each image builds one service module with GOWORK=off, so go.work cannot
# paper over a missing sibling module. For service module M (taken from the
# Dockerfile's last `WORKDIR /src/<dir>`):
#
#   needed  = M itself  +  every module in M/go.mod's `replace` directives
#             (go.mod must replace the whole transitive closure; Go refuses
#             to resolve a require without a replacement on disk)
#   closure = M itself  +  every SmartCourse module reachable from M's
#             `require` lines by following each module's own go.mod
#
# Findings:
#   MISSING  COPY'd by neither the Dockerfile nor ... i.e. in `needed` or
#            `closure` but not COPY'd  -> the image build breaks
#   EXTRA    COPY'd but not in `needed` -> needless coupling / bigger context
#   REPLACE-GAP  in `closure` but without a `replace` in M/go.mod -> build
#            break outside go.work
#   REPLACE-EXTRA in M/go.mod's replaces but not in `closure` -> stale
#            coupling that still forces a COPY
#
# Usage: scripts/check-dockerfile-modules.sh [--warn-extra]
#   --warn-extra  report EXTRA/REPLACE-EXTRA findings but do not fail on them
# Exit 1 on any failing finding. Bash 3.2 compatible (macOS).
set -euo pipefail

cd "$(dirname "$0")/.."
mod=github.com/AmeerHussainEmumba/SmartCourse
warn_extra=0
[ "${1:-}" = "--warn-extra" ] && warn_extra=1

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# replaces M -> sorted repo-relative module dirs from `=> ../x` lines
replaces() {
  local dir=$1
  awk '/=>/ {for (i=1;i<=NF;i++) if ($i=="=>") print $(i+1)}' "$dir/go.mod" | while read -r target; do
    if [ -d "$dir/$target" ]; then
      abs=$(cd "$dir/$target" && pwd -P)
      echo "${abs#"$(pwd -P)"/}"
    else
      echo "?missing:$target"
    fi
  done | sort -u
}

# requires M -> sorted repo-relative dirs of SmartCourse modules named in M/go.mod require lines
requires() {
  awk -v m="$mod/" 'index($1, m)==1 && $1 !~ /^replace/ && $2 ~ /^v/ {print substr($1, length(m)+1)}' "$1/go.mod" | sort -u
}

# closure M -> M plus everything reachable through require lines
closure() {
  local start=$1 seen="$tmp/seen.$$" queue="$tmp/queue.$$"
  : >"$seen"; echo "$start" >"$queue"
  while [ -s "$queue" ]; do
    cur=$(head -n1 "$queue"); sed -i.bak '1d' "$queue" && rm -f "$queue.bak"
    grep -qx "$cur" "$seen" && continue
    echo "$cur" >>"$seen"
    [ -f "$cur/go.mod" ] || continue
    requires "$cur" >>"$queue"
  done
  sort -u "$seen"
}

fail=0
for df in Dockerfile.*; do
  bin=${df#Dockerfile.}
  module=$(awk '$1=="WORKDIR" && $2 ~ /^\/src\/(services|tools|libs)\// {w=$2} END {sub("^/src/","",w); print w}' "$df")
  if [ -z "$module" ] || [ ! -f "$module/go.mod" ]; then
    echo "FAIL $df: cannot determine the built module (expected a final 'WORKDIR /src/services/<m>')"; fail=1; continue
  fi

  copied=$(awk '$1=="COPY" && $2 ~ /^(libs|services|tools)\// {print $2}' "$df" | sed 's#/$##' | sort -u)
  replaced=$( (replaces "$module"; echo "$module") | sort -u)
  closed=$(closure "$module")

  report() { # label, lines, failing?
    if [ -n "$2" ]; then
      if [ "$3" = "1" ]; then echo "FAIL $df ($module): $1:"; fail=1; else echo "WARN $df ($module): $1:"; fi
      sed 's/^/       /' <<<"$2"
      return 1
    fi
    return 0
  }

  bad=0
  missing=$(comm -13 <(echo "$copied") <(sort -u <(echo "$replaced"; echo "$closed")))
  report "MISSING from the Dockerfile (image build would break)" "$missing" 1 || bad=1
  extra=$(comm -23 <(echo "$copied") <(echo "$replaced"))
  report "EXTRA COPY lines not in the module's replace set (needless coupling)" "$extra" $((1 - warn_extra)) || bad=1
  gap=$(comm -13 <(echo "$replaced") <(echo "$closed"))
  report "REPLACE-GAP: required (transitively) but no replace directive in $module/go.mod" "$gap" 1 || bad=1
  rextra=$(comm -23 <(echo "$replaced") <(echo "$closed"))
  report "REPLACE-EXTRA: replaced in $module/go.mod but not reachable from its requires" "$rextra" $((1 - warn_extra)) || bad=1
  [ "$bad" -eq 0 ] && echo "ok   $df -> $module (COPY set == replace set == require closure)"
done

exit $fail
