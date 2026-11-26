#!/usr/bin/env bash
# Decides whether a skip-e2e label on a PR is honoured (docs/guides/ci.md).
#
#   scripts/ci-e2e-gate.sh --events events.json --owners .github/e2e-skip-owners --codeowners .github/CODEOWNERS
#
# events.json is the output of `gh api --paginate repos/<repo>/issues/<n>/events`
# (one array, or several concatenated). The label counts only when its LAST
# `labeled` event is not followed by an `unlabeled` one and the person who
# applied that last event is a code owner: a login listed in the --owners file
# or a user (not a team) in the --codeowners file. Anyone else's label is
# ignored and E2E runs.
#
# The owner files must come from the PR's BASE commit, never its head (the
# workflow fetches them from the base sha), or a PR could make itself an owner.
#
# Prints key=value lines: skip=true|false, by=<login or empty>, reason=<text>.
set -euo pipefail

events="" label=skip-e2e owners=() codeowners=()
while [ $# -gt 0 ]; do
  case $1 in
    --events) events=$2; shift ;;
    --label) label=$2; shift ;;
    --owners) owners+=("$2"); shift ;;
    --codeowners) codeowners+=("$2"); shift ;;
    *) echo "usage: $0 --events FILE [--label NAME] [--owners FILE]... [--codeowners FILE]..." >&2; exit 2 ;;
  esac
  shift
done
[ -n "$events" ] || { echo "usage: $0 --events FILE ..." >&2; exit 2; }

# Last labeled/unlabeled event for the label: its kind and actor.
last=$(jq -s -r --arg l "$label" '
  add // []
  | map(select((.event == "labeled" or .event == "unlabeled") and .label.name == $l))
  | last // empty
  | "\(.event) \(.actor.login // "")"' "$events")

if [ -z "$last" ]; then
  echo "skip=false"; echo "by="; echo "reason=label $label is not on the PR"; exit 0
fi
kind=${last%% *}; actor=${last#* }
if [ "$kind" != labeled ]; then
  echo "skip=false"; echo "by="; echo "reason=label $label was removed"; exit 0
fi

# Owner logins, lower-cased: plain lists (comments stripped) and CODEOWNERS user entries.
logins=$(
  {
    for f in ${owners[@]+"${owners[@]}"}; do
      [ -f "$f" ] && sed -e 's/#.*//' "$f" | tr -s ' \t' '\n\n' | sed -e 's/^@//' -e '/^$/d'
    done
    for f in ${codeowners[@]+"${codeowners[@]}"}; do
      [ -f "$f" ] && sed -e 's/#.*//' "$f" | awk '{ for (i = 2; i <= NF; i++) if ($i ~ /^@[^\/]+$/) print substr($i, 2) }'
    done
  } | tr 'A-Z' 'a-z' | sort -u
)

if [ -n "$actor" ] && grep -qxF "$(echo "$actor" | tr 'A-Z' 'a-z')" <<<"$logins"; then
  echo "skip=true"; echo "by=$actor"; echo "reason=label $label applied by code owner $actor"
else
  echo "skip=false"; echo "by=$actor"; echo "reason=label $label applied by $actor, who is not a code owner: ignored, E2E runs"
fi
