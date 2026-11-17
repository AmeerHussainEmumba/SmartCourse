#!/usr/bin/env bash
# Fails if a cmd/* binary links a package it shouldn't — the regression test
# for the Week 2 review's point that every binary used to build every domain
# module (and the Temporal SDK) through one shared composition root. See
# docs/plans/module-split.md.
#
# Each rule is: <binary> <forbidden internal modules, comma-separated> <forbid Temporal SDK: yes|no>
# Add a line here when a new binary is created. When a binary legitimately
# starts using another module, change its line in the same PR.
set -euo pipefail

cd "$(dirname "$0")/.."
mod=$(go list -m)

rules=(
  "identity-api  course,enrollment,progress,analytics,workflows  yes"
  "analytics-api identity,course,enrollment,progress,workflows   yes"
  "course-api    identity,analytics,workflows                    no"
  "worker        identity                                        no"
)

fail=0
for rule in "${rules[@]}"; do
  read -r bin forbidden no_temporal <<<"$rule"
  deps=$(go list -deps "./cmd/$bin")

  pattern="^${mod}/internal/(${forbidden//,/|})\$"
  if [ "$no_temporal" = "yes" ]; then
    pattern="${pattern}|^go\\.temporal\\.io/"
  fi

  if hits=$(grep -E "$pattern" <<<"$deps"); then
    echo "FAIL cmd/$bin links packages it must not:"
    sed 's/^/  /' <<<"$hits" | head -10
    fail=1
  else
    echo "ok   cmd/$bin"
  fi
done

exit $fail
