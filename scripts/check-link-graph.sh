#!/usr/bin/env bash
# Fails if a service binary links a package it shouldn't — the regression test
# for the Week 2 review's point that every binary used to build every domain
# module (and the Temporal SDK) through one shared composition root. Each
# binary is built from inside its own module with GOWORK=off, exactly as its
# Dockerfile does. See docs/plans/module-split.md.
#
# Each rule is: <module dir> <binary> <forbidden package prefixes, comma-
# separated, relative to the repo module path> <forbid the Temporal SDK: yes|no>
# Add a line here when a new binary is created. When a binary legitimately
# starts using another module, change its line in the same PR.
set -euo pipefail

cd "$(dirname "$0")/.."
mod=github.com/AmeerHussainEmumba/SmartCourse

rules=(
  "services/identity  identity-api  services/course,services/analytics,services/worker,libs/temporal,libs/mongostore,libs/asynqx,libs/kafka  yes"
  "services/analytics analytics-api services/identity,services/course,services/worker,libs/temporal,libs/mongostore,libs/asynqx,libs/kafka   yes"
  "services/course    course-api    services/identity,services/analytics,services/worker                no"
  "services/worker    worker        services/identity                                                   no"
)

fail=0
for rule in "${rules[@]}"; do
  read -r dir bin forbidden no_temporal <<<"$rule"
  deps=$(cd "$dir" && GOWORK=off go list -deps "./cmd/$bin")

  pattern="^${mod}/(${forbidden//,/|})(/|\$)"
  if [ "$no_temporal" = "yes" ]; then
    pattern="${pattern}|^go\\.temporal\\.io/"
  fi

  if hits=$(grep -E "$pattern" <<<"$deps"); then
    echo "FAIL $bin links packages it must not:"
    sed 's/^/  /' <<<"$hits" | head -10
    fail=1
  else
    echo "ok   $bin"
  fi
done

exit $fail
