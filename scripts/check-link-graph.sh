#!/usr/bin/env bash
# Fails if a service binary links a package it shouldn't — the regression test
# for the Week 2 review's point that every binary used to build every domain
# module (and the Temporal SDK) through one shared composition root. Each
# binary is built from inside its own module with GOWORK=off, exactly as its
# Dockerfile does. See docs/plans/module-split.md.
#
# Each rule is: <module dir> <binary> <forbidden package prefixes, comma-
# separated, relative to the repo module path> <forbid the Temporal SDK: yes|no>
# <package budget>
# Add a line here when a new binary is created. When a binary legitimately
# starts using another module, change its line in the same PR.
#
# Telemetry (ADR-0032, Phase 7): every binary links libs/telemetry, which pulls
# in the OpenTelemetry SDK, the OTLP/HTTP exporter (and with it gRPC and
# protobuf), the Prometheus client and the GORM/go-redis tracing hooks. That is
# allowed for all four, deliberately, and it is what moved identity-api from
# 388 to 553 packages (analytics-api 453 -> 618, course-api 655 -> 725, worker
# 747 -> 813, counts from `go list -deps`). The forbidden lists below still
# keep Temporal, Asynq, Kafka and other services out of identity-api and
# analytics-api: telemetry must not become the excuse for a larger graph.
# The package budget is the count at the time of writing plus about 5%; raise
# it in the same PR as the dependency that needs it, never silently.
# LINK_BUDGET_SLACK (default 0) is added to every budget; scripts/test-guards.sh
# sets it negative to prove the budget check fails.
set -euo pipefail

cd "$(dirname "$0")/.."
mod=github.com/AmeerHussainEmumba/SmartCourse

# analytics-api MAY link libs/mongostore (ADR-0031 deviation note): the Failed
# Events metric is served from failed_events. identity-api stays forbidden, and
# so does every other forbidden prefix below; course-api's rule is unchanged.
rules=(
  "services/identity  identity-api  services/course,services/analytics,services/worker,libs/temporal,libs/mongostore,libs/asynqx,libs/kafka  yes 580"
  "services/analytics analytics-api services/identity,services/course,services/worker,libs/temporal,libs/asynqx,libs/kafka   yes 650"
  "services/course    course-api    services/identity,services/analytics,services/worker                no  760"
  "services/worker    worker        services/identity                                                   no  855"
  "services/publishing publishing-worker services/identity,services/course,services/analytics,services/worker,libs/asynqx,libs/kafka,libs/eventbus,libs/events,libs/mongostore,libs/relay,libs/idempotency,libs/ratelimit no 680 github.com/(segmentio|twmb|confluentinc|IBM/sarama|hibiken/asynq|golang-migrate)/|go.mongodb.org/mongo-driver/v2/mongo$"
  "services/audit     audit-api     services/identity,services/course,services/analytics,services/worker,libs/temporal,libs/asynqx   yes 680"
  "services/audit     audit-worker  services/identity,services/course,services/analytics,services/worker,libs/temporal,libs/asynqx   yes 680"
)
# publishing-worker (Wave B5): no Kafka, Mongo, Asynq or migration code and no other
# service; Temporal is its whole job. The last column is an extra regex for
# third-party packages. The Postgres driver (pgx, gorm) is NOT forbidden: it is
# linked through libs/runtime -> libs/infra even under runtime.WithoutDependencies,
# so the binary links but never opens a connection. Splitting infra out of runtime
# would let that move into the forbidden list. 648 packages when measured (budget
# is that plus about 5%).
slack=${LINK_BUDGET_SLACK:-0}

fail=0
for rule in "${rules[@]}"; do
  read -r dir bin forbidden no_temporal budget extra <<<"$rule"
  deps=$(cd "$dir" && GOWORK=off go list -deps "./cmd/$bin")

  pattern="^${mod}/(${forbidden//,/|})(/|\$)"
  if [ "$no_temporal" = "yes" ]; then
    pattern="${pattern}|^go\\.temporal\\.io/"
  fi
  if [ -n "${extra:-}" ]; then
    pattern="${pattern}|^(${extra})"
  fi

  if hits=$(grep -E "$pattern" <<<"$deps"); then
    echo "FAIL $bin links packages it must not:"
    sed 's/^/  /' <<<"$hits" | head -10
    fail=1
  else
    count=$(wc -l <<<"$deps" | tr -d ' ')
    limit=$((budget + slack))
    if [ "$count" -gt "$limit" ]; then
      echo "FAIL $bin links $count packages, which exceeds its package budget of $limit (raise it deliberately in scripts/check-link-graph.sh)"
      fail=1
    else
      echo "ok   $bin ($count packages, budget $limit)"
    fi
  fi
done

exit $fail
