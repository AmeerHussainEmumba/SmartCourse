SHELL := /bin/bash
MIGRATE := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Load .env into every recipe's environment, if it exists — plain Go has no
# built-in dotenv support (that's a Node/Python convention, not a Go or Make
# one), so without this, `go run` here would see none of it. `include`
# parses KEY=value lines as Make variables; `export` re-exports every
# variable Make knows (not just ones from this file) to child processes,
# which is what makes `go run ./services/identity/cmd/identity-api` (or the other services/*/cmd/*)
# actually see them.
ifneq (,$(wildcard .env))
    include .env
    export
endif

DSN ?= $(POSTGRES_DSN)

# Schema-per-service (ADR-0024): four independent migration sets, one per
# Postgres schema, each tracked by its own x-migrations-table so they don't
# collide on a single shared schema_migrations row. Order matters only for
# `up`/`migrate-down-all` — platform first (citext/pg_trgm live in `public`,
# needed before identity/course's tables that use them), `cutover` last (it
# moves the old platform tables' rows into the per-service tables and drops
# `platform`, so every service set must exist first) — not for
# individual-service migrate-down, where cross-schema ordering doesn't apply
# (ADR-0024: no cross-schema FKs).
MIGRATION_SERVICES := platform identity course analytics notification cutover
MIGRATION_SERVICES_REVERSED := cutover notification analytics course identity platform

.PHONY: dev-core dev-workflow dev-events dev-observability dev-full dev-down stack-up register-schemas \
        migrate-up migrate-down migrate-down-all migrate-create db-roles \
        build run \
        lint check-link-graph check-module-graph check-dockerfile-modules check-module-hygiene check-dev-defaults check-http-hardening check-pending-e2e test-guards verify-deliverables test test-race test-integration test-e2e seed fmt

## --- Local environment (ADR-0018, docs/guides/local-stack.md) -----------
#
# Profiles are cumulative:
#   dev-core           Postgres, Redis, MinIO, Mailpit, Kafka, Schema Registry,
#                      MongoDB, Swagger UI and nine of the ten binaries (every
#                      service except publishing-worker, so course PUBLISHING
#                      does not work on `core`).
#   dev-workflow       core + Temporal, Temporal UI and publishing-worker.
#   dev-observability  workflow + Prometheus, Grafana, Jaeger.
#   dev-full           everything.
# `up -d` builds only images that do not exist yet; after a code change run
# `make dev-core UP_FLAGS=--build` (or `docker compose --profile core build`).
# Migrations are an explicit step (RFC 5.8): `make migrate-up`, or `make stack-up`
# for the whole first-run sequence.
UP_FLAGS ?=

dev-core:
	docker compose --profile core up -d $(UP_FLAGS)

dev-workflow:
	docker compose --profile workflow up -d $(UP_FLAGS)

# Deprecated alias, kept so old muscle memory and docs still work: Kafka, the
# Schema Registry and MongoDB moved into `core` when the services were split
# (docs/plans/microservices-split.md section 6), so there is no separate
# `events` profile any more.
dev-events: dev-core
	@echo "dev-events is an alias of dev-core: Kafka, Schema Registry and MongoDB are in the core profile now."

dev-observability:
	docker compose --profile observability up -d $(UP_FLAGS)

dev-full:
	docker compose --profile full up -d $(UP_FLAGS)

# Stops every profile's containers; volumes (data) are kept. `docker compose
# down -v` would delete them.
dev-down:
	docker compose --profile full down

# Registers every event subject with the Schema Registry (BACKWARD
# compatibility, idempotent): the host-side twin of compose's `schema-init`.
# Needs the registry up (`make dev-core`); SCHEMA_REGISTRY_URL from .env,
# default http://localhost:8081.
register-schemas:
	cd libs/events && go run ./cmd/register-schemas -registry "$(or $(SCHEMA_REGISTRY_URL),http://localhost:8081)"

# Optional first-run convenience on a FRESH stack: starts the infrastructure,
# applies the migrations and the per-service roles, registers the schemas, starts
# every service of STACK_PROFILE (default workflow: all ten binaries), then
# creates the first admin when IDENTITY_ADMIN_EMAIL and IDENTITY_ADMIN_PASSWORD
# are set (env or .env). It runs against the compose project's published ports
# with the dev DSN unless .env overrides POSTGRES_DSN. Safe to repeat: migrations,
# roles, schemas and the admin are all idempotent.
#   IDENTITY_ADMIN_EMAIL=admin@smartcourse.local IDENTITY_ADMIN_PASSWORD='...' make stack-up
STACK_PROFILE ?= workflow
STACK_DSN ?= $(or $(POSTGRES_DSN),postgres://smartcourse:smartcourse@localhost:5433/smartcourse?sslmode=disable)

stack-up:
	docker compose --profile $(STACK_PROFILE) up -d --wait postgres schema-registry
	@$(MAKE) --no-print-directory migrate-up DSN='$(STACK_DSN)' \
		SVC_IDENTITY_PASSWORD='$(or $(SVC_IDENTITY_PASSWORD),svc_identity_dev)' \
		SVC_COURSE_PASSWORD='$(or $(SVC_COURSE_PASSWORD),svc_course_dev)' \
		SVC_ANALYTICS_PASSWORD='$(or $(SVC_ANALYTICS_PASSWORD),svc_analytics_dev)' \
		SVC_NOTIFICATION_PASSWORD='$(or $(SVC_NOTIFICATION_PASSWORD),svc_notification_dev)'
	@$(MAKE) --no-print-directory register-schemas
	docker compose --profile $(STACK_PROFILE) up -d --wait $(UP_FLAGS)
	@if [ -n "$(IDENTITY_ADMIN_EMAIL)" ] && [ -n "$(IDENTITY_ADMIN_PASSWORD)" ]; then \
		docker compose --profile $(STACK_PROFILE) run --rm --no-deps --entrypoint identity-admin \
			-e IDENTITY_ADMIN_EMAIL -e IDENTITY_ADMIN_PASSWORD -e IDENTITY_ADMIN_NAME identity-api; \
	else \
		echo "stack-up: IDENTITY_ADMIN_EMAIL / IDENTITY_ADMIN_PASSWORD not set: no admin created (see env.example, First admin)"; \
	fi

## --- Migrations (migrations/README.md) -----------------------------------

migrate-up:
	@for svc in $(MIGRATION_SERVICES); do \
		echo "--- $$svc ---"; \
		$(MIGRATE) -path migrations/$$svc -database "$(DSN)&x-migrations-table=$${svc}_schema_migrations" up || exit 1; \
	done
	@$(MAKE) --no-print-directory db-roles

# Creates/re-converges the per-service Postgres roles and their grants
# (scripts/db-roles.sh; passwords come from SVC_*_PASSWORD in .env, see
# env.example). Runs as the migration owner ($(DSN)); safe to repeat.
db-roles:
	@DB_OWNER_DSN="$(DSN)" ./scripts/db-roles.sh

# Rolls back one migration for a single service: `make migrate-down service=course`.
migrate-down:
	@if [ -z "$(service)" ]; then echo "usage: make migrate-down service=<$(MIGRATION_SERVICES)>"; exit 1; fi
	$(MIGRATE) -path migrations/$(service) -database "$(DSN)&x-migrations-table=$(service)_schema_migrations" down 1

# Full teardown of every service's schema, reverse of migrate-up's order —
# a local dev reset, not something CI or production ever calls.
migrate-down-all:
	@for svc in $(MIGRATION_SERVICES_REVERSED); do \
		echo "--- $$svc ---"; \
		$(MIGRATE) -path migrations/$$svc -database "$(DSN)&x-migrations-table=$${svc}_schema_migrations" down -all || exit 1; \
	done

# `make migrate-create service=course name=add_something`
migrate-create:
	@if [ -z "$(service)" ]; then echo "usage: make migrate-create service=<$(MIGRATION_SERVICES)> name=<name>"; exit 1; fi
	$(MIGRATE) create -ext sql -dir migrations/$(service) -seq $(name)

## --- Running the binaries (ADR-0022 + ADR-0023: eleven binaries, six services) -----------
#
# Two ways to run the app:
#   - Natively (fastest iteration): `make dev-core` for infra only, then
#     these targets in separate terminals. Each run-*-api target defaults
#     HTTP_PORT to a distinct port (matching docker-compose.yml's host
#     port mapping for the same service) so all three can run at once
#     without fighting over 8080 — override with `HTTP_PORT=... make
#     run-course-api` if you need something else.
#   - Fully containerized: `make dev-core` on its own now also builds and
#     starts the service containers (every binary in docs/plans/microservices-split.md),
#     since they're in the `core` profile too (docker-compose.yml) —
#     migrations still need `make migrate-up` as a separate, explicit
#     step either way (RFC §5.8 — no auto-migration on container start).

# run-<binary> and build-<binary> are generic pattern rules at the end of this file.

## --- Quality gates (CI runs the same targets — see .github/workflows/ci.yml)

fmt:
	gofmt -l .

lint:
	scripts/for-each-module.sh golangci-lint run ./...

# Fails if a service binary links a module or the Temporal SDK it shouldn't
# (docs/plans/module-split.md).
check-link-graph:
	scripts/check-link-graph.sh

# Fails if a Go module requires a sibling module it may not (a service importing
# another service, a library importing a service) — scripts/check-module-graph.sh.
check-module-graph:
	scripts/check-module-graph.sh

# Each Dockerfile's COPY list must equal its module's replace and require
# closure; a mismatch breaks the image build or couples it to extra modules.
check-dockerfile-modules:
	scripts/check-dockerfile-modules.sh

# go.work vs go.mod files, replace targets exist, go mod tidy is a no-op, and
# no third-party version skew across modules.
check-module-hygiene:
	scripts/check-module-hygiene.sh

# Static policy scans: placeholder signing keys and the seed guard, containers
# that run as root, published compose ports; server timeouts, body cap and
# trusted-proxy policy.
check-dev-defaults:
	scripts/check-dev-defaults.sh

check-http-hardening:
	scripts/check-http-hardening.sh

# Fails when an end-to-end spec (E1 to E14, test/e2e) is still a skipped
# placeholder past the phase that owns it: scripts/check-pending-e2e.sh.
check-pending-e2e:
	scripts/check-pending-e2e.sh

# Mutation test of the guards themselves: injects violations into a copy of
# the tree and checks the right guard fails (slow; copies the repo).
test-guards:
	scripts/test-guards.sh

# Live checks against a running stack (docs/plans/week-3-acceptance-matrix.md);
# rows for features not built yet print PENDING and do not fail the run.
verify-deliverables:
	scripts/verify-deliverables.sh

test:
	scripts/for-each-module.sh go test ./...

test-race:
	scripts/for-each-module.sh go test -race ./...

test-integration:
	scripts/for-each-module.sh go test -race -tags=integration ./...

# The container-tier end-to-end scenarios (test/e2e/harness, build tag `e2e`):
# a dozen containers and the real service binaries as subprocesses, so it needs
# Docker and several minutes and is not part of test-integration. One scenario:
# `make test-e2e E2E_RUN=TestE2E_E01`.
E2E_RUN ?= TestE2E_
test-e2e:
	cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -timeout 40m -v -run '$(E2E_RUN)' ./...

# `make seed` has two steps (tools/seed/README.md). First the first admin is
# created inside the identity-api image with its identity-admin CLI
# (/usr/local/bin/identity-admin, services/identity/README.md); it is the one
# account the HTTP API cannot create, and it needs the stack's own database and
# Redis settings, which is why it runs through `docker compose run` against the
# identity-api service. Then tools/seed runs on the HOST as a plain HTTP client
# of the published ports (identity-api 8090, course-api 8091, analytics-api 8092):
# it touches no database, Redis, MinIO or Temporal itself. Override the admin
# with SEED_ADMIN_EMAIL / SEED_ADMIN_PASSWORD and the targets with IDENTITY_URL,
# COURSE_URL, ANALYTICS_URL (see env.example).
SEED_ADMIN_EMAIL ?= admin@smartcourse.local
SEED_ADMIN_PASSWORD ?= change-me-admin-password

# The recipe lines start with @ so make does not echo the admin password.
seed:
	@IDENTITY_ADMIN_EMAIL="$(SEED_ADMIN_EMAIL)" IDENTITY_ADMIN_PASSWORD="$(SEED_ADMIN_PASSWORD)" \
	  docker compose run --rm -T -e IDENTITY_ADMIN_EMAIL -e IDENTITY_ADMIN_PASSWORD identity-api identity-admin
	@SEED_ADMIN_EMAIL="$(SEED_ADMIN_EMAIL)" SEED_ADMIN_PASSWORD="$(SEED_ADMIN_PASSWORD)" go run ./tools/seed

## --- Binaries (docs/plans/microservices-split.md) -----------------------
# One module per service, one or two binaries each. BINARY is <service>/<binary>.
BINARIES := identity/identity-api identity/identity-worker identity/identity-admin \
            course/course-api course/course-worker \
            analytics/analytics-api analytics/analytics-worker \
            notification/notification-worker publishing/publishing-worker \
            audit/audit-api audit/audit-worker

# `make build` builds every binary into bin/; `make build-course-api` builds one.
build:
	@for b in $(BINARIES); do go build -o bin/$${b##*/} ./services/$${b%%/*}/cmd/$${b##*/} || exit 1; done

build-%:
	@b=$$(for x in $(BINARIES); do case $$x in */$*) echo $$x;; esac; done); [ -n "$$b" ] || { echo "unknown binary $*"; exit 1; }; go build -o bin/$* ./services/$${b%%/*}/cmd/$*

# `make run-course-api` runs one binary natively (its environment comes from your shell or .env).
run-%:
	@b=$$(for x in $(BINARIES); do case $$x in */$*) echo $$x;; esac; done); [ -n "$$b" ] || { echo "unknown binary $*"; exit 1; }; go run ./services/$${b%%/*}/cmd/$*
