SHELL := /bin/bash
MIGRATE := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Load .env into every recipe's environment, if it exists — plain Go has no
# built-in dotenv support (that's a Node/Python convention, not a Go or Make
# one), so without this, `go run` here would see none of it. `include`
# parses KEY=value lines as Make variables; `export` re-exports every
# variable Make knows (not just ones from this file) to child processes,
# which is what makes `go run ./cmd/identity-api` (or the other cmd/*)
# actually see them.
ifneq (,$(wildcard .env))
    include .env
    export
endif

DSN ?= $(POSTGRES_DSN)

.PHONY: dev-core dev-workflow dev-events dev-observability dev-full dev-down \
        migrate-up migrate-down migrate-create \
        run-identity-api run-course-api run-analytics-api run-worker \
        build build-identity-api build-course-api build-analytics-api build-worker \
        lint test test-race test-integration seed fmt

## --- Local environment (ADR-0018) ---------------------------------------

dev-core:
	docker compose --profile core up -d

dev-workflow:
	docker compose --profile workflow up -d

dev-events:
	docker compose --profile events up -d

dev-observability:
	docker compose --profile observability up -d

dev-full:
	docker compose --profile full up -d

dev-down:
	docker compose down

## --- Migrations (migrations/README.md) -----------------------------------

migrate-up:
	$(MIGRATE) -path migrations -database "$(DSN)" up

migrate-down:
	$(MIGRATE) -path migrations -database "$(DSN)" down 1

migrate-create:
	$(MIGRATE) create -ext sql -dir migrations -seq $(name)

## --- Running the binaries (ADR-0022 + ADR-0023: four services) -----------
#
# Two ways to run the app:
#   - Natively (fastest iteration): `make dev-core` for infra only, then
#     these targets in separate terminals. Each run-*-api target defaults
#     HTTP_PORT to a distinct port (matching docker-compose.yml's host
#     port mapping for the same service) so all three can run at once
#     without fighting over 8080 — override with `HTTP_PORT=... make
#     run-course-api` if you need something else.
#   - Fully containerized: `make dev-core` on its own now also builds and
#     starts the identity-api/course-api/analytics-api/worker containers,
#     since they're in the `core` profile too (docker-compose.yml) —
#     migrations still need `make migrate-up` as a separate, explicit
#     step either way (RFC §5.8 — no auto-migration on container start).

run-identity-api:
	HTTP_PORT=$${HTTP_PORT:-8090} go run ./cmd/identity-api

run-course-api:
	HTTP_PORT=$${HTTP_PORT:-8091} go run ./cmd/course-api

run-analytics-api:
	HTTP_PORT=$${HTTP_PORT:-8092} go run ./cmd/analytics-api

run-worker:
	go run ./cmd/worker

build: build-identity-api build-course-api build-analytics-api build-worker

build-identity-api:
	go build -o bin/identity-api ./cmd/identity-api

build-course-api:
	go build -o bin/course-api ./cmd/course-api

build-analytics-api:
	go build -o bin/analytics-api ./cmd/analytics-api

build-worker:
	go build -o bin/worker ./cmd/worker

## --- Quality gates (CI runs the same targets — see .github/workflows/ci.yml)

fmt:
	gofmt -l .

lint:
	golangci-lint run ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -race -tags=integration ./...

seed:
	go run ./cmd/seed
