SHELL := /bin/bash
MIGRATE := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Load .env into every recipe's environment, if it exists — plain Go has no
# built-in dotenv support (that's a Node/Python convention, not a Go or Make
# one), so without this, `go run` here would see none of it. `include`
# parses KEY=value lines as Make variables; `export` re-exports every
# variable Make knows (not just ones from this file) to child processes,
# which is what makes `go run ./cmd/api` (or ./cmd/worker) actually see them.
ifneq (,$(wildcard .env))
    include .env
    export
endif

DSN ?= $(POSTGRES_DSN)

.PHONY: dev-core dev-workflow dev-events dev-observability dev-full dev-down \
        migrate-up migrate-down migrate-create \
        run-api run-worker build build-api build-worker lint test test-race test-integration seed fmt

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

## --- Running the binary (ADR-0022: two services, api and worker) ---------
#
# Two ways to run the app — don't run both at once, they'll fight over
# port 8080:
#   - Natively (fastest iteration): `make dev-core` for infra only, then
#     these targets in separate terminals.
#   - Fully containerized: `make dev-core` on its own now also builds and
#     starts the api/worker containers, since they're in the `core`
#     profile too (docker-compose.yml) — migrations still need `make
#     migrate-up` as a separate, explicit step either way (RFC §5.8 — no
#     auto-migration on container start).

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

build: build-api build-worker

build-api:
	go build -o bin/api ./cmd/api

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
