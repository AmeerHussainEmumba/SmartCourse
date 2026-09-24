SHELL := /bin/bash
MIGRATE := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
DSN ?= $(shell grep -E '^POSTGRES_DSN=' .env 2>/dev/null | cut -d= -f2-)

.PHONY: dev-core dev-workflow dev-events dev-observability dev-full dev-down \
        migrate-up migrate-down migrate-create \
        run-api run-worker build lint test test-race test-integration seed fmt

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

## --- Running the binary ---------------------------------------------------

run-api:
	go run ./cmd/smartcourse --mode=api

run-worker:
	go run ./cmd/smartcourse --mode=worker

build:
	go build -o bin/smartcourse ./cmd/smartcourse

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
	go test -race -tags=integration ./test/integration/...

seed:
	go run ./cmd/smartcourse/seed
