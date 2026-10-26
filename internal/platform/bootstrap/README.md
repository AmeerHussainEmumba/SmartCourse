# internal/platform/bootstrap

The composition root (RFC §4.4) — the one package allowed to import every domain module, because wiring them together into a running system is the one legitimate reason to need all of them at once.

## Why this exists, and why it moved here

Before ADR-0022, this wiring (`registerModules`, `buildRelay`) lived directly in `cmd/smartcourse/modules.go`, since there was only one binary to call it. Splitting into `cmd/api` and `cmd/worker` (two separate `package main`s) meant it had to move somewhere both could import — Go doesn't allow one `main` package to import another, and duplicating this wiring into two files would have meant it drifting the first time someone updated one and forgot the other. `internal/platform/` is the natural home: it's the shared kernel every module already depends on, and this is the one piece every *binary* depends on. ADR-0023 later split `cmd/api` three ways (`cmd/identity-api`, `cmd/course-api`, `cmd/analytics-api`) — the same reasoning applied again, which is why `RegisterModules` is now three narrower functions instead of one.

## What's here

- **`Dependencies`** — the shared Postgres/Redis clients every service needs. `Connect` builds them once; `HealthCheckers` exposes the readiness checks each `cmd/*-api` wires into its own `/health/ready`.
- **`RegisterIdentityRoutes`** — wires `internal/identity`'s HTTP routes onto a Gin engine. Called only by `cmd/identity-api`.
- **`RegisterCourseRoutes`** — wires `internal/course`, `internal/enrollment`, and `internal/progress`'s HTTP routes onto a Gin engine. Called only by `cmd/course-api`. The three stay together specifically because `internal/enrollment` locks the `courses` row in the same transaction as an enrollment insert (ADR-0012) — that requires one process, one connection.
- **`RegisterAnalyticsRoutes`** — wires `internal/analytics`'s HTTP routes onto a Gin engine. Called only by `cmd/analytics-api`.
- **`BuildRelay`** — wires every module's relay handlers (today, `internal/analytics` and `internal/enrollment`) into one `relay.Relay`. Called only by `cmd/worker`.

## What this does *not* change

Every module still shares one PostgreSQL connection, one set of Go packages, and one transaction boundary where it matters (`internal/enrollment` locking `courses` in the same transaction as an enrollment insert, per ADR-0012). This package is about which *binary* calls which module's wiring — it has no opinion on, and doesn't touch, the modular-monolith data architecture ADR-0001 and ADR-0012 established, regardless of how many binaries exist. See ADR-0022 and ADR-0023 for the full decision records, and `docs/rfc/differences-from-requirements.md` DR-010 for the broader "should each service own its own tables" question this package deliberately still does not answer.
