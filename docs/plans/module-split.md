# Plan: split the build graph into per-service and per-library Go modules

**Status:** Draft, in progress. Phase 1 started 2026-11-16 on branch `refactor/module-split` (branched from `feat/m2-publish-workflow`, the Week 2 PR).
**Origin:** Week 2 review feedback on PR #11. Estimates below are working estimates (medium confidence), not measurements.

## Purpose

ADR-0022, ADR-0023 and ADR-0024 made the services deployable independently (four binaries, schema-per-service) but left one build graph: every `cmd/*` imports `internal/platform/bootstrap`, which imports every domain module. Measured on `refactor/module-split` before any change, each binary links the same 6 domain packages, 589 packages in total, 61 of them `go.temporal.io/sdk`. So `identity-api` builds course, enrollment, analytics and Temporal code it never runs, and a compile error in `internal/course` breaks `identity-api`.

ADR-0024 explicitly deferred the `go.mod` / shared-library split to a later record. This plan is that follow-up.

## Objectives

1. Each service's binary links only the code it uses (verified by a CI guard, not by convention).
2. Each service lives in its own folder with its own `go.mod`.
3. Shared code is split into library modules, one per package, so different owners can control different modules in future.
4. Behaviour is unchanged: seat-limit transaction (ADR-0012), outbox and relay semantics (ADR-0006, ADR-0021), schema-per-service (ADR-0024), publish workflow (ADR-0025).
5. Docs, tests and the onboarding material stay accurate, and the result is re-checked against the frozen baseline and the PRD.

## Scope

In scope: `cmd/*`, `internal/*`, `internal/platform/*`, `test/`, Dockerfiles, Makefile, CI, `.golangci.yml`, every README, the RFC, the ADR log, `differences-from-requirements.md`, `docs/guides/testing.md`, `docs/for-the-devs/GETTING_STARTED.md`, `ONBOARDING.md`, and the three hosted onboarding docs (Crash Course, Week 1 to Week 2, Bridging Guide).

Out of scope: per-service event consumption (per-consumer offsets in the relay). It belongs with M3 Kafka. M3 work (`feat/m3-events-phase1`) will be rebased on top of this afterwards.

## Target layout

```
go.work
libs/<pkg>/          one module per shared package: auth, cache, config, db, errors,
                     httpx, infra, logging, outbox, ratelimit, relay, router, temporal
libs/testenv/        the integration-test environment (was test/integration/testenv)
services/identity/   internal/identity + cmd/identity-api
services/course/     course, enrollment, progress, workflows + cmd/course-api
services/analytics/  internal/analytics + cmd/analytics-api
services/worker/     cmd/worker, imports handler and workflow packages from the services
```

Constraints:

- **Course, enrollment and progress stay in one module and one service** (ADR-0012 row lock needs one process and one transaction).
- **Go's `internal/` rule.** `services/worker` cannot import `services/course/internal/...`. Whatever the worker needs (outbox handlers, the publish workflow) must live in exported packages.
- **`replace` directives are not inherited.** Every service `go.mod` repeats the `replace` for each library it depends on, including indirect ones. `go.work` hides this locally, so CI builds with `GOWORK=off`.
- **Library dependency graph (no cycles):** `httpx -> errors`, `router -> httpx`, `auth -> config, errors, httpx`, `ratelimit -> errors, httpx`, `cache -> config`, `db -> config`; `outbox`, `relay`, `logging`, `errors`, `config` have no internal dependencies.

## Phases

| # | Phase | Work | Exit criterion | Est. |
|---|---|---|---|---|
| 0 | Align | Confirm with the supervisor: does this gate Week 2, is a worker that imports service handler packages acceptable, `go.work` only or tagged versions. | Answers recorded here | 0.5d |
| 1 | Per-service wiring, one module | Delete `internal/platform/bootstrap`. Each `cmd/*` wires only what it uses. Split out `infra` (DB and Redis) and `temporal` (client) so no binary pulls in what it doesn't use. | `go list -deps` for `identity-api` and `analytics-api` shows no other domain package and no Temporal SDK; `course-api` shows no analytics or identity. All existing tests pass. | 1d |
| 2 | Library modules | Create the 13 library modules (the original 11 plus `infra` and `temporal` from Phase 1) and `libs/testenv`. Rewrite the imports of `internal/platform` and `testenv`. Also: commit `go.work`, add `scripts/for-each-module.sh`, point Makefile and CI at it, and make the Dockerfiles copy `libs/` (pulled forward from Phase 4 so the repo keeps building). | Each library builds and tests on its own | 1.5-2d |
| 3 | Service modules | Move code into `services/*`, add `go.mod` per service, expose worker-facing packages. | Each service builds with `GOWORK=off` | 1.5d |
| 4 | Tooling | Selective Dockerfiles (repo-root context; copy only needed `go.mod`/`go.sum`, then source; BuildKit cache mounts; `.dockerignore`). Makefile and CI loop over modules. Affected-modules CI job. Trim `.golangci.yml`. | CI green, four images build | 1.5d |
| 5 | Tests | See "Test plan". | Coverage equal to before plus new guards | 0.5-1d |
| 6 | Docs and ownership | See "Documentation". Add `CODEOWNERS`, per-module `CHANGELOG`, versioning policy in the new ADR. | No stale paths | 1-1.5d |
| 7 | Requirements re-check | See "Requirements re-check". | Sign-off table committed | 0.5d |
| 8 | Release mechanics (gated) | Per-module semver tags, private module setup, services pinned to tags. Only when different people actually own different modules. | | +2-3d |

Phases 2 and 3 are separate PRs so that a change in direction after review only touches one of them. Estimated total for phases 0 to 7: about 8-9 working days.

## Test plan

- Existing test files move with their packages. Starting inventory: analytics 4, course 7, enrollment 3, identity 2, progress 2, workflows 2, platform auth 4, config 1, httpx 1, relay 1.
- **Build-graph guard (new, Phase 1):** `scripts/check-link-graph.sh` runs `go list -deps` for each `cmd/*` and fails if a forbidden package appears. Wired into `make` and CI. This is the regression test for the review comment.
- **Standalone module builds (new, Phase 3-4):** every module is built and tested with `GOWORK=off`.
- **Worker wiring test (new):** the worker registers the same workflow name, activities and outbox handlers as before.
- **Integration suite unchanged:** seat-limit concurrency, publish workflow, outbox to analytics.
- Update `docs/guides/testing.md`, `test/README.md` and the Makefile targets.

## Documentation

- READMEs: root, `docs/README.md` (add `plans/`), `cmd/README.md` and each `cmd/*`, `internal/README.md`, every module and platform README (they move), `migrations/README.md`, `proto/README.md`, `test/README.md`. Delete `internal/platform/bootstrap/README.md` (Phase 1).
- ADR: new ADR-0026 (supersedes the single-module part of ADR-0001, resolves ADR-0024's deferred item, records the worker coupling and the versioning policy). Amend ADR-0022 and ADR-0023.
- RFC: §3-§4 (repo layout, module boundaries, §4.4 enforcement), build and deployment sections.
- `differences-from-requirements.md`: a DR entry if the layout reads the baseline differently.
- `GETTING_STARTED.md` and the local `ONBOARDING.md`.
- Hosted docs, after the code lands so paths are real: Crash Course (Block 5 pointer, capstone path), Week 1 to Week 2, Bridging Guide (module tour).

## Requirements re-check

Compare the result against `docs/baseline/SmartCourse.md`, `docs/baseline/Execution Guidelines.md` and `docs/prd/smartcourse-prd.md`. Output: a table of requirement, where it is implemented, status after the split. Invariants to confirm: three API services plus worker still deployable independently; schema-per-service; seat-limit transaction; outbox and relay semantics; Week 1 and Week 2 acceptance criteria. Any deviation is logged as a DR entry.

## Risks

- **Version skew and cascades:** a change to `errors` touches `httpx`, `auth`, `ratelimit` and every service. Invisible with `replace`, real release work with tags.
- **Overhead:** about 16 `go.mod` files for roughly 1,765 lines of platform code.
- **Worker coupling:** the worker imports other services' handler packages. Needs an owner and, long term, per-service consumption (M3).
- **`config` coupling (unverified):** 26 importers. If it holds every service's settings in one struct, split it per service in Phase 2.
- **Docker caching** gets worse with a repo-root context unless Dockerfiles copy selectively.
- **Shared blast radius:** a break in a library still breaks its consumers.
- **M3 rebase:** the relay moves; small conflict expected.

## Progress log

- 2026-11-16: branch `refactor/module-split` created from `feat/m2-publish-workflow`. Plan written. Phase 1 started.
- 2026-11-16: **Phase 1 code complete (uncommitted).** `internal/platform/bootstrap` deleted; added `internal/platform/infra` (Postgres/Redis, health checks) and `internal/platform/temporal` (lazy client); each `cmd/*` now has its own `wire.go`. `go list -deps` per binary, before to after: identity-api 589 to 388 packages (Temporal 61 to 0, links only `identity`); analytics-api 589 to 386 (Temporal 61 to 0, only `analytics`); course-api 589 to 581 (Temporal client only, `course`+`enrollment`+`progress`); worker 589 to 586 (`analytics`+`course`+`enrollment`+`workflows`). Counts include the standard library. Added `scripts/check-link-graph.sh` (`make check-link-graph`, CI step); it fails on all four binaries against the pre-change code and passes now. Verified: `go build`, `go vet` (with and without `-tags=integration`), `gofmt`, unit tests. **Not run:** the integration suite (`-tags=integration`) because Docker wasn't running locally; CI runs it. Docs updated so far: READMEs and code comments that named `bootstrap`, `internal/platform/README.md`, `docs/README.md`. Still stale until Phase 6: ADR log (append-only; ADR-0026 will supersede), RFC lines 206 and 1549, `GETTING_STARTED.md`, DR log, `ONBOARDING.md`, hosted onboarding docs.
- Phase 0 (supervisor alignment) is still open and is not blocked by Phase 1.
- 2026-11-17: **Phase 2 complete (branch `refactor/module-split-libs`, stacked on Phase 1).** 13 library modules plus `libs/testenv`, each with its own `go.mod` seeded from the root's pinned versions then pruned with `go mod tidy`, so no dependency version drifted. `replace` directives had to list the full transitive set of sibling libraries (Go doesn't inherit them) — this is the risk flagged at planning time, now confirmed. `go.work` is committed (it was git-ignored; `scripts/for-each-module.sh` reads it for the module list). `testenv` finds `migrations/` by walking up from its own file, so its path logic changed with the move. Makefile test/lint targets and CI build/vet/test/coverage steps now loop over modules with `GOWORK=off`; `go test ./...` at the root no longer reaches the libraries. Dockerfiles copy `libs/` before `go mod download`; all four images build. Verified: every module builds and vets standalone with `GOWORK=off`, link-graph guard passes, full integration suite passes (`make test-integration`). Still open: CI's golangci-lint action lints only the root module (to be restructured in Phase 4); selective Dockerfile copying and `GOWORK=off` in Dockerfiles (Phase 4).
