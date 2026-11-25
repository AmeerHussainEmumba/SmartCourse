# Plan: split the build graph into per-service and per-library Go modules

**Status:** Phases 1 to 8 implemented locally (nothing pushed or tagged); Phase 0 (supervisor alignment) still open. Started 2026-11-16 on branch `refactor/module-split`, branched from `feat/m2-publish-workflow` (the Week 2 PR); work is stacked across `-libs`, `-services`, `-tooling`, `-docs` and `-tests` branches.
**Origin:** Week 2 review feedback on PR #11. Estimates below are working estimates (medium confidence), not measurements.

> **Superseded in part by the microservices split (ADR-0033..0035, `docs/plans/microservices-split.md`), 2026-11-26.** This plan is kept as the record of the module split. What changed afterwards: `services/worker` was deleted and replaced by per-service workers (11 binaries in six services: identity, course, analytics, notification, publishing, audit); no service imports another service (the worker exception named below is gone, services require only `libs/*`, and `tools/seed` and `test/e2e` require no service either); the `platform` schema was removed; Kafka is the only transport; the repository now has 33 Go modules (the "13 libraries", "19", "20" module counts below are historical). Package counts per binary below are the pre-split measurements.

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
services/identity/   identity/ + cmd/identity-api            (module .../services/identity)
services/course/     course/ enrollment/ progress/ workflows/ + cmd/course-api
services/analytics/  analytics/ + cmd/analytics-api
services/worker/     cmd/worker; audit/ notification/ search/ (README-only plans)   [2026-11-26: deleted; see the note at the top]
tools/seed/          the seed tool (needs identity and course)                       [2026-11-26: now drives the HTTP APIs, requires no service]
```

Constraints:

- **Course, enrollment and progress stay in one module and one service** (ADR-0012 row lock needs one process and one transaction; still true after the split).
- **Go's `internal/` rule.** `services/worker` and `tools/seed` cannot import another module's `internal/...` packages. Decision taken in Phase 3: domain packages are *not* under `internal/`; they are ordinary exported packages (`services/course/course`, ...). Cross-service imports are prevented by `go.mod` requires plus `scripts/check-module-graph.sh`, not by Go's `internal/` rule. A narrower alternative (keep `internal/`, export small facade packages for the worker and seed) was rejected because seed needs most of each service's surface.
- **`replace` directives are not inherited.** Every service `go.mod` repeats the `replace` for each library it depends on, including indirect ones. `go.work` hides this locally, so CI builds with `GOWORK=off`.
- **Library dependency graph (no cycles):** `httpx -> errors`, `router -> httpx`, `auth -> config, errors, httpx`, `ratelimit -> errors, httpx`, `cache -> config`, `db -> config`; `outbox`, `relay`, `logging`, `errors`, `config` have no internal dependencies.

## Phases

| # | Phase | Work | Exit criterion | Est. |
|---|---|---|---|---|
| 0 | Align | Confirm with the supervisor: does this gate Week 2, is a worker that imports service handler packages acceptable, `go.work` only or tagged versions. | Answers recorded here | 0.5d |
| 1 | Per-service wiring, one module | Delete `internal/platform/bootstrap`. Each `cmd/*` wires only what it uses. Split out `infra` (DB and Redis) and `temporal` (client) so no binary pulls in what it doesn't use. | `go list -deps` for `identity-api` and `analytics-api` shows no other domain package and no Temporal SDK; `course-api` shows no analytics or identity. All existing tests pass. | 1d |
| 2 | Library modules | Create the 13 library modules (the original 11 plus `infra` and `temporal` from Phase 1) and `libs/testenv`. Rewrite the imports of `internal/platform` and `testenv`. Also: commit `go.work`, add `scripts/for-each-module.sh`, point Makefile and CI at it, and make the Dockerfiles copy `libs/` (pulled forward from Phase 4 so the repo keeps building). | Each library builds and tests on its own | 1.5-2d |
| 3 | Service modules | Move code into `services/*` and `tools/seed`, add `go.mod` per module, delete the root `go.mod`, rewrite depguard (cross-service rules become module boundaries), add `check-module-graph.sh`, selective Dockerfiles, CI/Makefile paths. | Each module builds with `GOWORK=off`; guards and lint pass | 1.5d |
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

Compare the result against `docs/baseline/SmartCourse.md`, `docs/baseline/Execution Guidelines.md` and `docs/prd/smartcourse-prd.md`. Output: a table of requirement, where it is implemented, status after the split. Invariants to confirm (as written in 2026-11): three API services plus worker still deployable independently; schema-per-service; seat-limit transaction; outbox and relay semantics; Week 1 and Week 2 acceptance criteria. Any deviation is logged as a DR entry.

## Risks

- **Version skew and cascades:** a change to `errors` touches `httpx`, `auth`, `ratelimit` and every service. Invisible with `replace`, real release work with tags.
- **Overhead:** about 16 `go.mod` files for roughly 1,765 lines of platform code.
- **Worker coupling:** the worker imports other services' handler packages. Needs an owner and, long term, per-service consumption (M3). *(Resolved 2026-11-26 by ADR-0033: each service runs its own consumers and relay; no service imports another.)*
- **`config` coupling (unverified):** 26 importers. If it holds every service's settings in one struct, split it per service in Phase 2.
- **Docker caching** gets worse with a repo-root context unless Dockerfiles copy selectively.
- **Shared blast radius:** a break in a library still breaks its consumers.
- **M3 rebase:** the relay moves; small conflict expected.

## Progress log

- 2026-11-16: branch `refactor/module-split` created from `feat/m2-publish-workflow`. Plan written. Phase 1 started.
- 2026-11-16: **Phase 1 code complete (uncommitted).** `internal/platform/bootstrap` deleted; added `internal/platform/infra` (Postgres/Redis, health checks) and `internal/platform/temporal` (lazy client); each `cmd/*` now has its own `wire.go`. `go list -deps` per binary, before to after: identity-api 589 to 388 packages (Temporal 61 to 0, links only `identity`); analytics-api 589 to 386 (Temporal 61 to 0, only `analytics`); course-api 589 to 581 (Temporal client only, `course`+`enrollment`+`progress`); worker 589 to 586 (`analytics`+`course`+`enrollment`+`workflows`). Counts include the standard library. Added `scripts/check-link-graph.sh` (`make check-link-graph`, CI step); it fails on all four binaries against the pre-change code and passes now. Verified: `go build`, `go vet` (with and without `-tags=integration`), `gofmt`, unit tests. **Not run:** the integration suite (`-tags=integration`) because Docker wasn't running locally; CI runs it. Docs updated so far: READMEs and code comments that named `bootstrap`, `internal/platform/README.md`, `docs/README.md`. Still stale until Phase 6: ADR log (append-only; ADR-0026 will supersede), RFC lines 206 and 1549, `GETTING_STARTED.md`, DR log, `ONBOARDING.md`, hosted onboarding docs.
- Phase 0 (supervisor alignment) is still open and is not blocked by Phase 1.
- 2026-11-17: **Phase 2 complete (branch `refactor/module-split-libs`, stacked on Phase 1).** 13 library modules plus `libs/testenv`, each with its own `go.mod` seeded from the root's pinned versions then pruned with `go mod tidy`, so no dependency version drifted. `replace` directives had to list the full transitive set of sibling libraries (Go doesn't inherit them) — this is the risk flagged at planning time, now confirmed. `go.work` is committed (it was git-ignored; `scripts/for-each-module.sh` reads it for the module list). `testenv` finds `migrations/` by walking up from its own file, so its path logic changed with the move. Makefile test/lint targets and CI build/vet/test/coverage steps now loop over modules with `GOWORK=off`; `go test ./...` at the root no longer reaches the libraries. Dockerfiles copy `libs/` before `go mod download`; all four images build. Verified: every module builds and vets standalone with `GOWORK=off`, link-graph guard passes, full integration suite passes (`make test-integration`). Still open: CI's golangci-lint action lints only the root module (to be restructured in Phase 4); selective Dockerfile copying and `GOWORK=off` in Dockerfiles (Phase 4).
- 2026-11-17: **Phase 3 complete (branch `refactor/module-split-services`, stacked on Phase 2).** Four service modules plus `tools/seed`; root `go.mod`/`go.sum` deleted (`go.work` has no root module). `internal/` no longer exists. `scripts/check-link-graph.sh` now builds each binary from inside its own module with `GOWORK=off`; new `scripts/check-module-graph.sh` fails if a service requires another service (only `services/worker` and `tools/seed` may) or a library requires a service — verified it fails when `services/identity` is made to require `services/course`. `.golangci.yml` keeps only the rules *inside* `services/course` (course/enrollment/progress/workflows); verified depguard fires on a probe import and that `golangci-lint v2.14.0` reports 0 issues in all 19 modules. Dockerfiles copy only the modules each service needs and build with `GOWORK=off` and BuildKit cache mounts; all four images build, which also confirms the `replace` closures are sufficient. CI: `setup-go` cache key now `**/go.sum`, golangci-lint action is install-only and runs per module, module-graph step added. Docs updated so far: `services/README.md` (replaces `internal/README.md` and `cmd/README.md`), root README layout, `test/README.md`, `migrations/README.md`, code comments and module READMEs. Verified: full integration suite across all modules. Still stale until Phase 6: everything under `docs/` (ADR, RFC, DR, guides, GETTING_STARTED), `ONBOARDING.md`, hosted onboarding docs.
- 2026-11-17: **Phase 4 complete (branch `refactor/module-split-tooling`).** Most Phase 4 work landed earlier because Phases 2 and 3 couldn't leave the repo building without it: per-module Makefile/CI targets, selective Dockerfiles with `GOWORK=off` and cache mounts, per-module lint, both guards in CI. This phase added the deployment smoke test and README path fixes. **Smoke test on the real stack** (`docker compose --profile workflow up --build`, `make migrate-up`): identity-api, course-api and analytics-api all return 200 on `/health/ready`; the worker retried through Temporal's cold start and then started; logging in through identity-api works; publishing a seeded course through course-api returned 202 and the workflow reached state `ready`, so course-api to Temporal to worker to Postgres works from the new images. **Deliberately not built:** a separate "affected modules" CI job. CI runs every module on every change, which is a superset of it; revisit if CI time becomes a problem. **Finding, not caused by this work:** `make seed` run from the host failed at its publish step with a gRPC `no children to pick from` error against the Temporal address in the local `.env` (not inspected; it is not readable from this session). With `TEMPORAL_HOST_PORT=localhost:7233` set explicitly the seed starts the workflow. A full seed run on the main database was therefore not completed here; the local database holds a partial seed (16 users, 1 course, since published through the API). Recommended: check `TEMPORAL_HOST_PORT` in `.env`.
- 2026-11-17: **Phase 5 complete (branch `refactor/module-split-tests`).** Two independent testing sessions with a "break it" brief, one for behaviour and concurrency and one for security and abuse, each in an isolated worktree, writing tests only. Result: 42 new test files, 159 new top-level tests (113 in the default suite, 46 reproducers behind the `bugrepro` build tag), 4 fuzz targets, and five new scripts. Verified by the lead: the full integration suite passes across all 19 modules; all 46 reproducers fail and none pass; three were re-run independently and failed with the described symptoms. **36 open defects** in production code (3 high, 16 medium, 2 low-medium, 14 low, 1 info), none fixed, recorded in `docs/plans/module-split-findings.md`. Two issues in this repo's own tooling were found and fixed: `check-module-graph.sh` missed a single-line `require`, and `go mod tidy` was not a no-op in five modules. Three of the new scripts (`test-guards.sh`, `check-dockerfile-modules.sh`, `check-module-hygiene.sh`) are wired into the Makefile and CI; `check-dev-defaults.sh` and `check-http-hardening.sh` fail by design on the open findings and are not wired. The tester sessions' worktrees were removed after merge; their branches remain.
- 2026-11-17: **Phase 6 complete.** Updated: ADR-0026 (and status notes on ADR-0001/0022/0023/0024), the RFC (§4.1 to §4.4, repository layout, CI, ADR table), DR-020, `GETTING_STARTED.md`, `docs/guides/testing.md` (path fixes plus an adversarial-tests section), `docs/guides/versioning.md`, every module and platform README, `services/README.md`, `libs/README.md`, root and `test/` READMEs, the local `ONBOARDING.md`, and the three hosted onboarding docs (Crash Course, Week 1 to Week 2, Bridging Guide; each with comments marking what changed). `ONBOARDING.md` remains untracked, as before. The hosted Bridging Guide's testing section does not yet describe the adversarial tier.
- 2026-11-17: **Phase 7 complete.** `docs/plans/module-split-assessment.md`: baseline traceability (32 rows: 16 Met, 10 Partial, 6 Not built; three rows downgraded from Met after the adversarial phase), the Week 2 review comments, invariants, test evidence, a rating, and a suggested order of next steps. Findings that correct earlier project claims: analytics covers 7 of the baseline's **nine** named metrics (docs say "7 of 10"), and `VerifyAssets` checks lesson content size, not uploaded assets.
- 2026-11-17: **Phase 8 built, not exercised.** Release script, manual workflow, versioning guide, `CODEOWNERS` (placeholder single owner), per-module changelogs. Verified locally: dry run, refusal without a changelog section, refusal on a malformed version, and creating then deleting a local tag with nothing on the remote. No tag has been created or pushed; moving services to pinned versions waits for a second owner and a private-module setup.
- 2026-11-17: **Bug-fix phase complete (branch `refactor/module-split-fixes`).** All 36 findings fixed by four fixing sessions with non-overlapping directory ownership (libs and servers; identity and auth; course, enrollment and progress; analytics) plus three infrastructure fixes by the lead, and merged. Every reproducer now passes and lost its `bugrepro` tag. One new migration (`migrations/analytics/000002_enrollment_mirror_ordering`), ADR-0027 (the relay holds each row's claim through handling), and API contract changes: `POST /auth/register` returns 202 with no tokens, non-owner course access is 404 not 403, analytics access rule (students: platform totals and courses they are enrolled in), new 409/404/413/429 responses, new hardening environment variables. Verified: full suite green across 19 modules, all guard scripts and both static policy scans pass (and are now in CI), four non-root images build, and a live stack on a reset database passed a smoke test of the changed behaviours. Two tests had to be corrected (body cap now returns 413 for 1 MiB inputs; a relay test checked before its retry cooldown). `ONBOARDING.md` is still untracked by the owner's choice. Nothing pushed.
- 2026-11-17: **Supervisor-reported bugs and one regression.** The supervisor reported two points: set the publishing state before starting the workflow with a 409 for a duplicate, and withdraw can still double-decrement. The withdraw point is F-01 (already fixed; re-verified on the current code and hammered three times under `-race`). The publish point was **not** in our scan and is now F-37, fixed (ADR-0028, migration `000006_publish_started_at`, seven new tests plus a rewritten one, verified against the real Temporal server). While verifying F-37 live, a regression our own F-11 fix caused was found: instructors with a token version above 1 were rejected by `course-api` after a Redis restart even with a fresh token (F-38); fixed by seeding the version in Redis at login and refresh. Both new tests were shown to fail against the old code. Totals: 38 defects found, 38 fixed.
- 2026-11-17: **End-to-end test for F-38 added (`test/e2e`, the 20th Go module).** `TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks` runs real identity routes and real course routes as two engines on the production router stack, each with its own connection limited to its own schemas, against one shared Postgres and Redis, using a real admin role change, real logins and a real refresh. Passes under `-race`; fails when identity's seeding is disabled; the module is registered in `go.work`, `scripts/check-module-graph.sh` (allowed to depend on identity and course, like `tools/seed`), `CODEOWNERS` and the docs (module count now 20; ADR-0026 gets a dated addendum). It carries a `doc.go` because golangci-lint finds no files in a module whose only file is behind a build tag.
