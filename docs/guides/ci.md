# CI policy

`.github/workflows/ci.yml` runs only what a change can affect. The decision is made by `scripts/ci-select.sh` from the list of changed files; the workflow calls it and consumes its outputs through matrices. Nothing about the selection is written as inline YAML expressions.

## What runs

| Change | Runs |
|---|---|
| Push to `main` | Everything (all modules, all images, migrations, all E2E scenarios). |
| One service: `services/<svc>/**`, `Dockerfile.<svc>-*`, `migrations/<svc>/**` | That service's module (build, vet, golangci-lint, govulncheck, unit + integration tests with `-race`), its images, E2E scenarios mapped to it (below), the cheap guards. |
| `libs/**` (or `tools/*`, `test/e2e`) | The changed modules **and every module that requires them**, directly or transitively. The dependents are read from each module's `go.mod`, not from a list. A `libs/**` or `test/e2e` change runs all E2E scenarios. |
| `go.work`, `go.work.sum`, root `Makefile`, `scripts/**`, `.github/**`, `docker-compose*`, `.golangci.*`, `.dockerignore`, a Dockerfile that belongs to no service (`Dockerfile.schema-init`), `migrations/platform`, `migrations/cutover`, any path nothing else claims | Everything, including all E2E scenarios. |
| Docs only (`docs/**`, any `*.md`, `LICENSE`, `.gitignore`, ...) | Only the cheap guards and the compose check. E2E does not run (judgement call: see "Decisions to confirm"). |

The cheap guards (`guards` job) always run: link graph, module graph, Dockerfile COPY lists, module hygiene, dev defaults, HTTP hardening, pending E2E, the E2E scenario map, and the unit tests of the selection logic. `make test-guards` (it copies the repo) runs on full runs only. `compose-config` always runs.

The selection and its reason are printed in each run's summary ("CI selection").

Run it locally against your branch:

```
scripts/ci-select.sh --base origin/main            # JSON: modules, images, e2e, reason
git diff --name-only origin/main... | scripts/ci-select.sh --files -
scripts/test-ci-select.sh                          # make test-ci-select: the unit tests
```

## The `ci-ok` job

Matrix job names change with the selection, so branch protection should require the single job **`ci-ok`**. It fails when any job failed or was cancelled; jobs that were legitimately skipped (nothing selected, E2E waived) do not fail it.

## E2E on PRs into main

`make test-e2e` (`test/e2e`, build tag `e2e`) runs on every push to `main` and on every PR whose **base branch is `main`**. A PR to any other base does not run it.

Which scenarios run is the map `test/e2e/scenario-map.json` (test function name to the services it exercises). For a service-only PR the `-run` regexp is the scenarios mapped to a changed service; for `libs/**`, `test/e2e` or a shared file it is all of them. Only tests the default `make test-e2e` runs (`TestE2E_*`) are ever selected, so a subset is never wider than a full run.

Keeping the map right: `make check-e2e-map` (a cheap guard, also covered by `make test-guards`) fails when an `e2e`-tagged Test function has no entry or an entry names a function that does not exist. When you add a scenario, add its entry: list every service it drives or asserts on, and when in doubt list the service. A wrongly omitted service means a PR to that service skips the scenario.

## Skipping E2E: the `skip-e2e` label

A code owner can waive E2E on a PR into `main` by adding the label `skip-e2e`.

- The workflow reads who applied the label from the issue events API (`GET /repos/{repo}/issues/{n}/events`: the last `labeled` event for it, not followed by `unlabeled`).
- That person must be an owner: a login in `.github/e2e-skip-owners` or a **user** (not a team) named in `.github/CODEOWNERS`. Both files, and `scripts/ci-e2e-gate.sh` itself, are read from the PR's **base** commit through the API, never from the PR, so a PR cannot make itself an owner. Team entries in CODEOWNERS are not expanded; list the people in `.github/e2e-skip-owners`.
- A label from anyone else is ignored and E2E runs; the run summary says so.
- When honoured, the `E2E skipped` job and the run summary show `E2E SKIPPED: the skip-e2e label was applied by code owner @<login>`. Every other check still runs.
- Removing the label (or an owner removing it and someone else re-adding it) restores E2E on the next run; adding or removing the label re-triggers CI.

Limits to know: the workflow file itself comes from the PR for `pull_request` events, so the real control against tampering with this gate is review of changes under `.github/` (CODEOWNERS `*` covers them) plus branch protection. The label is an accountability mechanism, not a security boundary.

To change who may skip: edit `.github/e2e-skip-owners` (one GitHub login per line). The change takes effect for PRs opened after it merges.

## Pinned tools

`golang-migrate` is installed at `v4.20.1` (the version every `go.mod` already resolves; `libs/testenv` requires it directly), in CI and in the Makefile (`MIGRATE_VERSION`). Bump the Makefile variable, the `migrations` job's `go install` line and `libs/testenv/go.mod` together. golangci-lint `v2.14.0`, govulncheck `v1.10.0`, Go `1.26.11`.

## Decisions to confirm

- Docs-only PRs into `main` run no E2E (rule 1 says docs-only runs only the cheap guards; rule 2 says E2E on every PR into main). If you want E2E there too, change `docs_only` handling in `scripts/ci-select.sh` to return `all`.
- `tools/seed` and `tools/guardcheck` changes run their own modules (and dependents) but no E2E.
- The three `TestAdversarial_*` tests (certificate mirror stall, MongoDB outage, two replicas) are in the map but are not run by `make test-e2e` today (its default filter is `TestE2E_`), so CI never runs them. Widening the default filter is a separate decision.

## Testing the workflow

The workflow cannot be run locally. Verify a change to it with throwaway PRs against `main` (close them without merging):

1. Service-only: change a comment in `services/audit/`. Expect `select` to list `services/audit`, a `module services/audit` job, audit images, an E2E run with a regexp of audit scenarios, no other module jobs.
2. Libs: touch a file in `libs/relay/`. Expect the dependents of `libs/relay` as module jobs and E2E `all`.
3. Docs only: edit `docs/README.md`. Expect only `guards` and `compose-config`, plus `E2E skipped` (not required).
4. Shared: touch `Makefile`. Expect a full run.
5. Skip by an owner: on PR 1, an owner adds `skip-e2e`. Expect the `E2E skipped` job naming the owner and no `e2e` job.
6. Skip by a non-owner: a user not in the owner files adds the label. Expect E2E to run and the summary to say the label was ignored.
7. PR into a branch other than `main`: no E2E, no gate.
