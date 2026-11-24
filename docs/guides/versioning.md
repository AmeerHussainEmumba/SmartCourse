# Versioning and releasing modules

Applies from ADR-0026: SmartCourse is a monorepo of 25 Go modules (18 libraries under `libs/`, `libs/testenv`, four services under `services/`, `tools/seed`, and the cross-service tests in `test/e2e`). This guide says how they depend on each other today, when that changes, and how a module is released.

## Today: one team, `replace` directives, no tags

While one team owns every module, modules reference each other through `replace` directives in their `go.mod` (pointing at a relative path) and, locally, through `go.work`. Nothing is tagged and nothing needs to be. A change to a library is visible to every consumer in the same commit, and CI proves they still build and pass: it builds and tests **every module with `GOWORK=off`**, runs both guards (`make check-link-graph`, `make check-module-graph`) and lints each module.

This is the cheapest setup that is still correct. Do not introduce tags until the trigger below.

## The trigger: a second owner

Switch a module from `replace` to pinned versions when **a different person or team takes ownership of it** (or of a module that depends on it) and needs to change it on its own schedule. Before that, tags add release work without protecting anyone. `CODEOWNERS` (`.github/CODEOWNERS`) is where ownership is recorded; when you add a second owner there, revisit this section.

## What counts as a breaking change

For a module that is being tagged, the owner decides, using this rule: it is a **major** version change if it removes or changes the signature of an exported identifier, changes behaviour its README documents (error codes, headers, defaults, SQL schema it expects), or raises the minimum Go version. A new exported identifier is **minor**. A bug fix or internal change is **patch**. Until a module reaches `v1.0.0` it may change in minor versions, but its `CHANGELOG.md` must still say what changed.

## Releasing a module

1. Make sure the module's `CHANGELOG.md` has a `## [X.Y.Z]` section describing the release. (The script refuses to tag without it.)
2. Check what will happen:
   ```
   scripts/release-module.sh libs/auth v0.1.0 --dry-run
   ```
   It verifies the version format, that the module is in `go.work`, that its directory is clean, that it builds, vets and tests standalone with `GOWORK=off`, and that the module graph check passes.
3. Create the tag locally, or publish it:
   ```
   scripts/release-module.sh libs/auth v0.1.0           # local tag only
   scripts/release-module.sh libs/auth v0.1.0 --push    # also pushes it
   ```
   or run the **Release module** workflow from the GitHub Actions tab (manual only).

Tags use the directory as a prefix, which is how Go finds nested modules: `libs/auth/v0.1.0`, `services/course/v0.3.0`. **A pushed tag cannot be cleanly retracted** (the Go module proxy caches it), so release only what you are ready to keep. If a bad version goes out, publish a fixed one and add a `retract` directive to the module's `go.mod`.

## Switching a consumer from `replace` to a pinned version

Do this only once the library has a tag and the consumer's owner wants to be insulated from library changes.

1. In the consumer's `go.mod`, change the `require` line from `v0.0.0` to the released version, e.g. `github.com/AmeerHussainEmumba/SmartCourse/libs/auth v0.1.0`.
2. Delete that library's `replace` line **and the `replace` lines of anything only needed through it**.
3. Run `GOWORK=off go mod tidy` in the consumer. If the repository is private, set `GOPRIVATE=github.com/AmeerHussainEmumba/*` and make sure the build environment (CI, Docker) can authenticate to GitHub; this is the part that does not work out of the box today.
4. Update `Dockerfile.<binary>`: a pinned library no longer needs its directory copied.
5. Remove the library from the consumer's expected set in `scripts/check-module-graph.sh` if the rule listed it.

A consumer can mix: pin the libraries it wants stability from and keep `replace` for the rest.

## Cascades

A change to a base library (`libs/errors`, `libs/config`) affects everything above it: `httpx`, `auth`, `ratelimit`, `router`, `infra`, and every service. With `replace` that is automatic. Once tagged, release bottom-up: `errors` and `config` first, then `httpx`, then `auth`/`ratelimit`/`router`, then the services, bumping each `require` in turn. The dependency order is in `libs/README.md`.

## Things that go wrong

- **Forgot a `replace`.** Builds under `go.work`, fails under `GOWORK=off`. Always verify with `scripts/for-each-module.sh go build ./...`.
- **Ran `go test ./...` at the repo root.** It tests nothing: Go does not cross module boundaries. Use `make test`.
- **Tagged the wrong thing.** Tags are per module and prefixed with the directory. A tag named just `v0.1.0` applies to nothing here.
