# internal/platform/config

Fail-fast environment configuration (RFC §11.8). `Load()` reads every `cmd/*` binary's settings from the environment and validates them before anything else runs.

## Why this exists as its own package

"Fail-fast" means two specific things here, both load-bearing enough to be worth stating explicitly rather than left implicit in the code:

- **No secret has a default.** `JWT_SIGNING_KEY`, `POSTGRES_DSN`, `REDIS_ADDR`, and the rest of the `req()`-wrapped vars have no fallback value — a missing one fails boot, not first use. The alternative (silently falling back to a development constant) is exactly the kind of mistake that reaches production unnoticed.
- **Every error is collected, not just the first.** `Load()` doesn't return on the first missing/malformed var; it appends every validation failure into one slice and returns them together. An operator fixing a broken `.env` sees every problem in one run instead of fixing one var, rerunning, hitting the next one, repeating.

`PostgresConfig.SearchPath` and `TemporalConfig` are the two fields worth knowing about if you're new to this codebase: the former is schema-per-service's connection-level `search_path` (ADR-0024, differs per binary); the latter is all-optional with working defaults, deliberately — see `TemporalConfig`'s own doc comment for why requiring it would be disproportionate (only `course-api`/`worker` ever use it, and only at the moment of an actual publish, not at boot — ADR-0025).

## Testing

`config_test.go` (no build tag — pure env-var logic, no infra) verifies the fail-fast behavior directly: every required var missing individually and in combination (confirming the aggregation, not just that *an* error comes back), `JWT_SIGNING_KEY`'s length check, and that a malformed optional var (an int/bool/duration that fails to parse) is rejected rather than silently falling back to its default.
