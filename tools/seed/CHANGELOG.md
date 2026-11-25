# Changelog — tools/seed

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Behaviour change (breaking for anyone running it by hand): the seed is now a plain HTTP client of identity-api, course-api and analytics-api (health). It no longer imports any service module or `libs/*`, no longer talks to Postgres, Redis, MinIO or Temporal, and `go.mod` has no requirements. It registers users, promotes instructors through `PATCH /admin/users/{id}/role`, creates courses, modules and lessons, publishes through the real endpoint and waits for `ready`, enrolls and marks progress over HTTP. Configuration is `IDENTITY_URL`, `COURSE_URL`, `ANALYTICS_URL`, `SEED_ADMIN_EMAIL`, `SEED_ADMIN_PASSWORD`, `SEED_ALLOW`, `SEED_SKIP_PUBLISH`, `SEED_READY_WAIT_S`, `SEED_PUBLISH_TIMEOUT_S`; `POSTGRES_DSN`, `REDIS_ADDR`, `MINIO_ENDPOINT`, `TEMPORAL_HOST_PORT` are no longer read.
- The first admin is now `SEED_ADMIN_EMAIL` (default `admin@smartcourse.local`, the `env.example` value), created beforehand by `identity-admin` (the Makefile `seed` target runs it through `docker compose run`); the seed logs in as it. The old `admin@smartcourse.dev` / `SeedPass123!` admin is gone; instructors and students keep `SeedPass123!`.
- Adds an archived course (publish, then archive) to the dataset, so every lifecycle state is present. Idempotency now logs in to detect accounts, uses the 409 on a taken slug for courses and lists the student's own enrollments, so an interrupted run is topped up instead of being a no-op.
- Safety: refuses non-loopback targets unless `SEED_ALLOW=1`, and refuses when `APP_ENV` is set to anything but `development`. Waits out a 429 (register 5 a minute per IP, DR-022) and sends a distinct `X-Forwarded-For` per request. Exit codes: 0 done, 1 failure or refusal, 2 done without the courses that need publishing (Temporal or the workers unavailable); `SEED_SKIP_PUBLISH=1` skips them on purpose and exits 0.
- Makefile `seed` target: runs `identity-admin` through `docker compose run --rm identity-api`, then the seed on the host; the POSTGRES/REDIS/MINIO/TEMPORAL address rewriting and `SEED_TEMPORAL_HOST_PORT` are removed.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

- Docs only: `make seed` now overrides compose-internal addresses for the host-run seed (`TEMPORAL_HOST_PORT` always; `POSTGRES_DSN`, `REDIS_ADDR`, `MINIO_ENDPOINT` when they name the compose-internal host). The change is in the Makefile; no Go code changed.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
