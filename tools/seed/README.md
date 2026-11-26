# tools/seed

Populates a running stack with a realistic dataset (instructors, courses at
every lifecycle state, students with partial progress) so a contributor gets
a working system with data in one command:

```
make seed
```

`make seed` does two things (see the `seed` target in the Makefile):

1. **Creates the first admin.** The HTTP API can only grant admin to an
   existing admin, so `docker compose run --rm -T identity-api identity-admin`
   creates it first, inside the identity-api image
   (`/usr/local/bin/identity-admin`, `services/identity/README.md`), from
   `SEED_ADMIN_EMAIL` / `SEED_ADMIN_PASSWORD` (defaults: the values documented
   in `env.example`, `admin@smartcourse.local` / `change-me-admin-password`).
   Idempotent: an existing admin is left untouched.
2. **Runs `go run ./tools/seed` on the host** against the published ports.

The seed is a plain HTTP client. It requires no service module (the module has
no dependencies at all, only the standard library), touches no database,
Redis, MinIO or Temporal, and so also exercises the real public contracts.
`scripts/check-module-graph.sh` enforces that it requires no service.

Needs the stack up: `make dev-workflow` (the `core` services plus Temporal and
the workers, which publishing needs) and `make migrate-up`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `IDENTITY_URL` | `http://localhost:8090` | identity-api |
| `COURSE_URL` | `http://localhost:8091` | course-api |
| `ANALYTICS_URL` | `http://localhost:8092` | analytics-api (health check only) |
| `SEED_ADMIN_EMAIL` | `admin@smartcourse.local` | the admin to log in as |
| `SEED_ADMIN_PASSWORD` | `change-me-admin-password` | its password (never logged) |
| `SEED_ALLOW` | unset | `1` allows non-loopback targets |
| `SEED_SKIP_PUBLISH` | unset | `1` skips everything that needs the publish workflow |
| `SEED_READY_WAIT_S` | `120` | how long to wait for each service's `/health/ready` |
| `SEED_PUBLISH_TIMEOUT_S` | `60` | how long to wait for one publish to reach `ready` |

## Safety

It creates accounts with a publicly documented password, so it refuses to run
unless every target URL is loopback (`localhost`, `127.0.0.0/8`, `::1`), or
`SEED_ALLOW=1` is set for a disposable environment; and it refuses when
`APP_ENV` is set to anything but `development`. It never logs passwords,
tokens or request bodies.

## What it does

1. Waits for identity-api, course-api and analytics-api `/health/ready`;
   fails with a message naming the dependency and exit code 1 if one does not
   become ready.
2. Logs in as the first admin. If that fails it says to run `identity-admin`.
3. For each instructor and student: logs in; if the login is refused,
   registers (`POST /auth/register`) and logs in; instructors are promoted
   through `PATCH /admin/users/{id}/role` and log in again (a role change
   invalidates the old token, ADR-0015 addendum).
4. For each course: creates it, its modules and lessons, publishes through
   `POST /courses/{id}/publish`, polls `publish-status` until `ready`, then
   enrolls the planned students (`POST /enrollments`) and marks lessons
   complete (`PUT /enrollments/{id}/lessons/{key}/complete`).
5. Prints a summary: accounts, the shared password, counts.

## What it creates

- 3 instructors and 12 students (plus the admin from `identity-admin`). Every
  instructor and student shares the password `SeedPass123!`, printed at the
  end of the run. This data never leaves a developer's own machine
  (ADR-0018's local-only compose profiles); it is not a credential that
  protects anything.
- 6 courses, one for every lifecycle state: a draft never published
  (Kubernetes in Production); a published course with no enrollments; a
  published course filled exactly to its enrollment limit (ADR-0012's
  seat-limit path); published courses with students not started, partway
  through and fully complete (the last exercises the course-completion event);
  and a published-then-archived course (hidden from the catalogue).

## Rate limits (DR-022)

identity-api allows 5 registrations and 20 logins a minute per client IP. Every
request carries its own `X-Forwarded-For` address, which the services believe
only when `TRUSTED_PROXIES` covers the peer (the same mechanism the E2E harness
uses for loopback; the dev compose stack does not set it). When the limit
applies anyway, the seed waits 20 seconds on a 429 and retries (up to 6 times
per request), so a first run against a stack with the limits active takes a few
minutes instead of failing. Set `TRUSTED_PROXIES=127.0.0.1` on identity-api for
a fast run on a bare-metal dev setup.

## Idempotency

Accounts are found by logging in, courses by their slug (a 409 on create),
enrollments by listing the student's own. A second run creates nothing and
prints `0 created, 15 already existed`; a run that was interrupted is topped
up. Limits: a course left as a draft, archived, or half-built by an earlier
interrupted run is not repaired (the API has no way to look up your own
unpublished course), and the seed warns when it sees a course that should be
published but is not; reset the database for a clean seed
(`docker compose down -v`).

## Without publishing

Publishing needs Temporal and the workers (`make dev-workflow`). If the publish
call keeps answering 503, or a course never reaches `ready`, the seed says so,
stops creating courses that need publishing, still creates the draft course
and all the accounts, prints which courses are missing and exits with code 2.
`SEED_SKIP_PUBLISH=1` asks for that on purpose and exits 0.

## Exit codes

0 done; 1 a dependency is not ready, a call failed, or the guard refused; 2
done except for courses that need publishing (publishing unavailable).

## Tests

`go test -race ./...` runs the unit tests against an in-process fake of the
three APIs (`fake_test.go`). The real-binary path (identity-api, course-api,
analytics-api against disposable Postgres and Redis, with `identity-admin`)
was exercised by hand and is covered end to end by the compose stack run, not
by a committed integration test: it would duplicate the E2E harness and pull
the container libraries into this otherwise dependency-free module.
