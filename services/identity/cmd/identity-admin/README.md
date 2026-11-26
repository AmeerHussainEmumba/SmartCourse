# services/identity/cmd/identity-admin

Creates the platform's first admin. The HTTP API can only grant admin to an existing admin, so `tools/seed` and the E2E harness run this before they use it.

```
IDENTITY_ADMIN_EMAIL     required
IDENTITY_ADMIN_PASSWORD  required (registration rules: 8 to 128 characters)
IDENTITY_ADMIN_NAME      optional, default "Administrator"
docker compose run --rm -e IDENTITY_ADMIN_EMAIL -e IDENTITY_ADMIN_PASSWORD identity-api identity-admin
```

It also needs `POSTGRES_DSN` (`svc_identity`), `POSTGRES_SEARCH_PATH=identity,public` and `REDIS_ADDR` (groups Postgres, Redis; `ARGON2_*` are validated, they have defaults). It signs no token, so `JWT_SIGNING_KEY` is NOT needed, and it reads no Kafka, MinIO or MongoDB setting (`requirements` in `main.go`, `config_test.go`). The binary is built into the identity-api image.

Behaviour: `Service.BootstrapAdmin` validates with the registration rules, hashes with the configured Argon2 settings and, in one transaction, writes the user, `identity.user_registered`, the role change to admin and `identity.user_role_changed`. An existing account with that email is left untouched (never promoted, password unchanged) and the command exits 0 with "already exists". The password and its hash are never printed. The email is marked verified; no mail is sent. Exit codes: 0 created or exists, 1 failure, 2 missing or invalid input.

Tests: `main_test.go` (exit codes, no secret in output, default name), `main_integration_test.go` (real Postgres as `svc_identity`: twice gives one admin and two outbox rows), and the service-level `identity/bootstrap_integration_test.go` (validation, concurrency, existing student not promoted).
