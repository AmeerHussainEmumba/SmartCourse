# services/course/certificate

Certificate issuance and retrieval (RFC §4.3, §8.4). A certificate is a record with a verification code, not a rendered document (RFC assumption A3).

## Why this module looks the way it does

**Issuance is a relay consumer of `progress.course_completed`, not part of `MarkComplete`.** RFC §8.4 makes it asynchronous: the completion transaction in `services/course/progress` stays small and writes the outbox event it always wrote; `Handlers()` here reacts to it. The module exports `func Handlers() []relay.Handler` and starts nothing itself. **The worker must register it** (`rl.Register(h)` for each of `certificate.Handlers()`); until it does, completions are not turned into certificates.

**Exactly once comes from the database, not from the handler.** `certificates.enrollment_id` is `UNIQUE` (RFC §5.3.4, failure mode F13) and the insert is `ON CONFLICT (enrollment_id) DO NOTHING`, so duplicate, replayed and concurrent deliveries are clean no-ops: no error, no second row, no aborted transaction. The relay's `processed_events` marker (written in the same transaction as the certificate) is the second line, and it cannot be the only one: two outbox rows for one enrollment have different `event_id`s, which the marker does not collapse. A cheap `SELECT` before the insert only avoids generating a code on a replay; correctness does not depend on it.

**Completion can be revoked, the certificate cannot (RFC §8.3, §8.4, assumption A5).** The RFC is explicit here, so this is not a deviation: "an already-issued certificate is never revoked" and certificates are immutable. This module has no update or delete path. Two consequences are implemented and tested:

- an issued certificate stays when its enrollment moves off `completed`;
- a `course_completed` event first handled after the enrollment moved back to `active` still issues, because the event reports a fact that was true when it was written. Only `withdrawn` is refused (see below).

Today nothing in `services/course` moves a `completed` enrollment back to `active` (`MarkComplete` treats `completed` as a no-op), so the tests simulate the regression with an `UPDATE`. If that transition is ever built, this module needs no change.

**Events that can never produce a certificate are acknowledged, not failed.** An event for an unknown enrollment, a `withdrawn` one (the status/timestamp CHECKs and `Withdraw`'s guard make completed-then-withdrawn unreachable, so such an event is inconsistent), or whose `student_id` does not match the enrollment returns `OutcomeSkipped`: a `slog` warning with the reason, no row, no error. Returning an error would only cycle the row through the relay's retries and then drop it silently ("mark published anyway", Week 3 Phase 4 replaces that with a dead-letter park). A malformed payload does return an error, so the relay records it in `last_error`. Choosing skip over park is a decision this module made because no parking store exists yet; revisit it when `failed_events` lands.

**The snapshot comes from `course`'s own tables.** `course_title` is the title of the published version (the one completion is judged against; the newest version if none is published); `student_name` is `instructor_mirror.full_name`, the mirror ADR-0024 introduced, never `identity.users`. The mirror is fed by identity events through a different relay path, so in principle it can lag the completion event; in that case the handler returns an error and the relay retries rather than issuing a nameless certificate. A student who registered before the mirror existed and never updated their profile has no mirror row; the certificate for them stays blocked until one exists. That gap is unverified here (no backfill was checked).

**The verification code is a bearer token.** 16 bytes from `crypto/rand`, unpadded base32 (26 characters, 128 bits), `UNIQUE`, and guarded by a `CHECK` on its shape. It is not derived from any id and not time-ordered.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `GET /certificates` | student | The caller's certificates, newest first, as `{"items": [...]}` |
| `GET /certificates/{id}` | owner | Non-owner and unknown id are the same 404 `not_found` (DR-022); malformed id is 400 |
| `GET /certificates/verify/{code}` | public | Returns only `course_title`, `student_name`, `issued_at`. Malformed code is 400 without a database read, unknown code is 404. 30 per minute per IP in `course-api` (`libs/ratelimit`, passed in as middleware by `wire.go`) |

A 200 from the public route means "this certificate was issued", not "the holder is currently complete": certificates are never revoked.

## Schema

`migrations/course/000007_certificates` extends the table 000003 created: `student_id`, `course_id`, `course_version_id` (nullable, the version completed), the code-format CHECK and two indexes. It is reversible; `TestMigration000007_DownThenUpAgain` runs down, up (with a populated row, checking the backfill) and down/up again. The RFC's table has no `student_id`, `course_id` or `course_version_id`; they are added so listing and the ownership check are single indexed reads (see the CHANGELOG).

## Not built here

- **No `progress.certificate_issued` event.** RFC §4.6 lists `CertificateIssued` for the archive and notifications, but `libs/events` has no schema or catalog entry for it, so writing the outbox row now would produce an event the codec cannot encode. Add the proto and catalog entry, then write the outbox row in `IssueForCompletion`'s transaction when `OutcomeIssued`.
- No rendered PDF (A3), no admin re-issue or revoke.
- No audit-log row for issuance (RFC §5.4 lists it; `audit_log` is Mongo, Week 3 Phase 4).

## Testing

Integration tests (build tag `integration`, real Postgres and Redis via `libs/testenv`):

- `service_integration_test.go`: issuance and snapshot, snapshot survives rename and retitle, replay and duplicate-with-new-id, **20 concurrent deliveries yield one certificate**, UNIQUE and code-format constraints, unknown / withdrawn / student-mismatch events acknowledged, malformed payload errors, name-not-mirrored errors then succeeds on redelivery, certificate survives the A5 regression, owner-only list and get, verify.
- `relay_integration_test.go`: through the real `libs/relay` worker pool: one event issues once; replay with the marker and with the marker deleted; 20 outbox rows for one enrollment across 8 workers; a poison row ahead of a valid one does not block it.
- `certificate_http_test.go`: 401 / 200 / non-owner 404 identical to unknown / malformed 400, public verify exposes exactly three fields and leaks no id or code, unknown 404, malformed 400, rate limit 429.
- `migration_integration_test.go`: migration 000007 down and up.
- `services/course/cmd/course-api/wire_test.go`: the real `registerRoutes` boots, keeps `POST /enrollments`, serves the certificate routes and rate-limits the public one.

Run: `make test-integration`. Full index: `docs/guides/testing.md`.
