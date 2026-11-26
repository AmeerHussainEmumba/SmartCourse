# Week 3 security and abuse findings (adversarial pass)

**Date:** 2026-11-25 · **Branch:** worktree of `week3/start-now` (049f561) · **Companion to:** `docs/plans/module-split-findings.md` (first pass), `docs/plans/week-3.md` sections 9 and 10, DR-022 to DR-026

> **Superseded in part by the microservices split (ADR-0033..0035, `docs/plans/microservices-split.md`), 2026-11-26.** This register describes the code as of 2026-11-25 and is kept as written. The "worker admin surface" (`services/worker/admin`, port 8093) is now the audit service's `audit-api` (host port 8093, `services/audit/admin`), which uses `RequireFreshPrivilegeStrict` (fail closed on a Redis miss, per its own source comment). Paths moved as follows (verified in the tree on 2026-11-26; whether each finding is fixed is **not** re-verified here):
>
> | Finding | Old path | Now |
> |---|---|---|
> | S-01, O-1, O-2, O-4 (admin surface) | `services/worker/admin/admin.go` | `services/audit/admin/admin.go` (`audit-api`) |
> | S-08, S-10, O-12 (notification templates and handlers) | `services/worker/notification/{templates,handlers}.go` | `services/notification/notification/{templates,handlers}.go` (`notification-worker`) |
> | S-08 (`failPublish`) | `services/course/workflows/workflow.go` | `services/publishing/publish/workflow.go` (`publishing-worker`); the activity error text originates in `services/course/course/assets.go` and runs in `course-worker` |
> | S-11 (`/metrics` on the public API port), O-8 (`:9100` on all interfaces) | `libs/infra/observability.go` (`RegisterMetricsRoute`), each API's `main`/wire; `libs/config/admin.go` | every binary serves `/metrics`, `/health/live` and `/health/ready` on an internal listener (`METRICS_HTTP_ADDR`, `libs/runtime/server.go`); `RegisterMetricsRoute` and `libs/config/admin.go` no longer exist. Compose publishes the internal listeners on 127.0.0.1 ports 9101 to 9104 (APIs) and 9111 to 9116 (workers) |
> | S-13 (floating image tags) | `docker-compose.yml` `minio/minio:latest` etc. | no `:latest` tag remains in `docker-compose.yml` as of 2026-11-26; MinIO and `mc` are the `pgsty` fork pinned by tag and digest (ADR-0019 amendment) |
> | S-15 (Mongo, Kafka, Registry without authentication) | `docker-compose.yml:403` | same services, compose line numbers have moved |
> | O-10 (`Dockerfile.worker` copies `libs/testenv`) | `Dockerfile.worker` | there is no `Dockerfile.worker`; one `Dockerfile.<binary>` per long-running binary (ten, plus `Dockerfile.schema-init`; the `identity-admin` CLI is built into `Dockerfile.identity-api`); not re-read for `libs/testenv` |
> | S-03, S-04, S-05, O-5 (idempotency) | `libs/idempotency`, `platform.idempotency_keys` | same library; table is `course.idempotency_keys` |
> | S-06, S-07, S-09, O-6, all others | `services/{analytics,course,identity}/...`, `libs/...` | unchanged paths |
> | Run commands below | `services/worker` (admin, notification) | `services/audit` (`./admin/`), `services/notification` (`./notification/`); the E2E tier now runs the ten-binary topology, and the older adversarial E2E files are in `test/e2e/legacy/` |

> **Status update, 2026-11-26 (libs/idempotency).** **S-03, S-04, S-05 are fixed** together with behaviour B-02/B-03: the response is stored byte for byte (`response_raw`, `response_headers`; migration `course/000009`). Regression tests: `TestS03_ResponseWithNULEscape_IsStoredHandlerRunsOnce`, `TestS04_ReplayIsByteIdentical`, `TestS05_EmptyBody2xx_IsIdempotent` in `libs/idempotency`. The register rows below are kept as written.

## Purpose and rules of engagement

A security-minded tester (malicious student, malicious instructor, admin with a stale token, network attacker) tried to break the finished Week 3 layer: worker admin HTTP surface, `/metrics`, certificates, analytics, `libs/idempotency`, asset upload, notification templates and SMTP, the dead-letter store and replay, compose exposure, JWT handling and token staleness, trace and request-id headers, error bodies, dependencies, config defaults, Dockerfiles, the E2E harness, and the project's own guard scripts. Authorised review of our own system on disposable local infrastructure only (testcontainers and the container harness). **No production code was changed.**

## How to read this

- Severity is the tester's CVSS-style judgement in one sentence, not an agreed rating.
- Reproducers are real tests behind the build tag `bugrepro` and **fail today by design**. Passing tests with `Adversarial`/`Held` in the name are the evidence that an attack held.
- Findings marked "possibly in flight" overlap with the fix agent's current work (fail-closed token version on the worker admin surface and analytics issues route, archive schema metadata, replay audit entries). They are reported anyway.
- A finding without a reproducer is listed under Observations with the reason.

Run commands (from the repository root):

| Module | Command |
|---|---|
| `services/worker` (admin) *(now `services/audit`)* | `cd services/worker && go test -tags='integration bugrepro' -run 'Bugrepro\|Adversarial' ./admin/` |
| `services/worker` (notification) *(now `services/notification`)* | `cd services/worker && go test -tags=bugrepro -run 'Bugrepro\|Adversarial' ./notification/` |
| `libs/idempotency` | `cd libs/idempotency && go test -tags='integration bugrepro' -run 'Bugrepro\|Adversarial' ./` |
| `services/analytics` | `cd services/analytics && go test -tags='integration bugrepro' -run 'Bugrepro\|Adversarial' ./analytics/` |
| `services/course` | `cd services/course && go test -tags='integration bugrepro' -run 'Bugrepro\|Adversarial' ./course/ ./certificate/` |
| guards and compose | `cd test/e2e && go test -tags=bugrepro -run 'Guard\|Compose' ./` |
| whole stack | `cd test/e2e && GOWORK=off go test -tags='e2e bugrepro' -timeout 25m -run TestE2E_Adversarial_Surface ./` |

Docker "context deadline exceeded" while a container starts is known flakiness; rerun the single test.

## Summary

| Severity | Count | IDs |
|---|---|---|
| High | 0 | |
| Medium | 4 | S-01, S-06, S-09, S-11 |
| Low-medium | 4 | S-07, S-08, S-10, S-12 |
| Low | 3 | S-03, S-13, S-14 |
| Informational | 3 | S-04, S-05, S-15 |

(S-02 is withdrawn: the payload-bomb probe passed and is recorded under "Held". S-12 groups six guard bypasses, S-12a to S-12f.)

## Medium

| ID | Where | What is wrong | Reproducer | Expected vs actual | Who is wrong |
|---|---|---|---|---|---|
| S-01 (possibly in flight) | `libs/auth/middleware.go:90-93,133-139`; `services/worker/admin/admin.go:75` | The worker admin routes use `RequireFreshPrivilege(rdb)`, which treats a missing Redis key as token version 1 (fail open on a cache miss). After a Redis flush, restart or eviction a demoted admin's old token is accepted again on list, detail and **replay** (the F-03 class, on the new surface). CVSS-style: network, needs a stale admin token plus a Redis loss event, high impact (replays events, reads payloads with PII), so medium. | `TestBugrepro_Admin_RedisFlush_ResurrectsDemotedAdminToken` (`services/worker/admin/adversarial_bugrepro_test.go`) | Expected 401/503 after the flush; actual 200 on `GET /api/v1/admin/failed-events` and on replay. | Code (use `RequireFreshPrivilegeWithSource` with a Postgres source) |
| S-06 (possibly in flight) | `services/analytics/analytics/handler.go:24-26` | `GET /analytics/issues` (dead-letter counts by consumer and event type, ages) has `RequireAuth` + `RequireRole("admin")` and no freshness check. A demoted admin's token (token version bumped in Redis) still gets 200 for its remaining TTL. Medium: operational information to a revoked admin. | `TestBugrepro_AnalyticsIssues_StaleAdminTokenAccepted` | Expected 401 `token_stale`; actual 200 with the report. | Code |
| S-09 | `services/course/course/handler.go:42` | Same F-03 class on the instructor surface: course writes use `RequireFreshPrivilege(svc.rdb)` with no durable source, so after a Redis flush a suspended or demoted instructor's stale token creates, edits, publishes, archives and requests upload URLs again. | `TestBugrepro_Course_RedisFlush_ResurrectsSuspendedInstructorToken` (`services/course/course/adversarial_bugrepro_test.go`) | Expected 401/503 after the flush; actual 201 on `POST /courses`. | Code |
| S-11 | `libs/infra/observability.go` (`RegisterMetricsRoute`), identity, course and analytics `main`/wire | `/metrics` is served unauthenticated on the **same port as the public API** of identity, course and analytics. The code comment says "it should not face the internet", but the API port is the one that does; only compose's loopback binding and an ingress filter stand between it and the world. Exposes route templates, traffic and error counts, DB pool state, runtime and process statistics, useful for reconnaissance. Medium (information disclosure, no auth, depends on deployment). | `TestE2E_Adversarial_Surface/Bugrepro_MetricsUnauthenticatedOnPublicAPIPort` (`test/e2e/adversarial_surface_e2e_test.go`, tag `e2e bugrepro`) | Expected 401/404 on the API port (serve metrics on a private listener, as the worker does); actual 200. | Code and deployment docs |

## Low-medium

| ID | Where | What is wrong | Reproducer | Expected vs actual | Who is wrong |
|---|---|---|---|---|---|
| S-07 | `services/analytics/analytics/handler.go:24-30` | Per-course analytics routes (enrollment counts, completion rate, daily series) accept a suspended or demoted instructor's stale token until it expires (F-11 class; only admin routes check freshness). | `TestBugrepro_AnalyticsCourse_StaleInstructorTokenAccepted` | Expected 401 after the token version bump; actual 200. | Code (or a recorded decision that 15 minutes of read access is accepted) |
| S-08 | `services/course/course/assets.go` (`verifyAsset`, error wrapping); `services/course/workflows/workflow.go:109-111` (`failPublish`); `services/worker/notification/templates.go` (publish_failed) | The error text of a storage failure (SDK message with host, port, bucket path) is part of `AppError.Error()`. `failPublish` stores the Temporal `ApplicationError.Message()` (the full chain, up to 500 chars) as `publish_error` and in `course.publish_failed`'s `reason`; the worker emails it to the instructor and analytics keeps it. A low-trust instructor can read internal infrastructure detail by publishing while storage is degraded, or via any activity error that wraps a driver error. | `TestBugrepro_VerifyAssets_StorageErrorTextLeaksIntoPublishReason` | Expected a generic reason ("asset storage unavailable") with detail logged only; actual `temporarily_unavailable: stat asset ... Head "http://minio.internal.svc:9000/..." dial tcp 10.42.7.19:9000 ...`. The test shows the error string; the chain from that string to the email is by code reading (`failPublish`, `writePublishFailed`, `delivery.go:114`), not exercised end to end. | Code |
| S-10 | `services/worker/notification/handlers.go:56-77`; `templates.go` (welcome) | The Welcome mail goes to every address that registers (open registration, address not verified first) and greets the recipient with the registrant's free-text `full_name` (up to 200 characters) from the platform's own sender and domain. An unauthenticated caller can make SmartCourse send "Hi <any text, including a phishing line and URL>" to any third-party mailbox at 5 a minute per IP. HTML is escaped and headers are safe, so this is content abuse (spam and phishing relay), not injection. | `TestBugrepro_WelcomeMail_CarriesAttackerControlledURLToThirdParty` (`services/worker/notification/adversarial_bugrepro_test.go`) | Expected: no stranger-controlled URL in the platform's mail (send Welcome only after verification, or strip URLs and reduce the name to a safe subset); actual: the URL is delivered verbatim. | Code |
| S-12 | `scripts/check-dev-defaults.sh` | The project's own guard can be bypassed by changes an attacker-minded developer could make. S-12a: check B is satisfied by any comment containing "production" and "placeholder/weak" in `libs/config/config.go`. S-12b: check D is satisfied by a comment containing "APP_ENV" in `tools/seed/main.go`. S-12c: check G only matches list-form ports, so `ports: ["5433:5432"]` or `0.0.0.0:...` inline (the form the whole file uses) is invisible. S-12d: check C only matches double-quoted literals, so backtick strings hide credentials. S-12e: check A only understands `KEY: value`, so list-form `- APP_ENV=production` plus `- JWT_SIGNING_KEY=...` passes. S-12f: the guard exits 1 on the repository itself (flags `test/e2e/harness/api.go:17` `Password = "correct-horse-battery-1"`, a throwaway test password), which trains people to ignore or allowlist it. | `TestBugrepro_Guard_PlaceholderKeyCheckSatisfiedByAComment`, `..._SeedProductionCheckSatisfiedByAComment`, `..._InlinePortsOnAllInterfacesNotReported`, `..._RawStringCredentialNotReported`, `..._ListFormEnvironmentNotChecked`, `..._FailsOnTheRepositoryItself` (`test/e2e/adversarial_guards_bugrepro_test.go`; scratch trees in temp dirs, repo untouched) | Expected each unsafe change to FAIL or WARN; actual: silent (S-12f: exit 1 on clean repo). | Guard scripts (and `test-guards.sh`, which passes 27/27 because it never tries these forms) |

## Low

| ID | Where | What is wrong | Reproducer | Expected vs actual | Who is wrong |
|---|---|---|---|---|---|
| S-03 | `libs/idempotency/idempotency.go:262-277` (`complete`, `storable`) | A 2xx response whose JSON contains a NUL escape (`\u0000`) cannot be stored (jsonb rejects it); `complete` fails, the key is **released**, and the retry runs the handler again. The "at most once per key" guarantee silently fails for that response. Needs a route that echoes unfiltered user text into a 2xx body (F-17's `rejectNUL` closes most today), so low. | `TestBugrepro_ResponseWithNULEscape_ReleasesKey_HandlerRunsTwice` (`libs/idempotency/adversarial_bugrepro_test.go`) | Expected 1 handler run per key; actual 2. | Code (store the body as `bytea`/`text`, or fail the response rather than release) |
| S-13 | `docker-compose.yml:44,62,322,336` | `minio/minio:latest`, `minio/mc:latest`, `temporalio/auto-setup:latest`, `temporalio/ui:latest` are floating tags; an upstream change or compromise reaches every developer machine and the E2E harness on the next pull. Every other image is pinned. | `TestBugrepro_Compose_ImagesAreNotFloatingTags` (`test/e2e/adversarial_compose_bugrepro_test.go`) | Expected pinned versions or digests; actual `:latest`. | Code (compose) |
| S-14 | `go.mod` toolchain 1.26.5 in all modules; `go.opentelemetry.io/otel/sdk@v1.44.0` | `govulncheck ./...` (run in every module of `go.work`, 2026-11-25) reports **reachable** vulnerabilities: standard library at go1.26.5 (net/http GO-2026-6859/6617/6613/6612/6611/6610/6605/6603, crypto/tls GO-2026-6607/6090/6089, crypto/x509 GO-2026-6809, net/textproto 6608, encoding/xml, encoding/asn1, net/url, **html/template GO-2026-6599/6600 via `notification.Render`**), fixed in go1.26.11; `otel/sdk` v1.44.0 (GO-2026-6974, attribute length limit bypass) fixed in v1.47.0; `otlptrace`/`otlptracehttp` v1.44.0 (GO-2026-6505, exporter logging may leak endpoint URLs) fixed in v1.45.0. services/worker alone: 23 findings. The Dockerfiles build with `golang:1.26.11-alpine`, so shipped images carry the patched stdlib; developer machines, `go test` and CI on 1.26.5 do not, and the OTel module findings apply to every build. | Command: `cd <module> && /tmp/gobin/govulncheck ./...` (install: `GOBIN=/tmp/gobin go install golang.org/x/vuln/cmd/govulncheck@latest`). Not a Go test: a dependency report cannot be a failing unit test without pinning a moving database. | Expected no reachable findings; actual as listed. | Code (bump `go` directive and otel modules) |

## Informational

| ID | Where | What | Reproducer |
|---|---|---|---|
| S-04 | `libs/idempotency/idempotency.go` (jsonb storage) | A replayed response is not byte-identical to the original: key order, duplicate keys and number formatting are normalised by jsonb. A client or signature check comparing bodies sees a difference. | `TestBugrepro_ReplayIsNotByteIdentical` |
| S-05 | `libs/idempotency/idempotency.go:253` (`storable`) | A 2xx with an empty body (204) is never stored, so the key is released and every retry re-executes. No current idempotent route answers 204 (latent). | `TestBugrepro_EmptyBody2xx_IsNotIdempotent` |
| S-15 | `docker-compose.yml:403` (`mongodb`), kafka, registry | MongoDB (failed events with payloads, audit archive), Kafka and Schema Registry run with no authentication. Loopback-bound in compose (held, see below), but nothing in config forces auth outside development; `MONGO_URI`/Kafka settings have no TLS or credential requirement check in `libs/config`. Observation: a deployment note, not a defect of the dev file. | none (no failing behaviour in dev; see Observations) |

## Observations (no reproducer, with reasons)

- **O-1 Payload at the 15 MiB cap is returned whole.** `GET /api/v1/admin/failed-events/{id}` returned a 15,729,033 byte body in about 0.26 s in-process for a cap-sized payload; the response has no size cap, so an admin (or stale admin token, see S-01) can pull large bodies repeatedly. Held for correctness (no 5xx, over-cap payload refused at park time), recorded here for sizing. Test: `TestAdversarial_Admin_PayloadBombAtCap` (passes; not a defect today).
- **O-2 Failed events carry PII to admins by design.** The detail route returns the payload (emails, names). Role gate is correct (`TestAdversarial_Admin_JWTForgeryMatrix`, `TestAdmin_Authorization`); there is no field-level redaction or audit of reads. A policy question for DR-022 owners, not a test failure. Replay audit entries are in flight.
- **O-3 Rate limiter fails open when Redis is down** (`libs/ratelimit/ratelimit.go`, documented, RFC section 13.4). During a Redis outage `GET /certificates/verify/{code}` is unthrottled; the 128-bit code (26 base32 characters, `crypto/rand`) makes enumeration infeasible, so this is accepted risk, but the register, login and reset limits fail open the same way.
- **O-4 No JWT audience claim and one signing key for every service.** `ParseAccessToken` checks signature, method (HMAC only), issuer and expiry; there is no `aud`, so a token minted for one service is valid at every other, and a leak of the single `JWT_SIGNING_KEY` is a leak for the worker admin surface too. Wrong issuer, expired, alg none, wrong key, non-UUID subject, odd role values and the opaque refresh token as an access token were all rejected (`TestAdversarial_Admin_JWTForgeryMatrix`). Token-type confusion is structurally impossible because refresh tokens are opaque, not JWTs.
- **O-5 Idempotency stores pre-signed URLs.** A replayed `POST /assets/upload-url` returns the stored response for 24 h while the URL expires after 15 minutes, so a retry after expiry gets a dead URL under the same key, and the bearer-style URL sits in `idempotency_keys` for a day. Low; no test written because it is a design property rather than a wrong behaviour.
- **O-6 Certificate issuance does not require `status = completed`.** `IssueForCompletion` only refuses `withdrawn` (`services/course/certificate/service.go:60-65`). Reaching this needs a forged or replayed `progress.course_completed` event (outbox or Kafka write access, or an admin replay of a stale record), so no test.
- **O-7 `traceparent` from any client is accepted** (`libs/telemetry/ginmw.go:40`), so a caller chooses the trace id and the sampled flag and can force sampling cost. Malformed and oversized values did not crash or inject (E2E subtest `Held_TraceAndRequestIDHeadersCannotInjectOrCrash`, passed). Low.
- **O-8 Worker admin (`:8093`) and metrics (`:9100`) default to all interfaces** (`libs/config/admin.go:19`, `telemetry.go:44`). Compose maps them to 127.0.0.1 on the host; in a pod network they are reachable. `/metrics` on `:9100` is unauthenticated by documented design.
- **O-9 `scripts/verify-deliverables.sh` defaults to the seed admin password** (`SeedPass123!`) and takes arbitrary base URLs from the environment, so pointing it at a non-local host sends the known credentials there. It only talks to the URLs given (default localhost); no real-service access happens by default.
- **O-10 `Dockerfile.worker` copies `libs/testenv`** (testcontainers code) into the build stage. It is not in the final image (final stage is `alpine:3.20`, `USER app`, no `ENV` secrets), but it widens the build supply chain.
- **O-11 Trailing-slash redirect** (`/certificates/verify/{code}/` answers 301) happens before the rate limiter; harmless.
- **O-12 Welcome-mail style text in `Hi {{.Name}}` is unrestricted** besides control characters; see S-10 (the mitigation there also covers course titles in the enrollment mail: an instructor controls the title a student receives).

## What held (attacks that failed, with evidence)

| Attack | Evidence |
|---|---|
| JWT forgery on the worker admin routes: alg none, wrong key, wrong issuer, empty issuer, expired, no expiry, non-UUID/empty subject, `Admin`/`admin `/empty/`superuser` roles, opaque refresh token and its hash as access token, truncated token, lowercase `bearer`, `Basic`, raw and doubled schemes | `TestAdversarial_Admin_JWTForgeryMatrix` |
| Student or instructor calling list/detail/replay; replay by a student changing state; 404 vs 403 existence oracle (identical 403 for known and unknown ids) | `TestAdmin_Authorization` (existing), `TestAdversarial_Admin_EventIDOddities` |
| NoSQL operator injection and parameter pollution through `status`, `consumer`, `cursor`, `limit` (including `{"$ne":null}`, `consumer[$ne]`, `status[$ne]`, duplicate params, negative, zero, overflow, `1e3`, `0x10`, non-UTF-8, over-long, tampered or oversized cursors) | `TestAdversarial_Admin_ListQueryInjection` (values are typed strings in `bson.D`; bracket forms are ignored, never operators; no 5xx, no Mongo text) |
| Path parameter abuse on detail/replay (NUL, traversal, SQL, emoji, 5000 chars, nil UUID) | `TestAdversarial_Admin_EventIDOddities` |
| Mongo/driver error with a DSN and credentials leaking through 503 bodies or headers (including `/health/ready`) | `TestAdversarial_Admin_StoreErrorsDoNotLeak` |
| Log and response injection through `X-Request-Id` (CR/LF, spaces, markup, 500 characters, NUL, non-ASCII) | `TestAdversarial_Admin_RequestIDInjection` |
| Replay of a record parked for an unknown or forged consumer name running another consumer's handler | `TestAdversarial_Admin_ReplayDoesNotCrossConsumers` |
| Payload bomb: cap-sized detail served, over-cap refused at park, garbage payload replay fails cleanly without a 5xx; 3 MiB replay body capped | `TestAdversarial_Admin_PayloadBombAtCap`, `TestAdversarial_Admin_ReplayHugeBodyIsCapped` |
| Idempotency: key length and charset (255 ok, 256, space, tab, DEL, NUL, non-ASCII, CR, LF rejected), duplicate-header smuggling, SQL metacharacters as keys, cross-user replay, cross-endpoint replay, query-string change under one key, oversized body (413) not leaving the key stuck | `TestAdversarial_KeyAndBodyAbuse`, `TestAdversarial_ReplayNeverCrossesUsersOrEndpoints`, `TestAdversarial_OversizedBodyIs413AndKeyNotStuck`; existing `TestKeysAreScopedPerUser` |
| Certificates: spoofed `X-Forwarded-For`, `X-Real-IP`, `Forwarded`, `X-Client-IP`, `True-Client-IP` do not bypass the 30/min limit on the production router; public view is exactly three fields; hostile and malformed codes are 400/404; unknown codes are indistinguishable; cross-tenant list and get answer like unknown ids; odd id forms never match | `TestAdversarial_Verify_RateLimitNotBypassedBySpoofedHeaders`, `TestAdversarial_Verify_FormatAbuseAndFieldMinimisation`, `TestAdversarial_Certificates_OwnerRoutesAbuse` |
| Analytics: non-owner instructor, unenrolled and withdrawn students get 404 identical to an unknown course; hostile `limit` and path ids never 5xx | `TestAdversarial_Analytics_ParameterAbuse`, `TestAdversarial_Analytics_WithdrawnStudentLosesAccess`; existing `TestInstructorOwnership_*` |
| Asset upload: other instructor's course answers like an unknown course; student 403; `text/html`, `image/svg+xml`, parameterised and CRLF/NUL content types, size 0, negative, int64 max and overflow, float and string sizes, traversal `course_id`/`lesson_id`, foreign lesson, mass-assigned `key`/`bucket` fields all rejected without 5xx; issued key is server-generated and course-scoped | `TestAdversarial_UploadURL_HostileRequests`; existing `TestAssetKey_*`, `TestStore_PresignedURL*` |
| SMTP and templates: header, MIME boundary and HTML injection from name, title, reason, activity and recipient name (CR/LF, U+2028, U+0085, NUL, encoded-word, markup, template syntax) in every template kind | `TestAdversarial_Message_NoHeaderOrMIMEInjectionFromAnyField`; existing `TestSMTP_HeaderInjectionIsNeutralised`, `TestSMTP_RecipientAddressInjectionRefused` |
| Compose exposure: every host port mapping is 127.0.0.1 | `TestAdversarial_Compose_EveryPublishedPortIsLoopbackOnly` |
| `/health/ready` on the worker returns bare words, not driver errors | `TestAdversarial_Admin_StoreErrorsDoNotLeak`, `TestAdmin_Health*` |
| Dockerfiles: final stage non-root `USER app`, no `ENV` secrets | `scripts/check-dev-defaults.sh` check F reported no warnings; `Dockerfile.worker` read |
| Secrets in logs: no `slog` call in the repository logs a DSN, URI, password or signing key (grep over production code) | static search, no hits; not a test |
| Existing guards: `scripts/check-http-hardening.sh` all controls present; `scripts/test-guards.sh` 27 passed, 0 failed | ran both |

Whole-stack results (E2E subtests `Held_MetricsCarryNoRequestData`, `Held_TraceAndRequestIDHeadersCannotInjectOrCrash`, `Held_ErrorBodiesLeakNothing`, `Held_WorkerAdminPortSeparationAndRoles`) are listed in the section "E2E run" below.

## E2E run

`TestE2E_Adversarial_Surface` ran twice on the real container stack (identity, course, analytics, worker processes; Postgres, Redis, Kafka, Mongo, Mailpit): `Bugrepro_MetricsUnauthenticatedOnPublicAPIPort` FAILED (S-11: identity 14,100, course 17,152 and analytics 9,574 bytes of metrics served with no credentials); `Held_MetricsCarryNoRequestData`, `Held_TraceAndRequestIDHeadersCannotInjectOrCrash` (control request 200, all 12 hostile requests answered with a status, no 5xx, nothing echoed, no forged log line in any process log), `Held_ErrorBodiesLeakNothing` (no SQL, DSN, driver, stack, path or key text in 14 hostile requests across the four services) and `Held_WorkerAdminPortSeparationAndRoles` (admin routes 401/403 for none, student, instructor and garbage tokens; `/metrics` not on the admin port and admin routes not on the metrics port; no `/debug/pprof`, `/debug/vars`, `/.env` and similar on any port) PASSED. The first run's quick pass of the trace subtest was re-verified with a control request after the second run was added, because a raw-socket client that silently fails would have passed it trivially.

## What was not covered

- Timing side channels (404 vs 403 response times) were not measured; only body and status equality were compared.
- No test against a real SMTP server with hostile recipients beyond the fake and Mailpit tests that already exist; mail bombing through many enrollments was reasoned about (each enrollment mails the enrollee only) but not load-tested.
- Kafka, Schema Registry, Temporal and Mongo were reviewed in `docker-compose.yml` only (ports and auth); no protocol-level attack on them.
- A demoted admin's stale token on the real stack after a real Redis restart is only covered by the in-process reproducers (S-01, S-09); the existing `redis_flush_e2e_test.go` covers the login re-seeding path.
- Argon2 and login lockouts were not re-tested (first pass: F-02, F-15, F-26); the new public route's only limiter is the certificate verify limit.
- Archive, audit and the replay audit entries (in flight) were not attacked.
- `govulncheck` output was triaged from the worker module in detail; the other modules show the same stdlib and otel entries (counts per module in the run log), their individual symbol traces were not reviewed.
- Pre-signed URL behaviour against MinIO (type and size binding, expiry, retargeting) was taken from the existing `store_integration_test.go` rather than re-attacked.
