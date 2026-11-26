# Findings from the adversarial test phase

> **Status update (2026-11-17, later the same day): all 36 findings are fixed** on branch `refactor/module-split-fixes`, by four fixing sessions with non-overlapping directory ownership, merged and verified (full suite green; every reproducer now passes and is a regular regression test with its original name). The register below is kept as the record of what was found. Read the section "How each was fixed, and what is still imperfect" at the end for the design choices and the trade-offs that remain. The original text, which says nothing is fixed, describes the state when the register was written.

> **Superseded in part by the microservices split (ADR-0033..0035, `docs/plans/microservices-split.md`), 2026-11-26.** The register below describes the code as of 2026-11-17 and is kept as written. Some file paths and process names have moved; most `services/course`, `services/identity` and `services/analytics` domain paths are unchanged. Where the finding names the old shared worker, the in-process relay handlers or the shared `platform.outbox`, read it with this mapping (verified against the tree on 2026-11-26; line numbers are not re-checked):
>
> | Finding | Old location or process | Now |
> |---|---|---|
> | F-04 (claim released before handlers), F-05 (handler panic kills the worker), F-28 (`outbox.last_error`, give-up path) | `libs/relay/relay.go` running handlers in the shared `services/worker` (relay and Temporal worker in one process) | `libs/relay/relay.go` is now publish-only (the claim and the publish path stay there); handlers run as Kafka consumers in `libs/eventbus/consumers.go`, inside each service's own worker (`course-worker`, `analytics-worker`, `notification-worker`); the Temporal worker is `publishing-worker`, a separate process; the outbox is `identity.outbox` / `course.outbox` |
> | F-09 (`enrollment_mirror` ordering, "4-worker pool") | analytics handlers run by the relay pool in the shared worker | `analytics-aggregator` consumer in `services/analytics/cmd/analytics-worker` (Kafka, per-partition order; the fix itself is unchanged in `services/analytics/analytics`) |
> | F-16 (`http.Server` timeouts and body cap) | `services/*/cmd/*/main.go` | per-binary `services/<service>/cmd/<binary>/{main,wire}.go` over `libs/runtime/server.go` and `libs/router` |
> | F-19 (seed passwords) | `tools/seed/main.go` | same path; the seed now drives the HTTP APIs (`identity-admin` creates the first admin) |
> | F-37 (publish state before the workflow) | `services/course/course` and the worker's workflow | claim stays in `services/course/course`; the workflow is `services/publishing/publish`, the activities `services/course/activities` run in `course-worker` |
> | all others | `services/{identity,course,analytics}/...`, `libs/...` | unchanged paths |

**Date:** 2026-11-17 · **Companion to:** `docs/plans/module-split.md` (phase 5), `docs/plans/module-split-assessment.md`

## Purpose

Two separate testing sessions, one on behaviour and concurrency and one on security and abuse, tried to break the system after the module split. This is the consolidated register of what they found in **production code**. It is a list of defects, not a to-do list: **none of these has been fixed.** The testers were told not to change production code, and fixing behaviour is outside what the module split was asked to do. Each finding has a reproducer; deciding what to fix and in what order is for the project owner.

## How to read this

- **Severity is the tester's assessment**, not an agreed rating. Treat it as a starting point for triage.
- **Verification:** the full default suite passes with all the new tests merged. Reproducers live behind the build tag `bugrepro` and **fail today by design**; run one with `cd services/course && go test -tags='integration bugrepro' -run <Name> ./enrollment/`. Three of the reproducers (F-01, F-06, F-08) were re-run independently by the lead and failed with exactly the described symptom; the rest are the testers' reports.
- **F-01 was found independently by both testers** (the behaviour tester and the security tester reached it from different directions), which raises confidence in it.
- **Reachability caveat:** no production code creates a second course version today (only `CreateCourse` creates a version), so findings that need a republish (F-07, and the republish halves of F-08 and F-20) are reachable only through direct SQL until a republish feature exists. They are latent defects in code that will matter once republish lands. F-08's other half (a failed first publish leaving the course searchable) is reachable today.
- 36 open findings at the time of writing: 3 high, 16 medium, 2 low-medium, 14 low, 1 informational. A 37th (F-37, below) was reported afterwards by the supervisor; all 37 are now fixed. Two further issues the testers found in this repository's own guard scripts were fixed (listed at the end).

## High

| ID | Where | What is wrong | Reproducer | Suggested fix |
|---|---|---|---|---|
| F-01 | `services/course/enrollment/service.go:120-150` (`Withdraw`) | Status is read before the course row lock and the guarded `UPDATE`'s row count is ignored, so a concurrent double withdraw (double-click, retry) decrements `enrollment_count` twice and writes two `student_withdrawn` events. Seats leak, so a limited course can later be oversold (ADR-0012's guarantee breaks on this path); with one enrollment the `count >= 0` CHECK turns it into a 500. Deterministic. | `TestBugrepro_ConcurrentDoubleWithdraw_DriftsEnrollmentCount` (enrollment); `TestBug_Withdraw_ConcurrentDoubleWithdraw_LeaksSeats` (securitytest) | Re-read status under the course lock, or decrement only when the `UPDATE` affected a row |
| F-02 | `libs/router/router.go:24` | `gin.New()` with no trusted-proxy setting, so any `X-Forwarded-For`/`X-Real-IP` is believed and the per-IP rate limiter keys on it. Rotating the header bypasses the register and login limits; combined with F-26 (argon2 memory) it is a resource-exhaustion route. | `TestBug_ClientIPIsSpoofableViaForwardedHeaders` (libs/router); `TestBug_RateLimit_PerIPLimitBypassedBySpoofedForwardedFor` (identity) | `SetTrustedProxies` from config, default to none |
| F-03 | `libs/auth/middleware.go:104`; `services/identity/identity/service.go:316` | Revocation state lives only in Redis and a missing key counts as token version 1. After a Redis restart, flush or eviction, a demoted admin's old token works again on admin routes. | `TestBug_RedisFlush_ResurrectsRevokedAdminToken` (identity) | On a cache miss read `users.token_version` from Postgres and repopulate |

## Medium

| ID | Where | What is wrong | Reproducer | Suggested fix |
|---|---|---|---|---|
| F-04 | `libs/relay/relay.go:201-218` (`poll`) | The `FOR UPDATE SKIP LOCKED` claim transaction commits immediately, so the lock is gone before any handler runs. ADR-0006 and the code comment ("without double-processing a row") are false for multiple replicas: a second replica runs the handler concurrently and then fails on the `processed_events` unique violation. In-transaction effects stay safe through idempotency; non-transactional side effects would double. | `TestBugrepro_TwoRelays_SameEventRunsConcurrently` (libs/relay) | Hold the claim transaction through handling, or add a lease column |
| F-05 | `libs/relay/relay.go:253-258` | No `recover()` around handler execution. A panic in any handler kills the whole worker process (relay and Temporal worker); the row stays unpublished, so it crash-loops. | `TestBugrepro_HandlerPanicCrashesWholeProcess` (libs/relay) | `recover()` in `process()`, count it as a failed delivery |
| F-06 | `services/course/progress/service.go:27-70`, `repository.go:52-78` | No per-enrollment lock under READ COMMITTED: two concurrent completions of the last two lessons each see only their own update, so neither completes the enrollment. All lessons end complete but the enrollment stays `active` with no `course_completed` event, until a later `MarkComplete`. | `TestBugrepro_ConcurrentFinalLessons_EnrollmentNeverCompletes` (progress) | Lock the enrollment row, or re-evaluate completion after commit |
| F-07 | `services/course/progress/repository.go:52-62`, `enrollment/repository.go` (`initializeProgress`) | Progress rows are created only at enroll time. After a republish adds a lesson, existing enrollments have no row for it, `MarkComplete` silently does nothing, and the student can never complete. (Latent: needs republish, see caveat.) | `TestBugrepro_LessonAddedByRepublish_ExistingEnrollmentCanNeverComplete` (progress) | Create missing rows lazily, or on promote |
| F-08 | `services/course/course/repository.go:199-220` (`SearchCourses`) | Search filters on `archived_at` only, never on `published_version_id`, and `IndexSearch` runs before `PromoteAndPublish`. A course whose publish failed after indexing stays in search results but 404s on detail; during a republish search shows the new title while detail serves the old one. Contradicts ADR-0010's "readers never see a half-published version". | `TestBugrepro_SearchIndexesUnpublishedCourse_BeforePromote`, `TestBugrepro_SearchServesNewVersionMetadata_BeforePromote` (course) | Join on the published version; or index after promote |
| F-09 | `services/analytics/analytics/repository.go:82`, `handlers.go` | `enrollment_mirror` has no ordering guard (unlike `user_role_mirror`). If `student_withdrawn` is processed before `student_enrolled` (the 4-worker pool has no per-aggregate order) the withdrawal updates 0 rows and is lost; the mirror and `active_enrollments` end permanently wrong. | `TestBugrepro_WithdrawnBeforeEnrolled_MirrorEndsActive` (analytics) | Add `last_event_id` ordering as `user_role_mirror` has |
| F-10 | `services/course/course/service.go:256`, `:369` (`DeleteModule`, `DeleteLesson`) | Unlike the update paths, they never check the item belongs to the draft version, so the owner can delete modules and lessons of the published, supposedly immutable version. The cascade breaks live content, the detail cache keeps serving it, and students' completion denominators change. | `TestBug_OwnerCanDeleteModuleAndLessonsOfPublishedVersion` (securitytest) | Add the same draft-version check the update paths use |
| F-11 | `libs/auth/middleware.go`; `services/course/course/handler.go:35-60`; `services/identity/identity/handler.go` (`/me`) | Only admin routes check token freshness. After demotion or suspension the old access token keeps working everywhere else until its 15-minute TTL: a suspended user can call `/me`, a demoted instructor can still create, edit, publish and archive courses. | `TestBug_SuspendedUser_AccessTokenStillAccepted` (identity); `TestBug_DemotedInstructor_StaleTokenStillWrites` (securitytest) | Fresh-privilege check on instructor writes, or a shorter TTL |
| F-12 | `libs/config/config.go:164,179` | Only `len(key) >= 32` is checked, so the signing key published in `env.example` and `docker-compose.yml` is accepted with `APP_ENV=production`. Anyone who reads the repo can mint admin tokens for a deployment that kept the placeholder. | `TestBug_Load_ProductionAcceptsPublishedDefaultSigningKey` (libs/config); `scripts/check-dev-defaults.sh` | Reject known placeholders and low-entropy keys outside development |
| F-13 | `libs/router/health.go:35` | Unauthenticated `/health/ready` returns raw `err.Error()` per checker, exposing host, port, user and database from pgx/redis errors. | `TestBug_HealthReadyLeaksDriverErrors` (libs/router) | Return a boolean per dependency, log the detail |
| F-14 | `services/identity/identity/service.go:391-420`, `:475` | `used_at` is checked outside the transaction with no row lock. Ten concurrent confirms with one reset token all succeeded; one verification token was redeemed three times. A leaked token can race the victim. | `TestBug_PasswordResetToken_ConcurrentConfirm_MultipleSuccesses`, `TestBug_EmailVerificationToken_ConcurrentConfirm_MultipleSuccesses` (identity) | `UPDATE ... WHERE used_at IS NULL` and check rows affected |
| F-15 | `services/identity/identity/handler.go:45-52` | Reset, resend, refresh, logout and confirm routes have no rate limiter. Eighty reset requests all returned 202, creating 40 simultaneously valid tokens (earlier tokens are never invalidated): unlimited email bombing of a victim. | `TestBug_PasswordResetAndResend_NoRateLimit_EmailBombing` (identity) | Per-IP and per-address limits; revoke old tokens on a new request |
| F-16 | `services/*/cmd/*/main.go`; DTOs | `http.Server` sets only `ReadHeaderTimeout`: no read, write or idle timeout, and no body cap anywhere. A 16 MiB login body was fully read, an 8 MiB `category` was stored (201); `LoginRequest.Password` has no maximum. | `TestBug_NoRequestBodyLimit_LoginAcceptsHugeBody` (identity); `TestBug_UnboundedStringFields_And_NoBodyLimit` (securitytest); `scripts/check-http-hardening.sh` | Set the timeouts, `http.MaxBytesHandler` (about 1 MiB), `max=` tags |
| F-17 | `services/course/course/service.go`, `repository.go:379`, DTOs | Valid-JSON or valid-URL input gives 5xx: NUL bytes in search, filters, titles or lesson content; integers outside int32 (`enrollment_limit`, `position`, `duration_s`); deeply nested content that passes the size check but violates the jsonb CHECK. `q=%` and `_` act as LIKE wildcards (`q=%` matched every course). | `TestBug_NULBytes_Cause500`, `TestBug_OutOfRangeIntegers_Cause500`, `TestBug_CatalogueSearch_LikePatternInjection` (securitytest) | Reject NUL, bound ints, escape LIKE metacharacters, map pg 22021/22003/23514 to 400 |
| F-18 | `services/analytics/analytics/handler.go:24` | All three routes need only a valid token and registration is open, so any student can read platform totals and any course's enrollments, completion rate and daily series. Analytics stores no course owner, so ownership cannot be enforced without a schema and event change. | `TestBug_Analytics_StudentCanReadAnyCoursesStatsAndPlatformTotals` (analytics) | `RequireRole(instructor, admin)`; owner check once the mirror carries `instructor_id` |
| F-19 | `tools/seed/main.go:41,112` | Seeds `admin@smartcourse.dev` and every account with the constant password `SeedPass123!` and never checks `cfg.Env`. Pointed at a real DSN it creates a publicly known admin. | `scripts/check-dev-defaults.sh` (static) | Refuse unless `APP_ENV=development`, or generate passwords |

## Low-medium

| ID | Where | What is wrong | Reproducer |
|---|---|---|---|
| F-20 | `services/course/course/service.go:729` (`GetPublishedDetail`), `BuildOutline` | The whole detail payload, including `enrollment_count` and `enrollment_limit`, is cached for 24 hours, so the detail page shows the count frozen at publish time while the catalogue shows the live one. | `TestBugrepro_CachedDetailServesStaleEnrollmentCount` |
| F-21 | `services/course/course/service.go:417` (`Publish`), `:622` (`PromoteAndPublish`) | No `archived_at` check: an archived course accepts a publish and starts a workflow; promotion then emits `course_published` for a course nobody can see, and analytics counts it as published. | `TestBugrepro_PublishAcceptedForArchivedCourse` |

## Low

| ID | Where | What is wrong | Reproducer |
|---|---|---|---|
| F-22 | `services/course/progress/service.go:61-70` | `markEnrollmentCompleted`'s result is ignored, so concurrent same-final-lesson calls write a duplicate `course_completed` event. | `TestBugrepro_ConcurrentSameFinalLesson_EmitsDuplicateCompletionEvents` |
| F-23 | `services/course/course/service.go:622-640` | `PromoteAndPublish`'s idempotency guard is check-then-act outside the transaction, so concurrent executions emit duplicate `course_published` events (analytics is idempotent on it). | `TestBugrepro_ConcurrentPromoteAndPublish_EmitsDuplicateCoursePublishedEvents` |
| F-24 | `services/analytics/analytics/repository.go:162-170` | `IncrementDailyEnrollment` buckets by the DB's `current_date` at processing time, not the payload's `enrolled_at`; an enrollment at 23:59:59 UTC processed after midnight lands on the wrong day. | `TestBugrepro_DailyEnrollment_UsesProcessingDayNotEnrolledAt` |
| F-25 | `services/course/course/service.go:481-500` | `ValidateMetadata` requires a module but not a lesson, so a course of empty modules can be published yet can never be completed. | `TestBugrepro_CourseWithEmptyModuleCanBePublished` |
| F-26 | `libs/config/config.go:166-171` | `uint32(optInt(...))` wraps negatives: `ARGON2_MEMORY_KIB=-1` asks for about 4 TiB, `ITERATIONS=0` or `PARALLELISM=0` panics argon2; non-positive TTLs are accepted. 16 concurrent argon2 logins pinned about 1 GiB of heap and nothing bounds argon2 concurrency. | `TestBug_Load_AcceptsNonsensicalArgon2Parameters`, `TestBug_Load_AcceptsNonPositiveTokenTTLs` (libs/config) |
| F-27 | `services/course/progress/service.go:48` | `MarkComplete` for a `lesson_key` not in the enrollment returns success (204). | `TestBugrepro_MarkCompleteUnknownLessonKeyReportsSuccess` |
| F-28 | `libs/relay/relay.go:326-335` | `outbox.last_error` is never written; a given-up event is marked published with no trace in the row, and the give-up path does not persist the final attempt count. There is no dead-letter store (the code says so). | `TestBugrepro_GivenUpEventRecordsNoLastError` |
| F-29 | `libs/router/router.go:19-30`, `libs/httpx/httpx.go:31` | No security headers (nosniff, HSTS, frame, CSP, `Cache-Control: no-store` on token responses); a client-supplied `X-Request-Id` is echoed and logged with no length or charset check. | `TestBug_NoSecurityHeaders`, `TestBug_RequestIDNotValidated` (libs/router) |
| F-30 | `services/identity/identity/service.go:132`; `libs/auth/password.go:85-90` | `DummyHash` is hard-wired to 64 MiB / t=3 / p=2 and ignores `ARGON2_*`. With any other configured cost, known and unknown logins differ in time (35 ms against 505 ms with the test config): enumeration by timing. Equal with production defaults. | `TestBug_DummyHashIgnoresConfiguredArgon2_LoginTimingEnumeration` |
| F-31 | `services/identity/identity/service.go:132` | The account limiter counts every attempt, successes included, so six legitimate logins in a minute lock the owner out, and anyone can lock out any known email with five bad logins per minute. | `TestBug_RateLimit_SuccessfulLoginsConsumeFailedLoginBudget`, `TestBug_RateLimit_AnyoneCanLockOutAnyKnownEmail` |
| F-32 | `services/identity/identity/service.go:100`, `:154` | Register answers 409 for an existing email (enumeration); a suspended account answers 403 only for the right password, confirming credential validity. | `TestBug_Register_EnumeratesExistingEmails`, `TestBug_Login_SuspendedAccountRevealsPasswordCorrectness` |
| F-33 | `services/identity/identity/service.go:316-345` | A role change for a nonexistent user id writes a Redis `user:tv:<ghost>` key and an outbox row before returning 404; an admin can demote or delete themself or the last admin (no guard). | `TestBug_AdminRoleChange_NonexistentUserLeavesOutboxAndRedisResidue`, `TestBug_AdminCanSelfDemote_NoLastAdminGuard` |
| F-34 | `Dockerfile.*` | No `USER` in the final stage, so every service runs as root; the build image is the floating tag `golang:1-alpine`. | `scripts/check-dev-defaults.sh` |
| F-35 | `docker-compose.yml:51-52,320-322` | MinIO and the observability ports are published on all interfaces next to default credentials. | `scripts/check-dev-defaults.sh` |

## Informational

| ID | Where | Note |
|---|---|---|
| F-36 | `services/course/course/service.go:136` | Ownership denial answers 403 `not_owner` for an existing course and 404 for an unknown id: an existence oracle (UUIDv7 ids are partly guessable). `asset_key` is stored unvalidated; nothing serves it yet, so there is no traversal today. Reproducer: `TestBug_OwnershipCheck_DistinguishesExistingFromMissingCourses`. |

## Fixed during the phase

| What | Fix |
|---|---|
| `scripts/check-module-graph.sh` did not see a hand-written single-line `require <mod> <ver>` (found by `scripts/test-guards.sh`) | Guard rewritten; the mutation probe now passes |
| `go mod tidy` was not a no-op (stale `libs/cache/go.sum` lines, `golang-jwt` marked indirect in `services/identity`, `goleak` direct/indirect mismatch, missing `go.sum` lines) (found by `scripts/check-module-hygiene.sh`) | `go mod tidy` run in every module; the check now passes and runs in CI |

## Observations that are not defects

- Events with no registered handler are marked published immediately, so a consumer deployed later never sees earlier events (pinned by a characterization test). Relevant to M3. *(2026-11-26: no longer applies; the relay publishes every row to Kafka and each consumer group reads from the log, ADR-0033.)*
- `handleCourseArchived` withdraws enrollments without emitting `student_withdrawn`, so the analytics `enrollment_mirror` keeps them `active` until reconciliation; the handler's own comment says so.
- `course_mirror.published` is never cleared on archive; documented as "ever published".
- The rate limiter fails open when Redis is down; `RequireFreshPrivilege` fails closed with a 503. Pinned by a test. Worth stating as policy.
- Normal shutdown logs `relay poll failed: context canceled` at ERROR level, which is log noise. *(Not re-checked after the split.)*

## What held up

The testers also recorded what they attacked that did not break, which is what the assessment cites as verified: the seat limit under 150 contenders for 25 seats; two relays with no lost or doubled events; outbox rollback never delivered; poison events do not starve the queue; analytics replay is idempotent; blue-green readers never see a half-published payload; every JWT forgery tried (alg none, key confusion, tampered payloads, oversized tokens) rejected with a clean 401; refresh-token rotation and family revocation; no hidden cross-schema SQL in any service (checked by parsing the Go source and by running each service on a connection restricted to its own `search_path`); no resource leaks after 1,600 randomly cancelled requests; no IDOR across the 37 route shapes for 6 actors; no SQL, driver or stack text in any response apart from the generic 500 envelope.

## Not tested

A real Temporal server (only the test environment and a fake client); a true `kill -9` of the relay; a non-UTC database session timezone; Redis down entirely for the data path; real network behaviour (slow bodies, real proxy topology); MinIO and asset-key handling (no upload code exists); dependency CVE scanning; container runtime hardening beyond a static scan.

## How each was fixed, and what is still imperfect

All reproducers lost their `bugrepro` build tag and run in the default suite. None of the fixes needed a new config variable except the hardening settings in `env.example`; two needed a migration or none (see below). The fixers were told to report anything that was a judgment call; those are listed here so they can be overruled.

| ID | Fix | Caveat or decision to confirm |
|---|---|---|
| F-01 | `Withdraw` decrements the count and emits its event only if the guarded `UPDATE` changed a row; ADR-0012's course-row lock is untouched | none |
| F-02 | Gin trusts no proxy by default; new `TRUSTED_PROXIES` setting (IPs/CIDRs); an invalid entry stops the service starting | **A deployment behind a load balancer must set `TRUSTED_PROXIES`**, or every client shares the proxy's address and per-IP limits become global |
| F-03 | On a Redis miss the middleware reads `users.token_version` from Postgres through a new `auth.TokenVersionSource` and writes it back with `SET NX` (so a concurrent role change is never overwritten); Redis errors still fail closed | direct SQL that suspends a user without bumping `token_version` is caught only on a Redis miss |
| F-04 | Each relay worker claims one row (`FOR UPDATE SKIP LOCKED LIMIT 1`) in its own transaction, held through handling and the mark-published; no schema change | each worker now holds one extra DB connection while handling, so the pool needs about 2x the worker count; `WithBatchSize` is kept but is a no-op. See ADR-0027 |
| F-05 | `recover()` around handler execution; a panic is logged with the stack and counted as a failed delivery | none |
| F-06 | `MarkComplete` locks the enrollment row (not the course row, so no deadlock with `Withdraw`) | none |
| F-07 | Missing progress rows for the published version are created lazily in `MarkComplete`; `GetProgress` shows lessons without a row as incomplete | none |
| F-08 | Search joins on the published version and returns the live enrollment count; promotion refreshes the index in its own transaction | kept the IndexSearch-before-Promote order rather than indexing after promote (which would change the workflow graph) |
| F-09 | `enrollment_mirror` gets a `last_event_id` ordering guard like `user_role_mirror`; **migration** `migrations/analytics/000002_enrollment_mirror_ordering` | a withdrawal arriving first creates a placeholder row (`enrolled_at` NULL) that the enroll event fills in; the down migration deletes placeholders |
| F-10 | `DeleteModule`/`DeleteLesson` return 409 `no_draft` for a non-draft version | none |
| F-11 | `RequireFreshPrivilege` on all instructor write routes; `/me` uses the same check | one Redis `GET` per instructor write; fails closed with 503 if Redis is down |
| F-12 | Outside development, `Load` rejects known placeholder keys and keys with low entropy; the error never echoes the key | `APP_ENV` defaults to `development` when unset, so a deployment that forgets to set it stays lenient. **Set `APP_ENV` explicitly in every real deployment** |
| F-13 | `/health/ready` returns only `ok`/`failed` per dependency; detail goes to the log | response body shape changed |
| F-14 | One conditional `UPDATE ... WHERE used_at IS NULL ... RETURNING` inside the same transaction | none |
| F-15 | Per-IP limits on every unauthenticated identity route; a per-address email budget (5/hour, silent over budget so it cannot probe addresses); a new reset or verification token invalidates earlier unused ones | the limits (reset 5/min, refresh 60/min, and so on) are the fixer's choices |
| F-16 | New `httpx.MaxBodyBytes` (1 MiB default, `MAX_BODY_BYTES`) and `router.NewServer` setting read/write/idle timeouts (15s/30s/120s) | defaults are the fixer's choices; 413 `PAYLOAD_TOO_LARGE` is new |
| F-17 | NUL bytes rejected (400); lesson content depth capped at 32; integer upper bounds; LIKE metacharacters escaped; Postgres 22021/22003/23514 map to 400 | **the bounds are the fixer's choice and need confirming**: `enrollment_limit` 1,000,000, `position` 100,000, `duration_s` 86,400, category 100 chars, tag 50, asset_key 500. The `total_duration_s` column is a 32-bit INT, so a very large number of lessons could still overflow its sum (error ignored, as before) |
| F-18 | Rule given by the project owner: any authenticated user may read platform totals; per-course stats: admin any course, instructor any course, student only a course they have an active or completed enrollment in (from analytics' own mirror); everyone else 404 | **instructor ownership is not enforced** (the `course_published` event carries no instructor id), so an instructor can read any course. Enforcing it needs `instructor_id` in that event's payload, a mirror column and a backfill. A withdrawn student gets 404 (the fixer's choice) |
| F-19 | `tools/seed` refuses to run unless `APP_ENV=development` | none |
| F-20 | `GetPublishedDetail` overlays live `enrollment_count`/`enrollment_limit` on the cached payload | none |
| F-21 | `Publish` returns 409 `course_archived`; `PromoteAndPublish` refuses and emits nothing | the activity error is retryable, so Temporal retries it up to 5 times before failing the workflow |
| F-22 | One `course_completed` event, guarded by the row lock and the `UPDATE`'s row count | none |
| F-23 | `PromoteAndPublish` takes the course row lock and checks idempotency inside the transaction | none |
| F-24 | `IncrementDailyEnrollment` buckets by the payload's `enrolled_at` in UTC | none |
| F-25 | `ValidateMetadata` requires at least one lesson | none |
| F-26 | `ARGON2_*` and TTLs range-validated before narrowing (memory 8 to 1,048,576 KiB, iterations 1 to 100, parallelism 1 to 64, TTLs > 0) | **argon2 concurrency is still unbounded** (the register mentioned it; the item only asked for range validation), so a flood of logins can still use a lot of memory |
| F-27 | Unknown `lesson_key` returns 404 `lesson_not_found` | was 204 |
| F-28 | `outbox.last_error` and the final attempt count are persisted on failure and give-up | there is still **no dead-letter store** |
| F-29 | `httpx.SecurityHeaders` (nosniff, X-Frame-Options DENY, Referrer-Policy, a restrictive CSP, `Cache-Control: no-store`); HSTS only over TLS or with `HSTS_ENABLED=true`; client `X-Request-Id` kept only if 1-64 chars of `[A-Za-z0-9._-]` | none |
| F-30 | The dummy hash is built from the configured Argon2 parameters | none |
| F-31 | Only failures count; 5/min per account+IP and 20/min per account across all IPs, both checked before verifying the password | the all-IP cap went from 5 to 20 so the owner is not locked out by five failures from elsewhere; **a distributed attacker can still push an account to 20 failures a minute and block its logins from every IP for that minute**, which is inherent to a stuffing cap |
| F-32 | Register answers 202 with the same body whether or not the email exists (new: verification email; existing: password-reset email); a suspended or deleted account answers login like a wrong password | **API contract change** (was 201 with tokens, 409 for an existing email); a small timing difference remains between the new-email and existing-email paths |
| F-33 | The existence check runs first inside the transaction; a change that would leave no active admin returns 409 `last_admin` (serialised with an advisory lock) | self-demotion is allowed when another active admin exists (a legitimate handover) |
| F-34 | Final stage runs as uid 10001; build image pinned to `golang:1.26.5-alpine` | none |
| F-35 | All 18 published compose ports bind to `127.0.0.1` | developer stack only |
| F-36 | A non-owner of an existing course gets 404 `not_found`, identical to an unknown id | **API contract change** (was 403 `not_owner`); admins have no instructor write access, as before |

### Judgment calls: confirmed

The project owner reviewed the caveats above on 2026-11-18 and accepted them (integer bounds, rate-limit numbers, 404 for a withdrawn student on analytics, self-demotion when another admin exists, body and timeout defaults). They are recorded as DR-022 in `docs/rfc/differences-from-requirements.md`. The supervisor has not yet reviewed them; the module-split PRs are where she can.

### Tests changed to fit the fixes

Tests that pinned the old behaviour were updated, with the reason recorded in the commit: the three 403 `not_owner` tests became 404; analytics HTTP tests that used student tokens against any course now use instructor tokens; the register tests now expect 202 and then log in; `TestSecurity_RateLimit_AccountLimit_StopsDistributedStuffing` uses 20 failures across 20 IPs; `TestSecurity_RefreshForSuspendedUser_IsDenied_AndLoginToo` expects 401 on login. The identity security harness now serves on the real `libs/router` stack so the router-level fixes (F-02, F-16) are tested end to end.

### Still open, by design

Instructor ownership of analytics (F-18), argon2 concurrency (F-26), a dead-letter store (F-28), and the follow-ups noted above. None blocks the findings being closed; each is listed so it is not forgotten.

## Reported by the supervisor after the scan (2026-11-17)

The supervisor's Week 2 review raised two points: set the publishing state before starting the workflow and return 409 if a publish is already running, and withdraw can still double-decrement the seat count. Whether the QA scan had already covered each was checked against the code, not assumed.

| Point | Covered by the scan? | Status |
|---|---|---|
| **Withdraw can double-decrement the seat count** | **Yes: F-01**, found independently by both testers. The review most likely observed the pre-fix Week 2 code | Fixed. Re-verified on the current code: `Withdraw` decrements only when its guarded `UPDATE` changed a row, under the course row lock; the course-archive handler, the only other decrement, locks the same row and decrements by rows actually changed. The double-withdraw, churn and archive-race tests pass three times in a row with `-race` |
| **Set the publishing state before starting the workflow; 409 if a publish is already running** | **No.** Not in the register. The scan's publish test asserted that 20 concurrent calls share one workflow ID, which encoded the old attach-and-202 behaviour instead of questioning it | **F-37, fixed (ADR-0028).** See below |

### F-37 — publish wrote its state after starting the workflow, and never refused a duplicate (medium)

- **Where:** `services/course/course/service.go` (`Publish`) and `repository.go` (`SetVersionPublishing`, now removed).
- **What was wrong:** the workflow was started first and the `publishing` state written afterwards with an unconditional `UPDATE`. (1) A second publish while one was running attached to the running workflow and also returned 202, so a client could not tell a duplicate from a fresh publish. (2) A workflow that failed fast wrote `publish_failed` from inside an activity, and the late `publishing` write then overwrote it and cleared the error, leaving the version stuck "publishing" with a finished workflow. (3) A failed state write after a successful start left a running workflow with no record of it.
- **Fix:** `Publish` now claims the version with one conditional `UPDATE` (`ClaimPublish`) before starting the workflow; a duplicate gets **409 `publish_in_progress`**; a failed workflow start releases the claim and restores the previous state and error; a claim older than 15 minutes is treated as abandoned and may be reclaimed. New migration `migrations/course/000006_publish_started_at` (round-trip checked up, down, up).
- **Tests:** seven new tests in `publish_claim_integration_test.go` and `TestAdvPublish_ConcurrentPublishRequests_OneWinsRestGet409` (the old test, rewritten: exactly one of 20 concurrent calls wins, 19 get 409). Run against the old code the duplicate and fast-failing tests fail (`ok=20 conflict=0`; state overwritten to `publishing` with the error cleared), so they do detect the defect.
- **Contract change:** a duplicate `POST /courses/{id}/publish` is now 409, not 202.
- **Trade-offs:** the 15-minute lease is a chosen number; a process that dies between claiming and starting blocks that course's publish for up to 15 minutes. ADR-0028 records both and the trigger for replacing the lease with a Temporal liveness check.

### F-38 — instructors locked out of `course-api` after a Redis restart (caused by our own fix; found in live verification)

- **Found:** while smoke-testing F-37 against the real stack. After the compose stack was recreated (empty Redis, persistent Postgres), every instructor call to `course-api` returned `401 token_stale`, **including with a token issued seconds earlier**. The token's version (2) matched the database (2).
- **Cause:** a cross-effect of two fixes. F-11 put `RequireFreshPrivilege` on all instructor write routes. That variant has no durable source (`course-api` cannot read `identity.users` under ADR-0024), so on a Redis miss it assumes version 1 and rejects any token with a higher version. F-03 repopulates Redis from Postgres only on identity's own routes. Nothing repopulated it at login, so any user whose version is above 1 (anyone ever promoted, demoted or suspended-then-restored) stayed locked out of every course write after a Redis flush, until a role change happened to rewrite the key.
- **Why the tests missed it:** the course and security tests populate Redis themselves, and the live checks earlier in the session ran on a stack whose Redis still held the keys.
- **Fix:** identity now seeds the user's current token version in Redis at login and at refresh, with `SET NX`, so it can never overwrite a newer value written by a role change; it is best effort and cannot fail a login. After a flush, a user holding an old token gets one `token_stale` and a re-login restores access. `course-api` stays strict (a revoked token is still refused whenever Redis holds the current version). The tempting alternative, trusting the token on a miss, was rejected: it would let a revoked token through whenever Redis is empty, which is exactly the hole F-03 closed.
- **Tests:** three layers, because `services/course` cannot import identity. `TestFix_LoginAndRefreshAfterRedisFlush_SeedTokenVersion` (identity) covers the seeding and fails without the fix (`redis="", want 3`). `TestCourseAPI_InstructorWithVersionAboveOne_RedisFlush_RecoversOnceVersionSeeded` (`services/course/securitytest`) drives real `course-api` writes and pins the contract the seeding relies on: after a flush a version-2 token is refused with `token_stale` (never a 5xx); once the version is seeded the same token works with no role change; an older token and a demoted user's token are still refused; and `SET NX` never rolls a newer version back. It fails if the token-version check is removed (checked by mutation: the write succeeds with 201 after a flush). The end-to-end path (a real admin role change, real logins and a real refresh against real identity and course routes, after a flush) is automated in `test/e2e` (`TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks`, a new module because neither service's own tests can host it). It fails when identity's seeding is disabled (checked by mutation: `login should have re-seeded the cached version (want 2), got ""`) and it confirms revocation still holds after a restart plus a re-login.
- **Still imperfect:** between a Redis flush and a user's next login, their existing instructor token is refused on `course-api` writes (one `401 token_stale`, then re-login). A durable version source for `course-api`, for example a `token_version` column on `instructor_mirror` fed by identity events, would remove that window; it is a larger change and is not done.
