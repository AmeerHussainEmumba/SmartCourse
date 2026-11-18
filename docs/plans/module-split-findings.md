# Findings from the adversarial test phase

**Date:** 2026-11-17 · **Companion to:** `docs/plans/module-split.md` (phase 5), `docs/plans/module-split-assessment.md`

## Purpose

Two separate testing sessions, one on behaviour and concurrency and one on security and abuse, tried to break the system after the module split. This is the consolidated register of what they found in **production code**. It is a list of defects, not a to-do list: **none of these has been fixed.** The testers were told not to change production code, and fixing behaviour is outside what the module split was asked to do. Each finding has a reproducer; deciding what to fix and in what order is for the project owner.

## How to read this

- **Severity is the tester's assessment**, not an agreed rating. Treat it as a starting point for triage.
- **Verification:** the full default suite passes with all the new tests merged. Reproducers live behind the build tag `bugrepro` and **fail today by design**; run one with `cd services/course && go test -tags='integration bugrepro' -run <Name> ./enrollment/`. Three of the reproducers (F-01, F-06, F-08) were re-run independently by the lead and failed with exactly the described symptom; the rest are the testers' reports.
- **F-01 was found independently by both testers** (the behaviour tester and the security tester reached it from different directions), which raises confidence in it.
- **Reachability caveat:** no production code creates a second course version today (only `CreateCourse` creates a version), so findings that need a republish (F-07, and the republish halves of F-08 and F-20) are reachable only through direct SQL until a republish feature exists. They are latent defects in code that will matter once republish lands. F-08's other half (a failed first publish leaving the course searchable) is reachable today.
- 36 open findings: 3 high, 16 medium, 2 low-medium, 14 low, 1 informational. Two further issues the testers found in this repository's own guard scripts were fixed (listed at the end).

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

- Events with no registered handler are marked published immediately, so a consumer deployed later never sees earlier events (pinned by a characterization test). Relevant to M3.
- `handleCourseArchived` withdraws enrollments without emitting `student_withdrawn`, so the analytics `enrollment_mirror` keeps them `active` until reconciliation; the handler's own comment says so.
- `course_mirror.published` is never cleared on archive; documented as "ever published".
- The rate limiter fails open when Redis is down; `RequireFreshPrivilege` fails closed with a 503. Pinned by a test. Worth stating as policy.
- Normal shutdown logs `relay poll failed: context canceled` at ERROR level, which is log noise.

## What held up

The testers also recorded what they attacked that did not break, which is what the assessment cites as verified: the seat limit under 150 contenders for 25 seats; two relays with no lost or doubled events; outbox rollback never delivered; poison events do not starve the queue; analytics replay is idempotent; blue-green readers never see a half-published payload; every JWT forgery tried (alg none, key confusion, tampered payloads, oversized tokens) rejected with a clean 401; refresh-token rotation and family revocation; no hidden cross-schema SQL in any service (checked by parsing the Go source and by running each service on a connection restricted to its own `search_path`); no resource leaks after 1,600 randomly cancelled requests; no IDOR across the 37 route shapes for 6 actors; no SQL, driver or stack text in any response apart from the generic 500 envelope.

## Not tested

A real Temporal server (only the test environment and a fake client); a true `kill -9` of the relay; a non-UTC database session timezone; Redis down entirely for the data path; real network behaviour (slow bodies, real proxy topology); MinIO and asset-key handling (no upload code exists); dependency CVE scanning; container runtime hardening beyond a static scan.
