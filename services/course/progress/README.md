# services/course/progress

Lesson-level progress and course completion (RFC §4.3, §8.3).

## Why this module looks the way it does

**It never inserts `lesson_progress` rows — only updates them.** Rows are pre-created by `services/course/enrollment` at enrollment time (RFC §8.1 step 9). This module's job starts after that: mark a row complete, and check whether that completion finishes the course.

**`MarkComplete` is idempotent by construction, not by a special case.** The `UPDATE ... WHERE completed_at IS NULL` in `repository.go` means calling it twice on an already-completed lesson affects zero rows the second time — no branch needed to detect "already done," no risk of double-counting. `TestMarkComplete_IsIdempotent` asserts `completed_at` is byte-for-byte identical across both calls.

**Completion is checked against the CURRENT published version, not the version the student enrolled in** (RFC assumption A5). If an instructor republishes a course with a new lesson, a previously-completed student can correctly move back to incomplete — `isFullyComplete` in `repository.go` always counts lessons in `courses.published_version_id` at check time, never a version pinned at enrollment.

**Certificate issuance is not part of `MarkComplete`.** RFC §8.4 makes it asynchronous: `services/course/certificate` consumes `progress.course_completed` as a relay handler and writes `progress.certificate_issued` in the transaction that inserts the certificate.

**Events written (outbox, aggregate `enrollment`, key `enrollment_id`).** `progress.lesson_completed` (`LessonCompletedPayload`: enrollment, student, lesson key, `completed_at`) is written when `markLessonComplete` changes a row, in the same transaction, so a lesson completes and is announced once: an idempotent repeat (RFC §8.3), a repeat on an already completed enrollment, a concurrent duplicate and a rejected request write nothing (added 2026-11-24, DR-026; lessons completed before that have no event). When the completion finishes the course it is followed by `progress.course_completed` (`CourseCompletedPayload`, unchanged on the wire). The payload structs live in `events.go`; this module does not import `libs/events`, and `services/worker/cmd/worker/payload_contract_test.go` runs them through the transport's strict codec so a drift from the schema fails in CI.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `PUT /enrollments/{id}/lessons/{lessonKey}/complete` | owner | Idempotent |
| `GET /enrollments/{id}/progress` | owner | |

## Testing

Integration tests (`service_integration_test.go`, build tag `integration`, real Postgres): completion transitions only once every lesson is done (not before); idempotent re-marking; ownership rejection on both `MarkComplete` and `GetProgress` (the latter previously had zero coverage — `TestGetProgress_ReflectsRealStateAndRejectsNonOwner` is the regression test, asserting it reflects real marked-complete state, not just an empty/default shape).

HTTP-level tests (`progress_http_test.go`, build tag `integration`, real `gin.Engine`): `RegisterRoutes` boots without panicking (the regression test for a router-panic bug class found live in `services/course/course`, invisible to any Service-level test since it never touches gin's router at all); both endpoints reject a request with no `Authorization` header; a full authenticated round trip — mark a lesson complete, then read it back via `GetProgress` — works end to end over real HTTP; ownership is rejected (403, `not_your_enrollment`) at the HTTP layer the same way the service-level tests already prove it at the Go-call layer.

Run: `make test-integration`. Full index: `docs/guides/testing.md`.
