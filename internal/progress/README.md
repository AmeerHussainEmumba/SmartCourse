# internal/progress

Lesson-level progress and course completion (RFC §4.3, §8.3).

## Why this module looks the way it does

**It never inserts `lesson_progress` rows — only updates them.** Rows are pre-created by `internal/enrollment` at enrollment time (RFC §8.1 step 9). This module's job starts after that: mark a row complete, and check whether that completion finishes the course.

**`MarkComplete` is idempotent by construction, not by a special case.** The `UPDATE ... WHERE completed_at IS NULL` in `repository.go` means calling it twice on an already-completed lesson affects zero rows the second time — no branch needed to detect "already done," no risk of double-counting. `TestMarkComplete_IsIdempotent` asserts `completed_at` is byte-for-byte identical across both calls.

**Completion is checked against the CURRENT published version, not the version the student enrolled in** (RFC assumption A5). If an instructor republishes a course with a new lesson, a previously-completed student can correctly move back to incomplete — `isFullyComplete` in `repository.go` always counts lessons in `courses.published_version_id` at check time, never a version pinned at enrollment.

**Certificate issuance is not implemented here yet — deliberately, matching the RFC's own milestone plan.** RFC §8.4 specifies certificates are issued asynchronously, triggered by the `CourseCompleted` event via a Kafka consumer — infrastructure that's explicitly M3 scope (RFC §21). `MarkComplete` already writes the `progress.course_completed` outbox event on every transition, so M3's consumer has something to consume from day one; it just doesn't exist yet. This is not a silent gap — see the comment in `service.go`'s `MarkComplete`.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `PUT /enrollments/{id}/lessons/{lessonKey}/complete` | owner | Idempotent |
| `GET /enrollments/{id}/progress` | owner | |

## Testing

Integration tests (`service_integration_test.go`, build tag `integration`, real Postgres): completion transitions only once every lesson is done (not before); idempotent re-marking; ownership rejection on both `MarkComplete` and `GetProgress` (the latter previously had zero coverage — `TestGetProgress_ReflectsRealStateAndRejectsNonOwner` is the regression test, asserting it reflects real marked-complete state, not just an empty/default shape).

HTTP-level tests (`progress_http_test.go`, build tag `integration`, real `gin.Engine`): `RegisterRoutes` boots without panicking (the regression test for a router-panic bug class found live in `internal/course`, invisible to any Service-level test since it never touches gin's router at all); both endpoints reject a request with no `Authorization` header; a full authenticated round trip — mark a lesson complete, then read it back via `GetProgress` — works end to end over real HTTP; ownership is rejected (403, `not_your_enrollment`) at the HTTP layer the same way the service-level tests already prove it at the Go-call layer.

Run: `make test-integration`. Full index: `docs/guides/testing.md`.
