# internal/course

Courses, versions, modules, lessons, and the publish lifecycle (RFC §4.3, §7).

## Why this module looks the way it does

**Immutable versions, blue-green promotion (ADR-0010).** A course has a `draft_version_id` (being edited) and a `published_version_id` (what students see). All authoring — `AddModule`, `AddLesson`, `UpdateDraft` — operates only on the draft. `Publish` is the one method that changes what a student can observe, and it does so in a single transaction (`Repository.PromoteVersion`): swap the pointer, mark the old version `superseded`, clear the draft pointer. A failed publish leaves the live version untouched.

**Course creation is a three-step transaction, in a specific order, on purpose.** `courses.draft_version_id` and `course_versions.course_id` are FKs pointing at each other. The course row must exist before the version row (which references it), and the version row must exist before the course row can point its `draft_version_id` at it. Getting this order wrong is a foreign-key violation, not a logic bug that passes tests by accident — `TestCoursePublish_FullLifecycle` exercises the real sequence against a real foreign key.

**`pq.StringArray`, not `[]string`, for the `tags` column.** GORM has no built-in `Valuer`/`Scanner` for a bare Go slice against a Postgres `text[]` column — a plain `[]string{}` silently serializes as SQL `NULL`, which then fails the column's `NOT NULL` constraint. Caught by this package's own integration test, not by inspection; see the comment on `CourseVersion.Tags` in `domain.go` if you're tempted to "simplify" it back to `[]string`.

**Ownership is checked in the service layer, never inferred from the URL.** Every draft-mutating method calls `requireOwner` first, which loads the course and compares `InstructorID` against the caller — not just `RequireRole("instructor")` at the route level, which only proves *a* role, not ownership of *this* course (RFC §11.4). `TestUpdateDraft_RejectsNonOwner` is the test that would fail if this check were ever accidentally dropped.

**Publish is synchronous (no Temporal) but its validation IS concurrent, genuinely.** `runPublishChecks` mirrors RFC §7.4's activity graph: `ValidateMetadata` gates first (deterministic, no retry — a failure is the instructor's error), then lesson-content validation and the totals recompute run concurrently via `errgroup` — two real, independent database round trips, not the same work split across goroutines for appearance. What's still missing is *durable, crash-recoverable* orchestration across a process restart mid-publish — that's what Temporal (M2) buys and an in-process `errgroup` doesn't claim to. `Service.Publish`'s doc comment marks exactly where `StartWorkflow` replaces today's call. See `docs/adr/smartcourse-adr.md` ADR-0021 for why the concurrent-validation piece was pulled forward from M2 rather than waiting.

**The catalogue query is raw SQL, not GORM `Preload`.** Listing courses with instructor names is one indexed query with a `JOIN` against `users`, keyset-paginated (`WHERE (created_at, id) < (?, ?)`, never `OFFSET`) — RFC §13.2. This reads the same database `users` table by SQL, not by importing `internal/identity`'s Go types, so it doesn't cross the module boundary `depguard` enforces.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `GET /courses` | — | Catalogue, keyset-paginated (`?limit=&cursor=`) |
| `GET /courses/{slug}` | — | Published detail: version + modules + lessons |
| `POST /courses` | instructor | Creates course + initial draft |
| `PATCH /courses/{id}` | instructor, owner | Edits the draft |
| `POST /courses/{id}/modules` | instructor, owner | |
| `POST /modules/{id}/lessons` | instructor, owner (resolved via module → version → course) | |
| `DELETE /modules/{id}`, `DELETE /lessons/{id}` | instructor, owner | |
| `POST /courses/{id}/publish` | instructor, owner | Synchronous in M1 — see above |

## Testing

Integration tests (`service_integration_test.go`, build tag `integration`, real Postgres via `test/integration/testenv`): the full create → author → publish → catalogue → detail lifecycle; blue-green state transitions; ownership rejection; invalid-slug rejection; publish-with-no-modules rejection. Run: `make test-integration`. Full index: `docs/guides/testing.md`.
