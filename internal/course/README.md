# internal/course

Courses, versions, modules, lessons, and the publish lifecycle (RFC §4.3, §7).

## Why this module looks the way it does

**Immutable versions, blue-green promotion (ADR-0010).** A course has a `draft_version_id` (being edited) and a `published_version_id` (what students see). All authoring — `AddModule`, `AddLesson`, `UpdateDraft` — operates only on the draft. `Publish` is the one method that changes what a student can observe, and it does so in a single transaction (`Repository.PromoteVersion`): swap the pointer, mark the old version `superseded`, clear the draft pointer. A failed publish leaves the live version untouched.

**Course creation is a three-step transaction, in a specific order, on purpose.** `courses.draft_version_id` and `course_versions.course_id` are FKs pointing at each other. The course row must exist before the version row (which references it), and the version row must exist before the course row can point its `draft_version_id` at it. Getting this order wrong is a foreign-key violation, not a logic bug that passes tests by accident — `TestCoursePublish_FullLifecycle` exercises the real sequence against a real foreign key.

**`pq.StringArray`, not `[]string`, for the `tags` column.** GORM has no built-in `Valuer`/`Scanner` for a bare Go slice against a Postgres `text[]` column — a plain `[]string{}` silently serializes as SQL `NULL`, which then fails the column's `NOT NULL` constraint. Caught by this package's own integration test, not by inspection; see the comment on `CourseVersion.Tags` in `domain.go` if you're tempted to "simplify" it back to `[]string`.

**Ownership is checked in the service layer, never inferred from the URL.** Every draft-mutating method calls `requireOwner` first, which loads the course and compares `InstructorID` against the caller — not just `RequireRole("instructor")` at the route level, which only proves *a* role, not ownership of *this* course (RFC §11.4). `TestUpdateDraft_RejectsNonOwner` is the test that would fail if this check were ever accidentally dropped.

**Publish is a real Temporal workflow now (M2, ADR-0025), not a synchronous call.** `Service.Publish` starts `internal/workflows.PublishCourseWorkflow` (deterministic workflow ID `publish-{course_id}-{version_number}`) and returns immediately with a `202` and that workflow ID — it does not run `ValidateMetadata` or anything else inline. The workflow executes RFC §7.4's full graph — `ValidateMetadata` → `{VerifyAssets, ComputeMetadata}` → `BuildOutline` → `{IndexSearch, WarmCache}` → `PromoteAndPublish` — calling straight back into exported, Temporal-agnostic methods on this package's `Service` (this package never imports the Temporal SDK; `internal/workflows` imports `course`, never the other way). `GET /courses/{id}/publish-status` polls the result by reading `course_versions` directly, not Temporal's API — the state transitions are already written there by the activities as they run. `PromoteAndPublish` is the one activity whose effect a student can observe, and it's atomic with the `course.course_published` outbox write (M1's synchronous `Publish` did these as two separate transactions; Temporal's at-least-once activity execution required fixing that, since the method must now tolerate running twice). See ADR-0009 for why Temporal applies to publishing and nowhere else, and ADR-0025 for the full M2 record, including three bugs found only by actually running the workflow against a live Temporal server.

**`IndexSearch` and `WarmCache` are real, working pull-forwards from M3, not stand-ins.** `BuildOutline` assembles one outline shared by both: `IndexSearch` upserts `course_search_index` (full-text `tsvector`, served by `GET /courses/search?q=`, ranked separately from `ListCatalogue`'s plain `?q=` substring filter); `WarmCache` writes a Redis key scoped to the specific version number (`course:detail:{course_id}:v{n}`), which `GetPublishedDetail` reads through before falling back to Postgres. Neither has scheduled reconciliation yet (the same gap `internal/analytics` already carries per ADR-0017) — a document that drifts from its course's live data has no self-healing path until M3.

**The catalogue query is raw SQL, not GORM `Preload`.** Listing courses with instructor names is one indexed query, `LEFT JOIN`ed against `instructor_mirror` (not `users`), keyset-paginated (`WHERE (created_at, id) < (?, ?)`, never `OFFSET`) — RFC §13.2.

**This module has its own relay consumer now (`handlers.go`, ADR-0024), its first.** `identity` and `course` are different Postgres schemas, with no foreign key or join crossing that boundary — before ADR-0024 this is where `ListCatalogue` really did `JOIN users u ON u.id = c.instructor_id`, reading the same database directly, which was fine until the schema split made it a cross-schema join. `instructor_mirror` replaces it: a denormalized copy of every registered user's display name (not just instructors — `identity.user_registered` fires for everyone), kept current by consuming `identity.user_registered` and `identity.user_profile_updated` from the relay, the same way `internal/analytics`'s mirror tables work. `ListCatalogue` uses `LEFT JOIN` with `COALESCE(..., '')`, not an inner join — the mirror is populated asynchronously, so a course published in the same instant as its instructor's very first request could theoretically outrace it; that's a one-poll-interval gap, not a reason to drop the course from the catalogue entirely. `last_event_id` on `instructor_mirror` guards against the relay's worker pool committing a user's registered/profile_updated events out of order — see `internal/analytics/README.md` for the full explanation of why that race exists and how the guard works; this module hit the identical class of bug and uses the identical fix.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `GET /courses` | — | Catalogue, keyset-paginated (`?limit=&cursor=`), filterable (`?category=&level=&q=`) |
| `GET /courses/search` | — | Full-text ranked search over `course_search_index` (`?q=`, `?limit=`) — M2, ADR-0025 |
| `GET /courses/{slug}` | — | Published detail: version + modules + lessons, cache-read-through |
| `POST /courses` | instructor | Creates course + initial draft |
| `PATCH /courses/{id}` | instructor, owner | Edits the draft |
| `POST /courses/{id}/modules` | instructor, owner | |
| `PATCH /modules/{id}` | instructor, owner | Draft-only (same rule as `POST .../modules`) |
| `POST /modules/{id}/lessons` | instructor, owner (resolved via module → version → course) | |
| `PATCH /lessons/{id}` | instructor, owner (resolved via lesson → module → version → course) | Draft-only |
| `DELETE /modules/{id}`, `DELETE /lessons/{id}` | instructor, owner | |
| `POST /courses/{id}/publish` | instructor, owner | Starts the publish workflow, returns `202` — see above |
| `GET /courses/{id}/publish-status` | instructor, owner | Polls the workflow's result (route registered as `:slug`, not `:id` — a gin routing requirement, not a URL contract change; see `handler.go`) |
| `POST /courses/{id}/archive` | instructor, owner | Soft delete — see below |

### Why archive, not delete

There's no `DELETE /courses/{id}`. Deleting a course a student has enrollment, progress, or (eventually) a certificate against would orphan every row referencing it — this is the RFC's documented soft-delete design, not a gap. `POST /courses/{id}/archive` sets `archived_at`; `ListCatalogue` and `enrollment.Service.Enroll` both already gate on `archived_at IS NULL` (the latter predates this endpoint — it was written anticipating archival), so one column takes effect everywhere a student-facing read or write touches the course. There's deliberately no "unarchive" — nothing in the brief calls for one, and adding it speculatively just to look complete isn't the goal.

Archiving also writes `course.course_archived` to the outbox, in the same transaction as `archived_at`. `internal/enrollment`'s relay handler consumes it to withdraw every active enrollment on that course — archiving a course outrolls its active students, not just closes it to new ones. See `internal/enrollment/README.md` for that side; the point of the event, from here, is that this module never calls into `enrollment`'s Go API to make that happen — the outbox is the only channel (RFC §4.4/ADR-0006).

### Why module/lesson updates are draft-only

`UpdateModule`/`UpdateLesson`, like `AddModule`/`AddLesson`, reject a module or lesson that doesn't belong to the course's current draft version. A published version is immutable by design (ADR-0010) — allowing an in-place edit to it would let a student observe a course change mid-session with no publish event and no outbox write, which is exactly the inconsistency blue-green promotion exists to prevent.

### A confirmed bug in `UpdateLesson`, and the fix

The first version of `UpdateLesson` unconditionally overwrote the lesson's `content` column with `json.Marshal(req.Content)`. `Content` has no `required` binding (it never did — `CreateLessonRequest` doesn't require it either), so a PATCH that only meant to change `duration_s` — the ordinary way to call PATCH — bound `req.Content` to `nil`, marshalled it to the literal 4-byte JSON `null`, and silently destroyed the lesson's real content. Caught by a live integration test, not by inspection: a lesson seeded with real content had its DB `content` column become `null` after an update that never touched content at all.

Fixed: `nil` (the field omitted, or sent as an explicit JSON `null`) now means "leave the stored content alone." An explicit `"content": {}` is a real, non-nil empty map, and still overwrites — the distinction is deliberate, not an accident of Go's JSON decoding. `TestUpdateLesson_OmittingContentPreservesIt` is the regression test.

## Testing

Spread across several files, all build tag `integration`, real Postgres/Redis via `test/integration/testenv`:

- **`service_integration_test.go`** — the full create → author → publish → catalogue → detail → search lifecycle; blue-green state transitions; ownership rejection; invalid-slug rejection; `ValidateMetadata` rejecting a draft with no modules; module/lesson update (including that a non-owner is rejected, that an edited lesson's duration is reflected after publish, and that omitting `content` on update preserves it rather than wiping it); course archive (hidden from catalogue and detail, non-owner rejected, `course.course_archived` written to the outbox exactly once); catalogue filtering by category, level, and title search. These tests drive the publish activities directly (`ValidateMetadata` through `PromoteAndPublish`, via `runPublishPipeline` in the test file) rather than calling `Service.Publish`, which starts a real Temporal workflow and needs a live server — see below for where `Publish` itself and the actual workflow are each tested.
- **`service_more_integration_test.go`** — the lower-priority methods `service_integration_test.go` didn't reach: `DeleteModule`/`DeleteLesson` (and the `ResolveModuleCourseID`/`ResolveLessonCourseID` lookups they depend on), `GetPublishStatus`'s non-owner and not-found branches, and `GetPublishedDetail`'s cache-miss-falls-back-to-Postgres-and-refills-the-cache path — every other test reaches `GetPublishedDetail` only after `WarmCache` has already populated Redis, so this path was otherwise dead as far as tests were concerned.
- **`publish_integration_test.go`** — `Service.Publish` itself, the method that actually calls `ExecuteWorkflow`, tested with a fake `client.Client` (embedding the real interface, overriding only `ExecuteWorkflow`) rather than a live Temporal server: that it builds the right deterministic workflow ID and `StartWorkflowOptions`, that it writes `course_versions.workflow_id`/`state` via `SetVersionPublishing` afterward, that a course with no draft never reaches the fake client at all, and that the fake client returning an error becomes `apperrors.ErrUnavailable` without persisting anything.
- **`handlers_integration_test.go`** — the real `handleUserRegistered`/`handleUserProfileUpdated` relay handlers, exercised through a real outbox event and a real relay (not a direct call to `UpsertInstructorMirror` the way `instructor_mirror_integration_test.go` does for the ordering-guard case specifically) — proves the actual payload-decoding path works, including a malformed-payload case.
- **`pagination_integration_test.go`** — `ListCatalogue` with a real, non-empty cursor (every other test only ever reads page 1): publishes more courses than fit on one page, follows `NextCursor`, confirms no duplicates and consistent ordering across the page boundary.
- **`course_http_test.go`** — this module's HTTP-level test tier: boots the real `gin.Engine`/`RegisterRoutes` (the regression test for the gin wildcard-routing panic found live, ADR-0025), binding-validation rejection, `RequireAuth`/`RequireRole` rejection, and one full authenticated round trip over real HTTP.
- **`instructor_mirror_integration_test.go`** — the `last_event_id` out-of-order-delivery ordering guard specifically (ADR-0024), via a direct repository call rather than the relay (that's what `handlers_integration_test.go` covers instead).

The workflow itself — `internal/workflows.PublishCourseWorkflow` — is tested in `internal/workflows/`, not here: `publish_workflow_integration_test.go` for real activities against real Postgres/Redis (Temporal's own `testsuite.TestWorkflowEnvironment`, no live server), `publish_workflow_test.go` for pure orchestration logic with mocked activities. Run: `make test-integration`. Full index: `docs/guides/testing.md`.
