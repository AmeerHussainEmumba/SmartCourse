# internal/analytics

Platform and course-level aggregate metrics (RFC §4.3, §9; SmartCourse.md FR-5). The first real consumer registered with `internal/platform/relay` — see that package's README and `docs/adr/smartcourse-adr.md` ADR-0021 for why this module exists now rather than waiting for Kafka in M3.

## Why this module looks the way it does

**`service.go` is thin, but it exists, and that was a real fix, not a formality.** Unlike `identity`/`course`/`enrollment`/`progress`, this module's main work doesn't react to direct HTTP writes — it reacts to outbox events, via handler functions registered with the relay (`handlers.go`). The only synchronous surface is a read: `GET /analytics/platform`. Until review caught it, that one read went `Handler` → `*gorm.DB` directly, skipping the service layer every other module has — an easy shortcut to take when a module "only" has one endpoint, and exactly the kind of inconsistency that's invisible until someone copies the pattern elsewhere. `Service.GetPlatformSnapshot` just delegates to `Repository`, but the layering is what's load-bearing: a future second read endpoint has an obvious place to put real logic, instead of a second ad-hoc `*gorm.DB` call.

**Every aggregate is recomputed from source, not incrementally maintained**, deliberately: `RecomputePlatformSnapshot` and `RecomputeCourseStats` (`repository.go`) run a fresh `COUNT`/`AVG` against `users`/`courses`/`enrollments` on every relevant event, rather than a running `+1`/`-1`. This is ADR-0017's rebuildability principle applied eagerly instead of on ADR-0017's 60-second schedule (that scheduled reconciliation path doesn't exist yet — see below). It trades a slightly heavier query per event for the complete absence of an entire bug class: an incremental counter that drifts from reality because of a bug, a missed event, or a manual correction has no way to self-correct; a recompute-from-source is correct by construction every time it runs.

**`analytics_daily_enrollments` is the one genuine exception** — it's incremented, not recomputed, because "an enrollment happened on this UTC day" is an immutable historical fact once true. There's no drift risk a recompute would guard against, so the simpler operation is also the correct one. See the comment on `IncrementDailyEnrollment`.

**`progress.course_completed`'s payload doesn't carry `course_id`** — only `enrollment_id` and `student_id` (RFC's events are deliberately minimal at the source; see `internal/progress/service.go`). `handleCourseCompleted` looks it up via a join to `enrollments` inside its own handler transaction, rather than the event growing a field "just in case" a future consumer wants it.

## What this closes, and what it doesn't

Closes 5 of 10 metrics from SmartCourse.md FR-5: total students, total instructors, total courses published, average courses per student (all via `analytics_platform_snapshot`), and per-course enrollment/completion stats (`analytics_course_stats`, `analytics_daily_enrollments`). Still missing: "Most Popular Courses" (needs a ranking read, not yet built), and "Failed Events / Workflow Issues" (needs the MongoDB `failed_events` collection, M3 scope — the relay's own give-up path currently only logs, per ADR-0021's documented limitation).

**Scheduled reconciliation (ADR-0017's other half) doesn't exist yet.** Today's numbers are event-driven only. They should already be correct given the recompute-from-source approach above, but there's no periodic job independently re-deriving them as a check — that's a real, honest gap, not a subtle one.

## Testing

`service_integration_test.go` (build tag `integration`, real Postgres): `TestGetPlatformSnapshot_ReflectsRecompute` seeds users via raw SQL, runs `RecomputePlatformSnapshot` inside a transaction exactly as the relay's handler does, and asserts the HTTP-facing `Service.GetPlatformSnapshot` reads back what was just written — proving the `Repository` → `Service` → handler path genuinely works end to end, not just that each piece compiles. The individual event handlers (`handlers.go`) are still exercised only indirectly — by `internal/platform/relay`'s tests (synthetic handlers, not these) and manually against the running system. Dedicated tests per handler (`handleStudentEnrolled`, `handleCourseCompleted`, etc.) are a natural next addition, same pattern as this file.
