# Changelog — services/analytics

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Security: `GET /analytics/issues` (admin only) now checks the token version (`auth.RequireFreshPrivilegeStrict`, fail closed on a Redis miss: 401 `token_stale`; 503 when Redis is unreachable or not wired). Before, a demoted admin's old token kept reading it (observed by the E10 and access scenarios). `api/openapi.yaml` documents the 401. Tests that call the route as an admin must seed `user:tv:<id>` in Redis.

- Added: Workflow Issues, the ninth FR-5 metric (DR-026). Migration `000004` (`publish_failures`, `workflow_issue_stats`), a relay handler for `course.publish_failed`, `NewWorkflowIssuesSource`, `ReconcileWorkflowIssueStats` (run by `Reconciler.Run`), and `analytics.PublishWorkflow`. `cmd/analytics-api/wire.go` attaches the source, so `GET /analytics/issues` returns `workflow_issues` as an object (`failed`, `by_workflow`) instead of `null`. Counts publish failures since the event existed; earlier ones are not backfilled. 9 of 9 FR-5 metrics now have a source.

- analytics-api wires telemetry in `cmd/analytics-api/main.go`: tracing, RED metrics, `/metrics`, bounded flush on shutdown (Phase 7). It does not expose the outbox gauges (no access to the outbox table).

- Added: `services/analytics/failedevents` (`Source`, `LazyStore`): the Failed Events metric over `libs/mongostore` (ADR-0031). `cmd/analytics-api/wire.go` attaches it when `MONGO_URI` is set; MongoDB connects lazily so startup and other routes never depend on it. `registerRoutes` now returns a close func. `go.mod` requires `libs/mongostore`; `Dockerfile.analytics-api` copies it; `scripts/check-link-graph.sh` no longer forbids it for analytics-api only.
- Changed (behaviour): `GET /analytics/issues` needs only a failed-events source; `workflow_issues` is `null` when no workflow source is attached (none is built). The failed-events section gained `replayed`, `by_consumer` and `oldest_unresolved_age_seconds`. A source error wrapping the new `analytics.ErrSourceUnavailable` answers 503 instead of 500. `FailedEventsSummary` gained `Replayed`, `ByConsumer`, `OldestUnresolvedAgeSeconds`. `api/openapi.yaml` does not yet describe the new response fields of `/analytics/issues`.

- Added: `GET /analytics/popular-courses` (FR-5 "Most Popular Courses"), served from the Redis sorted set `analytics:popular_courses` with a Postgres fallback; `Service.WithPopularCache`.
- Added: `NewReconciler(db, rdb).Run(ctx)` (RFC 9.2): recomputes `platform_snapshot`, `course_stats` and `daily_enrollments` from the mirrors and refreshes the sorted set. Not scheduled by this module.
- Added: `GET /analytics/issues` (admin only) with `FailedEventsSource` and `WorkflowIssuesSource` interfaces and `Service.WithIssueSources`; answers 503 until a real source is attached. 8 of 9 FR-5 metrics are live; this one is deferred.
- Changed (behaviour, breaking for callers): an instructor may now read per-course analytics only for a course they own (`course_mirror.instructor_id`, from `course_published`); others get 404. Rows published before migration `000003` have no owner and are admin-only until the next `course_published`. A course analytics has never seen is a 404 for instructors.
- Changed (exported API): `UpsertCourseMirror` takes the instructor id as a fourth argument. `RecomputeCourseStats` and `RecomputePlatformSnapshot` now lock their row before reading.
- Migration: `migrations/analytics/000003_course_mirror_instructor` adds nullable `course_mirror.instructor_id`.
- go-redis is now a direct dependency.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
