# Changelog — services/analytics

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

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
