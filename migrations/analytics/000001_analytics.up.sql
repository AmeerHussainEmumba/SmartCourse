-- Analytics (RFC §5.3.6, §9). Owns its own schema (ADR-0024): every table
-- here is written only by internal/analytics, fed only by relay events
-- (internal/analytics/handlers.go) — never a direct query into
-- course.courses or course.enrollments.
--
-- Mirror tables hold exactly the fields analytics' Recompute* functions
-- need, upserted row-by-row as each event is consumed. The Recompute*
-- functions still recompute aggregates from these (not incremental
-- counters) rather than maintaining running totals directly — ADR-0017's
-- rebuildability principle, now scoped to analytics' own mirrored data
-- instead of another service's live tables: a bug in a counter is still
-- fixable by recomputing, it just recomputes from here now.
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

-- last_event_id (platform.outbox's own BIGSERIAL id, monotonic by
-- insertion order) guards against out-of-order delivery: the relay's
-- worker pool (default 4 workers, internal/platform/relay) has no
-- per-aggregate ordering, so a user's user_registered and a later
-- user_role_changed for the same user can commit out of order. Without
-- this guard, whichever event's handler happens to finish last wins,
-- which can silently revert a promotion back to the registration event's
-- role. repository.go's UpsertUserRoleMirror only applies an update when
-- EXCLUDED.last_event_id is actually newer than what's stored.
CREATE TABLE user_role_mirror (
    user_id       UUID PRIMARY KEY,
    role          TEXT NOT NULL,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE course_mirror (
    course_id  UUID PRIMARY KEY,
    published  BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE enrollment_mirror (
    enrollment_id UUID PRIMARY KEY,
    course_id     UUID NOT NULL,
    student_id    UUID NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('active','completed','withdrawn')),
    enrolled_at   TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_enrollment_mirror_course ON enrollment_mirror(course_id);

-- Aggregate read models — same shape as before ADR-0024, renamed now that
-- the `analytics_` prefix is redundant inside the analytics schema, and no
-- longer FK'd into course.courses (no cross-schema FKs, ADR-0024).

CREATE TABLE course_stats (
    course_id            UUID PRIMARY KEY,
    total_enrollments    BIGINT NOT NULL DEFAULT 0,
    active_enrollments   BIGINT NOT NULL DEFAULT 0,
    completions          BIGINT NOT NULL DEFAULT 0,
    completion_rate      NUMERIC(5,4) NOT NULL DEFAULT 0,
    avg_completion_s     BIGINT,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE daily_enrollments (
    day       DATE NOT NULL,
    course_id UUID NOT NULL,
    count     INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (day, course_id)
);

CREATE TABLE platform_snapshot (
    id                       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    total_students           BIGINT NOT NULL DEFAULT 0,
    total_instructors        BIGINT NOT NULL DEFAULT 0,
    total_courses_published  BIGINT NOT NULL DEFAULT 0,
    avg_courses_per_student  NUMERIC(8,3) NOT NULL DEFAULT 0,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
