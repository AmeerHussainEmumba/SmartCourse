-- instructor_mirror (ADR-0024): course's own denormalized copy of every
-- registered user's display name (not just instructors — identity.
-- user_registered fires for everyone; only ever looked up via
-- courses.instructor_id, so the name undersells what's actually stored),
-- fed from identity.user_registered and identity.user_profile_updated
-- (internal/course/handlers.go). Exists because ListCatalogue
-- (repository.go) used to JOIN directly against identity.users — a
-- cross-schema join ADR-0024 rules out — and the catalogue genuinely
-- needs the instructor's name, not just their id. No FK into
-- identity.users, same cross-schema-FK policy as courses.instructor_id
-- (migrations/course/000001_courses.up.sql).
--
-- last_event_id guards against the relay's worker pool (no per-aggregate
-- ordering) processing a user's registered and profile_updated events out
-- of order — same issue and same fix as analytics.user_role_mirror
-- (ADR-0024, internal/analytics/README.md).
SET search_path TO course, public;

CREATE TABLE instructor_mirror (
    user_id       UUID PRIMARY KEY,
    full_name     TEXT NOT NULL,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
