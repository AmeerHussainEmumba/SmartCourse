-- Workflow Issues, the ninth FR-5 metric (DR-024, DR-026). The workflow
-- engine's failures are course-schema facts (course_versions.state), which this
-- schema may not read (ADR-0024). They arrive as course.publish_failed events
-- instead, one per failed publish, and are mirrored here.
--
-- publish_failures is the event-fed mirror: one row per course.publish_failed
-- event, keyed by the outbox event id so a replayed or duplicated delivery
-- inserts nothing. workflow_issue_stats is the aggregate GET /analytics/issues
-- reads: failed publishes per workflow. The relay handler recomputes a row
-- from the mirror after each insert and the scheduled reconciler recomputes
-- all of them, the same pattern as course_stats.
--
-- Only failures from events written after this migration are counted: the
-- event did not exist before, and there is no source this schema may read to
-- backfill earlier ones. The workflow column is the Temporal workflow type.
SET search_path TO analytics, public;

CREATE TABLE publish_failures (
    event_id        UUID PRIMARY KEY,
    workflow        TEXT NOT NULL,
    course_id       UUID NOT NULL,
    version_id      UUID NOT NULL,
    version_number  INT NOT NULL,
    failed_activity TEXT NOT NULL DEFAULT '',
    reason          TEXT NOT NULL DEFAULT '',
    failed_at       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_publish_failures_workflow ON publish_failures(workflow);
CREATE INDEX idx_publish_failures_course ON publish_failures(course_id);

CREATE TABLE workflow_issue_stats (
    workflow   TEXT PRIMARY KEY,
    failed     BIGINT NOT NULL DEFAULT 0 CHECK (failed >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
