-- Notification dispatcher (RFC §6.5 "notification-dispatcher", §6.7, ADR-0014).
-- Owns its own schema (ADR-0024): written only by services/worker/notification,
-- fed only by relay events — never a query into identity.users or
-- course.courses. Application SQL qualifies these tables as `notification.x`
-- because the worker's search_path also contains the analytics and course
-- schemas, and a bare name must never resolve by accident to another
-- service's table.
CREATE SCHEMA IF NOT EXISTS notification;
SET search_path TO notification, public;

-- Recipient mirror, built from identity.user_registered and
-- identity.user_profile_updated. last_event_id is the platform.outbox id of
-- the newest event that wrote `name`: the relay's worker pool has no
-- per-aggregate ordering, so a profile update can commit before the
-- registration it follows (same guard as analytics.user_role_mirror).
-- email is NULL on a row created by a profile update that arrived before
-- the registration; such a recipient is "unknown" until the email arrives.
-- status is never fed by the events consumed today, so it stays 'active';
-- it exists so a future role/status consumer can suppress mail.
CREATE TABLE recipients (
    user_id       UUID PRIMARY KEY,
    email         TEXT,
    name          TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'active',
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Titles, from course.course_published; used only to word the mail.
CREATE TABLE course_titles (
    course_id     UUID PRIMARY KEY,
    title         TEXT NOT NULL,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- enrollment -> course, from enrollment.student_enrolled, because
-- progress.course_completed carries enrollment_id and student_id but no
-- course_id (same reason analytics keeps an enrollment mirror).
CREATE TABLE enrollment_courses (
    enrollment_id UUID PRIMARY KEY,
    course_id     UUID NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per notification to send; the source of truth for "was it sent".
--
--   pending    waiting to be (re-)enqueued and sent
--   sending    claimed by a delivery task; claimed_at is the lease start
--   sent       the provider accepted it
--   parked     the recipient is not in the mirror yet (or has no email);
--              promoted to pending when the recipient arrives, never dropped
--   suppressed recipient status is not 'active'
--   failed     permanent failure (provider 5xx, or attempts exhausted)
--
-- UNIQUE (event_id, kind, recipient_id) is what makes a duplicate event, a
-- replay or two concurrent relay replicas insert ONE delivery.
-- enqueued_at is when the sweep last handed the row to the queue.
CREATE TABLE deliveries (
    id           UUID PRIMARY KEY,
    event_id     UUID NOT NULL,
    kind         TEXT NOT NULL,
    recipient_id UUID NOT NULL,
    params       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','sending','sent','parked','suppressed','failed')),
    attempts     INT NOT NULL DEFAULT 0,
    last_error   TEXT,
    enqueued_at  TIMESTAMPTZ,
    claimed_at   TIMESTAMPTZ,
    sent_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT deliveries_event_kind_recipient_key UNIQUE (event_id, kind, recipient_id)
);
-- The sweep's lookup: only rows that still need work.
CREATE INDEX idx_deliveries_open ON deliveries (created_at) WHERE status IN ('pending','sending');
CREATE INDEX idx_deliveries_parked ON deliveries (recipient_id) WHERE status = 'parked';
