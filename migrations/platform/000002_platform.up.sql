-- Platform schema (schema-per-service — see migrations/README.md and
-- ADR-0024). The outbox (ADR-0006), consumer dedup, and request
-- idempotency are shared integration infrastructure, not any one
-- service's domain data: every producer (identity/course/enrollment/
-- progress) writes its outbox row here, inside its own transaction.
-- Durability only requires the same database as the business write, not
-- the same schema, so this doesn't weaken ADR-0006's guarantee — and it's
-- the mechanism Kafka replaces wholesale in M3, not a piece of any one
-- service's owned data.
--
-- idempotency_keys.user_id no longer has a foreign key to identity.users
-- (same cross-schema-FK policy as courses.instructor_id and
-- enrollments.student_id — see migrations/course/000001_courses.up.sql):
-- no code reads this table yet, and the same reasoning applies uniformly.
CREATE SCHEMA IF NOT EXISTS platform;
SET search_path TO platform, public;

CREATE TABLE outbox (
    id             BIGSERIAL PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   UUID        NOT NULL,
    event_type     TEXT        NOT NULL,
    schema_version INT         NOT NULL,
    payload        BYTEA       NOT NULL,
    headers        JSONB       NOT NULL DEFAULT '{}',
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT
);
CREATE INDEX idx_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    consumer_name TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE idempotency_keys (
    key             TEXT        NOT NULL,
    user_id         UUID        NOT NULL,
    endpoint        TEXT        NOT NULL,
    request_hash    BYTEA       NOT NULL,
    response_status INT,
    response_body   JSONB,
    state           TEXT        NOT NULL DEFAULT 'in_progress'
                    CHECK (state IN ('in_progress','completed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (key, user_id, endpoint)
);
