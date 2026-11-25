-- identity.outbox: the identity service's own transactional outbox
-- (docs/plans/microservices-split.md section 3.1). Same shape as
-- platform.outbox after platform/000002 + 000003; the cutover set moves any
-- rows still in platform.outbox here. Foreign keys stay inside this schema:
-- aggregate_id / actor_id are plain UUIDs.
CREATE SCHEMA IF NOT EXISTS identity;
SET search_path TO identity, public;

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
    last_error     TEXT,
    trace_id       TEXT,
    span_id        TEXT,
    actor_id       UUID
);
CREATE INDEX idx_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;
