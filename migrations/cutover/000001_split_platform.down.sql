-- Restores an EMPTY `platform` schema with the three tables in the shape
-- platform/000002 + 000003 leave them (outbox with trace_id, span_id,
-- actor_id), so platform's own down migrations can run afterwards
-- (`make migrate-down-all`). Rows do NOT move back: whatever is in the
-- per-service tables stays there. Rolling the cutover back is a local-dev
-- reset step, not a production rollback path.
CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE IF NOT EXISTS platform.outbox (
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
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON platform.outbox(id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS platform.processed_events (
    consumer_name TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS platform.idempotency_keys (
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
