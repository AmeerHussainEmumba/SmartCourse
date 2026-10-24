-- Platform tables (RFC §5.3.5): the outbox (ADR-0006), consumer dedup, and
-- request idempotency. These exist from M0 deliberately — retrofitting them
-- after other write paths already exist means touching every one of those
-- paths a second time (RFC §21.1).

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
    user_id         UUID        NOT NULL REFERENCES users(id),
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
