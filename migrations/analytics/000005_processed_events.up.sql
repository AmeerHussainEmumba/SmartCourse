-- analytics.processed_events: the analytics service's own consumer dedup table
-- (docs/plans/microservices-split.md section 3.1), same shape as
-- platform.processed_events. The cutover set moves this service's rows
-- (by consumer_name) out of platform.processed_events.
SET search_path TO analytics, public;

CREATE TABLE processed_events (
    consumer_name TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);
