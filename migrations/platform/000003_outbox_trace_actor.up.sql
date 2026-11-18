-- Trace context and actor on the outbox (RFC §6.4 event envelope, §12.2;
-- ADR-0032). trace_id / span_id are W3C trace-context ids in lowercase hex
-- (32 and 16 chars); actor_id is the authenticated user who caused the
-- event, for audit. All nullable: rows written before this migration, and
-- writers with no trace or actor in scope (system jobs), leave them NULL.
-- ADD COLUMN with no default is a metadata-only change, and the existing
-- partial index idx_outbox_unpublished is untouched. actor_id is a plain
-- UUID with no foreign key (ADR-0024: nothing references across schemas).
SET search_path TO platform, public;

ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS trace_id TEXT,
    ADD COLUMN IF NOT EXISTS span_id  TEXT,
    ADD COLUMN IF NOT EXISTS actor_id UUID;
