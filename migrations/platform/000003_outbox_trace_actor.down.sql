-- Drops the trace/actor columns and any values written to them; the
-- columns are advisory metadata, so no event data is lost.
SET search_path TO platform, public;

ALTER TABLE outbox
    DROP COLUMN IF EXISTS actor_id,
    DROP COLUMN IF EXISTS span_id,
    DROP COLUMN IF EXISTS trace_id;
