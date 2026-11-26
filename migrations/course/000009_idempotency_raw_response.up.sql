SET search_path TO course, public;

-- Byte-exact replay (findings B-02, B-03, S-03/S-04/S-05). response_body is
-- JSONB: it re-serialises the body (key order, whitespace, numbers), cannot
-- hold an empty or non-JSON body, and rejects a \u0000 escape. These two
-- columns hold the response as the handler wrote it, plus the headers that
-- matter (Content-Type, Location). Rows written before this migration, and
-- rows moved by migrations/cutover, keep response_body and have NULL here;
-- the replay path falls back to response_body for them.
ALTER TABLE idempotency_keys ADD COLUMN response_raw     BYTEA;
ALTER TABLE idempotency_keys ADD COLUMN response_headers JSONB;
