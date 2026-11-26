-- Stored raw responses are lost; replays of those keys stop working (the
-- keys then replay an empty body). Only roll back on a disposable database.
SET search_path TO course, public;

ALTER TABLE idempotency_keys DROP COLUMN response_headers;
ALTER TABLE idempotency_keys DROP COLUMN response_raw;
