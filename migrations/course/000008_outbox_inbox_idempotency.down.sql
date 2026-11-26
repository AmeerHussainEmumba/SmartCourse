-- Drops the three tables and their rows. Unpublished outbox rows, dedup
-- markers and idempotency keys are lost: only roll back on a disposable
-- database.
SET search_path TO course, public;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS outbox;
