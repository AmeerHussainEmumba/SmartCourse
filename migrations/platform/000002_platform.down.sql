SET search_path TO platform, public;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS outbox;
DROP SCHEMA IF EXISTS platform CASCADE;
