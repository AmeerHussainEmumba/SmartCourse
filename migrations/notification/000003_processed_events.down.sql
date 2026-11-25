-- Drops the table and its dedup markers; events already handled could be
-- handled again if they are redelivered.
SET search_path TO notification, public;
DROP TABLE IF EXISTS processed_events;
