-- Drops the traceparent column and the values in it; they are advisory trace
-- metadata, so no delivery data is lost.
SET search_path TO notification, public;

ALTER TABLE deliveries DROP COLUMN IF EXISTS traceparent;
