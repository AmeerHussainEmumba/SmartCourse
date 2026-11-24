-- The W3C traceparent of the span that wrote a delivery row (ADR-0032), so the
-- sweeper, which enqueues the delivery task seconds later from a background
-- loop that is inside no trace, can continue the ORIGINAL request's trace: the
-- delivery job span becomes a descendant of the HTTP request that caused the
-- mail instead of the root of a new trace.
-- Advisory metadata: NULL for rows written before this migration and whenever
-- tracing was off. ADD COLUMN with no default is a metadata-only change.
SET search_path TO notification, public;

ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS traceparent TEXT;
