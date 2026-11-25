-- Cutover from the shared `platform` schema to per-service tables
-- (docs/plans/microservices-split.md section 3.1). Runs LAST, after every
-- service set has created its own outbox / processed_events / idempotency
-- tables. golang-migrate sends this file as one multi-statement command, so
-- it commits or rolls back as a unit; nothing is left half-moved.
--
-- Idempotent and safe on a fresh database: the whole body is guarded by
-- "platform.outbox exists", every insert is ON CONFLICT DO NOTHING, and on a
-- database with no platform rows it moves nothing and just drops the empty
-- tables and the schema.
--
-- Routing:
--   platform.outbox          event_type 'identity.%'  -> identity.outbox
--                            everything else          -> course.outbox
--     ALL rows move (published and unpublished: the outbox is pruned
--     regularly, so the remainder is the recent history). `id` and `event_id`
--     are copied as they are and rows are inserted in id order, so the
--     relative order of events per outbox is unchanged. The target id
--     sequences are then advanced past the highest copied id.
--   platform.processed_events by consumer_name:
--                            analytics-aggregator      -> analytics
--                            notification-dispatcher   -> notification
--                            audit-archiver            -> DROPPED (the audit
--                              service dedups by event id in MongoDB)
--                            every other consumer      -> course
--                              (course-instructor-mirror, certificate-issuer,
--                              enrollment-course-archival)
--   platform.idempotency_keys -> course.idempotency_keys
--
-- The `platform` schema is then dropped (RESTRICT, not CASCADE: an object
-- somebody added to it makes the migration fail loudly instead of vanishing).
-- The citext and pg_trgm extensions live in `public` and stay.

DO $cutover$
BEGIN
    IF to_regclass('platform.outbox') IS NOT NULL THEN
        INSERT INTO identity.outbox (id, event_id, aggregate_type, aggregate_id, event_type,
                                     schema_version, payload, headers, occurred_at, published_at,
                                     attempts, last_error, trace_id, span_id, actor_id)
        SELECT id, event_id, aggregate_type, aggregate_id, event_type,
               schema_version, payload, headers, occurred_at, published_at,
               attempts, last_error, trace_id, span_id, actor_id
          FROM platform.outbox
         WHERE event_type LIKE 'identity.%'
         ORDER BY id
        ON CONFLICT DO NOTHING;

        INSERT INTO course.outbox (id, event_id, aggregate_type, aggregate_id, event_type,
                                   schema_version, payload, headers, occurred_at, published_at,
                                   attempts, last_error, trace_id, span_id, actor_id)
        SELECT id, event_id, aggregate_type, aggregate_id, event_type,
               schema_version, payload, headers, occurred_at, published_at,
               attempts, last_error, trace_id, span_id, actor_id
          FROM platform.outbox
         WHERE event_type NOT LIKE 'identity.%'
         ORDER BY id
        ON CONFLICT DO NOTHING;

        PERFORM setval(pg_get_serial_sequence('identity.outbox', 'id'),
                       GREATEST((SELECT COALESCE(max(id), 0) FROM identity.outbox), 1),
                       (SELECT count(*) > 0 FROM identity.outbox));
        PERFORM setval(pg_get_serial_sequence('course.outbox', 'id'),
                       GREATEST((SELECT COALESCE(max(id), 0) FROM course.outbox), 1),
                       (SELECT count(*) > 0 FROM course.outbox));
    END IF;

    IF to_regclass('platform.processed_events') IS NOT NULL THEN
        INSERT INTO analytics.processed_events (consumer_name, event_id, processed_at)
        SELECT consumer_name, event_id, processed_at FROM platform.processed_events
         WHERE consumer_name = 'analytics-aggregator'
        ON CONFLICT DO NOTHING;

        INSERT INTO notification.processed_events (consumer_name, event_id, processed_at)
        SELECT consumer_name, event_id, processed_at FROM platform.processed_events
         WHERE consumer_name = 'notification-dispatcher'
        ON CONFLICT DO NOTHING;

        INSERT INTO course.processed_events (consumer_name, event_id, processed_at)
        SELECT consumer_name, event_id, processed_at FROM platform.processed_events
         WHERE consumer_name NOT IN ('analytics-aggregator', 'notification-dispatcher', 'audit-archiver')
        ON CONFLICT DO NOTHING;
    END IF;

    IF to_regclass('platform.idempotency_keys') IS NOT NULL THEN
        INSERT INTO course.idempotency_keys (key, user_id, endpoint, request_hash, response_status,
                                             response_body, state, created_at, expires_at)
        SELECT key, user_id, endpoint, request_hash, response_status,
               response_body, state, created_at, expires_at
          FROM platform.idempotency_keys
        ON CONFLICT DO NOTHING;
    END IF;
END
$cutover$;

DROP TABLE IF EXISTS platform.idempotency_keys;
DROP TABLE IF EXISTS platform.processed_events;
DROP TABLE IF EXISTS platform.outbox;
DROP SCHEMA IF EXISTS platform RESTRICT;
