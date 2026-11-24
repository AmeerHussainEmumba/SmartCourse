-- F-09: ordering guard for enrollment_mirror, same scheme as
-- user_role_mirror.last_event_id. The relay's worker pool has no
-- per-aggregate order, so student_withdrawn can commit before
-- student_enrolled. A status-bearing event now creates the row itself if it
-- arrives first (enrolled_at unknown until student_enrolled fills it in),
-- and a transition is only applied when its outbox id is newer than the
-- last one applied.
SET search_path TO analytics, public;

ALTER TABLE enrollment_mirror ADD COLUMN last_event_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE enrollment_mirror ALTER COLUMN enrolled_at DROP NOT NULL;
