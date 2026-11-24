SET search_path TO analytics, public;

-- Placeholder rows (withdrawn seen, enrolled never seen) cannot satisfy NOT NULL.
DELETE FROM enrollment_mirror WHERE enrolled_at IS NULL;
ALTER TABLE enrollment_mirror ALTER COLUMN enrolled_at SET NOT NULL;
ALTER TABLE enrollment_mirror DROP COLUMN last_event_id;
