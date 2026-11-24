SET search_path TO analytics, public;

DROP INDEX IF EXISTS idx_course_mirror_instructor;
ALTER TABLE course_mirror DROP COLUMN IF EXISTS instructor_id;
