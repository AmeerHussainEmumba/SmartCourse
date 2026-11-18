SET search_path TO course, public;

ALTER TABLE course_versions DROP COLUMN publish_started_at;
