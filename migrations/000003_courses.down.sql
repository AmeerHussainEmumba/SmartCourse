DROP TABLE IF EXISTS course_prerequisites;
ALTER TABLE IF EXISTS courses DROP CONSTRAINT IF EXISTS fk_published_version;
ALTER TABLE IF EXISTS courses DROP CONSTRAINT IF EXISTS fk_draft_version;
DROP TABLE IF EXISTS course_versions;
DROP TABLE IF EXISTS courses;
DROP TYPE IF EXISTS version_state;
