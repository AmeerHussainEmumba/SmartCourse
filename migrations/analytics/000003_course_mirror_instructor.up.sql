-- Instructor ownership for per-course analytics. course.course_published now
-- carries instructor_id (libs/events CoursePublished.instructor_id); the
-- handler writes it here so GET /analytics/courses/{id} can tell an owning
-- instructor from any other instructor.
--
-- The column is NULLABLE on purpose and there is no backfill: course_mirror
-- rows written before this migration have no owner, and this schema cannot
-- read course.courses to find one (ADR-0024). A row with NULL instructor_id
-- is readable only by admins until the next course.course_published for that
-- course fills it in (publishing again emits the event). See the Week 3
-- deviation recorded in services/analytics/analytics/README.md.
SET search_path TO analytics, public;

ALTER TABLE course_mirror ADD COLUMN instructor_id UUID;
CREATE INDEX idx_course_mirror_instructor ON course_mirror(instructor_id) WHERE instructor_id IS NOT NULL;
