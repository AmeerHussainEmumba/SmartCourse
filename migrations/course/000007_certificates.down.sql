SET search_path TO course, public;
DROP INDEX IF EXISTS idx_certificates_course;
DROP INDEX IF EXISTS idx_certificates_student;
ALTER TABLE certificates DROP CONSTRAINT IF EXISTS certificates_verification_code_format;
ALTER TABLE certificates
    DROP COLUMN IF EXISTS course_version_id,
    DROP COLUMN IF EXISTS course_id,
    DROP COLUMN IF EXISTS student_id;
