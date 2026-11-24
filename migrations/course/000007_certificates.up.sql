-- Certificates (RFC §5.3.4, §8.4). The `certificates` table was created
-- empty in 000003 with only the RFC's columns; nothing wrote to it until the
-- certificate module (services/course/certificate) existed. This migration
-- adds what that module needs:
--
--   student_id, course_id   denormalised from the enrollment so "my
--                           certificates" and the owner check are one
--                           indexed read on this table, not a join. Like
--                           enrollments.student_id, no foreign key into
--                           identity.users (ADR-0024).
--   course_version_id       the version the student completed (the
--                           published version when the event was handled,
--                           RFC A5). Nullable: only a course with no
--                           version at all could lack one, and a missing
--                           pin must not block issuing.
--
-- student_name and course_title (already present) are the immutable
-- snapshot RFC §5.3.4 asks for. The name is copied from instructor_mirror
-- (000005), never read from the identity schema.
--
-- UNIQUE (enrollment_id) from 000003 is the guarantee that issuance is
-- exactly once per enrollment under replay or concurrent delivery (RFC F13);
-- processed_events is only the second line of defence.
SET search_path TO course, public;

ALTER TABLE certificates
    ADD COLUMN student_id        UUID,
    ADD COLUMN course_id         UUID REFERENCES courses(id),
    ADD COLUMN course_version_id UUID REFERENCES course_versions(id);

-- Nothing wrote to the table before this migration, but backfill anyway so
-- the NOT NULL below cannot fail on a database where rows were inserted by hand.
UPDATE certificates c
   SET student_id = e.student_id, course_id = e.course_id
  FROM enrollments e
 WHERE e.id = c.enrollment_id;

ALTER TABLE certificates
    ALTER COLUMN student_id SET NOT NULL,
    ALTER COLUMN course_id  SET NOT NULL;

-- The verification code is an unguessable bearer token (26 chars of base32,
-- 128 bits from crypto/rand). The CHECK keeps a short or odd code, which
-- would be guessable, from ever being stored.
ALTER TABLE certificates
    ADD CONSTRAINT certificates_verification_code_format
    CHECK (verification_code ~ '^[A-Z2-7]{26}$');

-- GET /certificates: a student's certificates, newest first.
CREATE INDEX idx_certificates_student ON certificates(student_id, issued_at DESC);
-- Lookups by course (support, future per-course reporting).
CREATE INDEX idx_certificates_course ON certificates(course_id);
