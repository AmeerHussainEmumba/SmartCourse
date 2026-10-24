-- Enrollment, progress, certificates (RFC §5.3.4).
-- idx_one_active_enrollment is a PARTIAL unique index: prevents duplicate
-- active enrollments while permitting re-enrollment after withdrawal, which
-- a plain unique constraint would block. This is the database enforcing the
-- business rule from FR-1.

CREATE TYPE enrollment_status AS ENUM ('active','completed','withdrawn');

CREATE TABLE enrollments (
    id            UUID PRIMARY KEY,
    student_id    UUID NOT NULL REFERENCES users(id),
    course_id     UUID NOT NULL REFERENCES courses(id),
    status        enrollment_status NOT NULL DEFAULT 'active',
    enrolled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    withdrawn_at  TIMESTAMPTZ,
    CHECK ((status = 'completed') = (completed_at IS NOT NULL)),
    CHECK ((status = 'withdrawn') = (withdrawn_at IS NOT NULL))
);
CREATE UNIQUE INDEX idx_one_active_enrollment ON enrollments(student_id, course_id)
    WHERE status IN ('active','completed');
CREATE INDEX idx_enrollments_student ON enrollments(student_id, status);
CREATE INDEX idx_enrollments_course ON enrollments(course_id) WHERE status <> 'withdrawn';
CREATE INDEX idx_enrollments_enrolled_at ON enrollments(enrolled_at);

CREATE TABLE lesson_progress (
    id            UUID PRIMARY KEY,
    enrollment_id UUID NOT NULL REFERENCES enrollments(id) ON DELETE CASCADE,
    lesson_key    UUID NOT NULL,
    completed_at  TIMESTAMPTZ,
    last_position_s INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (enrollment_id, lesson_key)
);
CREATE INDEX idx_progress_enrollment ON lesson_progress(enrollment_id);

CREATE TABLE certificates (
    id                UUID PRIMARY KEY,
    enrollment_id     UUID NOT NULL UNIQUE REFERENCES enrollments(id),
    verification_code TEXT NOT NULL UNIQUE,
    student_name      TEXT NOT NULL,
    course_title      TEXT NOT NULL,
    issued_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
