-- Courses and versions (RFC §5.3.2). Blue-green content promotion (ADR-0010):
-- courses.published_version_id is what students see, courses.draft_version_id
-- is what's being edited. idx_one_draft_per_course makes concurrent publish
-- attempts on the same course impossible at the storage layer.

CREATE TYPE version_state AS ENUM ('draft','publishing','ready','publish_failed','superseded');

CREATE TABLE courses (
    id                   UUID PRIMARY KEY,
    instructor_id        UUID        NOT NULL REFERENCES users(id),
    slug                 TEXT        NOT NULL UNIQUE,
    published_version_id UUID,
    draft_version_id     UUID,
    enrollment_limit     INT         CHECK (enrollment_limit IS NULL OR enrollment_limit > 0),
    enrollment_count     INT         NOT NULL DEFAULT 0 CHECK (enrollment_count >= 0),
    archived_at          TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_courses_instructor ON courses(instructor_id);
CREATE INDEX idx_courses_published ON courses(id)
    WHERE published_version_id IS NOT NULL AND archived_at IS NULL;

CREATE TABLE course_versions (
    id               UUID PRIMARY KEY,
    course_id        UUID          NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    version_number   INT           NOT NULL,
    state            version_state NOT NULL DEFAULT 'draft',
    title            TEXT          NOT NULL,
    description      TEXT          NOT NULL DEFAULT '',
    category         TEXT,
    level            TEXT          CHECK (level IN ('beginner','intermediate','advanced')),
    tags             TEXT[]        NOT NULL DEFAULT '{}',
    language         TEXT          NOT NULL DEFAULT 'en',
    total_lessons    INT           NOT NULL DEFAULT 0,
    total_duration_s INT           NOT NULL DEFAULT 0,
    workflow_id      TEXT,
    publish_error    TEXT,
    published_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (course_id, version_number)
);
CREATE UNIQUE INDEX idx_one_draft_per_course ON course_versions(course_id)
    WHERE state IN ('draft','publishing');

ALTER TABLE courses
    ADD CONSTRAINT fk_published_version
    FOREIGN KEY (published_version_id) REFERENCES course_versions(id),
    ADD CONSTRAINT fk_draft_version
    FOREIGN KEY (draft_version_id) REFERENCES course_versions(id);

CREATE TABLE course_prerequisites (
    course_id       UUID NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    prerequisite_id UUID NOT NULL REFERENCES courses(id),
    PRIMARY KEY (course_id, prerequisite_id),
    CHECK (course_id <> prerequisite_id)
);
