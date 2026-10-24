-- Read models (RFC §5.3.6): search index and analytics aggregates. Separate,
-- denormalised tables populated asynchronously by consumers (M3) — never a
-- generated column or trigger (ADR-0016), so indexing stays observable and
-- replayable rather than an invisible part of the write transaction.

CREATE TABLE course_search_index (
    course_id        UUID PRIMARY KEY REFERENCES courses(id) ON DELETE CASCADE,
    version_id       UUID NOT NULL,
    title            TEXT NOT NULL,
    description      TEXT NOT NULL,
    instructor_name  TEXT NOT NULL,
    category         TEXT,
    level            TEXT,
    tags             TEXT[] NOT NULL DEFAULT '{}',
    enrollment_count INT  NOT NULL DEFAULT 0,
    search_vector    TSVECTOR NOT NULL,
    indexed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_search_vector ON course_search_index USING GIN(search_vector);
CREATE INDEX idx_search_title_trgm ON course_search_index USING GIN(title gin_trgm_ops);
CREATE INDEX idx_search_facets ON course_search_index(category, level);

CREATE TABLE analytics_course_stats (
    course_id            UUID PRIMARY KEY REFERENCES courses(id) ON DELETE CASCADE,
    total_enrollments    BIGINT NOT NULL DEFAULT 0,
    active_enrollments   BIGINT NOT NULL DEFAULT 0,
    completions          BIGINT NOT NULL DEFAULT 0,
    completion_rate      NUMERIC(5,4) NOT NULL DEFAULT 0,
    avg_completion_s     BIGINT,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE analytics_daily_enrollments (
    day       DATE NOT NULL,
    course_id UUID NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    count     INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (day, course_id)
);

CREATE TABLE analytics_platform_snapshot (
    id                       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    total_students           BIGINT NOT NULL DEFAULT 0,
    total_instructors        BIGINT NOT NULL DEFAULT 0,
    total_courses_published  BIGINT NOT NULL DEFAULT 0,
    avg_courses_per_student  NUMERIC(8,3) NOT NULL DEFAULT 0,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
