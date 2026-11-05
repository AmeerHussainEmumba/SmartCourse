-- Search read model (RFC §5.3.6): a separate, denormalised table populated
-- asynchronously by a consumer (M3, internal/search — not yet built) —
-- never a generated column or trigger (ADR-0016), so indexing stays
-- observable and replayable rather than an invisible part of the write
-- transaction.
--
-- Stays in the course schema for now, not its own: internal/search doesn't
-- exist yet (no handlers.go, no relay registration — see cmd/README.md),
-- so there's no service to own a separate schema. When it's built it
-- follows the same pattern internal/analytics did here (ADR-0024): its own
-- schema, fed from events, no FK back into course.courses. Revisit this
-- file at that point rather than before.
SET search_path TO course, public;

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
