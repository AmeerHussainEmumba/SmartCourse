-- Content (RFC §5.3.3). Hangs off course_version_id, not course_id: versions
-- are immutable snapshots, publishing copies the tree (ADR-0010).
-- lesson_key is stable across versions and is what lesson_progress joins on
-- (ADR-0011) — id is not, because republishing creates new lesson rows.

CREATE TABLE modules (
    id                UUID PRIMARY KEY,
    course_version_id UUID NOT NULL REFERENCES course_versions(id) ON DELETE CASCADE,
    module_key        UUID NOT NULL,
    position          INT  NOT NULL,
    title             TEXT NOT NULL,
    UNIQUE (course_version_id, position),
    UNIQUE (course_version_id, module_key)
);

CREATE TABLE lessons (
    id           UUID  PRIMARY KEY,
    module_id    UUID  NOT NULL REFERENCES modules(id) ON DELETE CASCADE,
    lesson_key   UUID  NOT NULL,
    position     INT   NOT NULL,
    title        TEXT  NOT NULL,
    content_type TEXT  NOT NULL CHECK (content_type IN ('video','text','pdf','link')),
    content      JSONB NOT NULL DEFAULT '{}'
                 CHECK (pg_column_size(content) < 65536),
    asset_key    TEXT,
    duration_s   INT   NOT NULL DEFAULT 0,
    UNIQUE (module_id, position),
    UNIQUE (module_id, lesson_key)
);
CREATE INDEX idx_lessons_key ON lessons(lesson_key);
