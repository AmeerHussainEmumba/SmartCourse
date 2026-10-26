// Package course owns courses, versions, modules, and lessons — the content
// tree and its lifecycle (RFC §4.3). Versions are immutable snapshots;
// publishing operates on a draft and atomically swaps a pointer on success
// (ADR-0010) — nothing a student can observe changes until that swap
// commits.
package course

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type VersionState string

const (
	StateDraft         VersionState = "draft"
	StatePublishing    VersionState = "publishing"
	StateReady         VersionState = "ready"
	StatePublishFailed VersionState = "publish_failed"
	StateSuperseded    VersionState = "superseded"
)

// Course mirrors `courses` (migrations/000003_courses.up.sql).
type Course struct {
	ID                 uuid.UUID `gorm:"primaryKey"`
	InstructorID       uuid.UUID
	Slug               string
	PublishedVersionID *uuid.UUID
	DraftVersionID     *uuid.UUID
	EnrollmentLimit    *int
	EnrollmentCount    int
	ArchivedAt         *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (Course) TableName() string { return "courses" }

// CourseVersion mirrors `course_versions`.
type CourseVersion struct {
	ID            uuid.UUID `gorm:"primaryKey"`
	CourseID      uuid.UUID
	VersionNumber int
	State         VersionState
	Title         string
	Description   string
	Category      *string
	Level         *string
	// pq.StringArray, not []string: GORM has no built-in Valuer/Scanner for
	// a plain Go slice against a Postgres text[] column, so a bare []string
	// silently serializes as SQL NULL against a NOT NULL column — caught by
	// this package's own integration test, not by inspection.
	Tags           pq.StringArray `gorm:"type:text[]"`
	Language       string
	TotalLessons   int
	TotalDurationS int
	WorkflowID     *string
	PublishError   *string
	PublishedAt    *time.Time
	CreatedAt      time.Time
}

func (CourseVersion) TableName() string { return "course_versions" }

// Module mirrors `modules`. Named Module (not CourseModule) because it's
// always referenced package-qualified (course.Module) outside this package.
type Module struct {
	ID              uuid.UUID `gorm:"primaryKey"`
	CourseVersionID uuid.UUID
	ModuleKey       uuid.UUID
	Position        int
	Title           string
}

func (Module) TableName() string { return "modules" }

// Lesson mirrors `lessons`. lesson_key is stable across versions
// (ADR-0011) — id is not, so progress joins on lesson_key, never id.
type Lesson struct {
	ID          uuid.UUID `gorm:"primaryKey"`
	ModuleID    uuid.UUID
	LessonKey   uuid.UUID
	Position    int
	Title       string
	ContentType string
	Content     []byte `gorm:"type:jsonb"`
	AssetKey    *string
	DurationS   int
}

func (Lesson) TableName() string { return "lessons" }

const maxLessonContentBytes = 65536 // matches the DB CHECK constraint; enforced here too so the error is a 400, not a constraint violation
