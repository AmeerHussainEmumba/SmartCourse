// Package progress owns lesson-level progress and course completion (RFC
// §4.3, §8.3). Certificate issuance (RFC §8.4) is M3 scope — it's
// event-triggered (CourseCompleted -> Kafka -> a consumer), and no
// consumer infrastructure exists yet (M0/M1). The `certificates` table
// exists from M0 (RFC §21's full-schema-up-front rule); this module
// doesn't write to it yet. See README.md.
package progress

import (
	"time"

	"github.com/google/uuid"
)

// LessonProgress mirrors `lesson_progress` (migrations/000005_enrollment.up.sql).
// Rows are pre-created at enrollment time (internal/enrollment) — this
// module only ever updates them, never inserts.
type LessonProgress struct {
	ID            uuid.UUID `gorm:"primaryKey"`
	EnrollmentID  uuid.UUID
	LessonKey     uuid.UUID
	CompletedAt   *time.Time
	LastPositionS int
	UpdatedAt     time.Time
}

func (LessonProgress) TableName() string { return "lesson_progress" }
