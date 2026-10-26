// Package enrollment owns enrollments, seat limits, and prerequisites (RFC
// §4.3, §8.1) — the module whose correctness under concurrency is the
// single most scrutinised property in this codebase (ADR-0012).
package enrollment

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusActive    Status = "active"
	StatusCompleted Status = "completed"
	StatusWithdrawn Status = "withdrawn"
)

// Enrollment mirrors `enrollments` (migrations/000005_enrollment.up.sql).
type Enrollment struct {
	ID          uuid.UUID `gorm:"primaryKey"`
	StudentID   uuid.UUID
	CourseID    uuid.UUID
	Status      Status
	EnrolledAt  time.Time
	CompletedAt *time.Time
	WithdrawnAt *time.Time
}

func (Enrollment) TableName() string { return "enrollments" }
