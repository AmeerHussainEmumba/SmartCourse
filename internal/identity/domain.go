// Package identity owns users, credentials, roles, and refresh/reset/
// verification tokens (RFC §4.3). Nothing outside this module touches the
// `users` table directly; other modules that need to know about a user
// (e.g. enrollment checking a student exists) declare their own narrow
// consumer-side interface (RFC §4.4) rather than importing this package's
// repository or domain types.
package identity

import (
	"time"

	"github.com/google/uuid"
)

type Role string

const (
	RoleStudent    Role = "student"
	RoleInstructor Role = "instructor"
	RoleAdmin      Role = "admin"
)

type UserStatus string

const (
	StatusActive    UserStatus = "active"
	StatusSuspended UserStatus = "suspended"
	StatusDeleted   UserStatus = "deleted"
)

// User mirrors the `users` table (migrations/000002_identity.up.sql).
type User struct {
	ID              uuid.UUID `gorm:"primaryKey"`
	Email           string
	PasswordHash    string
	FullName        string
	Role            Role
	Status          UserStatus
	TokenVersion    int16
	EmailVerifiedAt *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (User) TableName() string { return "users" }

// RefreshToken mirrors `refresh_tokens`. Only TokenHash is ever persisted —
// the raw token exists only in the response to the client (ADR-0015).
type RefreshToken struct {
	ID         uuid.UUID `gorm:"primaryKey"`
	UserID     uuid.UUID
	TokenHash  []byte
	FamilyID   uuid.UUID
	ExpiresAt  time.Time
	RevokedAt  *time.Time
	ReplacedBy *uuid.UUID
	CreatedAt  time.Time
}

func (RefreshToken) TableName() string { return "refresh_tokens" }

// PasswordResetToken mirrors `password_reset_tokens` (DR-008).
type PasswordResetToken struct {
	ID        uuid.UUID `gorm:"primaryKey"`
	UserID    uuid.UUID
	TokenHash []byte
	ExpiresAt time.Time
	UsedAt    *time.Time
	CreatedAt time.Time
}

func (PasswordResetToken) TableName() string { return "password_reset_tokens" }

// EmailVerificationToken mirrors `email_verification_tokens` (DR-008).
type EmailVerificationToken struct {
	ID        uuid.UUID `gorm:"primaryKey"`
	UserID    uuid.UUID
	TokenHash []byte
	ExpiresAt time.Time
	UsedAt    *time.Time
	CreatedAt time.Time
}

func (EmailVerificationToken) TableName() string { return "email_verification_tokens" }

const (
	passwordResetTokenTTL = 30 * time.Minute
	emailVerifyTokenTTL   = 24 * time.Hour
)
