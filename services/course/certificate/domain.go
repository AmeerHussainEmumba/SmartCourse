// Package certificate issues and serves course-completion certificates
// (RFC §4.3, §8.4). A certificate is a record with a verification code, not
// a rendered document (RFC assumption A3).
package certificate

import (
	"crypto/rand"
	"encoding/base32"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Certificate mirrors `certificates` (migrations/course/000003_enrollment.up.sql
// and 000007_certificates.up.sql). Immutable once written (RFC §8.4): the
// module has no update or delete path.
type Certificate struct {
	ID               uuid.UUID `gorm:"primaryKey"`
	EnrollmentID     uuid.UUID
	StudentID        uuid.UUID
	CourseID         uuid.UUID
	CourseVersionID  *uuid.UUID
	VerificationCode string
	StudentName      string
	CourseTitle      string
	IssuedAt         time.Time
}

func (Certificate) TableName() string { return "certificates" }

// verificationCodePattern is what newVerificationCode produces and what the
// certificates_verification_code_format CHECK accepts: 26 base32 characters.
var verificationCodePattern = regexp.MustCompile(`^[A-Z2-7]{26}$`)

// newVerificationCode returns 16 random bytes (128 bits) from crypto/rand as
// unpadded base32. The code is a bearer token for the public verification
// endpoint, so it must not be derivable from anything else, which rules out
// UUIDv7 (time-ordered) and hashes of the enrollment id.
func newVerificationCode() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]), nil
}
