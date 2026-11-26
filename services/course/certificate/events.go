package certificate

import (
	"time"

	"github.com/google/uuid"
)

// EventCertificateIssued is the outbox event_type written in the transaction
// that inserts the certificate row.
const EventCertificateIssued = "progress.certificate_issued"

// CertificateIssuedPayload is progress.v1.CertificateIssued (libs/events). The
// JSON names are the proto field names; TestPayloadContract_CertificateIssued
// (libs/events/eventstest) keeps them in step. There is no verification_code: it is a bearer
// token for the public verification endpoint and stays out of Kafka and the
// permanent archive.
type CertificateIssuedPayload struct {
	CertificateID uuid.UUID `json:"certificate_id"`
	EnrollmentID  uuid.UUID `json:"enrollment_id"`
	StudentID     uuid.UUID `json:"student_id"`
	CourseID      uuid.UUID `json:"course_id"`
	CourseTitle   string    `json:"course_title"`
	IssuedAt      time.Time `json:"issued_at"`
}
