// What this file checks: CertificateIssuedPayload (events.go) conforms to
// progress.v1.CertificateIssued in libs/events: the same field names, no more
// and no fewer (in particular no verification_code). See
// services/course/progress/payload_contract_test.go for why a producer test
// may import libs/events although the production code does not.
package certificate_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
)

func TestPayloadContract_CertificateIssued(t *testing.T) {
	raw, err := json.Marshal(certificate.CertificateIssuedPayload{
		CertificateID: uuid.New(), EnrollmentID: uuid.New(), StudentID: uuid.New(),
		CourseID: uuid.New(), CourseTitle: "Intro", IssuedAt: time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	eventstest.RequireConforms(t, certificate.EventCertificateIssued, raw)
}
