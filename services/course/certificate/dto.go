package certificate

const timeFormat = "2006-01-02T15:04:05Z07:00"

// CertificateResponse is what the owner sees. The verification code is
// included here and only here: it is what the holder shares with a verifier.
type CertificateResponse struct {
	ID               string  `json:"id"`
	EnrollmentID     string  `json:"enrollment_id"`
	CourseID         string  `json:"course_id"`
	CourseVersionID  *string `json:"course_version_id,omitempty"`
	CourseTitle      string  `json:"course_title"`
	StudentName      string  `json:"student_name"`
	VerificationCode string  `json:"verification_code"`
	IssuedAt         string  `json:"issued_at"`
}

// VerificationResponse is everything a third party learns from
// GET /certificates/verify/{code} (RFC §8.4): course title, student name and
// issue date, and nothing else — no ids, no student_id, no code echo.
type VerificationResponse struct {
	CourseTitle string `json:"course_title"`
	StudentName string `json:"student_name"`
	IssuedAt    string `json:"issued_at"`
}

func toCertificateResponse(c *Certificate) CertificateResponse {
	resp := CertificateResponse{
		ID: c.ID.String(), EnrollmentID: c.EnrollmentID.String(), CourseID: c.CourseID.String(),
		CourseTitle: c.CourseTitle, StudentName: c.StudentName, VerificationCode: c.VerificationCode,
		IssuedAt: c.IssuedAt.Format(timeFormat),
	}
	if c.CourseVersionID != nil {
		s := c.CourseVersionID.String()
		resp.CourseVersionID = &s
	}
	return resp
}

func toVerificationResponse(c *Certificate) VerificationResponse {
	return VerificationResponse{
		CourseTitle: c.CourseTitle, StudentName: c.StudentName, IssuedAt: c.IssuedAt.Format(timeFormat),
	}
}
