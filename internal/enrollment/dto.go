package enrollment

type EnrollRequest struct {
	CourseID string `json:"course_id" binding:"required,uuid"`
}

type EnrollmentResponse struct {
	ID          string  `json:"id"`
	CourseID    string  `json:"course_id"`
	Status      string  `json:"status"`
	EnrolledAt  string  `json:"enrolled_at"`
	WithdrawnAt *string `json:"withdrawn_at,omitempty"`
}

func toEnrollmentResponse(e *Enrollment) EnrollmentResponse {
	resp := EnrollmentResponse{
		ID: e.ID.String(), CourseID: e.CourseID.String(), Status: string(e.Status),
		EnrolledAt: e.EnrolledAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	if e.WithdrawnAt != nil {
		s := e.WithdrawnAt.Format("2006-01-02T15:04:05Z07:00")
		resp.WithdrawnAt = &s
	}
	return resp
}
