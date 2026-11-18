// Domain logic for analytics (RFC §4.3). Thin today — one read — but the
// layering exists so this module follows the same handler → service →
// repository shape as every other module, rather than the handler holding
// a *gorm.DB directly. That inconsistency was flagged in review: it's an
// easy pattern to copy by accident if left uncorrected, and the whole
// point of a uniform module shape (RFC §4.3) is that a contributor who
// knows one module already knows the rest.
package analytics

import (
	"context"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// CanReadCourse decides whether the caller may read a course's analytics:
// admins any course; instructors any course (analytics has no course
// owner: course.course_published carries no instructor_id, so ownership
// cannot be checked without an event change in services/course); students
// only courses they hold an active or completed enrollment in (withdrawn
// does not count). Everyone else gets false, which the handler maps to 404.
func (s *Service) CanReadCourse(ctx context.Context, id auth.Identity, courseID uuid.UUID) (bool, error) {
	switch id.Role {
	case "admin", "instructor":
		return true, nil
	case "student":
		return s.repo.StudentHasEnrollment(ctx, courseID, id.UserID)
	}
	return false, nil
}

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetPlatformSnapshot(ctx context.Context) (*PlatformSnapshot, error) {
	return s.repo.GetPlatformSnapshot(ctx)
}

func (s *Service) GetCourseStats(ctx context.Context, courseID uuid.UUID) (*CourseStats, error) {
	return s.repo.GetCourseStats(ctx, courseID)
}

func (s *Service) ListDailyEnrollments(ctx context.Context, courseID uuid.UUID) ([]DailyEnrollmentRow, error) {
	return s.repo.ListDailyEnrollments(ctx, courseID)
}
