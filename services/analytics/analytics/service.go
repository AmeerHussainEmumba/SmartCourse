// Domain logic for analytics (RFC §4.3). Thin — reads, plus the popular-courses
// cache fallback — but the
// layering exists so this module follows the same handler → service →
// repository shape as every other module, rather than the handler holding
// a *gorm.DB directly. That inconsistency was flagged in review: it's an
// easy pattern to copy by accident if left uncorrected, and the whole
// point of a uniform module shape (RFC §4.3) is that a contributor who
// knows one module already knows the rest.
package analytics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// CanReadCourse decides whether the caller may read a course's analytics:
// admins any course; instructors only a course they own (the instructor_id
// recorded from course.course_published; a course whose owner is not
// recorded yet is readable by admins only); students only courses they hold
// an active or completed enrollment in (withdrawn does not count). Everyone
// else gets false, which the handler maps to 404 like every other
// ownership denial (F-36).
func (s *Service) CanReadCourse(ctx context.Context, id auth.Identity, courseID uuid.UUID) (bool, error) {
	switch id.Role {
	case "admin":
		return true, nil
	case "instructor":
		return s.repo.InstructorOwnsCourse(ctx, courseID, id.UserID)
	case "student":
		return s.repo.StudentHasEnrollment(ctx, courseID, id.UserID)
	}
	return false, nil
}

type Service struct {
	repo      *Repository
	rdb       *redis.Client
	failed    FailedEventsSource
	workflows WorkflowIssuesSource
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// WithPopularCache lets GetPopularCourses read the Reconciler's Redis sorted
// set. Without it (or whenever Redis is empty or failing) the ranking comes
// straight from Postgres.
func (s *Service) WithPopularCache(rdb *redis.Client) *Service {
	s.rdb = rdb
	return s
}

// WithIssueSources attaches the dead-letter and workflow-failure readers
// (issues.go). Without both, GetIssues reports ErrIssuesNotConfigured.
func (s *Service) WithIssueSources(failed FailedEventsSource, workflows WorkflowIssuesSource) *Service {
	s.failed, s.workflows = failed, workflows
	return s
}

// ErrIssuesNotConfigured means no FailedEventsSource/WorkflowIssuesSource
// was attached to this Service.
var ErrIssuesNotConfigured = errors.New("analytics: failed-events and workflow-issues sources are not configured")

// GetIssues returns the Failed Events / Workflow Issues metric.
func (s *Service) GetIssues(ctx context.Context) (*IssuesReport, error) {
	if s.failed == nil || s.workflows == nil {
		return nil, ErrIssuesNotConfigured
	}
	fe, err := s.failed.SummarizeFailedEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("summarize failed events: %w", err)
	}
	wf, err := s.workflows.SummarizeWorkflowIssues(ctx)
	if err != nil {
		return nil, fmt.Errorf("summarize workflow issues: %w", err)
	}
	return &IssuesReport{FailedEvents: fe, WorkflowIssues: wf}, nil
}

const (
	defaultPopularLimit = 10
	maxPopularLimit     = 50

	// popularRedisTimeout bounds the cache read so a hung Redis costs the
	// caller a fraction of a second before the Postgres fallback, not the
	// request's whole budget.
	popularRedisTimeout = 300 * time.Millisecond
)

// GetPopularCourses returns the Most Popular Courses ranking (FR-5), at most
// limit entries (non-positive means the default, capped at maxPopularLimit).
// It reads the Redis sorted set the Reconciler maintains; if Redis is
// absent, down, slow or simply empty, it falls back to the same ranking in
// Postgres. A Redis problem is logged and never surfaces to the caller.
func (s *Service) GetPopularCourses(ctx context.Context, limit int) ([]PopularCourse, error) {
	if limit <= 0 {
		limit = defaultPopularLimit
	}
	if limit > maxPopularLimit {
		limit = maxPopularLimit
	}
	if s.rdb != nil {
		if out, err := s.popularFromRedis(ctx, limit); err != nil {
			slog.Warn("analytics: popular-courses cache unavailable, using Postgres", slog.Any("error", err))
		} else if len(out) > 0 {
			return out, nil
		}
	}
	return s.repo.PopularCourses(ctx, limit)
}

func (s *Service) popularFromRedis(ctx context.Context, limit int) ([]PopularCourse, error) {
	rctx, cancel := context.WithTimeout(ctx, popularRedisTimeout)
	defer cancel()
	zs, err := s.rdb.ZRevRangeWithScores(rctx, PopularCoursesKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PopularCourse, 0, len(zs))
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		id, err := uuid.Parse(member)
		if err != nil {
			continue // not ours; never fail the read over a stray member
		}
		out = append(out, PopularCourse{CourseID: id, Enrollments: int64(z.Score)})
	}
	return out, nil
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
