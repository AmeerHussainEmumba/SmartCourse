// Command seed populates a local database with a realistic dataset —
// instructors, courses at each lifecycle state, students with partial
// progress — so a contributor gets a working system with data in one
// command, per the Week 1 brief ("make seed").
//
// It runs entirely through the real service layer (identity/course/
// enrollment/progress), not raw INSERTs: every row it creates passes the
// same validation and transactional invariants a real request would, which
// makes this script double as an end-to-end smoke test of the four core
// modules wired together.
//
// Idempotent by design: it checks for its own sentinel admin account
// before doing anything else, so `make seed` run twice is a no-op the
// second time rather than a pile of duplicate-email errors.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/identity"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/db"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
)

// seedPassword is shared by every account this script creates. It's printed
// at the end precisely so nobody has to go looking for it — this data never
// leaves a developer's own machine (ADR-0018's local-only compose profiles).
const seedPassword = "SeedPass123!"

func main() {
	logger := logging.New("development", "info")

	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration error", slog.Any("error", err))
		os.Exit(1)
	}

	gdb, err := db.Open(cfg.Postgres)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	rdb, err := cache.Open(cfg.Redis)
	if err != nil {
		logger.Error("failed to connect to redis", slog.Any("error", err))
		os.Exit(1)
	}

	identityRepo := identity.NewRepository(gdb)
	identitySvc := identity.NewService(identityRepo, rdb, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	courseSvc := course.NewService(course.NewRepository(gdb))
	enrollmentSvc := enrollment.NewService(enrollment.NewRepository(gdb))
	progressSvc := progress.NewService(progress.NewRepository(gdb))

	ctx := context.Background()

	if _, err := identityRepo.GetUserByEmail(ctx, "admin@smartcourse.dev"); err == nil {
		logger.Info("seed data already present (admin@smartcourse.dev exists) — nothing to do")
		return
	}

	s := &seeder{
		ctx:           ctx,
		logger:        logger,
		identityRepo:  identityRepo,
		identitySvc:   identitySvc,
		courseSvc:     courseSvc,
		enrollmentSvc: enrollmentSvc,
		progressSvc:   progressSvc,
	}
	if err := s.run(); err != nil {
		logger.Error("seed failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type seeder struct {
	ctx           context.Context
	logger        *slog.Logger
	identityRepo  *identity.Repository
	identitySvc   *identity.Service
	courseSvc     *course.Service
	enrollmentSvc *enrollment.Service
	progressSvc   *progress.Service
}

func (s *seeder) run() error {
	admin, err := s.registerAs("Ada Admin", "admin@smartcourse.dev", identity.RoleAdmin)
	if err != nil {
		return fmt.Errorf("seed admin: %w", err)
	}
	s.logger.Info("seeded admin", slog.String("email", admin.email))

	instructors := make([]seededUser, 0, 3)
	for _, name := range []string{"Irfan Instructor", "Iman Instructor", "Idris Instructor"} {
		u, err := s.registerAs(name, slugEmail(name), identity.RoleInstructor)
		if err != nil {
			return fmt.Errorf("seed instructor %s: %w", name, err)
		}
		instructors = append(instructors, u)
	}
	s.logger.Info("seeded instructors", slog.Int("count", len(instructors)))

	students := make([]seededUser, 0, 12)
	for i := 1; i <= 12; i++ {
		name := fmt.Sprintf("Student %d", i)
		u, err := s.registerAs(name, fmt.Sprintf("student%d@smartcourse.dev", i), identity.RoleStudent)
		if err != nil {
			return fmt.Errorf("seed student %d: %w", i, err)
		}
		students = append(students, u)
	}
	s.logger.Info("seeded students", slog.Int("count", len(students)))

	// Course 1: published, open enrollment, students at every progress
	// stage (not started / partial / complete) — the main demo course.
	goCourse, err := s.publishedCourse(instructors[0].id, "Introduction to Go", "introduction-to-go",
		category("backend"), level("beginner"), nil,
		[]moduleSpec{
			{title: "Getting Started", lessons: []string{"Installing Go", "Your First Program"}},
			{title: "Core Concepts", lessons: []string{"Structs and Methods", "Interfaces", "Goroutines and Channels"}},
		})
	if err != nil {
		return fmt.Errorf("seed course 'Introduction to Go': %w", err)
	}
	if err := s.enrollWithProgress(goCourse, []progressPlan{
		{student: students[0], completeFraction: 1.0}, // finished the course
		{student: students[1], completeFraction: 0.4}, // partway through
		{student: students[2], completeFraction: 0.0}, // enrolled, not started
	}); err != nil {
		return fmt.Errorf("seed enrollments for 'Introduction to Go': %w", err)
	}

	// Course 2: published with a tight enrollment limit, filled to
	// capacity — demonstrates ADR-0012's seat-limit enforcement with real
	// data, not just the concurrency test's synthetic scenario.
	limit := 2
	pgCourse, err := s.publishedCourse(instructors[0].id, "Advanced PostgreSQL", "advanced-postgresql",
		category("backend"), level("advanced"), &limit,
		[]moduleSpec{
			{title: "Indexing", lessons: []string{"B-tree vs GIN", "Partial Indexes"}},
		})
	if err != nil {
		return fmt.Errorf("seed course 'Advanced PostgreSQL': %w", err)
	}
	if err := s.enrollWithProgress(pgCourse, []progressPlan{
		{student: students[3], completeFraction: 0.5},
		{student: students[4], completeFraction: 0.0},
	}); err != nil {
		return fmt.Errorf("seed enrollments for 'Advanced PostgreSQL': %w", err)
	}

	// Course 3: published, nobody enrolled yet — a freshly-live course.
	if _, err := s.publishedCourse(instructors[1].id, "Distributed Systems Essentials", "distributed-systems-essentials",
		category("backend"), level("intermediate"), nil,
		[]moduleSpec{
			{title: "Consensus", lessons: []string{"Why Consensus Is Hard", "Raft, In Practice"}},
		}); err != nil {
		return fmt.Errorf("seed course 'Distributed Systems Essentials': %w", err)
	}

	// Course 4: draft, never published — the "instructor still working on
	// it" state, exercised nowhere else in seed data.
	if err := s.draftCourse(instructors[2].id, "Kubernetes in Production", "kubernetes-in-production",
		category("devops"), level("advanced"),
		[]moduleSpec{
			{title: "Scheduling", lessons: []string{"Pods and Deployments"}},
		}); err != nil {
		return fmt.Errorf("seed course 'Kubernetes in Production': %w", err)
	}

	// Course 5: published, no modules worth demoing beyond one — a small,
	// low-friction course a new student would enroll in first.
	secCourse, err := s.publishedCourse(instructors[2].id, "Security Fundamentals", "security-fundamentals",
		category("security"), level("beginner"), nil,
		[]moduleSpec{
			{title: "Basics", lessons: []string{"Threat Modeling 101"}},
		})
	if err != nil {
		return fmt.Errorf("seed course 'Security Fundamentals': %w", err)
	}
	if err := s.enrollWithProgress(secCourse, []progressPlan{
		{student: students[5], completeFraction: 1.0},
	}); err != nil {
		return fmt.Errorf("seed enrollments for 'Security Fundamentals': %w", err)
	}

	s.logger.Info("seed complete",
		slog.String("admin_email", "admin@smartcourse.dev"),
		slog.String("instructor_email_example", "irfan-instructor@smartcourse.dev"),
		slog.String("student_email_example", "student1@smartcourse.dev"),
		slog.String("password_for_all_seeded_accounts", seedPassword),
	)
	return nil
}

type seededUser struct {
	id    uuid.UUID
	email string
}

// registerAs registers a user through the real signup flow (so password
// hashing, the verification-token write, and the outbox event all happen
// exactly as they would for a real signup), then promotes it past the
// student default where needed — registration intentionally never accepts
// a role (internal/identity/dto.go), so promotion is a second, explicit
// step, same as an admin would do through PATCH /admin/users/{id}/role.
func (s *seeder) registerAs(fullName, email string, role identity.Role) (seededUser, error) {
	resp, err := s.identitySvc.Register(s.ctx, identity.RegisterRequest{
		Email: email, Password: seedPassword, FullName: fullName,
	})
	if err != nil {
		return seededUser{}, err
	}
	id, err := uuid.Parse(resp.User.ID)
	if err != nil {
		return seededUser{}, fmt.Errorf("parse user id: %w", err)
	}
	if role != identity.RoleStudent {
		if _, err := s.identitySvc.UpdateUserRole(s.ctx, id, role, identity.StatusActive); err != nil {
			return seededUser{}, fmt.Errorf("promote to %s: %w", role, err)
		}
	}
	return seededUser{id: id, email: email}, nil
}

type moduleSpec struct {
	title   string
	lessons []string
}

// courseBuild is the shared setup behind publishedCourse and draftCourse:
// create the course, add every module and lesson from spec. Returns the
// course ID and, for each lesson in order, its lesson key — the stable
// identifier progress tracks against (ADR-0011), not the lesson's row ID.
func (s *seeder) courseBuild(instructorID uuid.UUID, title, slug string, cat, lvl *string, limit *int, modules []moduleSpec) (uuid.UUID, [][]uuid.UUID, error) {
	cResp, _, err := s.courseSvc.CreateCourse(s.ctx, instructorID, course.CreateCourseRequest{
		Title: title, Slug: slug, Description: title + " — seeded course data.",
		Category: cat, Level: lvl, EnrollmentLimit: limit,
	})
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("create course: %w", err)
	}
	courseID, err := uuid.Parse(cResp.ID)
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("parse course id: %w", err)
	}

	lessonKeysByModule := make([][]uuid.UUID, 0, len(modules))
	for i, mod := range modules {
		mResp, err := s.courseSvc.AddModule(s.ctx, courseID, instructorID, course.CreateModuleRequest{
			Title: mod.title, Position: i + 1,
		})
		if err != nil {
			return uuid.Nil, nil, fmt.Errorf("add module %q: %w", mod.title, err)
		}
		moduleID, err := uuid.Parse(mResp.ID)
		if err != nil {
			return uuid.Nil, nil, fmt.Errorf("parse module id: %w", err)
		}

		keys := make([]uuid.UUID, 0, len(mod.lessons))
		for j, lessonTitle := range mod.lessons {
			lResp, err := s.courseSvc.AddLesson(s.ctx, courseID, instructorID, moduleID, course.CreateLessonRequest{
				Title: lessonTitle, Position: j + 1, ContentType: "text",
				Content:   map[string]any{"body": lessonTitle + " — seeded lesson content."},
				DurationS: 300,
			})
			if err != nil {
				return uuid.Nil, nil, fmt.Errorf("add lesson %q: %w", lessonTitle, err)
			}
			key, err := uuid.Parse(lResp.LessonKey)
			if err != nil {
				return uuid.Nil, nil, fmt.Errorf("parse lesson key: %w", err)
			}
			keys = append(keys, key)
		}
		lessonKeysByModule = append(lessonKeysByModule, keys)
	}
	return courseID, lessonKeysByModule, nil
}

type seededCourse struct {
	id         uuid.UUID
	lessonKeys []uuid.UUID // flattened, publish order
}

func (s *seeder) publishedCourse(instructorID uuid.UUID, title, slug string, cat, lvl *string, limit *int, modules []moduleSpec) (seededCourse, error) {
	courseID, keysByModule, err := s.courseBuild(instructorID, title, slug, cat, lvl, limit, modules)
	if err != nil {
		return seededCourse{}, err
	}
	if _, err := s.courseSvc.Publish(s.ctx, courseID, instructorID); err != nil {
		return seededCourse{}, fmt.Errorf("publish course: %w", err)
	}
	var flat []uuid.UUID
	for _, keys := range keysByModule {
		flat = append(flat, keys...)
	}
	return seededCourse{id: courseID, lessonKeys: flat}, nil
}

func (s *seeder) draftCourse(instructorID uuid.UUID, title, slug string, cat, lvl *string, modules []moduleSpec) error {
	_, _, err := s.courseBuild(instructorID, title, slug, cat, lvl, nil, modules)
	return err
}

type progressPlan struct {
	student          seededUser
	completeFraction float64 // 0.0 = untouched, 1.0 = every lesson complete
}

// enrollWithProgress enrolls each planned student (through the real
// transactional Enroll path — ADR-0012's row lock included) and marks the
// requested fraction of lessons complete, in lesson order, through the
// real MarkComplete path so a 1.0 fraction also exercises course
// completion's outbox event, exactly as a real student finishing a course
// would.
func (s *seeder) enrollWithProgress(c seededCourse, plans []progressPlan) error {
	for _, p := range plans {
		e, err := s.enrollmentSvc.Enroll(s.ctx, p.student.id, c.id)
		if err != nil {
			return fmt.Errorf("enroll %s: %w", p.student.email, err)
		}
		n := int(float64(len(c.lessonKeys)) * p.completeFraction)
		for i := 0; i < n; i++ {
			if err := s.progressSvc.MarkComplete(s.ctx, p.student.id, e.ID, c.lessonKeys[i]); err != nil {
				return fmt.Errorf("mark lesson complete for %s: %w", p.student.email, err)
			}
		}
	}
	return nil
}

func category(c string) *string { return &c }
func level(l string) *string    { return &l }

func slugEmail(name string) string {
	out := make([]byte, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			out = append(out, byte(r-'A'+'a'))
		case r >= 'a' && r <= 'z':
			out = append(out, byte(r))
		case r == ' ':
			out = append(out, '-')
		}
	}
	return string(out) + "@smartcourse.dev"
}
