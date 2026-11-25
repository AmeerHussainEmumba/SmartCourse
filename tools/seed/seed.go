package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// errPublishUnavailable means the publish workflow cannot run (Temporal or the
// publishing worker is down): the start call kept answering 503, or the
// version never left `publishing`. The seed carries on with the data that does
// not need it and exits non-zero at the end.
var errPublishUnavailable = errors.New("publishing is unavailable (needs Temporal and the course/publishing workers: `make dev-workflow`)")

type account struct {
	name, email, role string
	id, token         string
}

type authResponse struct {
	AccessToken string `json:"access_token"`
	User        struct {
		ID   string `json:"id"`
		Role string `json:"role"`
	} `json:"user"`
}

// seeder drives the public HTTP APIs; it imports no service code.
type seeder struct {
	cfg      config
	logger   *slog.Logger
	out      io.Writer
	identity *client
	course   *client
	analytic *client

	admin       account
	instructors []account
	students    []account

	// publishDown is set when publishing proved unavailable (or was skipped on
	// request): published courses are then not created at all, and the run says so.
	publishDown bool
	pubSkipped  []string
	stale       []string // courses that exist but are not published as the dataset wants

	usersCreated, usersExisting     int
	coursesCreated, coursesExisting int
	modulesCreated, lessonsCreated  int
	enrollmentsDone, lessonsMarked  int
	publishedCount, archivedCount   int
	draftCount                      int
}

func newSeeder(cfg config, logger *slog.Logger, out io.Writer) *seeder {
	return &seeder{
		cfg: cfg, logger: logger, out: out,
		identity: newClient(cfg.IdentityURL, logger),
		course:   newClient(cfg.CourseURL, logger),
		analytic: newClient(cfg.AnalyticsURL, logger),
	}
}

// run executes the whole flow and returns the process exit code: 0 done, 1
// failure (a dependency is down, a call was refused), 2 done but the publish
// workflow was unavailable so the published courses are missing.
func (s *seeder) run(ctx context.Context) int {
	if err := s.waitReady(ctx); err != nil {
		s.logger.Error("a dependency is not ready", slog.Any("error", err))
		return 1
	}
	if err := s.seed(ctx); err != nil {
		s.logger.Error("seed failed", slog.Any("error", err))
		return 1
	}
	s.summary()
	if len(s.pubSkipped) > 0 && !s.cfg.SkipPublish {
		return 2
	}
	return 0
}

func (s *seeder) waitReady(ctx context.Context) error {
	for _, d := range []struct {
		name string
		c    *client
	}{{"identity-api", s.identity}, {"course-api", s.course}, {"analytics-api", s.analytic}} {
		if err := d.c.ready(ctx, s.cfg.ReadyWait); err != nil {
			return fmt.Errorf("%s at %s did not become ready within %s (is the stack up? `make dev-core`): %w",
				d.name, d.c.base, s.cfg.ReadyWait, err)
		}
		s.logger.Info("service ready", slog.String("service", d.name))
	}
	return nil
}

func (s *seeder) seed(ctx context.Context) error {
	admin, err := s.login(ctx, s.cfg.AdminEmail, s.cfg.AdminPassword)
	if err != nil {
		if statusOf(err) == http.StatusUnauthorized {
			return fmt.Errorf("cannot log in as the admin %s: create it first with identity-admin (`make seed` does), "+
				"or set SEED_ADMIN_EMAIL and SEED_ADMIN_PASSWORD to an existing admin", s.cfg.AdminEmail)
		}
		return fmt.Errorf("admin login: %w", err)
	}
	if admin.role != "admin" {
		return fmt.Errorf("%s logged in but is a %s, not an admin: promotion would fail", s.cfg.AdminEmail, admin.role)
	}
	s.admin = admin
	s.logger.Info("logged in as the admin", slog.String("email", admin.email))

	for _, n := range instructorNames {
		a, err := s.ensureUser(ctx, n, instructorEmail(n), roleInstructor)
		if err != nil {
			return fmt.Errorf("instructor %s: %w", n, err)
		}
		s.instructors = append(s.instructors, a)
	}
	for i := 0; i < studentCount; i++ {
		a, err := s.ensureUser(ctx, studentName(i), studentEmail(i), roleStudent)
		if err != nil {
			return fmt.Errorf("student %d: %w", i+1, err)
		}
		s.students = append(s.students, a)
	}
	s.logger.Info("accounts ready", slog.Int("instructors", len(s.instructors)), slog.Int("students", len(s.students)))

	if s.cfg.SkipPublish {
		s.publishDown = true
		s.logger.Warn("SEED_SKIP_PUBLISH=1: published and archived courses, and enrollments, are skipped")
	}
	for _, spec := range dataset {
		if err := s.ensureCourse(ctx, spec); err != nil {
			return fmt.Errorf("course %q: %w", spec.title, err)
		}
	}
	return nil
}

func (s *seeder) login(ctx context.Context, email, password string) (account, error) {
	var resp authResponse
	err := s.identity.do(ctx, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"email": email, "password": password}, &resp)
	if err != nil {
		return account{}, err
	}
	return account{email: email, id: resp.User.ID, token: resp.AccessToken, role: resp.User.Role}, nil
}

// ensureUser logs in as the account and registers it first when the login is
// refused (an existing account is thereby detected, not duplicated), then
// promotes it to the wanted role through the admin endpoint and logs in again,
// because a role change invalidates the old token (ADR-0015 addendum).
func (s *seeder) ensureUser(ctx context.Context, name, email, role string) (account, error) {
	a, err := s.login(ctx, email, seedPassword)
	switch {
	case err == nil:
		s.usersExisting++
	case statusOf(err) == http.StatusUnauthorized:
		if err := s.identity.do(ctx, http.MethodPost, "/api/v1/auth/register", "",
			map[string]string{"email": email, "password": seedPassword, "full_name": name}, nil); err != nil {
			return account{}, fmt.Errorf("register: %w", err)
		}
		if a, err = s.login(ctx, email, seedPassword); err != nil {
			return account{}, fmt.Errorf("login after register: %w", err)
		}
		s.usersCreated++
	default:
		return account{}, fmt.Errorf("login: %w", err)
	}
	a.name = name
	if a.role != role {
		err := s.identity.do(ctx, http.MethodPatch, "/api/v1/admin/users/"+url.PathEscape(a.id)+"/role", s.admin.token,
			map[string]string{"role": role, "status": "active"}, nil)
		if err != nil {
			return account{}, fmt.Errorf("promote to %s: %w", role, err)
		}
		if a, err = s.login(ctx, email, seedPassword); err != nil {
			return account{}, fmt.Errorf("login after promotion: %w", err)
		}
		a.name = name
		if a.role != role {
			return account{}, fmt.Errorf("promotion to %s did not take: role is %s", role, a.role)
		}
	}
	return a, nil
}

type idResponse struct {
	ID string `json:"id"`
}

type lessonResponse struct {
	LessonKey string `json:"lesson_key"`
}

type courseDetail struct {
	Course struct {
		ID string `json:"id"`
	} `json:"course"`
	Modules []struct {
		Lessons []lessonResponse `json:"lessons"`
	} `json:"modules"`
}

func (d courseDetail) lessonKeys() []string {
	var keys []string
	for _, m := range d.Modules {
		for _, l := range m.Lessons {
			keys = append(keys, l.LessonKey)
		}
	}
	return keys
}

func (s *seeder) ensureCourse(ctx context.Context, spec courseSpec) error {
	owner := s.instructors[spec.instructor]
	needsPublish := spec.state != stateDraft
	if needsPublish && s.publishDown {
		s.pubSkipped = append(s.pubSkipped, spec.title)
		s.logger.Warn("skipping a course that needs publishing", slog.String("course", spec.title))
		return nil
	}

	body := map[string]any{
		"title": spec.title, "slug": spec.slug, "description": spec.title + " - seeded course data.",
		"category": spec.category, "level": spec.level,
	}
	if spec.limit > 0 {
		body["enrollment_limit"] = spec.limit
	}
	var created idResponse
	err := s.course.do(ctx, http.MethodPost, "/api/v1/courses", owner.token, body, &created)
	if statusOf(err) == http.StatusConflict {
		return s.existingCourse(ctx, spec)
	}
	if err != nil {
		return fmt.Errorf("create course: %w", err)
	}
	s.coursesCreated++

	for i, m := range spec.modules {
		var mod idResponse
		if err := s.course.do(ctx, http.MethodPost, "/api/v1/courses/"+created.ID+"/modules", owner.token,
			map[string]any{"title": m.title, "position": i + 1}, &mod); err != nil {
			return fmt.Errorf("add module %q: %w", m.title, err)
		}
		s.modulesCreated++
		for j, l := range m.lessons {
			if err := s.course.do(ctx, http.MethodPost, "/api/v1/modules/"+mod.ID+"/lessons", owner.token, map[string]any{
				"title": l, "position": j + 1, "content_type": "text",
				"content": map[string]any{"body": l + " - seeded lesson content."}, "duration_s": 300,
			}, nil); err != nil {
				return fmt.Errorf("add lesson %q: %w", l, err)
			}
			s.lessonsCreated++
		}
	}

	if spec.state == stateDraft {
		s.draftCount++
		return nil
	}
	if err := s.publish(ctx, created.ID, owner.token); err != nil {
		if errors.Is(err, errPublishUnavailable) {
			s.publishDown = true
			s.pubSkipped = append(s.pubSkipped, spec.title+" (created as a draft; it stays a draft until the database is reset)")
			s.logger.Error("publishing is unavailable; creating only the data that does not need it", slog.String("course", spec.title))
			return nil
		}
		return err
	}
	s.publishedCount++
	if spec.state == stateArchived {
		if err := s.course.do(ctx, http.MethodPost, "/api/v1/courses/"+created.ID+"/archive", owner.token, nil, nil); err != nil {
			return fmt.Errorf("archive: %w", err)
		}
		s.archivedCount++
		return nil
	}
	return s.enroll(ctx, spec, created.ID)
}

// existingCourse handles a slug that is already taken (an earlier run): a
// published course is topped up with its enrollments and progress; a draft or
// archived one is left alone.
func (s *seeder) existingCourse(ctx context.Context, spec courseSpec) error {
	s.coursesExisting++
	if spec.state != statePublished {
		return nil
	}
	var detail courseDetail
	err := s.course.do(ctx, http.MethodGet, "/api/v1/courses/"+url.PathEscape(spec.slug), "", nil, &detail)
	if statusOf(err) == http.StatusNotFound {
		s.stale = append(s.stale, spec.title)
		s.logger.Warn("course exists but is not published (a half-finished earlier run): reset the database for a clean seed",
			slog.String("course", spec.title))
		return nil
	}
	if err != nil {
		return fmt.Errorf("read existing course: %w", err)
	}
	s.publishedCount++
	return s.enrollWithKeys(ctx, spec, detail.Course.ID, detail.lessonKeys())
}

// publish starts the real publish workflow and waits for `ready`.
func (s *seeder) publish(ctx context.Context, courseID, token string) error {
	// The start call can answer 503 for a while right after a cold start (the
	// first Temporal RPC races the server), so retry a few times.
	var err error
	for attempt := 1; attempt <= s.cfg.PublishStartTries; attempt++ {
		err = s.course.do(ctx, http.MethodPost, "/api/v1/courses/"+courseID+"/publish", token, nil, nil)
		if statusOf(err) != http.StatusServiceUnavailable {
			break
		}
		if serr := s.course.sleep(ctx, time.Duration(attempt)*s.cfg.PublishRetryStep); serr != nil {
			return serr
		}
	}
	if statusOf(err) == http.StatusServiceUnavailable {
		return errPublishUnavailable
	}
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	deadline := time.Now().Add(s.cfg.PublishTimeout)
	for {
		var st struct {
			State string  `json:"state"`
			Error *string `json:"error"`
		}
		if err := s.course.do(ctx, http.MethodGet, "/api/v1/courses/"+courseID+"/publish-status", token, nil, &st); err != nil {
			return fmt.Errorf("publish status: %w", err)
		}
		switch st.State {
		case "ready":
			return nil
		case "publish_failed":
			msg := "unknown error"
			if st.Error != nil {
				msg = *st.Error
			}
			return fmt.Errorf("publish failed: %s", msg)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("publish did not finish within %s: %w", s.cfg.PublishTimeout, errPublishUnavailable)
		}
		if err := s.course.sleep(ctx, s.cfg.PublishPoll); err != nil {
			return err
		}
	}
}

func (s *seeder) enroll(ctx context.Context, spec courseSpec, courseID string) error {
	if len(spec.plans) == 0 {
		return nil
	}
	var detail courseDetail
	if err := s.course.do(ctx, http.MethodGet, "/api/v1/courses/"+url.PathEscape(spec.slug), "", nil, &detail); err != nil {
		return fmt.Errorf("read published course: %w", err)
	}
	return s.enrollWithKeys(ctx, spec, courseID, detail.lessonKeys())
}

// enrollWithKeys enrolls each planned student through the real enroll endpoint
// (an existing enrollment is found, not repeated) and marks the planned share
// of lessons complete through the real progress endpoint (idempotent).
func (s *seeder) enrollWithKeys(ctx context.Context, spec courseSpec, courseID string, keys []string) error {
	for _, p := range spec.plans {
		st := s.students[p.student]
		enrollmentID, err := s.enrollOne(ctx, st, courseID)
		if err != nil {
			return fmt.Errorf("enroll %s: %w", st.email, err)
		}
		n := int(float64(len(keys)) * p.completeFraction)
		for i := 0; i < n; i++ {
			if err := s.course.do(ctx, http.MethodPut,
				"/api/v1/enrollments/"+enrollmentID+"/lessons/"+keys[i]+"/complete", st.token, nil, nil); err != nil {
				return fmt.Errorf("mark lesson complete for %s: %w", st.email, err)
			}
			s.lessonsMarked++
		}
	}
	return nil
}

func (s *seeder) enrollOne(ctx context.Context, st account, courseID string) (string, error) {
	var e idResponse
	err := s.course.do(ctx, http.MethodPost, "/api/v1/enrollments", st.token, map[string]string{"course_id": courseID}, &e)
	if err == nil {
		s.enrollmentsDone++
		return e.ID, nil
	}
	if code := codeOf(err); statusOf(err) != http.StatusConflict || (code != "already_enrolled" && code != "course_full") {
		return "", err
	}
	var list struct {
		Items []struct {
			ID       string `json:"id"`
			CourseID string `json:"course_id"`
		} `json:"items"`
	}
	if lerr := s.course.do(ctx, http.MethodGet, "/api/v1/enrollments", st.token, nil, &list); lerr != nil {
		return "", lerr
	}
	for _, it := range list.Items {
		if it.CourseID == courseID {
			return it.ID, nil
		}
	}
	return "", err // full, and not by this student
}

func (s *seeder) summary() {
	w := s.out
	_, _ = fmt.Fprintf(w, "\nSeed summary\n")
	_, _ = fmt.Fprintf(w, "  admin:       %s (credentials: SEED_ADMIN_EMAIL / SEED_ADMIN_PASSWORD; created by identity-admin)\n", s.cfg.AdminEmail)
	_, _ = fmt.Fprintf(w, "  instructors: %d, e.g. %s\n", len(s.instructors), instructorEmail(instructorNames[0]))
	_, _ = fmt.Fprintf(w, "  students:    %d, e.g. %s\n", len(s.students), studentEmail(0))
	_, _ = fmt.Fprintf(w, "  password for every instructor and student account: %s\n", seedPassword)
	_, _ = fmt.Fprintf(w, "  accounts:    %d created, %d already existed\n", s.usersCreated, s.usersExisting)
	_, _ = fmt.Fprintf(w, "  courses:     %d created, %d already existed (published %d, archived %d, draft %d this run)\n",
		s.coursesCreated, s.coursesExisting, s.publishedCount, s.archivedCount, s.draftCount)
	_, _ = fmt.Fprintf(w, "  modules/lessons created: %d / %d\n", s.modulesCreated, s.lessonsCreated)
	_, _ = fmt.Fprintf(w, "  enrollments created: %d; lesson completions sent: %d\n", s.enrollmentsDone, s.lessonsMarked)
	if len(s.pubSkipped) > 0 {
		why := errPublishUnavailable.Error()
		if s.cfg.SkipPublish {
			why = "SEED_SKIP_PUBLISH=1"
		}
		_, _ = fmt.Fprintf(w, "\nWARNING: not seeded because %s:\n", why)
		for _, t := range s.pubSkipped {
			_, _ = fmt.Fprintf(w, "  - %s\n", t)
		}
		_, _ = fmt.Fprintf(w, "  Everything else was created. Run the seed again once publishing works to add them.\n")
	}
	for _, t := range s.stale {
		_, _ = fmt.Fprintf(w, "WARNING: %q exists but is not published (half-finished earlier run); reset the database for a clean seed.\n", t)
	}
}
