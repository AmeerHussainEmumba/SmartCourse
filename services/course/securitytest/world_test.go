//go:build integration

// Package securitytest holds the cross-package adversarial tests for the
// course module's three HTTP surfaces (course, enrollment, progress). It
// lives in its own package, not inside services/course/course, so one real
// gin.Engine can carry all three route sets exactly the way
// services/course/cmd/course-api wires them. Test files only.
package securitytest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

func jwtCfg() config.JWTConfig {
	return config.JWTConfig{SigningKey: "test-signing-key-at-least-32-bytes-long!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: time.Hour, Issuer: "smartcourse-test"}
}

// fakeTemporal overrides only ExecuteWorkflow (what Service.Publish calls).
type fakeTemporal struct{ temporalclient.Client }

type fakeRun struct{ id string }

func (f *fakeRun) GetID() string                  { return f.id }
func (f *fakeRun) GetRunID() string               { return "" }
func (f *fakeRun) GetFirstExecutionRunID() string { return "" }
func (f *fakeRun) Get(context.Context, any) error { return nil }
func (f *fakeRun) GetWithOptions(context.Context, any, temporalclient.WorkflowRunGetOptions) error {
	return nil
}

func (fakeTemporal) ExecuteWorkflow(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
	return &fakeRun{id: o.ID}, nil
}

type actor struct {
	name  string
	id    uuid.UUID
	role  string
	token string
}

const (
	anon = iota
	studentA
	studentB
	instrA
	instrB
	admin
)

type world struct {
	t       *testing.T
	env     *testenv.Env
	r       *gin.Engine
	svc     *course.Service
	esvc    *enrollment.Service
	psvc    *progress.Service
	actors  [6]actor
	counter atomic.Int64
}

func newWorld(t *testing.T) *world {
	t.Helper()
	env := testenv.Setup(t)
	w := &world{t: t, env: env}
	w.svc = course.NewService(course.NewRepository(env.DB), env.RDB, fakeTemporal{}, "q")
	w.esvc = enrollment.NewService(enrollment.NewRepository(env.DB))
	w.psvc = progress.NewService(progress.NewRepository(env.DB))
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery())
	api := r.Group("/api/v1")
	course.RegisterRoutes(api, w.svc, jwtCfg())
	enrollment.RegisterRoutes(api, w.esvc, jwtCfg())
	progress.RegisterRoutes(api, w.psvc, jwtCfg())
	w.r = r

	for i, spec := range []struct{ name, role string }{
		{"anonymous", ""}, {"studentA", "student"}, {"studentB", "student"},
		{"instructorA", "instructor"}, {"instructorB", "instructor"}, {"admin", "admin"},
	} {
		a := actor{name: spec.name, role: spec.role}
		if spec.role != "" {
			a.id = uuid.Must(uuid.NewV7())
			if err := env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', ?, ?::user_role, 'active')`,
				a.id, a.id.String()+"@example.com", spec.name, spec.role).Error; err != nil {
				t.Fatalf("seed %s: %v", spec.name, err)
			}
			if spec.role == "instructor" {
				if err := env.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name) VALUES (?, ?)`, a.id, spec.name).Error; err != nil {
					t.Fatal(err)
				}
			}
			tok, err := auth.IssueAccessToken(jwtCfg(), a.id, spec.role, 1)
			if err != nil {
				t.Fatal(err)
			}
			a.token = tok
		}
		w.actors[i] = a
	}
	return w
}

func (w *world) next() string { return fmt.Sprintf("%d", w.counter.Add(1)) }

// do sends a request as actor idx.
func (w *world) do(idx int, method, path string, body any) *httptest.ResponseRecorder {
	return w.doRaw(w.actors[idx].token, method, path, body)
}

func (w *world) doRaw(token, method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	switch b := body.(type) {
	case nil:
	case []byte:
		buf.Write(b)
	case string:
		buf.WriteString(b)
	default:
		if err := json.NewEncoder(&buf).Encode(b); err != nil {
			w.t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	w.r.ServeHTTP(rec, req)
	return rec
}

// fixture ids -----------------------------------------------------------------

type courseFx struct {
	CourseID  uuid.UUID
	Slug      string
	ModuleID  uuid.UUID
	LessonID  uuid.UUID
	LessonKey uuid.UUID
	Title     string // unique marker, used to detect data leaks
}

// newDraft creates a draft course (1 module, 1 lesson) owned by actor owner.
func (w *world) newDraft(owner int) courseFx {
	w.t.Helper()
	ctx := context.Background()
	n := w.next()
	fx := courseFx{Slug: "sec-" + n + "-" + strings.ToLower(uuid.NewString()[:6]), Title: "SECRET-TITLE-" + uuid.NewString()}
	c, _, err := w.svc.CreateCourse(ctx, w.actors[owner].id, course.CreateCourseRequest{Title: fx.Title, Slug: fx.Slug, Description: "d"})
	if err != nil {
		w.t.Fatalf("CreateCourse: %v", err)
	}
	fx.CourseID = uuid.MustParse(c.ID)
	m, err := w.svc.AddModule(ctx, fx.CourseID, w.actors[owner].id, course.CreateModuleRequest{Title: "m1", Position: 1})
	if err != nil {
		w.t.Fatalf("AddModule: %v", err)
	}
	fx.ModuleID = uuid.MustParse(m.ID)
	l, err := w.svc.AddLesson(ctx, fx.CourseID, w.actors[owner].id, fx.ModuleID, course.CreateLessonRequest{Title: "l1", Position: 1, ContentType: "text", Content: map[string]any{"body": "hello"}, DurationS: 60})
	if err != nil {
		w.t.Fatalf("AddLesson: %v", err)
	}
	fx.LessonID = uuid.MustParse(l.ID)
	fx.LessonKey = uuid.MustParse(l.LessonKey)
	return fx
}

// newPublished creates and publishes a course through the same activity
// sequence the real workflow runs (services/course/workflows), without a
// Temporal server.
func (w *world) newPublished(owner int) courseFx {
	w.t.Helper()
	fx := w.newDraft(owner)
	w.publish(fx.CourseID, owner)
	return fx
}

func (w *world) publish(courseID uuid.UUID, owner int) {
	w.t.Helper()
	ctx := context.Background()
	c, err := w.svc.GetCourse(ctx, courseID)
	if err != nil || c.DraftVersionID == nil {
		w.t.Fatalf("publish setup: %v", err)
	}
	draft, old := *c.DraftVersionID, c.PublishedVersionID
	must := func(err error) {
		if err != nil {
			w.t.Fatalf("publish pipeline: %v", err)
		}
	}
	must(w.svc.ValidateMetadata(ctx, draft))
	must(w.svc.VerifyAssets(ctx, draft))
	must(w.svc.ComputeMetadata(ctx, draft))
	o, err := w.svc.BuildOutline(ctx, courseID, draft)
	must(err)
	must(w.svc.IndexSearch(ctx, o))
	must(w.svc.WarmCache(ctx, o))
	must(w.svc.PromoteAndPublish(ctx, courseID, draft, old, o.Title))
}

func (w *world) enroll(student int, courseID uuid.UUID) uuid.UUID {
	w.t.Helper()
	e, err := w.esvc.Enroll(context.Background(), w.actors[student].id, courseID)
	if err != nil {
		w.t.Fatalf("Enroll: %v", err)
	}
	return e.ID
}

// leak scan ---------------------------------------------------------------------

var internalLeaks = []string{
	"pq:", "pgx", "sqlstate", "gorm", "relation \"", "syntax error", "goroutine", ".go:", "panic", "runtime error",
	"select ", "insert into", "update ", "violates", "constraint", "duplicate key", "redis:", "dial tcp", "stack", "temporal",
	"/users/", "/srv/", "github.com", "course_versions", "enrollments_", "invalid input syntax", "out of range",
}

func assertNoInternalLeak(t *testing.T, label string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code < 400 {
		return
	}
	lower := strings.ToLower(rec.Body.String())
	for _, l := range internalLeaks {
		if strings.Contains(lower, l) {
			t.Errorf("%s: %d body leaks %q: %.300s", label, rec.Code, l, rec.Body.String())
		}
	}
	if rec.Code >= 500 {
		var b struct{ Code string }
		_ = json.Unmarshal(rec.Body.Bytes(), &b)
		if b.Code != "internal_error" && b.Code != "temporarily_unavailable" {
			t.Errorf("%s: %d with non-envelope body %.200q", label, rec.Code, rec.Body.String())
		}
	}
}

func okStatus(code int) bool { return code >= 200 && code < 300 }

var _ = http.StatusOK

var ctxBG = context.Background()

func issue(id uuid.UUID, role string) (string, error) {
	return auth.IssueAccessToken(jwtCfg(), id, role, 1)
}
