//go:build integration

// Most Popular Courses (FR-5, RFC §9.1): the ranking moves when source data
// changes through the real handlers, is served from the Redis sorted set the
// Reconciler maintains, and falls back to Postgres without ever failing the
// request when Redis is empty or unreachable. Plus the HTTP access matrix.
package analytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

type popularItem struct {
	CourseID    uuid.UUID `json:"course_id"`
	Enrollments int64     `json:"enrollments"`
}

// popularEnv wires a Service the way analytics-api does (Redis attached) to
// a test engine.
type popularEnv struct {
	*secEnv
	nextOutbox int64
}

func newPopularEnv(t *testing.T, withRedis bool) *popularEnv {
	env := setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	if withRedis {
		svc = svc.WithPopularCache(env.RDB)
	}
	return &popularEnv{secEnv: &secEnv{t: t, env: env, r: newTestEngine(t, svc)}, nextOutbox: 100}
}

func (p *popularEnv) publish(c uuid.UUID) {
	p.nextOutbox++
	feed(p.t, p.env, ev(p.nextOutbox, "course.course_published", map[string]any{"course_id": c}))
}

func (p *popularEnv) enroll(c uuid.UUID) uuid.UUID {
	p.nextOutbox++
	id := uuid.New()
	feed(p.t, p.env, ev(p.nextOutbox, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": id, "student_id": uuid.New(), "course_id": c, "enrolled_at": time.Now().UTC(),
	}))
	return id
}

func (p *popularEnv) withdraw(enrollmentID, c uuid.UUID) {
	p.nextOutbox++
	var student uuid.UUID
	if err := p.env.DB.Raw(`SELECT student_id FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&student); err != nil {
		p.t.Fatal(err)
	}
	feed(p.t, p.env, ev(p.nextOutbox, "enrollment.student_withdrawn", map[string]any{
		"enrollment_id": enrollmentID, "student_id": student, "course_id": c,
	}))
}

func (p *popularEnv) popular(role, query string) []popularItem {
	p.t.Helper()
	rec := p.get("/api/v1/analytics/popular-courses"+query, p.token(role))
	if rec.Code != http.StatusOK {
		p.t.Fatalf("GET popular-courses%s as %s: want 200, got %d: %s", query, role, rec.Code, rec.Body.String())
	}
	var body struct {
		Items []popularItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		p.t.Fatal(err)
	}
	return body.Items
}

func (p *popularEnv) reconcile() {
	p.t.Helper()
	if err := analytics.NewReconciler(p.env.DB, p.env.RDB).Run(context.Background()); err != nil {
		p.t.Fatalf("reconcile: %v", err)
	}
}

func TestPopularCourses_RankingMovesWithSourceData(t *testing.T) {
	p := newPopularEnv(t, true)

	a, b := uuid.New(), uuid.New()
	p.publish(a)
	p.publish(b)
	ea := p.enroll(a)
	p.enroll(a)
	p.enroll(b)
	p.reconcile()

	got := p.popular("student", "")
	if len(got) != 2 || got[0].CourseID != a || got[0].Enrollments != 2 || got[1].CourseID != b || got[1].Enrollments != 1 {
		t.Fatalf("want [a:2 b:1], got %+v", got)
	}

	// b overtakes a: two more enrollments in b, and one in a withdraws.
	p.enroll(b)
	p.enroll(b)
	p.withdraw(ea, a)
	p.reconcile()

	got = p.popular("student", "")
	if len(got) != 2 || got[0].CourseID != b || got[0].Enrollments != 3 || got[1].CourseID != a || got[1].Enrollments != 1 {
		t.Fatalf("after b overtakes a want [b:3 a:1], got %+v", got)
	}
	if got := p.popular("student", "?limit=1"); len(got) != 1 || got[0].CourseID != b {
		t.Fatalf("limit=1 want [b], got %+v", got)
	}
}

func TestPopularCourses_ServedFromRedisWhenPresent(t *testing.T) {
	p := newPopularEnv(t, true)
	a := uuid.New()
	p.publish(a)
	p.enroll(a)

	// The sorted set is the source of truth for the read path: a ranking
	// planted there is what comes back, Postgres is not consulted.
	planted := uuid.New()
	if err := p.env.RDB.ZAdd(context.Background(), analytics.PopularCoursesKey, redis.Z{Score: 9, Member: planted.String()}, redis.Z{Score: 1, Member: "not-a-uuid"}).Err(); err != nil {
		t.Fatal(err)
	}
	got := p.popular("admin", "")
	if len(got) != 1 || got[0].CourseID != planted || got[0].Enrollments != 9 {
		t.Fatalf("want the planted Redis entry only (stray member skipped), got %+v", got)
	}
}

func TestPopularCourses_RedisEmpty_FallsBackToPostgres(t *testing.T) {
	p := newPopularEnv(t, true)
	a, b := uuid.New(), uuid.New()
	p.publish(a)
	p.publish(b)
	p.enroll(a)
	p.enroll(a)
	p.enroll(b)
	// No reconcile has run, the sorted set does not exist.
	got := p.popular("instructor", "")
	if len(got) != 2 || got[0].CourseID != a || got[0].Enrollments != 2 || got[1].CourseID != b {
		t.Fatalf("want Postgres ranking [a:2 b:1], got %+v", got)
	}
}

func TestPopularCourses_RedisDown_FallsBackToPostgres_Never5xx(t *testing.T) {
	env := setup(t)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer func() { _ = dead.Close() }()
	svc := analytics.NewService(analytics.NewRepository(env.DB)).WithPopularCache(dead)
	p := &popularEnv{secEnv: &secEnv{t: t, env: env, r: newTestEngine(t, svc)}, nextOutbox: 100}

	a := uuid.New()
	p.publish(a)
	p.enroll(a)
	got := p.popular("student", "")
	if len(got) != 1 || got[0].CourseID != a || got[0].Enrollments != 1 {
		t.Fatalf("want Postgres ranking [a:1] with Redis down, got %+v", got)
	}
}

func TestPopularCourses_NoRedisClient_UsesPostgres(t *testing.T) {
	p := newPopularEnv(t, false)
	a := uuid.New()
	p.publish(a)
	p.enroll(a)
	if got := p.popular("student", ""); len(got) != 1 || got[0].CourseID != a {
		t.Fatalf("want [a], got %+v", got)
	}
}

func TestPopularCourses_UnpublishedAndEmptyCoursesExcluded(t *testing.T) {
	p := newPopularEnv(t, false)
	published, empty, unpublished := uuid.New(), uuid.New(), uuid.New()
	p.publish(published)
	p.publish(empty)
	p.enroll(published)
	p.enroll(unpublished) // enrollment event for a course analytics never saw published
	got := p.popular("student", "")
	if len(got) != 1 || got[0].CourseID != published {
		t.Fatalf("want only the published course that has enrollments, got %+v", got)
	}
	if got := p.popular("student", ""); got == nil {
		t.Fatal("items must serialise as an array, not null")
	}
}

func TestPopularCourses_HTTPMatrix(t *testing.T) {
	p := newPopularEnv(t, false)
	path := "/api/v1/analytics/popular-courses"
	if rec := p.get(path, ""); rec.Code != 401 {
		t.Errorf("anonymous: want 401, got %d", rec.Code)
	}
	if rec := p.get(path, p.token("admin")+"x"); rec.Code != 401 {
		t.Errorf("forged token: want 401, got %d", rec.Code)
	}
	for _, role := range []string{"student", "instructor", "admin"} {
		if rec := p.get(path, p.token(role)); rec.Code != 200 {
			t.Errorf("%s: want 200, got %d", role, rec.Code)
		}
	}
	for _, q := range []string{"?limit=0", "?limit=-1", "?limit=abc", "?limit=51", "?limit=99999999999999999999"} {
		if rec := p.get(path+q, p.token("student")); rec.Code != 400 {
			t.Errorf("%s: want 400, got %d: %s", q, rec.Code, rec.Body.String())
		}
	}
	if rec := p.get(path+"?limit=50", p.token("student")); rec.Code != 200 {
		t.Errorf("limit=50: want 200, got %d", rec.Code)
	}
}
