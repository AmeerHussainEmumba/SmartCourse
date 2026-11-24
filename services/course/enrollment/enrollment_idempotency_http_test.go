//go:build integration

// What this file checks, over real HTTP and real Postgres (RFC §8.1 step 2,
// §15.1; acceptance row P1-3): that POST /enrollments honours Idempotency-Key.
// A retried request with the same key gets the first response back instead of
// the 409 a plain duplicate gets, and ten concurrent requests with one key
// leave exactly one enrollment row.
package enrollment_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func newIdempotentEngine(env *testenv.Env) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	idem := idempotency.Middleware(env.DB, idempotency.Options{
		UserID: func(c *gin.Context) (uuid.UUID, bool) {
			id, ok := auth.FromContext(c.Request.Context())
			return id.UserID, ok
		},
	})
	enrollment.RegisterRoutes(r.Group("/api/v1"), enrollment.NewService(enrollment.NewRepository(env.DB)), testJWTCfg(), idem)
	return r
}

func postEnroll(r *gin.Engine, token, key, courseID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/enrollments", strings.NewReader(`{"course_id":"`+courseID+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(idempotency.Header, key)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func enrollmentRows(t *testing.T, env *testenv.Env, studentID uuid.UUID) int {
	t.Helper()
	var n int
	if err := env.DB.Raw(`SELECT count(*) FROM enrollments WHERE student_id = ?`, studentID).Scan(&n).Error; err != nil {
		t.Fatalf("count enrollments: %v", err)
	}
	return n
}

func TestPostEnroll_SameKeyTwice_ReplaysFirstResponse(t *testing.T) {
	env := testenv.Setup(t)
	r := newIdempotentEngine(env)
	courseID := seedCourse(t, env, nil)
	studentID := seedStudent(t, env)
	token, _ := auth.IssueAccessToken(testJWTCfg(), studentID, "student", 1)

	first := postEnroll(r, token, "enroll-1", courseID.String())
	second := postEnroll(r, token, "enroll-1", courseID.String())
	if first.Code != http.StatusCreated {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	if second.Code != http.StatusCreated || second.Header().Get(idempotency.ReplayedHeader) != "true" {
		t.Fatalf("retry: %d replayed=%q body=%s; want the stored 201", second.Code, second.Header().Get(idempotency.ReplayedHeader), second.Body.String())
	}
	if n := enrollmentRows(t, env, studentID); n != 1 {
		t.Fatalf("%d enrollment rows, want 1", n)
	}

	// A new key for the same course is a genuine duplicate and is still refused.
	if third := postEnroll(r, token, "enroll-2", courseID.String()); third.Code != http.StatusConflict {
		t.Fatalf("duplicate under a new key: %d want 409", third.Code)
	}
}

func TestPostEnroll_TenConcurrentSameKey_OneRow(t *testing.T) {
	env := testenv.Setup(t)
	r := newIdempotentEngine(env)
	courseID := seedCourse(t, env, nil)
	studentID := seedStudent(t, env)
	token, _ := auth.IssueAccessToken(testJWTCfg(), studentID, "student", 1)

	codes := make([]int, 10)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = postEnroll(r, token, "enroll-race", courseID.String()).Code
		}()
	}
	wg.Wait()
	created := 0
	for _, c := range codes {
		switch c {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
		default:
			t.Errorf("unexpected status %d in %v", c, codes)
		}
	}
	if created == 0 {
		t.Fatalf("no request got the 201: %v", codes)
	}
	if n := enrollmentRows(t, env, studentID); n != 1 {
		t.Fatalf("%d enrollment rows after 10 concurrent requests, want 1 (%v)", n, codes)
	}
}
