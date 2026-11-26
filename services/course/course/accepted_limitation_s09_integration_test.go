//go:build integration

package course_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// ACCEPTED LIMITATION (security finding S-09, owner decision 2026-11-26,
// docs/plans/week-3-findings-security.md): this test asserts documented
// behaviour, it is not a bug reproducer. Do not "fix" it without reopening
// the decision.
//
// Course-api's instructor writes use auth.RequireFreshPrivilege, which reads
// a missing Redis token-version key as version 1 (so a Redis flush does not
// lock every instructor out of authoring; libs/auth README, "What a
// token-version cache miss means"). The consequence, accepted by the owner:
//
//   - a suspended instructor's already-issued access token cannot write: the
//     bumped version in Redis refuses it (401);
//   - after a Redis flush, until identity-worker's token-version re-seeder
//     rewrites the versions (one tick, at most about 15 s), the same stale
//     token CAN write again (201);
//   - once the re-seeder has run, the write is refused again.
//
// The re-seeder itself is proven by identity's
// TestReseed_AfterRedisFlush_CourseAPIStopsAnsweringTokenStale; this test
// stands in for it by writing the version key back.
func TestAcceptedLimitation_S09_SuspendedInstructorWriteWindowAfterRedisFlush(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	gin.SetMode(gin.TestMode)
	r := gin.New()
	course.RegisterRoutes(r.Group("/api/v1"), svc, testJWTCfg())
	instr := seedInstructor(t, env)
	tok, _ := auth.IssueAccessToken(testJWTCfg(), instr, "instructor", 1)
	post := func(slug string) int {
		body := `{"title":"T","slug":"` + slug + `"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/courses", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec.Code
	}
	ctx := context.Background()
	suspended := func() {
		if err := env.RDB.Set(ctx, auth.TokenVersionRedisKey(instr.String()), 2, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}

	suspended()
	if got := post("s09-before"); got != http.StatusUnauthorized {
		t.Fatalf("a suspended instructor's stale token must not write: got %d, want 401", got)
	}

	// Redis flush: the version key is gone, a miss reads as version 1.
	if err := env.RDB.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if got := post("s09-window"); got != http.StatusCreated {
		t.Fatalf("documented window after a flush (before the re-seeder runs): got %d, want 201", got)
	}

	// The re-seeder writes the version back.
	suspended()
	if got := post("s09-after"); got != http.StatusUnauthorized {
		t.Fatalf("after the re-seeder the stale token must be refused again: got %d, want 401", got)
	}
}
