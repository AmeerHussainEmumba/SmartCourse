//go:build integration

// Package e2e_test holds tests that run more than one service together.
//
// This one covers F-38: after a Redis restart, an instructor whose
// token_version is above 1 must not be locked out of course-api. It runs the
// real identity routes and the real course routes, each on the production
// router stack and each with its own database connection restricted to its
// own schemas (identity,platform,public and course,platform,public, exactly
// as the compose services run), against one shared Postgres and one shared
// Redis. Nothing is faked except the Temporal client, which these routes
// never call. It uses real logins, a real admin role change and a real
// refresh, so it exercises the path no single service's tests can: identity
// seeds the token version at login, and course-api reads it.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

const password = "correct-horse-battery"

type authResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	User         struct {
		ID string `json:"id"`
	} `json:"user"`
}

// stack is identity-api and course-api in one process, as separate engines.
type stack struct {
	t        *testing.T
	env      *testenv.Env
	identity *gin.Engine
	course   *gin.Engine
	ipSeq    atomic.Int32
}

func newStack(t *testing.T) *stack {
	t.Helper()
	env := testenv.Setup(t)
	jwt := config.JWTConfig{SigningKey: "e2e-test-signing-key-at-least-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 7 * 24 * time.Hour, Issuer: "smartcourse-e2e"}
	argon := config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	gin.SetMode(gin.TestMode)

	identityDB := env.OpenWithSearchPath(t, "identity,platform,public")
	courseDB := env.OpenWithSearchPath(t, "course,platform,public")

	isvc := identity.NewService(identity.NewRepository(identityDB), env.RDB, jwt, argon, identity.LoggingEmailSender{})
	ie := router.New("test", nil)
	identity.RegisterRoutes(ie.Group("/api/v1"), isvc, jwt, env.RDB)

	csvc := course.NewService(course.NewRepository(courseDB), env.RDB, nil, "q") // Publish is never called here
	ce := router.New("test", nil)
	course.RegisterRoutes(ce.Group("/api/v1"), csvc, jwt)

	return &stack{t: t, env: env, identity: ie, course: ce}
}

func (s *stack) do(e *gin.Engine, method, path, token string, body any) *httptest.ResponseRecorder {
	s.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			s.t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "198.51.100." + string(rune('0'+s.ipSeq.Add(1)%10)) + ":4000" // spread across IPs so per-IP limits are not the subject
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func (s *stack) register(email string) {
	s.t.Helper()
	rec := s.do(s.identity, "POST", "/api/v1/auth/register", "", map[string]any{"email": email, "password": password, "full_name": "E2E"})
	if rec.Code != http.StatusAccepted {
		s.t.Fatalf("register %s: %d %s", email, rec.Code, rec.Body.String())
	}
}

func (s *stack) login(email string) authResp {
	s.t.Helper()
	rec := s.do(s.identity, "POST", "/api/v1/auth/login", "", map[string]any{"email": email, "password": password})
	if rec.Code != 200 {
		s.t.Fatalf("login %s: %d %s", email, rec.Code, rec.Body.String())
	}
	var a authResp
	if err := json.Unmarshal(rec.Body.Bytes(), &a); err != nil {
		s.t.Fatal(err)
	}
	return a
}

func (s *stack) setRole(adminToken, userID, role string) {
	s.t.Helper()
	rec := s.do(s.identity, "PATCH", "/api/v1/admin/users/"+userID+"/role", adminToken, map[string]any{"role": role, "status": "active"})
	if rec.Code != 200 {
		s.t.Fatalf("set role %s: %d %s", role, rec.Code, rec.Body.String())
	}
}

var slugSeq atomic.Int32

// createCourse is a write that course-api guards with the token-version check.
func (s *stack) createCourse(token string) (int, string) {
	s.t.Helper()
	n := slugSeq.Add(1)
	rec := s.do(s.course, "POST", "/api/v1/courses", token, map[string]any{
		"title": "E2E", "slug": "e2e-" + strings.Repeat("x", 1) + string(rune('a'+n%26)) + string(rune('a'+(n/26)%26)) + string(rune('a'+(n/676)%26)),
	})
	return rec.Code, rec.Body.String()
}

func (s *stack) cachedVersion(userID string) (string, bool) {
	v, err := s.env.RDB.Get(context.Background(), auth.TokenVersionRedisKey(userID)).Result()
	return v, err == nil
}

func isStale(code int, body string) bool {
	return code == 401 && strings.Contains(body, `"code":"token_stale"`)
}

func TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	// An admin (made admin directly, there is no public way to become one) and
	// a user who will be promoted to instructor through the real admin route.
	s.register("admin@e2e.test")
	if err := s.env.DB.Exec(`UPDATE identity.users SET role = 'admin' WHERE email = 'admin@e2e.test'`).Error; err != nil {
		t.Fatal(err)
	}
	admin := s.login("admin@e2e.test")

	s.register("teacher@e2e.test")
	student := s.login("teacher@e2e.test")
	if code, _ := s.createCourse(student.AccessToken); code != 403 {
		t.Fatalf("a student creating a course should be 403, got %d", code)
	}

	// Promotion bumps token_version to 2 and writes it to Redis (identity).
	s.setRole(admin.AccessToken, student.User.ID, "instructor")
	if v, ok := s.cachedVersion(student.User.ID); !ok || v != "2" {
		t.Fatalf("after promotion Redis should hold version 2, got %q (present=%v)", v, ok)
	}
	teacher := s.login("teacher@e2e.test") // role instructor, token version 2
	if code, body := s.createCourse(teacher.AccessToken); code != 201 {
		t.Fatalf("promoted instructor should create a course: %d %s", code, body)
	}

	// Redis restarts. The key is gone, and so is every cached version.
	if err := s.env.RDB.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.cachedVersion(student.User.ID); ok {
		t.Fatal("precondition: the cached version should be gone after the flush")
	}

	// The token issued before the restart is refused on a course write: a clean
	// token_stale (the client re-authenticates), never a 5xx and never silently
	// accepted.
	if code, body := s.createCourse(teacher.AccessToken); !isStale(code, body) {
		t.Fatalf("after a restart the existing version-2 token should be refused as token_stale, got %d %s", code, body)
	}

	// Logging in again is what restores access. Before F-38 it did not: the
	// freshly issued token was refused too, because nothing rewrote the key and
	// a missing key reads as version 1, so the instructor stayed locked out of
	// every course write until another role change.
	teacher2 := s.login("teacher@e2e.test")
	if v, ok := s.cachedVersion(student.User.ID); !ok || v != "2" {
		t.Fatalf("login should have re-seeded the cached version (want 2), got %q (present=%v)", v, ok)
	}
	if code, body := s.createCourse(teacher2.AccessToken); code != 201 {
		t.Fatalf("after a re-login the instructor should be able to write again: %d %s", code, body)
	}

	// The refresh path restores access too: a client that holds a refresh token
	// but whose access token went stale after a restart does not have to log in.
	if err := s.env.RDB.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	rec := s.do(s.identity, "POST", "/api/v1/auth/refresh", "", map[string]any{"refresh_token": teacher2.RefreshToken})
	if rec.Code != 200 {
		t.Fatalf("refresh: %d %s", rec.Code, rec.Body.String())
	}
	var refreshed authResp
	_ = json.Unmarshal(rec.Body.Bytes(), &refreshed)
	if code, body := s.createCourse(refreshed.AccessToken); code != 201 {
		t.Fatalf("after a refresh the instructor should be able to write again: %d %s", code, body)
	}

	// Revocation still holds across a restart. Demote the instructor (version 3),
	// restart Redis, and have the demoted user log in so the key is re-seeded
	// from the database. The old instructor token must stay refused.
	s.setRole(admin.AccessToken, student.User.ID, "student")
	if err := s.env.RDB.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	demoted := s.login("teacher@e2e.test") // seeds version 3
	if v, ok := s.cachedVersion(student.User.ID); !ok || v != "3" {
		t.Fatalf("login after demotion should seed version 3, got %q (present=%v)", v, ok)
	}
	if code, body := s.createCourse(refreshed.AccessToken); !isStale(code, body) {
		t.Fatalf("the old instructor token must stay refused after a demotion and a restart: %d %s", code, body)
	}
	if code, _ := s.createCourse(demoted.AccessToken); code != 403 {
		t.Fatalf("the demoted user's own token should be refused as not an instructor (403), got %d", code)
	}
}
