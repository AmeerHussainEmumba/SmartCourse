//go:build e2e

package e2e_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks is the
// F-38 test: after a Redis restart, an instructor whose token_version is above
// 1 must not be locked out of course-api. It runs on the container harness
// (the ten binaries, each as its own process), so it exercises the path no
// single service's tests can: identity-api seeds the token version at login
// and refresh, identity-worker's re-seeder rewrites it after a flush, and
// course-api reads it. Real logins, a real admin role change and a real refresh
// over HTTP.
//
// Steps and assertions:
//  1. A user is promoted to instructor through the admin route (token version
//     2, written to Redis by identity). Before the promotion a course write is
//     403; after it the instructor creates a course (201).
//  2. Redis is flushed (a restart that lost its data). The cached version is
//     gone. The token issued before the flush is refused on a course write with
//     a clean 401 token_stale (never a 5xx, never silently accepted), unless
//     identity-worker's re-seeder (every 15 s) already put version 2 back in
//     the instant between the flush and the request, which is checked.
//  3. Logging in again re-seeds version 2, and the new token writes (201).
//  4. After another flush the refresh path restores access too: a client that
//     holds a refresh token does not have to log in.
//  5. After another flush, with no login and no refresh and no request, the
//     identity-worker re-seeder alone writes version 2 back to Redis (the only
//     writer left), and the instructor's own token works again (201).
//  6. Revocation still holds across a restart: the instructor is demoted
//     (version 3), Redis is flushed, the demoted user logs in (seeding 3); the
//     old instructor token stays token_stale, the demoted user's own token is
//     403.
func TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	ctx := context.Background()

	cachedVersion := func(userID string) (string, bool) {
		v, err := s.Env.RDB.Get(ctx, auth.TokenVersionRedisKey(userID)).Result()
		return v, err == nil
	}
	createCourse := func(token string) (int, string) { // a write course-api guards with the token-version check
		t.Helper()
		slug := "f38-" + uuid.NewString()[:12]
		code, raw, err := harness.Do("POST", s.CourseURL+"/api/v1/courses", token, map[string]any{"title": "F-38 " + slug, "slug": slug})
		if err != nil {
			t.Fatalf("create course: %v", err)
		}
		return code, string(raw)
	}
	isStale := func(code int, body string) bool {
		return code == http.StatusUnauthorized && strings.Contains(body, `"code":"token_stale"`)
	}
	setRole := func(userID, role string) {
		t.Helper()
		code, raw, err := harness.Do("PATCH", s.IdentityURL+"/api/v1/admin/users/"+userID+"/role", s.AdminToken(t), map[string]any{"role": role, "status": "active"})
		if err != nil || code != http.StatusOK {
			t.Fatalf("set role %s: HTTP %d %s (err %v)", role, code, raw, err)
		}
	}

	// A user who will be promoted to instructor through the real admin route.
	// (The admin is the one the identity-admin CLI created.)
	email := "teacher-" + uuid.NewString()[:8] + "@e2e.example.test"
	s.Register(t, email, "Teacher E2E")
	student := s.Login(t, email)
	if code, _ := createCourse(student.Token); code != http.StatusForbidden {
		t.Fatalf("a student creating a course should be 403, got %d", code)
	}

	// Promotion bumps token_version to 2 and writes it to Redis (identity-api).
	setRole(student.ID.String(), "instructor")
	if v, ok := cachedVersion(student.ID.String()); !ok || v != "2" {
		t.Fatalf("after promotion Redis should hold version 2, got %q (present=%v)", v, ok)
	}
	teacher := s.Login(t, email) // role instructor, token version 2
	if code, body := createCourse(teacher.Token); code != http.StatusCreated {
		t.Fatalf("promoted instructor should create a course: %d %s", code, body)
	}

	// Redis restarts. The key is gone, and so is every cached version.
	s.FlushRedis(t)
	if _, ok := cachedVersion(student.ID.String()); ok {
		t.Log("the identity-worker re-seeder already restored the cached version between the flush and this read (it ticks every 15 s)")
	}

	// The token issued before the restart is refused on a course write: a clean
	// token_stale (the client re-authenticates), never a 5xx and never silently
	// accepted. If the re-seeder won the race the token is accepted and the
	// cached version must be back at 2: that is the same repair, earlier.
	code, body := createCourse(teacher.Token)
	switch {
	case isStale(code, body):
	case code == http.StatusCreated:
		if v, ok := cachedVersion(student.ID.String()); !ok || v != "2" {
			t.Fatalf("the old token was accepted after the flush but Redis does not hold version 2 (%q, present=%v): accepted without a version check", v, ok)
		}
		t.Log("the old token was accepted: the re-seeder restored version 2 first")
	default:
		t.Fatalf("after a restart the existing version-2 token should be refused as token_stale, got %d %s", code, body)
	}

	// Logging in again is what restores access (before F-38 it did not: a
	// missing key reads as version 1, so the instructor stayed locked out of
	// every course write until another role change).
	teacher2 := s.Login(t, email)
	if v, ok := cachedVersion(student.ID.String()); !ok || v != "2" {
		t.Fatalf("login should have re-seeded the cached version (want 2), got %q (present=%v)", v, ok)
	}
	if code, body := createCourse(teacher2.Token); code != http.StatusCreated {
		t.Fatalf("after a re-login the instructor should be able to write again: %d %s", code, body)
	}

	// The refresh path restores access too: a client that holds a refresh token
	// but whose access token went stale after a restart does not have to log in.
	s.FlushRedis(t)
	code, raw, err := harness.Do("POST", s.IdentityURL+"/api/v1/auth/refresh", "", map[string]any{"refresh_token": teacher2.RefreshToken})
	if err != nil || code != http.StatusOK {
		t.Fatalf("refresh: HTTP %d %s (err %v)", code, raw, err)
	}
	refreshed := decodeAccessToken(t, raw)
	if code, body := createCourse(refreshed); code != http.StatusCreated {
		t.Fatalf("after a refresh the instructor should be able to write again: %d %s", code, body)
	}

	// The identity-worker alone repairs it: flush, then neither login nor
	// refresh nor any request, only a wait. The re-seeder rewrites every user
	// whose version is above 1, so the instructor's token works again.
	s.FlushRedis(t)
	harness.Eventually(t, 90*time.Second, "identity-worker's re-seeder to write version 2 back to Redis", func() error {
		if v, ok := cachedVersion(student.ID.String()); !ok || v != "2" {
			return harness.Errf("cached version %q (present=%v), want 2", v, ok)
		}
		return nil
	})
	if code, body := createCourse(refreshed); code != http.StatusCreated {
		t.Fatalf("after the re-seeder restored the version the instructor's token should work without a re-login: %d %s", code, body)
	}

	// Revocation still holds across a restart. Demote the instructor (version
	// 3), restart Redis, and have the demoted user log in so the key is
	// re-seeded from the database. The old instructor token must stay refused.
	setRole(student.ID.String(), "student")
	s.FlushRedis(t)
	demoted := s.Login(t, email) // seeds version 3
	if v, ok := cachedVersion(student.ID.String()); !ok || v != "3" {
		t.Fatalf("login after demotion should seed version 3, got %q (present=%v)", v, ok)
	}
	if code, body := createCourse(refreshed); !isStale(code, body) {
		t.Fatalf("the old instructor token must stay refused after a demotion and a restart: %d %s", code, body)
	}
	if code, _ := createCourse(demoted.Token); code != http.StatusForbidden {
		t.Fatalf("the demoted user's own token should be refused as not an instructor (403), got %d", code)
	}
}
