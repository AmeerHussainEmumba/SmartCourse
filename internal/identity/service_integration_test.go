//go:build integration

// What this file checks, against real Postgres and Redis (RFC §18.2 — no
// mocks for the database): the full register → login → refresh-rotation →
// reuse-detection lifecycle, duplicate-email rejection via the CITEXT
// unique constraint, and the privilege-revocation flow (ADR-0015 addendum)
// end to end. Run with `make test-integration`; see docs/guides/testing.md.
package identity_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/identity"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func newTestService(t *testing.T) (*identity.Service, *testenv.Env) {
	t.Helper()
	return newTestServiceWithEmail(t, identity.LoggingEmailSender{})
}

func newTestServiceWithEmail(t *testing.T, sender identity.EmailSender) (*identity.Service, *testenv.Env) {
	t.Helper()
	env := testenv.Setup(t)
	repo := identity.NewRepository(env.DB)
	jwtCfg := config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 7 * 24 * time.Hour, Issuer: "smartcourse-test"}
	argonCfg := config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	svc := identity.NewService(repo, env.RDB, jwtCfg, argonCfg, sender)
	return svc, env
}

// capturingEmailSender records the raw tokens it's asked to send, so a
// test can redeem a password-reset/verification token without a real
// mailbox, and can optionally fail every send (failWith) to prove a
// delivery failure doesn't break the caller.
type capturingEmailSender struct {
	mu                     sync.Mutex
	lastPasswordResetToken string
	failWith               error
}

func (s *capturingEmailSender) SendPasswordReset(ctx context.Context, toEmail, rawToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPasswordResetToken = rawToken
	return s.failWith
}

func (s *capturingEmailSender) SendEmailVerification(ctx context.Context, toEmail, rawToken string) error {
	return s.failWith
}

func (s *capturingEmailSender) PasswordResetToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPasswordResetToken
}

func TestRegister_DuplicateEmail_Rejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	req := identity.RegisterRequest{Email: "dup@example.com", Password: "correct-horse-battery", FullName: "First User"}
	if _, err := svc.Register(ctx, req); err != nil {
		t.Fatalf("first registration: %v", err)
	}

	_, err := svc.Register(ctx, req)
	if err == nil {
		t.Fatal("expected second registration with the same email to fail")
	}
	appErr, ok := apperrors.As(err)
	if !ok || appErr.Code != "email_taken" {
		t.Fatalf("expected email_taken error, got: %v", err)
	}
}

func TestRegister_CaseInsensitiveEmail_Rejected(t *testing.T) {
	// Verifies the CITEXT column, not application code — an application-level
	// lower() check would race under concurrent registration (RFC §5.3.1).
	svc, _ := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Register(ctx, identity.RegisterRequest{Email: "Alice@Example.com", Password: "correct-horse-battery", FullName: "Alice"}); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	_, err := svc.Register(ctx, identity.RegisterRequest{Email: "alice@example.com", Password: "correct-horse-battery", FullName: "Alice Again"})
	if err == nil {
		t.Fatal("expected differently-cased duplicate email to be rejected by the CITEXT constraint")
	}
}

func TestLogin_WrongPassword_Rejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	_, err := svc.Register(ctx, identity.RegisterRequest{Email: "login@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err = svc.Login(ctx, identity.LoginRequest{Email: "login@example.com", Password: "wrong-password"})
	if err == nil {
		t.Fatal("expected wrong password to be rejected")
	}
}

// TestLogin_PerAccountRateLimit_BlocksAfterThreshold proves the ADR-0020
// GCRA wiring actually blocks, not just that it compiles and is called.
// Failed attempts against one account, from the service layer directly —
// the per-account limit is enforced in service.go regardless of source
// IP, specifically so spreading guesses across many IPs doesn't evade it.
func TestLogin_PerAccountRateLimit_BlocksAfterThreshold(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Register(ctx, identity.RegisterRequest{Email: "ratelimited@example.com", Password: "correct-horse-battery", FullName: "User"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// loginPerAccountLimit is 5/minute (service.go). Burn the budget with
	// wrong-password attempts.
	var lastErr error
	for i := 0; i < 5; i++ {
		_, lastErr = svc.Login(ctx, identity.LoginRequest{Email: "ratelimited@example.com", Password: "wrong-password"})
		if appErr, ok := apperrors.As(lastErr); !ok || appErr.Code != "invalid_credentials" {
			t.Fatalf("attempt %d: expected invalid_credentials while under the limit, got: %v", i+1, lastErr)
		}
	}

	// The next attempt — even with the CORRECT password — must be
	// rejected as rate-limited, not allowed through. This is the specific
	// behavior that distinguishes "the limiter is wired in" from "the
	// limiter type exists somewhere."
	_, err := svc.Login(ctx, identity.LoginRequest{Email: "ratelimited@example.com", Password: "correct-horse-battery"})
	appErr, ok := apperrors.As(err)
	if !ok || appErr.Code != "rate_limited" {
		t.Fatalf("expected rate_limited after exhausting the per-account budget, got: %v", err)
	}
}

func TestLogin_UnknownEmail_SameErrorAsWrongPassword(t *testing.T) {
	// RFC §11.3: identical failure for unknown email and wrong password, so
	// the response alone can't be used to enumerate accounts.
	svc, _ := newTestService(t)
	ctx := context.Background()

	_, err1 := svc.Login(ctx, identity.LoginRequest{Email: "nobody@example.com", Password: "anything"})
	appErr1, ok1 := apperrors.As(err1)

	if _, err := svc.Register(ctx, identity.RegisterRequest{Email: "known@example.com", Password: "correct-horse-battery", FullName: "User"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err2 := svc.Login(ctx, identity.LoginRequest{Email: "known@example.com", Password: "wrong-password"})
	appErr2, ok2 := apperrors.As(err2)

	if !ok1 || !ok2 || appErr1.Code != appErr2.Code {
		t.Fatalf("expected identical error codes for unknown email and wrong password, got %v vs %v", err1, err2)
	}
}

func TestRefresh_RotatesToken(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	auth1, err := svc.Register(ctx, identity.RegisterRequest{Email: "refresh@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	auth2, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: auth1.RefreshToken})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if auth2.RefreshToken == auth1.RefreshToken {
		t.Fatal("expected refresh to issue a new refresh token, not reuse the old one")
	}

	// The old (now-replaced) token must no longer work on its own.
	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: auth1.RefreshToken}); err == nil {
		t.Fatal("expected the original (replaced) refresh token to be rejected")
	}
}

// TestRefresh_ReuseDetection_RevokesWholeFamily is the concurrency-adjacent
// correctness test for ADR-0015's reuse-detection design: presenting a
// refresh token that's already been rotated away must burn the *entire*
// token family, not just the presented token — otherwise a stolen-but-not-
// yet-used old token and the legitimate rotated chain could coexist.
func TestRefresh_ReuseDetection_RevokesWholeFamily(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "reuse@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	originalRefresh := authResp.RefreshToken

	rotated, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: originalRefresh})
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	// Reuse: present the already-replaced original token again.
	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: originalRefresh}); err == nil {
		t.Fatal("expected reuse of a replaced token to be rejected")
	}

	// The legitimately-rotated token must now ALSO be rejected — reuse
	// detection burns the whole family, per ADR-0015.
	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: rotated.RefreshToken}); err == nil {
		t.Fatal("expected the entire token family to be revoked after reuse was detected")
	}
}

func TestUpdateUserRole_WritesTokenVersionToRedisCache(t *testing.T) {
	// Verifies the ADR-0015 addendum end to end: role change bumps
	// token_version in Postgres AND updates the Redis-cached value that
	// RequireFreshPrivilege reads — both stores, not just one.
	svc, env := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "target@example.com", Password: "correct-horse-battery", FullName: "Target User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	repo := identity.NewRepository(env.DB)
	user, err := repo.GetUserByEmail(ctx, "target@example.com")
	if err != nil {
		t.Fatalf("lookup user: %v", err)
	}
	if user.TokenVersion != 1 {
		t.Fatalf("expected initial token_version 1, got %d", user.TokenVersion)
	}

	if _, err := svc.UpdateUserRole(ctx, user.ID, identity.RoleInstructor, identity.StatusActive); err != nil {
		t.Fatalf("UpdateUserRole: %v", err)
	}

	updated, err := repo.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("re-lookup user: %v", err)
	}
	if updated.TokenVersion != 2 {
		t.Fatalf("expected token_version bumped to 2, got %d", updated.TokenVersion)
	}
	if updated.Role != identity.RoleInstructor {
		t.Fatalf("expected role updated to instructor, got %s", updated.Role)
	}

	cached, err := env.RDB.Get(ctx, auth.TokenVersionRedisKey(user.ID.String())).Int()
	if err != nil {
		t.Fatalf("read cached token_version from redis: %v", err)
	}
	if cached != 2 {
		t.Fatalf("expected Redis-cached token_version 2, got %d", cached)
	}

	_ = authResp // original tokens are intentionally not exercised further here
}

// TestConfirmPasswordReset_RevokesAllRefreshTokens is the regression test
// for a real bug: ConfirmPasswordReset bumped token_version (closing new
// access tokens) but never touched refresh_tokens at all — a stolen
// refresh token kept working through /auth/refresh after a "successful"
// password reset, because RequireFreshPrivilege only guards a small set
// of admin/privilege-sensitive routes, not /auth/refresh itself.
func TestConfirmPasswordReset_RevokesAllRefreshTokens(t *testing.T) {
	sender := &capturingEmailSender{}
	svc, _ := newTestServiceWithEmail(t, sender)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "resetme@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	preResetRefresh := authResp.RefreshToken

	if err := svc.RequestPasswordReset(ctx, identity.PasswordResetRequestDTO{Email: "resetme@example.com"}); err != nil {
		t.Fatalf("request password reset: %v", err)
	}
	resetToken := sender.PasswordResetToken()
	if resetToken == "" {
		t.Fatal("expected a password reset token to have been generated and sent")
	}

	if err := svc.ConfirmPasswordReset(ctx, identity.PasswordResetConfirmRequest{Token: resetToken, NewPassword: "a-brand-new-password"}); err != nil {
		t.Fatalf("confirm password reset: %v", err)
	}

	// The bug, made concrete: the refresh token issued BEFORE the reset
	// must stop working after it.
	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: preResetRefresh}); err == nil {
		t.Fatal("expected the pre-reset refresh token to be rejected after a password reset")
	}

	// And the new password must actually be the one that works now.
	if _, err := svc.Login(ctx, identity.LoginRequest{Email: "resetme@example.com", Password: "a-brand-new-password"}); err != nil {
		t.Fatalf("expected login with the new password to succeed: %v", err)
	}
}

// TestRefresh_ConcurrentCalls_ExactlyOneSucceeds is the regression test for
// a real concurrency bug: two simultaneous /auth/refresh calls presenting
// the same token both read "not yet rotated" before either had committed,
// so both passed the reuse check and both successfully minted a new token
// — the exact "two refreshes at once both succeed" bug flagged in review.
// Fixed with the same row-lock pattern ADR-0012 uses for enrollment
// (GetRefreshTokenByHashForUpdate), serialising concurrent callers instead
// of letting them race on a stale read.
func TestRefresh_ConcurrentCalls_ExactlyOneSucceeds(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "racer@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	const attempts = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	var winningToken string

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: authResp.RefreshToken})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
				winningToken = resp.RefreshToken
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("expected exactly 1 successful refresh out of %d concurrent attempts with the same token, got %d", attempts, successes)
	}

	// Every losing attempt saw what looks exactly like reuse (the token
	// had already been rotated by the winner) and revoked the whole family
	// in response (ADR-0015) — including the winner's own brand-new token.
	// That's intentional, not a bug: once any reuse of a family is
	// observed, the whole family is treated as potentially compromised
	// rather than the system guessing which concurrent caller was
	// legitimate.
	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: winningToken}); err == nil {
		t.Fatal("expected even the winning rotation's token to be rejected, since the race triggered family-wide revocation")
	}
}

// TestRegister_DoesNotFailWhenEmailDeliveryFails is the regression test for
// a real bug: Register returned an error (surfaced to the client as a 500)
// whenever the verification email failed to send — even though the user
// row had already durably committed. A client seeing "500" after a
// successful registration is exactly the kind of inconsistency this
// project's architecture otherwise goes out of its way to prevent.
func TestRegister_DoesNotFailWhenEmailDeliveryFails(t *testing.T) {
	sender := &capturingEmailSender{failWith: errors.New("simulated email provider outage")}
	svc, env := newTestServiceWithEmail(t, sender)
	ctx := context.Background()

	resp, err := svc.Register(ctx, identity.RegisterRequest{Email: "emailfails@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("expected registration to succeed even though email delivery fails, got: %v", err)
	}
	if resp.AccessToken == "" {
		t.Fatal("expected a real access token even though the verification email failed to send")
	}

	// The user must actually exist — registration genuinely succeeded, not
	// just "the function happened not to return an error."
	repo := identity.NewRepository(env.DB)
	user, err := repo.GetUserByEmail(ctx, "emailfails@example.com")
	if err != nil {
		t.Fatalf("expected the user to exist in the database: %v", err)
	}
	if user.Email != "emailfails@example.com" {
		t.Fatalf("unexpected user record: %+v", user)
	}
}
