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

	"github.com/google/uuid"

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
	lastEmailVerifyToken   string
	emailVerifySendCount   int
	failWith               error
}

func (s *capturingEmailSender) SendPasswordReset(ctx context.Context, toEmail, rawToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPasswordResetToken = rawToken
	return s.failWith
}

func (s *capturingEmailSender) SendEmailVerification(ctx context.Context, toEmail, rawToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastEmailVerifyToken = rawToken
	s.emailVerifySendCount++
	return s.failWith
}

func (s *capturingEmailSender) PasswordResetToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPasswordResetToken
}

func (s *capturingEmailSender) EmailVerifyToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEmailVerifyToken
}

func (s *capturingEmailSender) EmailVerifySendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emailVerifySendCount
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

// --- previously-untested Service methods: Logout, GetMe, UpdateMe,
// ResendEmailVerification, ConfirmEmailVerification (test-coverage audit) ---

func TestLogout_RevokesRefreshToken(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "logout@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := svc.Logout(ctx, identity.LogoutRequest{RefreshToken: authResp.RefreshToken}); err != nil {
		t.Fatalf("logout: %v", err)
	}

	if _, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: authResp.RefreshToken}); err == nil {
		t.Fatal("expected the logged-out refresh token to be rejected")
	}
}

// TestLogout_UnknownToken_IsANoOp proves Logout doesn't leak whether a
// refresh token ever existed — calling it with a token nobody issued (or
// one already revoked) must succeed silently, not error, the same no-
// enumeration posture RequestPasswordReset/ResendEmailVerification take.
func TestLogout_UnknownToken_IsANoOp(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	if err := svc.Logout(ctx, identity.LogoutRequest{RefreshToken: "never-issued-token"}); err != nil {
		t.Fatalf("expected logout with an unknown token to be a silent no-op, got: %v", err)
	}
}

func TestGetMe_ReturnsCurrentUser_RejectsUnknownID(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "getme@example.com", Password: "correct-horse-battery", FullName: "Original Name"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	userID, err := uuid.Parse(authResp.User.ID)
	if err != nil {
		t.Fatalf("parse user id: %v", err)
	}

	resp, err := svc.GetMe(ctx, userID)
	if err != nil {
		t.Fatalf("GetMe: %v", err)
	}
	if resp.Email != "getme@example.com" || resp.FullName != "Original Name" {
		t.Fatalf("unexpected GetMe response: %+v", resp)
	}

	if _, err := svc.GetMe(ctx, uuid.New()); !errors.Is(err, apperrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for an unknown user id, got: %v", err)
	}
}

// TestUpdateMe_PersistsChangeAndWritesProfileUpdatedEvent is the regression
// test for why UpdateMe exists as its own method rather than silently
// reusing some generic update path: course.instructor_mirror (ADR-0024)
// only stays current because this exact write emits
// identity.user_profile_updated in the same transaction as the column
// update — a module boundary this test can't reach across (RFC §4.4), so
// it asserts on the one thing it can see directly: the outbox row.
func TestUpdateMe_PersistsChangeAndWritesProfileUpdatedEvent(t *testing.T) {
	svc, env := newTestService(t)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "updateme@example.com", Password: "correct-horse-battery", FullName: "Old Name"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	userID, err := uuid.Parse(authResp.User.ID)
	if err != nil {
		t.Fatalf("parse user id: %v", err)
	}

	resp, err := svc.UpdateMe(ctx, userID, identity.UpdateProfileRequest{FullName: "New Name"})
	if err != nil {
		t.Fatalf("UpdateMe: %v", err)
	}
	if resp.FullName != "New Name" {
		t.Fatalf("expected UpdateMe to return the new name, got %q", resp.FullName)
	}

	again, err := svc.GetMe(ctx, userID)
	if err != nil {
		t.Fatalf("GetMe after update: %v", err)
	}
	if again.FullName != "New Name" {
		t.Fatalf("expected the persisted name to be updated, got %q", again.FullName)
	}

	var eventCount int64
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'identity.user_profile_updated' AND aggregate_id = ?`, userID).
		Row().Scan(&eventCount)
	if eventCount != 1 {
		t.Fatalf("expected exactly 1 identity.user_profile_updated outbox event, got %d", eventCount)
	}
}

// TestEmailVerification_ResendThenConfirm_MarksVerified exercises both
// ResendEmailVerification and ConfirmEmailVerification together — the
// pair only makes sense tested as a round trip, the same way
// TestConfirmPasswordReset_RevokesAllRefreshTokens tests its own request/
// confirm pair together.
func TestEmailVerification_ResendThenConfirm_MarksVerified(t *testing.T) {
	sender := &capturingEmailSender{}
	svc, env := newTestServiceWithEmail(t, sender)
	ctx := context.Background()

	authResp, err := svc.Register(ctx, identity.RegisterRequest{Email: "verifyme@example.com", Password: "correct-horse-battery", FullName: "User"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	userID, err := uuid.Parse(authResp.User.ID)
	if err != nil {
		t.Fatalf("parse user id: %v", err)
	}
	if sender.EmailVerifySendCount() != 1 {
		t.Fatalf("expected Register itself to send 1 verification email, got %d", sender.EmailVerifySendCount())
	}

	if err := svc.ResendEmailVerification(ctx, identity.ResendVerificationRequest{Email: "verifyme@example.com"}); err != nil {
		t.Fatalf("resend: %v", err)
	}
	if sender.EmailVerifySendCount() != 2 {
		t.Fatalf("expected resend to send a second verification email, got %d sends", sender.EmailVerifySendCount())
	}
	token := sender.EmailVerifyToken()
	if token == "" {
		t.Fatal("expected a verification token to have been generated and sent")
	}

	if err := svc.ConfirmEmailVerification(ctx, token); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	repo := identity.NewRepository(env.DB)
	user, err := repo.GetUserByID(ctx, userID)
	if err != nil {
		t.Fatalf("look up user: %v", err)
	}
	if user.EmailVerifiedAt == nil {
		t.Fatal("expected email_verified_at to be set after confirmation")
	}

	// A used (or garbage) token must be rejected, not silently re-accepted.
	if err := svc.ConfirmEmailVerification(ctx, token); err == nil {
		t.Fatal("expected re-using an already-confirmed verification token to fail")
	}
	if err := svc.ConfirmEmailVerification(ctx, "garbage-token-never-issued"); err == nil {
		t.Fatal("expected an unknown verification token to fail")
	}
}

// TestResendEmailVerification_NoEnumeration_AndNoOpOnceVerified proves the
// two cases ResendEmailVerification deliberately treats as silent
// successes (RFC §11.3-style no-enumeration, mirroring
// RequestPasswordReset): an unknown email, and an already-verified one.
func TestResendEmailVerification_NoEnumeration_AndNoOpOnceVerified(t *testing.T) {
	sender := &capturingEmailSender{}
	svc, _ := newTestServiceWithEmail(t, sender)
	ctx := context.Background()

	if err := svc.ResendEmailVerification(ctx, identity.ResendVerificationRequest{Email: "never-registered@example.com"}); err != nil {
		t.Fatalf("expected no error for an unknown email (no enumeration), got: %v", err)
	}
	if sender.EmailVerifySendCount() != 0 {
		t.Fatalf("expected no email sent for an unknown address, got %d sends", sender.EmailVerifySendCount())
	}

	if _, err := svc.Register(ctx, identity.RegisterRequest{Email: "alreadyverified@example.com", Password: "correct-horse-battery", FullName: "User"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	token := sender.EmailVerifyToken()
	if err := svc.ConfirmEmailVerification(ctx, token); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	sendsBeforeResend := sender.EmailVerifySendCount()
	if err := svc.ResendEmailVerification(ctx, identity.ResendVerificationRequest{Email: "alreadyverified@example.com"}); err != nil {
		t.Fatalf("expected no error resending for an already-verified account, got: %v", err)
	}
	if sender.EmailVerifySendCount() != sendsBeforeResend {
		t.Fatalf("expected no new email sent once already verified, sends went from %d to %d", sendsBeforeResend, sender.EmailVerifySendCount())
	}
}
