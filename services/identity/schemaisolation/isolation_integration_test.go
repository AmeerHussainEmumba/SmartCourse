//go:build integration

package schemaisolation_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

type tokenSender struct {
	mu                  sync.Mutex
	resetTok, verifyTok string
}

func (s *tokenSender) SendPasswordReset(_ context.Context, _, tok string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetTok = tok
	return nil
}
func (s *tokenSender) SendEmailVerification(_ context.Context, _, tok string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verifyTok = tok
	return nil
}

// identity-api connects as svc_identity (search_path identity,public). Every
// flow of the service must work with nothing else resolvable.
func TestSchemaIsolation_IdentityServiceMainFlows(t *testing.T) {
	env := testenv.Setup(t)
	conn := env.ServiceDB(t, "identity")
	ctx := context.Background()

	for _, q := range []string{`SELECT 1 FROM courses`, `SELECT 1 FROM enrollments`, `SELECT 1 FROM course_stats`} {
		if err := conn.Exec(q).Error; err == nil {
			t.Fatalf("%q succeeded as svc_identity; isolation is broken", q)
		}
	}

	sender := &tokenSender{}
	jwtCfg := config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 24 * time.Hour, Issuer: "iso"}
	argon := config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	svc := identity.NewService(identity.NewRepository(conn), env.RDB, jwtCfg, argon, sender)

	reg, err := svc.Register(ctx, identity.RegisterRequest{Email: "iso@example.com", Password: "correct-horse-battery", FullName: "Iso User"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := svc.Login(ctx, identity.LoginRequest{Email: "ISO@example.com", Password: "correct-horse-battery"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	ref, err := svc.Refresh(ctx, identity.RefreshRequest{RefreshToken: reg.RefreshToken})
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	uid := uuid.MustParse(reg.User.ID)
	if _, err := svc.GetMe(ctx, uid); err != nil {
		t.Fatalf("GetMe: %v", err)
	}
	if _, err := svc.UpdateMe(ctx, uid, identity.UpdateProfileRequest{FullName: "Renamed"}); err != nil {
		t.Fatalf("UpdateMe: %v", err)
	}
	if _, err := svc.UpdateUserRole(ctx, uid, identity.RoleInstructor, identity.UserStatus("active")); err != nil {
		t.Fatalf("UpdateUserRole: %v", err)
	}
	if err := svc.ResendEmailVerification(ctx, identity.ResendVerificationRequest{Email: "iso@example.com"}); err != nil {
		t.Fatalf("ResendEmailVerification: %v", err)
	}
	sender.mu.Lock()
	vt := sender.verifyTok
	sender.mu.Unlock()
	if vt != "" {
		if err := svc.ConfirmEmailVerification(ctx, vt); err != nil {
			t.Fatalf("ConfirmEmailVerification: %v", err)
		}
	}
	if err := svc.RequestPasswordReset(ctx, identity.PasswordResetRequestDTO{Email: "iso@example.com"}); err != nil {
		t.Fatalf("RequestPasswordReset: %v", err)
	}
	sender.mu.Lock()
	rt := sender.resetTok
	sender.mu.Unlock()
	if rt == "" {
		t.Fatal("no reset token captured")
	}
	if err := svc.ConfirmPasswordReset(ctx, identity.PasswordResetConfirmRequest{Token: rt, NewPassword: "another-long-password"}); err != nil {
		t.Fatalf("ConfirmPasswordReset: %v", err)
	}
	if err := svc.Logout(ctx, identity.LogoutRequest{RefreshToken: ref.RefreshToken}); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	// Events were written through identity.outbox via the service role.
	var n int64
	if err := conn.Raw(`SELECT count(*) FROM outbox WHERE event_type IN ('identity.user_registered','identity.user_profile_updated','identity.user_role_changed')`).Row().Scan(&n); err != nil || n != 3 {
		t.Fatalf("expected 3 identity events in the outbox, got %d (%v)", n, err)
	}
}
