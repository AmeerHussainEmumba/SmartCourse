package identity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin/binding"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

// BootstrapAdminResult reports what BootstrapAdmin did. It never carries the
// password or its hash.
type BootstrapAdminResult struct {
	UserID  uuid.UUID
	Created bool // false: the email already had an account, nothing was changed
	Role    Role // the account's role (an existing account keeps its own)
}

// BootstrapAdmin creates the platform's first admin, for the identity-admin
// CLI (tools/seed and the E2E harness call it before they drive the HTTP API,
// because the API can only grant admin to an existing admin).
//
// It is idempotent: an account with this email that already exists is left
// exactly as it is (never promoted, never given a new password), because the
// caller proved nothing about it. Otherwise it does what a registration and an
// admin's role change do, in ONE transaction: the user (password hashed with
// the configured Argon2 settings), the identity.user_registered event, the
// role change to admin, the token_version bump and the identity.user_role_changed
// event. The email is marked verified (no mail is sent: the operator vouches
// for the address). The password and email are validated by the registration
// rules (RegisterRequest's binding tags).
func (s *Service) BootstrapAdmin(ctx context.Context, email, password, fullName string) (*BootstrapAdminResult, error) {
	req := RegisterRequest{Email: email, Password: password, FullName: fullName}
	if err := binding.Validator.ValidateStruct(&req); err != nil {
		// The validator's message names the field and rule, never the value.
		return nil, fmt.Errorf("invalid admin credentials: %w", err)
	}

	if existing, err := s.repo.GetUserByEmail(ctx, req.Email); err == nil {
		return &BootstrapAdminResult{UserID: existing.ID, Role: existing.Role}, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	hash, err := s.gate.Hash(ctx, s.argon, req.Password)
	if err != nil {
		return nil, argonErr(err)
	}
	now := time.Now().UTC()
	user := &User{
		ID:              uuid.Must(uuid.NewV7()),
		Email:           req.Email,
		PasswordHash:    hash,
		FullName:        req.FullName,
		Role:            RoleStudent,
		Status:          StatusActive,
		TokenVersion:    1,
		EmailVerifiedAt: &now,
	}
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.CreateUser(ctx, user); err != nil {
			return err
		}
		if err := outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user", AggregateID: user.ID, EventType: "identity.user_registered", SchemaVersion: 1,
			Payload: map[string]any{"user_id": user.ID, "email": user.Email, "role": RoleStudent, "full_name": user.FullName},
		}); err != nil {
			return err
		}
		if err := txRepo.UpdateRoleAndStatus(ctx, user.ID, RoleAdmin, StatusActive); err != nil {
			return err
		}
		v, err := txRepo.BumpTokenVersion(ctx, user.ID)
		if err != nil {
			return err
		}
		user.TokenVersion = v
		user.Role = RoleAdmin
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user", AggregateID: user.ID, EventType: "identity.user_role_changed", SchemaVersion: 1,
			Payload: map[string]any{"user_id": user.ID, "role": RoleAdmin, "status": StatusActive},
		})
	})
	if isUniqueViolation(err) {
		// Lost a race with another run: the account exists now.
		if existing, gerr := s.repo.GetUserByEmail(ctx, req.Email); gerr == nil {
			return &BootstrapAdminResult{UserID: existing.ID, Role: existing.Role}, nil
		}
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	// The same cache write UpdateUserRole does; best effort here (the
	// re-seeder and the next login repair a miss).
	_ = s.rdb.Set(ctx, auth.TokenVersionRedisKey(user.ID.String()), user.TokenVersion, 0).Err()
	return &BootstrapAdminResult{UserID: user.ID, Created: true, Role: RoleAdmin}, nil
}
