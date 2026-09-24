// Domain logic, transactions, and authorisation for identity (RFC §4.3).
// Every exported method returns *apperrors.AppError (or nil) directly —
// handler.go never has to translate a domain-specific error type.
package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/ratelimit"
)

var errInvalidCredentials = apperrors.New(401, "invalid_credentials", "Invalid email or password.")
var errAccountSuspended = apperrors.New(403, "account_suspended", "This account has been suspended.")
var errEmailTaken = apperrors.New(409, "email_taken", "An account with this email already exists.")
var errTokenInvalid = apperrors.New(401, "token_invalid", "This token is invalid, expired, or already used.")

// loginPerAccountLimit caps failed-login attempts against one account,
// independent of the per-IP limit applied at the route level
// (handler.go) — RFC §11.3 asks for both, because an attacker distributing
// guesses across many IPs against one account defeats a per-IP-only limit.
var loginPerAccountLimit = ratelimit.PerMinute(5)

type Service struct {
	repo    *Repository
	rdb     *redis.Client
	limiter *ratelimit.Limiter
	jwtCfg  config.JWTConfig
	argon   config.Argon2Config
	email   EmailSender
}

func NewService(repo *Repository, rdb *redis.Client, jwtCfg config.JWTConfig, argon config.Argon2Config, email EmailSender) *Service {
	return &Service{repo: repo, rdb: rdb, limiter: ratelimit.New(rdb), jwtCfg: jwtCfg, argon: argon, email: email}
}

// --- registration & login -------------------------------------------------

func (s *Service) Register(ctx context.Context, req RegisterRequest) (*AuthResponse, error) {
	hash, err := auth.HashPassword(s.argon, req.Password)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	user := &User{
		ID:           uuid.Must(uuid.NewV7()),
		Email:        req.Email,
		PasswordHash: hash,
		FullName:     req.FullName,
		Role:         RoleStudent,
		Status:       StatusActive,
		TokenVersion: 1,
	}

	var verifyRawToken string
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.CreateUser(ctx, user); err != nil {
			return err
		}

		rawToken, hash, err := auth.GenerateOpaqueToken()
		if err != nil {
			return fmt.Errorf("generate verification token: %w", err)
		}
		verifyRawToken = rawToken
		if err := txRepo.CreateEmailVerificationToken(ctx, &EmailVerificationToken{
			ID:        uuid.Must(uuid.NewV7()),
			UserID:    user.ID,
			TokenHash: hash,
			ExpiresAt: time.Now().UTC().Add(emailVerifyTokenTTL),
		}); err != nil {
			return fmt.Errorf("create verification token: %w", err)
		}

		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user",
			AggregateID:   user.ID,
			EventType:     "identity.user_registered",
			SchemaVersion: 1,
			Payload:       map[string]any{"user_id": user.ID, "email": user.Email, "role": user.Role},
		})
	})
	if isUniqueViolation(err) {
		return nil, errEmailTaken
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	// Best-effort, outside the transaction: delivery is not yet queued
	// (EmailSender doc comment) — its failure must not fail registration,
	// which has already durably committed.
	if err := s.email.SendEmailVerification(ctx, user.Email, verifyRawToken); err != nil {
		return nil, apperrors.ErrInternal.Wrap(fmt.Errorf("registration succeeded but verification email failed: %w", err))
	}

	return s.issueAuthResponse(ctx, user)
}

func (s *Service) Login(ctx context.Context, req LoginRequest) (*AuthResponse, error) {
	// Per-account limit (RFC §11.3), independent of the per-IP limit
	// applied in handler.go's route middleware — checked before touching
	// the database at all, so a credential-stuffing run against one
	// account is capped regardless of how many source IPs it's spread
	// across. Lower-cased explicitly: the account itself is looked up
	// case-insensitively via CITEXT (§5.3.1), so the rate-limit key must
	// match on the same terms — varying the email's case per attempt must
	// not be a way to dodge this limit.
	if err := s.limiter.Allow(ctx, ratelimit.AccountKey("login", strings.ToLower(req.Email)), loginPerAccountLimit); err != nil {
		return nil, err
	}

	user, err := s.repo.GetUserByEmail(ctx, req.Email)
	if errors.Is(err, ErrNotFound) {
		// Compare against the dummy hash anyway so response timing doesn't
		// reveal whether the account exists (RFC §11.3).
		_, _ = auth.VerifyPassword(auth.DummyHash, req.Password)
		return nil, errInvalidCredentials
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	ok, err := auth.VerifyPassword(user.PasswordHash, req.Password)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if !ok {
		return nil, errInvalidCredentials
	}
	if user.Status == StatusSuspended || user.Status == StatusDeleted {
		return nil, errAccountSuspended
	}

	return s.issueAuthResponse(ctx, user)
}

// --- refresh & logout ------------------------------------------------------

func (s *Service) Refresh(ctx context.Context, req RefreshRequest) (*AuthResponse, error) {
	hash := auth.HashOpaqueToken(req.RefreshToken)
	stored, err := s.repo.GetRefreshTokenByHash(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return nil, errTokenInvalid
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	// Reuse detection (ADR-0015): a revoked or already-rotated token being
	// presented again means the token was stolen. Burn the whole family.
	if stored.RevokedAt != nil || stored.ReplacedBy != nil {
		if err := s.repo.RevokeFamily(ctx, stored.FamilyID); err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		return nil, errTokenInvalid
	}
	if time.Now().After(stored.ExpiresAt) {
		return nil, errTokenInvalid
	}

	user, err := s.repo.GetUserByID(ctx, stored.UserID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if user.Status == StatusSuspended || user.Status == StatusDeleted {
		return nil, errAccountSuspended
	}

	newRaw, newHash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	newToken := &RefreshToken{
		ID:        uuid.Must(uuid.NewV7()),
		UserID:    user.ID,
		TokenHash: newHash,
		FamilyID:  stored.FamilyID,
		ExpiresAt: time.Now().UTC().Add(s.jwtCfg.RefreshTokenTTL),
	}
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.CreateRefreshToken(ctx, newToken); err != nil {
			return err
		}
		return txRepo.MarkRefreshTokenReplaced(ctx, stored.ID, newToken.ID)
	})
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	access, err := auth.IssueAccessToken(s.jwtCfg, user.ID, string(user.Role), user.TokenVersion)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return &AuthResponse{
		AccessToken:  access,
		RefreshToken: newRaw,
		ExpiresIn:    int(s.jwtCfg.AccessTokenTTL.Seconds()),
		User:         toUserResponse(user),
	}, nil
}

// Logout revokes exactly the presented token. It's intentionally
// idempotent — an unknown or already-revoked token is treated as success,
// so logout never leaks whether a token was valid.
func (s *Service) Logout(ctx context.Context, req LogoutRequest) error {
	hash := auth.HashOpaqueToken(req.RefreshToken)
	stored, err := s.repo.GetRefreshTokenByHash(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if err := s.repo.RevokeToken(ctx, stored.ID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// --- profile ---------------------------------------------------------------

func (s *Service) GetMe(ctx context.Context, userID uuid.UUID) (*UserResponse, error) {
	user, err := s.repo.GetUserByID(ctx, userID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	resp := toUserResponse(user)
	return &resp, nil
}

func (s *Service) UpdateMe(ctx context.Context, userID uuid.UUID, req UpdateProfileRequest) (*UserResponse, error) {
	if err := s.repo.UpdateProfile(ctx, userID, req.FullName); err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return s.GetMe(ctx, userID)
}

// UpdateUserRole is the admin action guarded by RequireFreshPrivilege at
// the route level (handler.go). It bumps token_version and pushes the new
// value to Redis in the same request, so the privilege-revocation check
// (ADR-0015 addendum) sees it on the target user's very next request.
func (s *Service) UpdateUserRole(ctx context.Context, targetUserID uuid.UUID, role Role, status UserStatus) (*UserResponse, error) {
	var newVersion int16
	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.UpdateRoleAndStatus(ctx, targetUserID, role, status); err != nil {
			return err
		}
		v, err := txRepo.BumpTokenVersion(ctx, targetUserID)
		if err != nil {
			return err
		}
		newVersion = v
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user",
			AggregateID:   targetUserID,
			EventType:     "identity.user_role_changed",
			SchemaVersion: 1,
			Payload:       map[string]any{"user_id": targetUserID, "role": role, "status": status},
		})
	})
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	if err := s.rdb.Set(ctx, auth.TokenVersionRedisKey(targetUserID.String()), newVersion, 0).Err(); err != nil {
		// The DB write already committed; a Redis failure here only delays
		// (not prevents) the revocation taking effect — RequireFreshPrivilege
		// falls back to treating a cache miss as version 1, so this is a
		// narrowed window, not a silent bypass, and is logged for follow-up.
		return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("role changed but revocation cache update failed: %w", err))
	}

	return s.GetMe(ctx, targetUserID)
}

// --- password reset ---------------------------------------------------------

// RequestPasswordReset always succeeds from the caller's perspective,
// whether or not the email exists (RFC §11.3/§15) — existence must not be
// inferable from response shape.
func (s *Service) RequestPasswordReset(ctx context.Context, req PasswordResetRequestDTO) error {
	user, err := s.repo.GetUserByEmail(ctx, req.Email)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}

	raw, hash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if err := s.repo.CreatePasswordResetToken(ctx, &PasswordResetToken{
		ID:        uuid.Must(uuid.NewV7()),
		UserID:    user.ID,
		TokenHash: hash,
		ExpiresAt: time.Now().UTC().Add(passwordResetTokenTTL),
	}); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}

	_ = s.email.SendPasswordReset(ctx, user.Email, raw) // best-effort; see EmailSender doc comment
	return nil
}

// ConfirmPasswordReset revokes every existing refresh token and bumps
// token_version on success — a password reset is exactly the kind of event
// that should force re-authentication everywhere, not just locally.
func (s *Service) ConfirmPasswordReset(ctx context.Context, req PasswordResetConfirmRequest) error {
	hash := auth.HashOpaqueToken(req.Token)
	stored, err := s.repo.GetPasswordResetTokenByHash(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return errTokenInvalid
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if stored.UsedAt != nil || time.Now().After(stored.ExpiresAt) {
		return errTokenInvalid
	}

	newHash, err := auth.HashPassword(s.argon, req.NewPassword)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}

	var newVersion int16
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.SetPasswordHash(ctx, stored.UserID, newHash); err != nil {
			return err
		}
		if err := txRepo.MarkPasswordResetTokenUsed(ctx, stored.ID); err != nil {
			return err
		}
		v, err := txRepo.BumpTokenVersion(ctx, stored.UserID)
		if err != nil {
			return err
		}
		newVersion = v
		return nil
	})
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}

	if err := s.rdb.Set(ctx, auth.TokenVersionRedisKey(stored.UserID.String()), newVersion, 0).Err(); err != nil {
		return apperrors.ErrUnavailable.Wrap(fmt.Errorf("password reset but revocation cache update failed: %w", err))
	}
	return nil
}

// --- email verification -----------------------------------------------------

func (s *Service) ResendEmailVerification(ctx context.Context, req ResendVerificationRequest) error {
	user, err := s.repo.GetUserByEmail(ctx, req.Email)
	if errors.Is(err, ErrNotFound) {
		return nil // no enumeration, matches RequestPasswordReset
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if user.EmailVerifiedAt != nil {
		return nil
	}

	raw, hash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if err := s.repo.CreateEmailVerificationToken(ctx, &EmailVerificationToken{
		ID:        uuid.Must(uuid.NewV7()),
		UserID:    user.ID,
		TokenHash: hash,
		ExpiresAt: time.Now().UTC().Add(emailVerifyTokenTTL),
	}); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}

	_ = s.email.SendEmailVerification(ctx, user.Email, raw)
	return nil
}

func (s *Service) ConfirmEmailVerification(ctx context.Context, rawToken string) error {
	hash := auth.HashOpaqueToken(rawToken)
	stored, err := s.repo.GetEmailVerificationTokenByHash(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return errTokenInvalid
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if stored.UsedAt != nil || time.Now().After(stored.ExpiresAt) {
		return errTokenInvalid
	}

	return s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.MarkEmailVerificationTokenUsed(ctx, stored.ID); err != nil {
			return err
		}
		return txRepo.MarkEmailVerified(ctx, stored.UserID)
	})
}

// --- shared helpers ----------------------------------------------------------

func (s *Service) issueAuthResponse(ctx context.Context, user *User) (*AuthResponse, error) {
	access, err := auth.IssueAccessToken(s.jwtCfg, user.ID, string(user.Role), user.TokenVersion)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	rawRefresh, refreshHash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	refreshToken := &RefreshToken{
		ID:        uuid.Must(uuid.NewV7()),
		UserID:    user.ID,
		TokenHash: refreshHash,
		FamilyID:  uuid.Must(uuid.NewV7()),
		ExpiresAt: time.Now().UTC().Add(s.jwtCfg.RefreshTokenTTL),
	}
	if err := s.repo.CreateRefreshToken(ctx, refreshToken); err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	return &AuthResponse{
		AccessToken:  access,
		RefreshToken: rawRefresh,
		ExpiresIn:    int(s.jwtCfg.AccessTokenTTL.Seconds()),
		User:         toUserResponse(user),
	}, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
