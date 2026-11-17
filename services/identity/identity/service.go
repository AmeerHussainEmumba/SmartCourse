// Domain logic, transactions, and authorisation for identity (RFC §4.3).
// Every exported method returns *apperrors.AppError (or nil) directly —
// handler.go never has to translate a domain-specific error type.
package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit"
)

var errInvalidCredentials = apperrors.New(401, "invalid_credentials", "Invalid email or password.")
var errAccountSuspended = apperrors.New(403, "account_suspended", "This account has been suspended.")
var errEmailTaken = apperrors.New(409, "email_taken", "An account with this email already exists.")
var errLastAdmin = apperrors.New(409, "last_admin", "This change would leave the platform without an active admin.")
var errTokenInvalid = apperrors.New(401, "token_invalid", "This token is invalid, expired, or already used.")

// loginPerAccountLimit caps failed-login attempts against one account,
// independent of the per-IP limit applied at the route level
// (handler.go) — RFC §11.3 asks for both, because an attacker distributing
// guesses across many IPs against one account defeats a per-IP-only limit.
//
// Only FAILED attempts count (F-31). loginPerAccountIPLimit is the tight
// per-(account, IP) budget; loginPerAccountLimit is the looser all-IP cap
// that still stops credential stuffing spread over many addresses without
// letting five bad guesses lock the owner out.
var (
	loginPerAccountIPLimit = ratelimit.PerMinute(5)
	loginPerAccountLimit   = ratelimit.PerMinute(20)

	// Per-address budget for emails sent on behalf of anonymous requests
	// (reset, resend, register-over-existing). Exceeding it is silent.
	emailPerAddressLimit = ratelimit.PerHour(5)
)

type Service struct {
	repo      *Repository
	rdb       *redis.Client
	limiter   *ratelimit.Limiter
	jwtCfg    config.JWTConfig
	argon     config.Argon2Config
	email     EmailSender
	dummyHash string // built from the configured Argon2 parameters (F-30)
}

func NewService(repo *Repository, rdb *redis.Client, jwtCfg config.JWTConfig, argon config.Argon2Config, email EmailSender) *Service {
	dummy, err := auth.NewDummyHash(argon)
	if err != nil {
		slog.Error("identity: dummy hash from configured argon2 failed; using default", slog.Any("error", err))
		dummy = auth.DummyHash
	}
	return &Service{repo: repo, rdb: rdb, limiter: ratelimit.New(rdb), jwtCfg: jwtCfg, argon: argon, email: email, dummyHash: dummy}
}

// addressBudgetExceeded consumes one unit of the per-address email budget and
// reports whether it is exhausted.
func (s *Service) addressBudgetExceeded(ctx context.Context, scope, email string) bool {
	return s.limiter.Allow(ctx, ratelimit.AccountKey("email:"+scope, strings.ToLower(email)), emailPerAddressLimit) != nil
}

// --- registration & login -------------------------------------------------

// RegisterPublic is what POST /auth/register calls. It answers the same way
// (nil error) whether or not the email already has an account, so the
// endpoint cannot be used to enumerate accounts (F-32): a new address gets a
// verification email, an existing one gets a password-reset email instead.
// No session is issued; the client signs in with /auth/login.
func (s *Service) RegisterPublic(ctx context.Context, req RegisterRequest) error {
	user, verifyRaw, err := s.createUser(ctx, req)
	if errors.Is(err, errEmailTaken) {
		// Existing account: same response, different (best-effort) email.
		// The per-address budget bounds how many emails one address can
		// be sent; exceeding it is silent so it cannot be probed either.
		if s.addressBudgetExceeded(ctx, "register", req.Email) {
			return nil
		}
		existing, gerr := s.repo.GetUserByEmail(ctx, req.Email)
		if gerr != nil {
			return nil
		}
		if gerr := s.sendPasswordReset(ctx, existing); gerr != nil {
			slog.Error("register (existing email): reset email failed", slog.Any("error", gerr))
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.email.SendEmailVerification(ctx, user.Email, verifyRaw); err != nil {
		slog.Error("registration succeeded but verification email failed",
			slog.String("user_id", user.ID.String()), slog.Any("error", err))
	}
	return nil
}

// Register creates the account and returns a session. It still reports
// email_taken (409) to direct Go callers such as tools/seed; the HTTP route
// uses RegisterPublic so the API does not leak which emails exist.
func (s *Service) Register(ctx context.Context, req RegisterRequest) (*AuthResponse, error) {
	user, verifyRawToken, err := s.createUser(ctx, req)
	if err != nil {
		return nil, err
	}

	// Best-effort, outside the transaction: delivery is not yet queued
	// (EmailSender doc comment) — its failure must not fail registration,
	// which has already durably committed. A failed send just means the
	// user doesn't get their verification email promptly;
	// ResendEmailVerification exists to recover from that.
	if err := s.email.SendEmailVerification(ctx, user.Email, verifyRawToken); err != nil {
		slog.Error("registration succeeded but verification email failed",
			slog.String("user_id", user.ID.String()), slog.Any("error", err))
	}

	return s.issueAuthResponse(ctx, user)
}

// createUser persists the user, a verification token and the outbox event in
// one transaction. It returns errEmailTaken for a duplicate email.
func (s *Service) createUser(ctx context.Context, req RegisterRequest) (*User, string, error) {
	hash, err := auth.HashPassword(s.argon, req.Password)
	if err != nil {
		return nil, "", apperrors.ErrInternal.Wrap(err)
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
			// full_name lets course.instructor_mirror seed itself without
			// a cross-schema join into identity.users (ADR-0024).
			Payload: map[string]any{"user_id": user.ID, "email": user.Email, "role": user.Role, "full_name": user.FullName},
		})
	})
	if isUniqueViolation(err) {
		return nil, "", errEmailTaken
	}
	if err != nil {
		return nil, "", apperrors.ErrInternal.Wrap(err)
	}
	return user, verifyRawToken, nil
}

func (s *Service) Login(ctx context.Context, req LoginRequest) (*AuthResponse, error) {
	// Failure-counting limits (RFC §11.3, F-31). Both are CHECKED before the
	// database is touched but only CONSUMED by a failed attempt, so
	// successful logins never use up the budget and an attacker's failures
	// from other IPs cannot lock the owner out on their own IP:
	//   1. account+IP: loginPerAccountIPLimit failures per minute per pair;
	//   2. account (any IP): loginPerAccountLimit failures per minute in
	//      total, the cap against credential stuffing spread across IPs.
	// Keys are lower-cased: the account is looked up case-insensitively
	// (CITEXT, §5.3.1), so case variation must not dodge the limit.
	acct := strings.ToLower(req.Email)
	ipKey := ratelimit.AccountIPKey("login", acct, req.ClientIP)
	acctKey := ratelimit.AccountKey("login", acct)
	if err := s.limiter.Check(ctx, ipKey, loginPerAccountIPLimit); err != nil {
		return nil, err
	}
	if err := s.limiter.Check(ctx, acctKey, loginPerAccountLimit); err != nil {
		return nil, err
	}
	fail := func() (*AuthResponse, error) {
		_ = s.limiter.Allow(ctx, ipKey, loginPerAccountIPLimit)
		_ = s.limiter.Allow(ctx, acctKey, loginPerAccountLimit)
		return nil, errInvalidCredentials
	}

	user, err := s.repo.GetUserByEmail(ctx, req.Email)
	if errors.Is(err, ErrNotFound) {
		// Compare against a dummy hash built from the CONFIGURED Argon2
		// parameters (F-30) so timing doesn't reveal whether the account
		// exists (RFC §11.3).
		_, _ = auth.VerifyPassword(s.dummyHash, req.Password)
		return fail()
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	ok, err := auth.VerifyPassword(user.PasswordHash, req.Password)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	// A suspended/deleted account answers exactly like a wrong password
	// (F-32), so login is never an oracle for password correctness.
	if !ok || user.Status == StatusSuspended || user.Status == StatusDeleted {
		return fail()
	}

	return s.issueAuthResponse(ctx, user)
}

// --- refresh & logout ------------------------------------------------------

// Refresh runs entirely inside one transaction, locking the presented
// token's row first (GetRefreshTokenByHashForUpdate) — the ADR-0012
// pattern applied here to close a real concurrency bug: two requests
// refreshing with the same token at the same moment used to both read
// "not yet rotated" before either had committed, so both passed the reuse
// check and both successfully minted a new token. The row lock serialises
// them — the second caller blocks until the first commits, then sees
// ReplacedBy already set and correctly treats it as reuse, burning the
// family instead of also succeeding.
func (s *Service) Refresh(ctx context.Context, req RefreshRequest) (*AuthResponse, error) {
	hash := auth.HashOpaqueToken(req.RefreshToken)

	var result *AuthResponse
	// reuseDetected exists because of a subtlety easy to miss: returning a
	// non-nil error from inside Transaction's closure rolls back everything
	// written in it — including the RevokeFamily call below, which is
	// exactly the side effect that must survive a rejected request. So the
	// reuse-detected branch commits (returns nil) and signals the rejection
	// out-of-band instead; every other rejection in this function has
	// nothing to persist, so returning the error directly (and rolling
	// back) is correct for those.
	var reuseDetected bool
	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)

		stored, err := txRepo.GetRefreshTokenByHashForUpdate(ctx, hash)
		if errors.Is(err, ErrNotFound) {
			return errTokenInvalid
		}
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		// Reuse detection (ADR-0015): a revoked or already-rotated token
		// being presented again means the token was stolen (or, as above,
		// a second concurrent caller lost the race). Burn the whole family.
		if stored.RevokedAt != nil || stored.ReplacedBy != nil {
			if err := txRepo.RevokeFamily(ctx, stored.FamilyID); err != nil {
				return apperrors.ErrInternal.Wrap(err)
			}
			reuseDetected = true
			return nil
		}
		if time.Now().After(stored.ExpiresAt) {
			return errTokenInvalid
		}

		user, err := txRepo.GetUserByID(ctx, stored.UserID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if user.Status == StatusSuspended || user.Status == StatusDeleted {
			return errAccountSuspended
		}

		newRaw, newHash, err := auth.GenerateOpaqueToken()
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		newToken := &RefreshToken{
			ID:        uuid.Must(uuid.NewV7()),
			UserID:    user.ID,
			TokenHash: newHash,
			FamilyID:  stored.FamilyID,
			ExpiresAt: time.Now().UTC().Add(s.jwtCfg.RefreshTokenTTL),
		}
		if err := txRepo.CreateRefreshToken(ctx, newToken); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if err := txRepo.MarkRefreshTokenReplaced(ctx, stored.ID, newToken.ID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		access, err := auth.IssueAccessToken(s.jwtCfg, user.ID, string(user.Role), user.TokenVersion)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		result = &AuthResponse{
			AccessToken:  access,
			RefreshToken: newRaw,
			ExpiresIn:    int(s.jwtCfg.AccessTokenTTL.Seconds()),
			User:         toUserResponse(user),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if reuseDetected {
		return nil, errTokenInvalid
	}
	return result, nil
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

// UpdateMe writes identity.user_profile_updated so course.instructor_mirror
// (ADR-0024 — course's own denormalized copy of instructor display names,
// never a cross-schema join into identity.users) stays current when an
// instructor renames themselves after course.ListCatalogue has already
// mirrored their old name.
func (s *Service) UpdateMe(ctx context.Context, userID uuid.UUID, req UpdateProfileRequest) (*UserResponse, error) {
	if err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.repo.WithTx(tx).UpdateProfile(ctx, userID, req.FullName); err != nil {
			return err
		}
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user",
			AggregateID:   userID,
			EventType:     "identity.user_profile_updated",
			SchemaVersion: 1,
			Payload:       map[string]any{"user_id": userID, "full_name": req.FullName},
		})
	}); err != nil {
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
		// Existence first: nothing below (Redis key, outbox row) may happen
		// for an id that is not a user (F-33).
		current, err := txRepo.GetUserByID(ctx, targetUserID)
		if errors.Is(err, ErrNotFound) {
			return apperrors.ErrNotFound
		}
		if err != nil {
			return err
		}
		// Last-admin guard: never leave the platform without an active admin.
		// The advisory lock serialises concurrent demotions so two admins
		// demoting each other cannot both pass the check.
		if current.Role == RoleAdmin && current.Status == StatusActive && (role != RoleAdmin || status != StatusActive) {
			if err := txRepo.LockAdminSet(ctx); err != nil {
				return err
			}
			others, err := txRepo.CountOtherActiveAdmins(ctx, targetUserID)
			if err != nil {
				return err
			}
			if others == 0 {
				return errLastAdmin
			}
		}
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
		if appErr, ok := apperrors.As(err); ok {
			return nil, appErr
		}
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
	// Per-address budget (F-15): silent, so it is not an existence oracle.
	if s.addressBudgetExceeded(ctx, "reset", req.Email) {
		return nil
	}
	if err := s.sendPasswordReset(ctx, user); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// sendPasswordReset issues a new reset token — invalidating every earlier
// unused one, so only the newest e-mailed token works (F-15) — and sends it.
func (s *Service) sendPasswordReset(ctx context.Context, user *User) error {
	raw, hash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return err
	}
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.InvalidateUnusedPasswordResetTokens(ctx, user.ID); err != nil {
			return err
		}
		return txRepo.CreatePasswordResetToken(ctx, &PasswordResetToken{
			ID:        uuid.Must(uuid.NewV7()),
			UserID:    user.ID,
			TokenHash: hash,
			ExpiresAt: time.Now().UTC().Add(passwordResetTokenTTL),
		})
	})
	if err != nil {
		return err
	}
	_ = s.email.SendPasswordReset(ctx, user.Email, raw) // best-effort; see EmailSender doc comment
	return nil
}

// ConfirmPasswordReset revokes every existing refresh token (RevokeAllForUser
// — across every family/device, not just one) and bumps token_version on
// success — a password reset is exactly the kind of event that should
// force re-authentication everywhere, not just locally. The revocation
// call was missing entirely until a review caught it: token_version alone
// only closes new *access* tokens (and only on the small set of routes
// RequireFreshPrivilege actually guards — ADR-0015 addendum); a stolen
// refresh token kept right on working through /auth/refresh after a
// "successful" password reset.
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
		// Atomic single use (F-14): the conditional UPDATE is the gate;
		// exactly one concurrent confirm gets the row, the rest see
		// ErrNotFound and roll back without touching the password.
		consumed, err := txRepo.ConsumePasswordResetToken(ctx, hash)
		if err != nil {
			return err
		}
		if err := txRepo.SetPasswordHash(ctx, consumed.UserID, newHash); err != nil {
			return err
		}
		v, err := txRepo.BumpTokenVersion(ctx, consumed.UserID)
		if err != nil {
			return err
		}
		newVersion = v
		return txRepo.RevokeAllForUser(ctx, consumed.UserID)
	})
	if errors.Is(err, ErrNotFound) {
		return errTokenInvalid
	}
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
	if s.addressBudgetExceeded(ctx, "verify", req.Email) { // silent, F-15
		return nil
	}

	raw, hash, err := auth.GenerateOpaqueToken()
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	// Invalidate earlier unused tokens so only the newest one is valid.
	if err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.InvalidateUnusedEmailVerificationTokens(ctx, user.ID); err != nil {
			return err
		}
		return txRepo.CreateEmailVerificationToken(ctx, &EmailVerificationToken{
			ID:        uuid.Must(uuid.NewV7()),
			UserID:    user.ID,
			TokenHash: hash,
			ExpiresAt: time.Now().UTC().Add(emailVerifyTokenTTL),
		})
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

	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		// Atomic single use (F-14), see ConfirmPasswordReset.
		consumed, err := txRepo.ConsumeEmailVerificationToken(ctx, hash)
		if err != nil {
			return err
		}
		return txRepo.MarkEmailVerified(ctx, consumed.UserID)
	})
	if errors.Is(err, ErrNotFound) {
		return errTokenInvalid
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
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
