// The only file in this module that issues GORM queries directly (RFC
// §5.9) — service.go calls these methods and never touches *gorm.DB itself.
package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// WithTx returns a Repository bound to tx instead of the base connection —
// used when a caller needs multiple repository calls inside one
// transaction (e.g. Register: create user + write outbox row atomically).
func (r *Repository) WithTx(tx *gorm.DB) *Repository {
	return &Repository{db: tx}
}

func (r *Repository) DB() *gorm.DB { return r.db }

var ErrNotFound = errors.New("identity: not found")

func (r *Repository) CreateUser(ctx context.Context, u *User) error {
	return r.db.WithContext(ctx).Create(u).Error
}

func (r *Repository) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	var u User
	err := r.db.WithContext(ctx).Where("email = ?", email).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repository) GetUserByID(ctx context.Context, id uuid.UUID) (*User, error) {
	var u User
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repository) UpdateProfile(ctx context.Context, id uuid.UUID, fullName string) error {
	return r.db.WithContext(ctx).Model(&User{}).Where("id = ?", id).
		Updates(map[string]any{"full_name": fullName, "updated_at": time.Now().UTC()}).Error
}

// BumpTokenVersion increments token_version and returns the new value in a
// single round trip (UPDATE ... RETURNING) — the database is the source of
// truth for it; the caller (service.go) then writes the new value to Redis
// so RequireFreshPrivilege can check it without a DB round trip (ADR-0015
// addendum).
func (r *Repository) BumpTokenVersion(ctx context.Context, id uuid.UUID) (int16, error) {
	var u User
	err := r.db.WithContext(ctx).Model(&u).Clauses(clause.Returning{Columns: []clause.Column{{Name: "token_version"}}}).
		Where("id = ?", id).
		Update("token_version", gorm.Expr("token_version + 1")).Error
	if err != nil {
		return 0, err
	}
	return u.TokenVersion, nil
}

func (r *Repository) UpdateRoleAndStatus(ctx context.Context, id uuid.UUID, role Role, status UserStatus) error {
	return r.db.WithContext(ctx).Model(&User{}).Where("id = ?", id).
		Updates(map[string]any{"role": role, "status": status, "updated_at": time.Now().UTC()}).Error
}

func (r *Repository) SetPasswordHash(ctx context.Context, id uuid.UUID, hash string) error {
	return r.db.WithContext(ctx).Model(&User{}).Where("id = ?", id).
		Updates(map[string]any{"password_hash": hash, "updated_at": time.Now().UTC()}).Error
}

func (r *Repository) MarkEmailVerified(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&User{}).Where("id = ?", id).
		Update("email_verified_at", now).Error
}

// --- refresh tokens ---------------------------------------------------

func (r *Repository) CreateRefreshToken(ctx context.Context, t *RefreshToken) error {
	return r.db.WithContext(ctx).Create(t).Error
}

func (r *Repository) GetRefreshTokenByHash(ctx context.Context, hash []byte) (*RefreshToken, error) {
	var t RefreshToken
	err := r.db.WithContext(ctx).Where("token_hash = ?", hash).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *Repository) MarkRefreshTokenReplaced(ctx context.Context, id, replacedBy uuid.UUID) error {
	return r.db.WithContext(ctx).Model(&RefreshToken{}).Where("id = ?", id).
		Update("replaced_by", replacedBy).Error
}

// RevokeFamily revokes every non-revoked token in familyID — the response
// to detected refresh-token reuse (ADR-0015): presenting an
// already-replaced token indicates theft, so the whole family is burned.
func (r *Repository) RevokeFamily(ctx context.Context, familyID uuid.UUID) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&RefreshToken{}).
		Where("family_id = ? AND revoked_at IS NULL", familyID).
		Update("revoked_at", now).Error
}

func (r *Repository) RevokeToken(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&RefreshToken{}).Where("id = ?", id).
		Update("revoked_at", now).Error
}

// --- password reset -----------------------------------------------------

func (r *Repository) CreatePasswordResetToken(ctx context.Context, t *PasswordResetToken) error {
	return r.db.WithContext(ctx).Create(t).Error
}

func (r *Repository) GetPasswordResetTokenByHash(ctx context.Context, hash []byte) (*PasswordResetToken, error) {
	var t PasswordResetToken
	err := r.db.WithContext(ctx).Where("token_hash = ?", hash).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *Repository) MarkPasswordResetTokenUsed(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&PasswordResetToken{}).Where("id = ?", id).
		Update("used_at", now).Error
}

// --- email verification --------------------------------------------------

func (r *Repository) CreateEmailVerificationToken(ctx context.Context, t *EmailVerificationToken) error {
	return r.db.WithContext(ctx).Create(t).Error
}

func (r *Repository) GetEmailVerificationTokenByHash(ctx context.Context, hash []byte) (*EmailVerificationToken, error) {
	var t EmailVerificationToken
	err := r.db.WithContext(ctx).Where("token_hash = ?", hash).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *Repository) MarkEmailVerificationTokenUsed(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&EmailVerificationToken{}).Where("id = ?", id).
		Update("used_at", now).Error
}
