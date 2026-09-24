// JWT access tokens (ADR-0015): stateless, 15-minute, signature-verified.
// The signing method is explicitly asserted on parse — accepting the `alg`
// header unchecked permits the `alg: none` attack, and the jwt/v5 library's
// default must not be trusted to reject it on its own.
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/config"
)

type Claims struct {
	Role         string `json:"role"`
	TokenVersion int16  `json:"tv"` // ADR-0015 addendum — privilege-revocation check
	jwt.RegisteredClaims
}

var ErrInvalidToken = errors.New("invalid or expired token")

// IssueAccessToken mints a signed access token for userID. tokenVersion is
// embedded so RequireFreshPrivilege (middleware.go) can detect a token
// issued before a subsequent role/status change.
func IssueAccessToken(cfg config.JWTConfig, userID uuid.UUID, role string, tokenVersion int16) (string, error) {
	now := time.Now()
	claims := Claims{
		Role:         role,
		TokenVersion: tokenVersion,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			Issuer:    cfg.Issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(cfg.AccessTokenTTL)),
			ID:        uuid.NewString(),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(cfg.SigningKey))
	if err != nil {
		return "", fmt.Errorf("sign access token: %w", err)
	}
	return signed, nil
}

// ParseAccessToken verifies signature, issuer, and expiry, and explicitly
// rejects any signing method other than HMAC — the check that closes the
// alg:none / alg-confusion class of vulnerability.
func ParseAccessToken(cfg config.JWTConfig, raw string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(cfg.SigningKey), nil
	}, jwt.WithIssuer(cfg.Issuer), jwt.WithExpirationRequired())
	if err != nil || !token.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// UserID extracts and parses the subject claim as a UUID.
func (c *Claims) UserID() (uuid.UUID, error) {
	return uuid.Parse(c.Subject)
}
