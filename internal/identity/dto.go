// Explicit request/response DTOs (RFC §11.5). ORM models (domain.go) are
// never bound to a request and never returned in a response — binding to a
// model permits mass assignment (a student setting "role":"admin"), and
// returning one leaks whatever field gets added next, password_hash
// included.
package identity

import "time"

type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8,max=128"`
	FullName string `json:"full_name" binding:"required,min=1,max=200"`
	// Role intentionally omitted: everyone registers as a student
	// (StatusActive, RoleStudent). Instructor/admin is granted by an
	// existing admin via PATCH /admin/users/{id}/role — never
	// self-service, and never bound from a request DTO.
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type LogoutRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type UpdateProfileRequest struct {
	FullName string `json:"full_name" binding:"required,min=1,max=200"`
}

type PasswordResetRequestDTO struct {
	Email string `json:"email" binding:"required,email"`
}

type PasswordResetConfirmRequest struct {
	Token       string `json:"token" binding:"required"`
	NewPassword string `json:"new_password" binding:"required,min=8,max=128"`
}

type ResendVerificationRequest struct {
	Email string `json:"email" binding:"required,email"`
}

// AuthResponse is returned by register/login/refresh.
type AuthResponse struct {
	AccessToken  string       `json:"access_token"`
	RefreshToken string       `json:"refresh_token"`
	ExpiresIn    int          `json:"expires_in"` // seconds
	User         UserResponse `json:"user"`
}

type UserResponse struct {
	ID              string     `json:"id"`
	Email           string     `json:"email"`
	FullName        string     `json:"full_name"`
	Role            string     `json:"role"`
	EmailVerifiedAt *time.Time `json:"email_verified_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

func toUserResponse(u *User) UserResponse {
	return UserResponse{
		ID:              u.ID.String(),
		Email:           u.Email,
		FullName:        u.FullName,
		Role:            string(u.Role),
		EmailVerifiedAt: u.EmailVerifiedAt,
		CreatedAt:       u.CreatedAt,
	}
}
