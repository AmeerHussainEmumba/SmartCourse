package identity

import (
	"context"
	"log/slog"
)

// EmailSender is deliberately declared here, by the consumer (RFC §4.4),
// rather than identity importing a notification package. Password-reset
// and verification emails are, per RFC §6.7/ADR-0014, ordinary Asynq
// delivery jobs — that queue doesn't exist yet (M3 scope), so
// LoggingEmailSender stands in until it does. Swapping it for a real
// Asynq-backed implementation at wiring time (cmd/smartcourse) is the only
// change needed; nothing in service.go changes.
type EmailSender interface {
	SendPasswordReset(ctx context.Context, toEmail, rawToken string) error
	SendEmailVerification(ctx context.Context, toEmail, rawToken string) error
}

// LoggingEmailSender is the M0/M1 placeholder: it logs what would have been
// sent instead of sending it. This is intentional, not an oversight — see
// the EmailSender doc comment.
type LoggingEmailSender struct{}

func (LoggingEmailSender) SendPasswordReset(ctx context.Context, toEmail, rawToken string) error {
	slog.Info("would send password reset email", slog.String("to", toEmail))
	return nil
}

func (LoggingEmailSender) SendEmailVerification(ctx context.Context, toEmail, rawToken string) error {
	slog.Info("would send email verification email", slog.String("to", toEmail))
	return nil
}
