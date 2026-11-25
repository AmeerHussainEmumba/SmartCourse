// Command identity-admin creates the platform's FIRST admin from environment
// credentials. The HTTP API can only grant admin to an existing admin, so
// something has to bootstrap one: tools/seed and the E2E harness call this
// (`docker compose run --rm identity-api identity-admin`).
//
//	IDENTITY_ADMIN_EMAIL     required
//	IDENTITY_ADMIN_PASSWORD  required; the registration rules (8 to 128 characters)
//	IDENTITY_ADMIN_NAME      optional, default "Administrator"
//
// It goes through the identity service layer, so the password is hashed with
// the real Argon2 settings (ARGON2_*) and the same outbox events are written as
// for a registration followed by a role change. Idempotent: an account with
// that email that already exists is left untouched and the command exits 0.
// The password and its hash are never printed or logged.
//
// Exit codes: 0 created or already exists, 1 failure, 2 missing or invalid input.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

const defaultName = "Administrator"

// bootstrapper is the part of *identity.Service the command uses.
type bootstrapper interface {
	BootstrapAdmin(ctx context.Context, email, password, fullName string) (*identity.BootstrapAdminResult, error)
}

func main() {
	os.Exit(run(context.Background(), os.Getenv, os.Stdout, os.Stderr, connect))
}

// connect builds the identity service on the configured Postgres and Redis and
// returns the function that closes them.
// requirements are the configuration groups the CLI needs: Postgres and Redis
// (the service writes the user and the token-version key) and Argon (it hashes
// the password). It signs no token, so JWT_SIGNING_KEY is not required, and it
// neither reads nor writes Kafka or the object store.
var requirements = config.Requirements{Postgres: true, Redis: true, Argon: true}

// loadConfig is the CLI's configuration loader.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

func connect(_ context.Context) (bootstrapper, func(), error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("configuration error: %w", err)
	}
	deps, err := infra.Connect(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to dependencies: %w", err)
	}
	svc := identity.NewService(identity.NewRepository(deps.DB), deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	closeAll := func() {
		if sqlDB, err := deps.DB.DB(); err == nil {
			_ = sqlDB.Close()
		}
		_ = deps.RDB.Close()
	}
	return svc, closeAll, nil
}

// run is main without the process: it returns the exit code.
func run(ctx context.Context, getenv func(string) string, stdout, stderr io.Writer, open func(context.Context) (bootstrapper, func(), error)) int {
	email := strings.TrimSpace(getenv("IDENTITY_ADMIN_EMAIL"))
	password := getenv("IDENTITY_ADMIN_PASSWORD")
	name := strings.TrimSpace(getenv("IDENTITY_ADMIN_NAME"))
	if email == "" || password == "" {
		_, _ = fmt.Fprintln(stderr, "identity-admin: IDENTITY_ADMIN_EMAIL and IDENTITY_ADMIN_PASSWORD are required")
		return 2
	}
	if name == "" {
		name = defaultName
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	svc, closeAll, err := open(ctx)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "identity-admin:", err)
		return 1
	}
	defer closeAll()

	res, err := svc.BootstrapAdmin(ctx, email, password, name)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "identity-admin:", err)
		if isInputError(err) {
			return 2
		}
		return 1
	}
	if res.Created {
		_, _ = fmt.Fprintf(stdout, "identity-admin: created admin %s (user id %s)\n", email, res.UserID)
		return 0
	}
	_, _ = fmt.Fprintf(stdout, "identity-admin: %s already exists (user id %s, role %s); nothing changed\n", email, res.UserID, res.Role)
	return 0
}

// isInputError reports a validation failure (the registration rules) rather
// than an infrastructure one.
func isInputError(err error) bool {
	return strings.HasPrefix(err.Error(), "invalid admin credentials")
}
