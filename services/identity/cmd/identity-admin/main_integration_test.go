//go:build integration

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

// The CLI against a real database as the identity role: the first run creates
// the admin (registration + role change events, an Argon2id hash), the second is
// a no-op that exits 0, and neither prints the password.
func TestAdminCLI_CreatesTheFirstAdminOnceAgainstRealPostgres(t *testing.T) {
	tenv := testenv.Setup(t)
	db := tenv.ServiceDB(t, "identity")
	jwt := config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: time.Hour, Issuer: "t"}
	argon := config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	svc := identity.NewService(identity.NewRepository(db), tenv.RDB, jwt, argon, identity.LoggingEmailSender{})
	open := func(context.Context) (bootstrapper, func(), error) { return svc, func() {}, nil }
	m := map[string]string{"IDENTITY_ADMIN_EMAIL": "root@example.com", "IDENTITY_ADMIN_PASSWORD": secretPW, "IDENTITY_ADMIN_NAME": "Root"}

	var out1, err1 bytes.Buffer
	if code := run(context.Background(), env(m), &out1, &err1, open); code != 0 || !strings.Contains(out1.String(), "created admin") {
		t.Fatalf("first run: exit %d, out %q, err %q", code, out1.String(), err1.String())
	}
	var out2, err2 bytes.Buffer
	if code := run(context.Background(), env(m), &out2, &err2, open); code != 0 || !strings.Contains(out2.String(), "already exists") {
		t.Fatalf("second run: exit %d, out %q, err %q", code, out2.String(), err2.String())
	}
	for _, s := range []string{out1.String(), err1.String(), out2.String(), err2.String()} {
		if strings.Contains(s, secretPW) || strings.Contains(s, "argon2") {
			t.Fatalf("secret material in output: %q", s)
		}
	}

	var admins, users, events int
	var hash string
	db.Raw(`SELECT count(*) FROM users WHERE role = 'admin'`).Row().Scan(&admins)
	db.Raw(`SELECT count(*) FROM users`).Row().Scan(&users)
	db.Raw(`SELECT count(*) FROM outbox`).Row().Scan(&events)
	db.Raw(`SELECT password_hash FROM users`).Row().Scan(&hash)
	if admins != 1 || users != 1 || events != 2 {
		t.Errorf("admins=%d users=%d outbox=%d, want 1/1/2", admins, users, events)
	}
	if !strings.HasPrefix(hash, "$argon2id$") {
		t.Error("password is not stored as an Argon2id hash")
	}
}

