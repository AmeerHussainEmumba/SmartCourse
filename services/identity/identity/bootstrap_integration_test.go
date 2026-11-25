//go:build integration

package identity_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

const bootstrapPassword = "correct-horse-battery-staple"

func TestBootstrapAdmin_CreatesAdminLikeRegistrationPlusRoleChange(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	res, err := h.svc.BootstrapAdmin(ctx, "root@example.com", bootstrapPassword, "Root Admin")
	if err != nil || !res.Created || res.Role != identity.RoleAdmin {
		t.Fatalf("BootstrapAdmin = %+v, %v", res, err)
	}

	var role string
	var tv int
	var hash string
	var verified bool
	row := h.env.DB.Raw(`SELECT role::text, token_version, password_hash, email_verified_at IS NOT NULL FROM users WHERE id = ?`, res.UserID).Row()
	if err := row.Scan(&role, &tv, &hash, &verified); err != nil {
		t.Fatal(err)
	}
	if role != "admin" || tv != 2 || !verified {
		t.Errorf("role=%s token_version=%d verified=%v, want admin/2/true", role, tv, verified)
	}
	if hash == bootstrapPassword || !strings.HasPrefix(hash, "$argon2id$") {
		t.Errorf("password not stored as an Argon2id hash: %.12s", hash)
	}

	var types []string
	if err := h.env.DB.Raw(`SELECT event_type FROM outbox WHERE aggregate_id = ? ORDER BY id`, res.UserID).Scan(&types).Error; err != nil {
		t.Fatal(err)
	}
	if len(types) != 2 || types[0] != "identity.user_registered" || types[1] != "identity.user_role_changed" {
		t.Errorf("outbox events = %v, want registered then role_changed", types)
	}
	var payloads string
	h.env.DB.Raw(`SELECT string_agg(payload::text, ' ') FROM outbox WHERE aggregate_id = ?`, res.UserID).Row().Scan(&payloads)
	if strings.Contains(payloads, bootstrapPassword) || strings.Contains(payloads, "argon2") {
		t.Error("outbox payload carries the password or its hash")
	}

	if v, err := h.env.RDB.Get(ctx, auth.TokenVersionRedisKey(res.UserID.String())).Int(); err != nil || v != 2 {
		t.Errorf("redis token version = %d, %v, want 2", v, err)
	}

	// The credentials really work and the token carries the admin role.
	if rec := h.login("root@example.com", bootstrapPassword); rec.Code != 200 {
		t.Errorf("login as the new admin = %d: %s", rec.Code, rec.Body)
	}
}

func TestBootstrapAdmin_IsIdempotentAndNeverTouchesAnExistingAccount(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first, err := h.svc.BootstrapAdmin(ctx, "root@example.com", bootstrapPassword, "Root Admin")
	if err != nil || !first.Created {
		t.Fatalf("first: %+v, %v", first, err)
	}
	var hashBefore string
	h.env.DB.Raw(`SELECT password_hash FROM users WHERE id = ?`, first.UserID).Row().Scan(&hashBefore)

	// Same email in another case, different password: left alone.
	again, err := h.svc.BootstrapAdmin(ctx, "ROOT@example.com", "a-completely-different-pw", "Someone Else")
	if err != nil || again.Created || again.UserID != first.UserID {
		t.Fatalf("second: %+v, %v", again, err)
	}
	var hashAfter string
	var n int
	h.env.DB.Raw(`SELECT password_hash FROM users WHERE id = ?`, first.UserID).Row().Scan(&hashAfter)
	h.env.DB.Raw(`SELECT count(*) FROM outbox`).Row().Scan(&n)
	if hashAfter != hashBefore || n != 2 {
		t.Errorf("second run changed state: hash same=%v outbox rows=%d (want 2)", hashAfter == hashBefore, n)
	}

	// A plain student with that email is not promoted by someone holding the env vars.
	h.mustRegister("student@example.com", "student-password-1")
	st, err := h.svc.BootstrapAdmin(ctx, "student@example.com", bootstrapPassword, "X")
	if err != nil || st.Created || st.Role != identity.RoleStudent {
		t.Fatalf("existing student: %+v, %v", st, err)
	}
}

func TestBootstrapAdmin_RejectsWhatRegistrationRejects(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for name, c := range map[string][3]string{
		"short password": {"a@example.com", "short", "A"},
		"long password":  {"a@example.com", strings.Repeat("x", 129), "A"},
		"bad email":      {"not-an-email", bootstrapPassword, "A"},
		"empty name":     {"a@example.com", bootstrapPassword, ""},
	} {
		res, err := h.svc.BootstrapAdmin(ctx, c[0], c[1], c[2])
		if err == nil || res != nil {
			t.Errorf("%s accepted: %+v", name, res)
			continue
		}
		if strings.Contains(err.Error(), c[1]) && c[1] != "" {
			t.Errorf("%s: the error echoes the password", name)
		}
	}
	var n int
	h.env.DB.Raw(`SELECT (SELECT count(*) FROM users) + (SELECT count(*) FROM outbox)`).Row().Scan(&n)
	if n != 0 {
		t.Errorf("rejected input left %d rows behind", n)
	}
}

func TestBootstrapAdmin_ConcurrentRunsCreateExactlyOneAdmin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan *identity.BootstrapAdminResult, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if res, err := h.svc.BootstrapAdmin(ctx, "root@example.com", bootstrapPassword, "Root"); err == nil {
				results <- res
			} else {
				t.Errorf("BootstrapAdmin: %v", err)
			}
		}()
	}
	wg.Wait()
	close(results)
	created := 0
	for r := range results {
		if r.Created {
			created++
		}
	}
	var users, events int
	h.env.DB.Raw(`SELECT count(*) FROM users`).Row().Scan(&users)
	h.env.DB.Raw(`SELECT count(*) FROM outbox`).Row().Scan(&events)
	if created != 1 || users != 1 || events != 2 {
		t.Errorf("created=%d users=%d outbox=%d, want 1/1/2", created, users, events)
	}
}
