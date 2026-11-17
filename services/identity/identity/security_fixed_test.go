//go:build integration

// Regression tests for findings fixed in identity (and libs/auth,
// libs/ratelimit): F-03, F-11, F-14, F-15, F-30, F-31, F-32, F-33. They began
// as bugrepro reproducers; test names are unchanged. The router-level ones
// (F-02, F-16) are in security_router_integration_test.go.
package identity_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// service.go documents the account limit as capping FAILED logins. It used to
// count every attempt, so a legitimate user who signs in six times a minute
// locked themself out (F-31).
func TestBug_RateLimit_SuccessfulLoginsConsumeFailedLoginBudget(t *testing.T) {
	h := newHarness(t)
	email := uniq("succ")
	h.mustRegister(email, "correct-horse-battery")
	for i := 0; i < 6; i++ {
		rec := h.login(email, "correct-horse-battery", withIP(fmt.Sprintf("203.0.113.%d", 120+i)))
		if rec.Code != 200 {
			t.Fatalf("legitimate login #%d with the CORRECT password got %d", i+1, rec.Code)
		}
	}
	// Same IP too: successes never consume the budget.
	for i := 0; i < 8; i++ {
		if rec := h.login(email, "correct-horse-battery", withIP("203.0.113.140")); rec.Code != 200 {
			t.Fatalf("repeat login #%d from one IP got %d", i+1, rec.Code)
		}
	}
}

// Failures from other addresses must not lock the owner out (F-31).
func TestBug_RateLimit_AnyoneCanLockOutAnyKnownEmail(t *testing.T) {
	h := newHarness(t)
	victim := uniq("victim")
	h.mustRegister(victim, "correct-horse-battery")
	for i := 0; i < 5; i++ { // attacker burns budget from other IPs
		h.login(victim, "attacker-guess", withIP(fmt.Sprintf("198.51.100.%d", 10+i)))
	}
	if rec := h.login(victim, "correct-horse-battery", withIP("192.0.2.77")); rec.Code != 200 {
		t.Fatalf("the real owner, with the right password, from their own IP, is locked out: %d", rec.Code)
	}
}

// The per-account+IP failure budget still blocks a guesser on one address.
func TestFix_Login_FailuresFromOneIP_BlockThatIPOnly(t *testing.T) {
	h := newHarness(t)
	email := uniq("ipblock")
	h.mustRegister(email, "correct-horse-battery")
	for i := 0; i < 5; i++ {
		if rec := h.login(email, fmt.Sprintf("guess-%d-aaaa", i), withIP("198.51.100.50")); rec.Code != 401 {
			t.Fatalf("attempt %d: %d", i, rec.Code)
		}
	}
	if rec := h.login(email, "correct-horse-battery", withIP("198.51.100.50")); rec.Code != 429 {
		t.Fatalf("6th attempt from the same IP must be rate limited, got %d", rec.Code)
	}
	if rec := h.login(email, "correct-horse-battery", withIP("198.51.100.51")); rec.Code != 200 {
		t.Fatalf("another IP must be unaffected, got %d", rec.Code)
	}
}

// auth.DummyHash was hard-wired to 64MiB/t=3/p=2 and ignored ARGON2_* config;
// with any other cost an unknown e-mail answered in a different time than a
// known one (F-30). The service now builds the dummy from its own config.
func TestBug_DummyHashIgnoresConfiguredArgon2_LoginTimingEnumeration(t *testing.T) {
	h := newHarness(t) // lightArgon: 8MiB/t=1/p=1, a perfectly legal configuration
	const n = 6
	known := make([]string, n)
	for i := range known {
		known[i] = uniq("tk")
		h.mustRegister(known[i], "correct-horse-battery")
	}
	avg := func(f func(i int) string) time.Duration {
		var d time.Duration
		for i := 0; i < n; i++ {
			s := time.Now()
			h.login(f(i), "wrong-password-123", withIP(fmt.Sprintf("198.51.100.%d", 100+i)))
			d += time.Since(s)
		}
		return d / n
	}
	dKnown := avg(func(i int) string { return known[i] })
	dUnknown := avg(func(i int) string { return uniq("tg") })
	t.Logf("known=%v unknown=%v", dKnown, dUnknown)
	if dUnknown > 3*dKnown || dKnown > 3*dUnknown {
		t.Fatalf("known and unknown accounts are distinguishable by latency: %v vs %v", dKnown, dUnknown)
	}
}

// ConfirmPasswordReset used to check used_at outside the transaction: with
// concurrent confirmations every one succeeded (F-14).
func TestBug_PasswordResetToken_ConcurrentConfirm_MultipleSuccesses(t *testing.T) {
	h := newHarness(t)
	email := uniq("race")
	h.mustRegister(email, "old-password-123")
	h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": email})
	tok := h.mail.resetToken(email)

	const n = 10
	var ok int32
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": tok, "new_password": fmt.Sprintf("racer-password-%d", i)})
			if rec.Code == 204 {
				atomic.AddInt32(&ok, 1)
			}
		}(i)
	}
	close(gate)
	wg.Wait()
	if ok != 1 {
		t.Fatalf("a single-use reset token was redeemed %d times concurrently", ok)
	}
}

// Same race on email verification (F-14).
func TestBug_EmailVerificationToken_ConcurrentConfirm_MultipleSuccesses(t *testing.T) {
	h := newHarness(t)
	email := uniq("vrace")
	h.mustRegister(email, "correct-horse-battery")
	tok := h.mail.verifyToken(email)
	const n = 12
	var ok int32
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if h.call("GET", "/api/v1/auth/verify-email/confirm?token="+tok, nil).Code == 204 {
				atomic.AddInt32(&ok, 1)
			}
		}()
	}
	close(gate)
	wg.Wait()
	if ok != 1 {
		t.Fatalf("single-use verification token redeemed %d times", ok)
	}
}

// Suspending/deleting a user bumps token_version; /me now checks it (F-11).
func TestBug_SuspendedUser_AccessTokenStillAccepted(t *testing.T) {
	h := newHarness(t)
	victim := h.mustRegister(uniq("susp"), "correct-horse-battery")
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	if rec := h.call("GET", "/api/v1/me", nil, withToken(victim.AccessToken)); rec.Code != 200 {
		t.Fatalf("precondition: /me with a fresh token: %d", rec.Code)
	}
	if rec := h.call("PATCH", "/api/v1/admin/users/"+victim.User.ID+"/role", map[string]any{"role": "student", "status": "deleted"}, withToken(adm)); rec.Code != 200 {
		t.Fatalf("setup: %d", rec.Code)
	}
	if rec := h.call("GET", "/api/v1/me", nil, withToken(victim.AccessToken)); rec.Code == 200 {
		t.Fatalf("a DELETED user's access token still works on /me (got %d)", rec.Code)
	}
}

// Even when the revocation key was lost (Redis flush) a suspended user's
// token must not work: the fallback reads Postgres (F-03 + F-11).
func TestFix_SuspendedUser_AfterRedisFlush_TokenStillRejected(t *testing.T) {
	h := newHarness(t)
	victim := h.mustRegister(uniq("suspf"), "correct-horse-battery")
	h.env.DB.Exec(`UPDATE users SET status='suspended', token_version = token_version + 1 WHERE id = ?`, victim.User.ID)
	if err := h.env.RDB.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := h.call("GET", "/api/v1/me", nil, withToken(victim.AccessToken)); rec.Code != 401 {
		t.Fatalf("suspended user's old token after flush: %d", rec.Code)
	}
}

// Registration answered 409 email_taken: an enumeration oracle (F-32). It now
// answers 202 with the same body either way.
func TestBug_Register_EnumeratesExistingEmails(t *testing.T) {
	h := newHarness(t)
	email := uniq("taken")
	h.mustRegister(email, "correct-horse-battery")
	rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": email, "password": "correct-horse-battery", "full_name": "Dup"}, withIP("203.0.113.90"))
	if rec.Code == 409 || strings.Contains(rec.Body.String(), "email_taken") {
		t.Fatalf("duplicate registration is distinguishable from a fresh one: %d %s", rec.Code, rec.Body.String())
	}
	fresh := h.call("POST", "/api/v1/auth/register", map[string]any{"email": uniq("fresh"), "password": "correct-horse-battery", "full_name": "New"}, withIP("203.0.113.91"))
	if fresh.Code != rec.Code || fresh.Body.String() != rec.Body.String() {
		t.Fatalf("new vs existing differ: %d %s vs %d %s", fresh.Code, fresh.Body.String(), rec.Code, rec.Body.String())
	}
	// The owner of the existing address is told via a password-reset mail.
	if h.mail.resetToken(email) == "" {
		t.Fatal("expected the existing account's owner to receive a password-reset email")
	}
	// No second user row.
	var n int64
	h.env.DB.Raw(`SELECT count(*) FROM users WHERE email = ?`, email).Row().Scan(&n)
	if n != 1 {
		t.Fatalf("users with that email: %d", n)
	}
}

// A suspended account must answer exactly like a wrong password (F-32).
func TestBug_Login_SuspendedAccountRevealsPasswordCorrectness(t *testing.T) {
	h := newHarness(t)
	email := uniq("susp2")
	reg := h.mustRegister(email, "correct-horse-battery")
	h.env.DB.Exec(`UPDATE users SET status='suspended' WHERE id = ?`, reg.User.ID)
	right := h.login(email, "correct-horse-battery", withIP("203.0.113.91"))
	wrong := h.login(email, "wrong-password-xx", withIP("203.0.113.92"))
	if right.Code != wrong.Code || right.Body.String() != wrong.Body.String() {
		t.Fatalf("suspended account: right pw -> %d %s ; wrong pw -> %d %s", right.Code, right.Body.String(), wrong.Code, wrong.Body.String())
	}
}

// A role change for an id that is not a user must have no side effects (F-33).
func TestBug_AdminRoleChange_NonexistentUserLeavesOutboxAndRedisResidue(t *testing.T) {
	h := newHarness(t)
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	ghost := uuid.NewString()
	rec := h.call("PATCH", "/api/v1/admin/users/"+ghost+"/role", map[string]any{"role": "admin", "status": "active"}, withToken(adm))
	var events int64
	h.env.DB.Raw(`SELECT count(*) FROM outbox WHERE payload->>'user_id' = ?`, ghost).Row().Scan(&events)
	redisKeys, _ := h.env.RDB.Exists(t.Context(), "user:tv:"+ghost).Result()
	t.Logf("status=%d outbox_events=%d redis_key_exists=%d", rec.Code, events, redisKeys)
	if rec.Code != 404 {
		t.Fatalf("want 404, got %d", rec.Code)
	}
	if events != 0 || redisKeys != 0 {
		t.Fatalf("role change for a nonexistent user wrote %d outbox event(s) and %d redis key(s)", events, redisKeys)
	}
}

// The last active admin cannot be demoted, suspended or deleted (F-33).
func TestBug_AdminCanSelfDemote_NoLastAdminGuard(t *testing.T) {
	h := newHarness(t)
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	rec := h.call("PATCH", "/api/v1/admin/users/"+admin.User.ID+"/role", map[string]any{"role": "student", "status": "deleted"}, withToken(adm))
	if rec.Code == 200 {
		t.Fatalf("the only admin deleted themself via the API (%d): platform is now unadministrable", rec.Code)
	}
	if rec.Code != 409 || errCode(t, rec) != "last_admin" {
		t.Fatalf("want 409 last_admin, got %d %s", rec.Code, rec.Body.String())
	}
	// Another active admin makes the same change legitimate.
	other := h.mustRegister(uniq("adm2"), "correct-horse-battery")
	h.setRoleSQL(other.User.ID, "admin")
	if rec := h.call("PATCH", "/api/v1/admin/users/"+admin.User.ID+"/role", map[string]any{"role": "student", "status": "active"}, withToken(adm)); rec.Code != 200 {
		t.Fatalf("demotion with a second admin present: %d %s", rec.Code, rec.Body.String())
	}
}

// The revocation state lived only in Redis and a missing key meant version 1,
// so a Redis flush resurrected revoked admin tokens (F-03). A miss now reads
// users.token_version from Postgres and repopulates Redis.
func TestBug_RedisFlush_ResurrectsRevokedAdminToken(t *testing.T) {
	h := newHarness(t)
	victim := h.mustRegister(uniq("adm1"), "correct-horse-battery")
	other := h.mustRegister(uniq("adm2"), "correct-horse-battery")
	h.setRoleSQL(victim.User.ID, "admin")
	h.setRoleSQL(other.User.ID, "admin")
	victimOld := mustAccess(t, h, uuid.MustParse(victim.User.ID), "admin", 1)
	otherTok := mustAccess(t, h, uuid.MustParse(other.User.ID), "admin", 1)

	// other demotes victim: token_version 1 -> 2 in Postgres and Redis.
	if rec := h.call("PATCH", "/api/v1/admin/users/"+victim.User.ID+"/role", map[string]any{"role": "student", "status": "active"}, withToken(otherTok)); rec.Code != 200 {
		t.Fatalf("demote: %d %s", rec.Code, rec.Body.String())
	}
	probe := func(tok string) int {
		return h.call("PATCH", "/api/v1/admin/users/"+other.User.ID+"/role", map[string]any{"role": "admin", "status": "active"}, withToken(tok)).Code
	}
	if c := probe(victimOld); c != 401 {
		t.Fatalf("precondition: the revoked token must be rejected before the flush, got %d", c)
	}

	if err := h.env.RDB.FlushAll(context.Background()).Err(); err != nil { // what a Redis restart without persistence does
		t.Fatal(err)
	}
	if c := probe(victimOld); c == 200 {
		t.Fatalf("after a Redis flush the demoted admin's OLD token works again on an admin route (%d)", c)
	}
	// An admin whose version was never bumped still works after the flush
	// (version read from Postgres), and the victim key is repopulated.
	if c := probe(otherTok); c != 200 {
		t.Fatalf("a current admin token after a flush: %d", c)
	}
	if v, err := h.env.RDB.Get(context.Background(), "user:tv:"+victim.User.ID).Int(); err != nil || v != 2 {
		t.Fatalf("victim's version should be repopulated as 2: v=%d err=%v", v, err)
	}
}

// Reset/resend routes had no rate limit: unlimited e-mail bombing and many
// simultaneously valid tokens (F-15).
func TestBug_PasswordResetAndResend_NoRateLimit_EmailBombing(t *testing.T) {
	h := newHarness(t)
	email := uniq("bomb")
	h.mustRegister(email, "correct-horse-battery")
	var limited, accepted int
	for i := 0; i < 40; i++ {
		for _, p := range []string{"/api/v1/auth/password-reset/request", "/api/v1/auth/verify-email/resend"} {
			if h.call("POST", p, map[string]any{"email": email}, withIP("203.0.113.99")).Code == 429 {
				limited++
			} else {
				accepted++
			}
		}
	}
	var active int64
	h.env.DB.Raw(`SELECT count(*) FROM password_reset_tokens WHERE used_at IS NULL AND expires_at > now()`).Row().Scan(&active)
	t.Logf("accepted=%d limited=%d simultaneously valid reset tokens=%d", accepted, limited, active)
	if limited == 0 {
		t.Fatalf("80 reset/resend requests for one address from one IP were all accepted (%d valid reset tokens now exist for the victim)", active)
	}
	if active > 1 {
		t.Fatalf("%d simultaneously valid reset tokens; earlier ones must be invalidated", active)
	}
}

// Per-address limit holds even when the requests come from many IPs, and is
// silent (still 202) so it is not an existence oracle (F-15).
func TestFix_PasswordReset_PerAddressBudget_AcrossIPs(t *testing.T) {
	h := newHarness(t)
	email := uniq("peraddr")
	h.mustRegister(email, "correct-horse-battery")
	sent := 0
	for i := 0; i < 12; i++ {
		before := h.mail.resetToken(email)
		rec := h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": email}, withIP(fmt.Sprintf("203.0.113.%d", 150+i)))
		if rec.Code != 202 {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
		if h.mail.resetToken(email) != before {
			sent++
		}
	}
	if sent == 0 || sent > 5 {
		t.Fatalf("expected between 1 and 5 emails for one address, got %d", sent)
	}
}

// Only the newest reset token works after a second request (F-15).
func TestFix_PasswordReset_NewRequestInvalidatesOlderToken(t *testing.T) {
	h := newHarness(t)
	email := uniq("older")
	h.mustRegister(email, "correct-horse-battery")
	h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": email})
	first := h.mail.resetToken(email)
	h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": email})
	second := h.mail.resetToken(email)
	if first == second {
		t.Fatal("expected a second token")
	}
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": first, "new_password": "first-password-123"}); rec.Code != 401 {
		t.Fatalf("superseded reset token accepted: %d", rec.Code)
	}
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": second, "new_password": "second-password-123"}); rec.Code != 204 {
		t.Fatalf("newest reset token rejected: %d", rec.Code)
	}
}

// Likewise for verification tokens (F-15).
func TestFix_EmailVerification_ResendInvalidatesOlderToken(t *testing.T) {
	h := newHarness(t)
	email := uniq("vold")
	h.mustRegister(email, "correct-horse-battery")
	first := h.mail.verifyToken(email)
	h.call("POST", "/api/v1/auth/verify-email/resend", map[string]any{"email": email})
	second := h.mail.verifyToken(email)
	if first == second {
		t.Fatal("expected a second token")
	}
	if rec := h.call("GET", "/api/v1/auth/verify-email/confirm?token="+first, nil); rec.Code != 401 {
		t.Fatalf("superseded verification token accepted: %d", rec.Code)
	}
	if rec := h.call("GET", "/api/v1/auth/verify-email/confirm?token="+second, nil); rec.Code != 204 {
		t.Fatalf("newest verification token rejected: %d", rec.Code)
	}
}

// Refresh, logout and the two confirm routes are rate limited per IP (F-15).
func TestFix_RefreshLogoutConfirm_PerIPLimits(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		name, method, path string
		body               any
		burst              int
	}{
		{"refresh", "POST", "/api/v1/auth/refresh", map[string]any{"refresh_token": "nope"}, 60},
		{"logout", "POST", "/api/v1/auth/logout", map[string]any{"refresh_token": "nope"}, 60},
		{"reset-confirm", "POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": "nope", "new_password": "some-password-1"}, 30},
		{"verify-confirm", "GET", "/api/v1/auth/verify-email/confirm?token=nope", nil, 30},
	} {
		limited := false
		for i := 0; i < tc.burst+5 && !limited; i++ {
			limited = h.call(tc.method, tc.path, tc.body, withIP("203.0.113.180")).Code == 429
		}
		if !limited {
			t.Errorf("%s: no 429 after %d requests from one IP", tc.name, tc.burst+5)
		}
	}
}
