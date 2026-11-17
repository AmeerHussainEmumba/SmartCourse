//go:build integration && bugrepro

// Reproducers for REAL findings in identity (and the libs it uses). They are
// excluded from the default suite; run with
//
//	GOWORK=off go test -tags='integration bugrepro' -run TestBug ./...
//
// Every test here is expected to FAIL against the current code and to pass
// once the named defect is fixed.
package identity_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// libs/router.New never sets trusted proxies, so c.ClientIP() honours any
// X-Forwarded-For / X-Real-IP. The per-IP login/register limit
// (ratelimit.Middleware) is bypassed by rotating the header.
func TestBug_RateLimit_PerIPLimitBypassedBySpoofedForwardedFor(t *testing.T) {
	h := newHarness(t)
	var limited int
	for i := 1; i <= 12; i++ {
		rec := h.call("POST", "/api/v1/auth/register",
			map[string]any{"email": uniq("xff"), "password": "correct-horse-battery", "full_name": "X"},
			withIP("203.0.113.80"), withHeader("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i)))
		if rec.Code == 429 {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("12 registrations in a burst from ONE TCP peer were never limited (limit is 5/min): attacker rotates X-Forwarded-For")
	}
}

// service.go documents the account limit as capping FAILED logins, but it
// counts every attempt, successes included: a legitimate user who signs in
// six times a minute (multiple devices, token loss, a script) locks themself
// out; and anyone can lock out any known email by spamming it.
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
}

func TestBug_RateLimit_AnyoneCanLockOutAnyKnownEmail(t *testing.T) {
	h := newHarness(t)
	victim := uniq("victim")
	h.mustRegister(victim, "correct-horse-battery")
	for i := 0; i < 5; i++ { // attacker burns the victim's budget
		h.login(victim, "attacker-guess", withIP(fmt.Sprintf("198.51.100.%d", 10+i)))
	}
	if rec := h.login(victim, "correct-horse-battery", withIP("192.0.2.77")); rec.Code != 200 {
		t.Fatalf("the real owner, with the right password, from their own IP, is locked out: %d (account-only keying; consider keying on account+IP, or only counting failures and requiring a captcha/step-up)", rec.Code)
	}
}

// auth.DummyHash is hard-wired to 64MiB/t=3/p=2 and ignores ARGON2_* config.
// With any other configured cost an unknown e-mail answers in a different
// time than a known one (here ~14x), so login leaks account existence.
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

// ConfirmPasswordReset checks used_at OUTSIDE the transaction and never locks
// the token row: concurrent confirmations with one token all succeed (each
// sets a password; the last writer wins), so a leaked token can race the
// victim and keep the account.
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

// Same TOCTOU on email verification (lower impact).
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

// Suspending/deleting a user bumps token_version, but only the admin routes
// check it. RequireAuth accepts the old access token on every other route for
// up to JWT_ACCESS_TTL (15 min), and a demoted instructor keeps instructor
// powers for the same window (see the course module's reproducer).
func TestBug_SuspendedUser_AccessTokenStillAccepted(t *testing.T) {
	h := newHarness(t)
	victim := h.mustRegister(uniq("susp"), "correct-horse-battery")
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	if rec := h.call("PATCH", "/api/v1/admin/users/"+victim.User.ID+"/role", map[string]any{"role": "student", "status": "deleted"}, withToken(adm)); rec.Code != 200 {
		t.Fatalf("setup: %d", rec.Code)
	}
	if rec := h.call("GET", "/api/v1/me", nil, withToken(victim.AccessToken)); rec.Code == 200 {
		t.Fatalf("a DELETED user's access token still works on /me (got %d)", rec.Code)
	}
}

// Registration answers 409 email_taken: anyone can test whether an address has
// an account (the login/reset endpoints were carefully made uniform, this one
// was not).
func TestBug_Register_EnumeratesExistingEmails(t *testing.T) {
	h := newHarness(t)
	email := uniq("taken")
	h.mustRegister(email, "correct-horse-battery")
	rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": email, "password": "correct-horse-battery", "full_name": "Dup"}, withIP("203.0.113.90"))
	if rec.Code == 409 || strings.Contains(rec.Body.String(), "email_taken") {
		t.Fatalf("duplicate registration is distinguishable from a fresh one: %d %s", rec.Code, rec.Body.String())
	}
}

// Login checks the password BEFORE the account status, so a suspended account
// answers 403 account_suspended only to someone holding the right password:
// confirms credential validity (e.g. for stuffed credentials) to an attacker
// even though the account is blocked.
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

// There is no request-body size limit anywhere (no MaxBytesReader, no
// http.Server.MaxBytes), so a client can stream arbitrarily large bodies at
// every endpoint (LoginRequest.Password has no max at all).
func TestBug_NoRequestBodyLimit_LoginAcceptsHugeBody(t *testing.T) {
	h := newHarness(t)
	huge := `{"email":"nobody@example.com","password":"` + strings.Repeat("A", 16<<20) + `"}`
	rec := h.rawCall("POST", "/api/v1/auth/login", []byte(huge), withIP("203.0.113.95"))
	if rec.Code != http.StatusRequestEntityTooLarge && rec.Code != http.StatusBadRequest {
		t.Fatalf("a 16 MiB login body was fully read and processed: status %d (want 413/400)", rec.Code)
	}
}

// The admin role endpoint writes the role/outbox event and a Redis key for
// ids that do not exist (no existence check, 0 rows updated is not an error),
// then answers 404 after the side effects committed (a Redis user:tv:<ghost> key at least; the outbox event only if the row UPDATE precondition allows).
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
	if events != 0 || redisKeys != 0 {
		t.Fatalf("role change for a nonexistent user wrote %d outbox event(s) and %d redis key(s) before answering 404 (side effects committed for a user that does not exist)", events, redisKeys)
	}
}

// Admin can demote/suspend themself or the last remaining admin: no guard.
func TestBug_AdminCanSelfDemote_NoLastAdminGuard(t *testing.T) {
	h := newHarness(t)
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	rec := h.call("PATCH", "/api/v1/admin/users/"+admin.User.ID+"/role", map[string]any{"role": "student", "status": "deleted"}, withToken(adm))
	if rec.Code == 200 {
		t.Fatalf("the only admin deleted themself via the API (%d): platform is now unadministrable", rec.Code)
	}
}

// The privilege-revocation state lives only in Redis, and RequireFreshPrivilege
// treats a MISSING key as token_version 1. After a Redis restart / flush /
// eviction (no persistence is configured in compose or testenv) every
// previously revoked token whose tv is 1 is valid again on the admin routes,
// while a freshly issued token for the same admin (tv=2) is rejected as
// "stale" until the key is rewritten. Fail-open on the one control that
// revokes privilege.
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
		t.Fatalf("after a Redis flush the demoted admin's OLD token works again on an admin route (%d): a missing key is treated as version 1; consult Postgres (source of truth) on a miss", c)
	}
}

// /auth/password-reset/request and /auth/verify-email/resend (and /refresh,
// /logout, /password-reset/confirm, /verify-email/confirm) have NO rate limit:
// only register and login are wrapped in limiter.Middleware. Anyone can make the
// system send unlimited reset/verification e-mails to a victim address and
// mint unlimited valid reset tokens (earlier tokens are not invalidated).
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
}

var _ sync.Mutex
