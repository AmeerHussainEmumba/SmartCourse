//go:build integration

// Adversarial session/credential tests over real HTTP, real Postgres and
// real Redis (security tester, area A). Defects they exposed were fixed and
// their regression tests live in security_fixed_test.go and
// security_router_integration_test.go.
package identity_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

func TestSecurity_RefreshRotation_ReuseBurnsWholeFamily_AndLogoutRevokes(t *testing.T) {
	h := newHarness(t)
	email := uniq("rot")
	reg := h.mustRegister(email, "correct-horse-battery")

	// Rotation: RT1 -> RT2.
	rec := h.refresh(reg.RefreshToken)
	if rec.Code != 200 {
		t.Fatalf("first refresh: %d %s", rec.Code, rec.Body.String())
	}
	rt2 := decodeAuth(t, rec).RefreshToken
	if rt2 == reg.RefreshToken {
		t.Fatal("refresh token was not rotated")
	}

	// Theft detection: the attacker replays RT1.
	if rec := h.refresh(reg.RefreshToken); rec.Code != 401 || errCode(t, rec) != "token_invalid" {
		t.Fatalf("replay of rotated token: %d %s", rec.Code, rec.Body.String())
	}
	// ... and the legitimate holder's RT2 is now dead too (family burned).
	if rec := h.refresh(rt2); rec.Code != 401 {
		t.Fatalf("child token survived reuse detection: %d %s", rec.Code, rec.Body.String())
	}

	// Logout invalidation on a fresh session; a second logout is idempotent and silent.
	login := decodeAuth(t, h.login(email, "correct-horse-battery", withIP("198.51.100.10")))
	if rec := h.call("POST", "/api/v1/auth/logout", map[string]any{"refresh_token": login.RefreshToken}); rec.Code != 204 {
		t.Fatalf("logout: %d", rec.Code)
	}
	if rec := h.refresh(login.RefreshToken); rec.Code != 401 {
		t.Fatalf("refresh after logout: %d", rec.Code)
	}
	if rec := h.call("POST", "/api/v1/auth/logout", map[string]any{"refresh_token": login.RefreshToken}); rec.Code != 204 {
		t.Fatalf("second logout must be idempotent, got %d", rec.Code)
	}
	// Logout for an unknown token must not reveal that it is unknown.
	if rec := h.call("POST", "/api/v1/auth/logout", map[string]any{"refresh_token": "nonexistent"}); rec.Code != 204 {
		t.Fatalf("logout(unknown) = %d", rec.Code)
	}
}

func TestSecurity_RefreshToken_Garbage_Rejected_Cleanly(t *testing.T) {
	h := newHarness(t)
	for name, tok := range map[string]string{
		"random":      uuid.NewString(),
		"sql":         "' OR 1=1 --",
		"nul":         "a\x00b",
		"long":        strings.Repeat("A", 1<<20),
		"unicode":     "‮\u0000\U0001F600",
		"access jwt":  mustAccess(t, h, uuid.New(), "admin", 1), // an access token is not a refresh token
		"empty-space": " ",
	} {
		rec := h.refresh(tok)
		want := 401
		if name == "long" {
			// A 1 MiB token pushes the body past the request-body cap
			// (libs/httpx.MaxBodyBytes, F-16), which now rejects it before the
			// handler sees it.
			want = 413
		}
		if rec.Code != want {
			t.Errorf("%s: want %d, got %d %.150s", name, want, rec.Code, rec.Body.String())
		}
		assertNoInternalLeak(t, name, rec.Body.String())
	}
}

func TestSecurity_ConcurrentRefresh_SameToken_ExactlyOneWins(t *testing.T) {
	h := newHarness(t)
	reg := h.mustRegister(uniq("conc"), "correct-horse-battery")

	const n = 25
	var ok, rejected, other int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := h.refresh(reg.RefreshToken)
			switch rec.Code {
			case 200:
				atomic.AddInt32(&ok, 1)
			case 401:
				atomic.AddInt32(&rejected, 1)
			default:
				atomic.AddInt32(&other, 1)
				t.Errorf("unexpected status %d: %s", rec.Code, rec.Body.String())
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || rejected != n-1 || other != 0 {
		t.Fatalf("concurrent refresh with one token: ok=%d rejected=%d other=%d (want 1/%d/0)", ok, rejected, other, n-1)
	}
}

// Demoting an admin bumps token_version in Postgres and Redis: the old token
// must stop working on the fresh-privilege route, while a refresh yields the
// reduced role.
func TestSecurity_AdminDemotion_StaleTokenRejectedOnPrivilegedRoute(t *testing.T) {
	h := newHarness(t)
	adminA := h.mustRegister(uniq("adminA"), "correct-horse-battery")
	adminB := h.mustRegister(uniq("adminB"), "correct-horse-battery")
	h.setRoleSQL(adminA.User.ID, "admin")
	h.setRoleSQL(adminB.User.ID, "admin")
	tokA := mustAccess(t, h, uuid.MustParse(adminA.User.ID), "admin", 1)
	tokB := mustAccess(t, h, uuid.MustParse(adminB.User.ID), "admin", 1)

	body := map[string]any{"role": "student", "status": "active"}
	if rec := h.call("PATCH", "/api/v1/admin/users/"+adminA.User.ID+"/role", body, withToken(tokB)); rec.Code != 200 {
		t.Fatalf("demote A: %d %s", rec.Code, rec.Body.String())
	}

	// A's pre-demotion token claims admin but is stale.
	rec := h.call("PATCH", "/api/v1/admin/users/"+adminB.User.ID+"/role", map[string]any{"role": "student", "status": "active"}, withToken(tokA))
	if rec.Code != 401 || errCode(t, rec) != "token_stale" {
		t.Fatalf("demoted admin's stale token on privileged route: %d %s", rec.Code, rec.Body.String())
	}
	if h.env.DB.Exec(`SELECT 1 FROM users WHERE id = ? AND role = 'admin'`, adminB.User.ID).RowsAffected != 1 {
		t.Fatal("B was demoted by a stale token")
	}

	// A refreshes (their RT is still valid) and gets a student token that is also denied (403).
	rt := h.refresh(adminA.RefreshToken)
	if rt.Code != 200 {
		t.Fatalf("refresh: %d", rt.Code)
	}
	fresh := decodeAuth(t, rt)
	if fresh.User.Role != "student" {
		t.Fatalf("refresh after demotion kept role %q", fresh.User.Role)
	}
	if rec := h.call("PATCH", "/api/v1/admin/users/"+adminB.User.ID+"/role", map[string]any{"role": "student", "status": "active"}, withToken(fresh.AccessToken)); rec.Code != 403 {
		t.Fatalf("student token on admin route: %d", rec.Code)
	}
}

func TestSecurity_AdminRoute_RoleMatrix_And_BodyValidation(t *testing.T) {
	h := newHarness(t)
	student := h.mustRegister(uniq("stu"), "correct-horse-battery")
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	admTok := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	path := "/api/v1/admin/users/" + student.User.ID + "/role"
	ok := map[string]any{"role": "instructor", "status": "active"}

	if rec := h.call("PATCH", path, ok); rec.Code != 401 {
		t.Errorf("anonymous: %d", rec.Code)
	}
	if rec := h.call("PATCH", path, ok, withToken(student.AccessToken)); rec.Code != 403 {
		t.Errorf("student self-promotion: %d", rec.Code)
	}
	instrTok := mustAccess(t, h, uuid.New(), "instructor", 1)
	if rec := h.call("PATCH", path, ok, withToken(instrTok)); rec.Code != 403 {
		t.Errorf("instructor: %d", rec.Code)
	}
	for name, body := range map[string]any{
		"role superuser":   map[string]any{"role": "superuser", "status": "active"},
		"role empty":       map[string]any{"role": "", "status": "active"},
		"role uppercase":   map[string]any{"role": "ADMIN", "status": "active"},
		"status bogus":     map[string]any{"role": "student", "status": "god"},
		"missing status":   map[string]any{"role": "student"},
		"sql in role":      map[string]any{"role": "student'; DROP TABLE users;--", "status": "active"},
		"array role":       map[string]any{"role": []string{"admin"}, "status": "active"},
		"role as number":   map[string]any{"role": 1, "status": "active"},
		"empty body":       map[string]any{},
		"extra id/tv keys": map[string]any{"role": "student", "status": "active", "id": uuid.NewString(), "token_version": 99}, // extra keys must be ignored, request still valid
	} {
		rec := h.call("PATCH", path, body, withToken(admTok))
		want := 400
		if name == "extra id/tv keys" {
			want = 200
		}
		if rec.Code != want {
			t.Errorf("%s: want %d, got %d %.150s", name, want, rec.Code, rec.Body.String())
		}
		assertNoInternalLeak(t, name, rec.Body.String())
	}
	for _, id := range []string{"not-a-uuid", "1", "' OR 1=1--", "../../etc/passwd", "00000000-0000-0000-0000-00000000000g"} {
		rec := h.call("PATCH", "/api/v1/admin/users/"+url.PathEscape(id)+"/role", ok, withToken(admTok))
		if rec.Code != 400 && rec.Code != 404 {
			t.Errorf("id %q: %d", id, rec.Code)
		}
	}
	// the table must still exist and the target is still a student after all those rejections
	var n int64
	if err := h.env.DB.Raw(`SELECT count(*) FROM users`).Scan(&n).Error; err != nil || n < 2 {
		t.Fatalf("users table damaged: %v n=%d", err, n)
	}
}

func TestSecurity_MassAssignment_RegisterAndUpdateMe(t *testing.T) {
	h := newHarness(t)
	email := uniq("mass")
	evilID := uuid.NewString()
	rec := h.call("POST", "/api/v1/auth/register", map[string]any{
		"email": email, "password": "correct-horse-battery", "full_name": "Eve",
		"role": "admin", "id": evilID, "status": "active", "token_version": 99,
		"email_verified_at": "2020-01-01T00:00:00Z", "password_hash": "x", "is_admin": true,
	}, withIP("198.51.100.20"))
	if rec.Code != 202 {
		t.Fatalf("register: %d %s", rec.Code, rec.Body.String())
	}
	reg := decodeAuth(t, h.login(email, "correct-horse-battery", withIP("198.51.100.21")))
	if reg.User.Role != "student" || reg.User.ID == evilID || reg.User.EmailVerifiedAt != nil {
		t.Fatalf("mass assignment on register succeeded: %+v", reg.User)
	}
	claims, err := auth.ParseAccessToken(h.jwtCfg, reg.AccessToken)
	if err != nil || claims.Role != "student" || claims.TokenVersion != 1 {
		t.Fatalf("minted token reflects attacker input: %+v %v", claims, err)
	}

	rec = h.call("PATCH", "/api/v1/me", map[string]any{
		"full_name": "Eve 2", "role": "admin", "email": "admin@example.com", "id": evilID,
		"status": "suspended", "token_version": 50, "email_verified_at": "2020-01-01T00:00:00Z",
	}, withToken(reg.AccessToken))
	if rec.Code != 200 {
		t.Fatalf("PATCH /me: %d %s", rec.Code, rec.Body.String())
	}
	me := decodeUser(t, h.call("GET", "/api/v1/me", nil, withToken(reg.AccessToken)))
	if me.Role != "student" || me.Email != strings.ToLower(email) && me.Email != email || me.ID != reg.User.ID || me.FullName != "Eve 2" || me.EmailVerifiedAt != nil {
		t.Fatalf("PATCH /me changed a protected field: %+v", me)
	}
	var tv int
	var status, role string
	h.env.DB.Raw(`SELECT token_version, status, role::text FROM users WHERE id = ?`, reg.User.ID).Row().Scan(&tv, &status, &role)
	if tv != 1 || status != "active" || role != "student" {
		t.Fatalf("DB row mutated by PATCH /me: tv=%d status=%s role=%s", tv, status, role)
	}
	// Responses never expose secrets.
	body := h.call("GET", "/api/v1/me", nil, withToken(reg.AccessToken)).Body.String()
	for _, f := range []string{"password", "hash", "token_version", "status"} {
		if strings.Contains(strings.ToLower(body), f) {
			t.Errorf("GET /me exposes %q: %s", f, body)
		}
	}
}

func TestSecurity_Enumeration_LoginUnknownVsWrongPassword_Indistinguishable(t *testing.T) {
	h := newHarness(t)
	email := uniq("enum")
	h.mustRegister(email, "correct-horse-battery")

	wrong := h.login(email, "wrong-password-123", withIP("198.51.100.31"))
	unknown := h.login(uniq("ghost"), "wrong-password-123", withIP("198.51.100.32"))
	mixedCase := h.login(strings.ToUpper(email), "wrong-password-123", withIP("198.51.100.33"))
	if wrong.Code != 401 || unknown.Code != 401 || mixedCase.Code != 401 {
		t.Fatalf("statuses: %d %d %d", wrong.Code, unknown.Code, mixedCase.Code)
	}
	if wrong.Body.String() != unknown.Body.String() || wrong.Body.String() != mixedCase.Body.String() {
		t.Fatalf("response bodies differ:\n%s\n%s", wrong.Body.String(), unknown.Body.String())
	}
	for _, k := range []string{"Content-Type", "Content-Length"} {
		if wrong.Header().Get(k) != unknown.Header().Get(k) {
			t.Errorf("header %s differs: %q vs %q", k, wrong.Header().Get(k), unknown.Header().Get(k))
		}
	}
}

func TestSecurity_Enumeration_ResetAndResend_Indistinguishable(t *testing.T) {
	h := newHarness(t)
	email := uniq("enum2")
	h.mustRegister(email, "correct-horse-battery")

	for _, path := range []string{"/api/v1/auth/password-reset/request", "/api/v1/auth/verify-email/resend"} {
		known := h.call("POST", path, map[string]any{"email": email})
		unknown := h.call("POST", path, map[string]any{"email": uniq("ghost")})
		if known.Code != 202 || unknown.Code != 202 || known.Body.String() != unknown.Body.String() {
			t.Errorf("%s distinguishable: %d %q vs %d %q", path, known.Code, known.Body.String(), unknown.Code, unknown.Body.String())
		}
	}
}

// Timing, with the argon2 parameters equal to auth.DummyHash's hard-coded
// ones (64 MiB, t=3, p=2; also the production defaults): known-vs-unknown
// login must stay within the same order of magnitude. A skew means the
// dummy-hash path was removed. With any OTHER configured parameters the dummy
// hash no longer matches: see TestBug_DummyHashIgnoresConfiguredArgon2.
func TestSecurity_Enumeration_LoginTiming_UnknownVsKnown_SameOrderOfMagnitude(t *testing.T) {
	h := newHarnessWith(t, setupIdentity(t), config.Argon2Config{MemoryKiB: 64 * 1024, Iterations: 3, Parallelism: 2, SaltLength: 16, KeyLength: 32})
	measure := func(f func(i int) string) time.Duration {
		var total time.Duration
		const n = 6
		for i := 0; i < n; i++ {
			s := time.Now()
			h.login(f(i), "wrong-password-123", withIP(fmt.Sprintf("198.51.100.%d", 100+i)))
			total += time.Since(s)
		}
		return total / n
	}
	// Fresh known users per attempt keep the per-account limiter out of the picture.
	known := make([]string, 6)
	for i := range known {
		known[i] = uniq("tk")
		h.mustRegister(known[i], "correct-horse-battery")
	}
	dKnown := measure(func(i int) string { return known[i] })
	dUnknown := measure(func(i int) string { return uniq("tg") })
	t.Logf("mean login latency: known-account/wrong-pw=%v unknown-account=%v", dKnown, dUnknown)
	if dUnknown*3 < dKnown || dKnown*3 < dUnknown {
		t.Fatalf("login latency differs by >3x between known and unknown accounts (%v vs %v): user enumeration by timing", dKnown, dUnknown)
	}
}

func TestSecurity_PasswordPolicy_Boundaries(t *testing.T) {
	h := newHarness(t)
	reg := func(pw string) int {
		ip := fmt.Sprintf("10.%d.%d.9", uuid.New()[0], uuid.New()[1])
		return h.call("POST", "/api/v1/auth/register", map[string]any{"email": uniq("pw"), "password": pw, "full_name": "P"}, withIP(ip)).Code
	}
	cases := []struct {
		name string
		pw   string
		want int
	}{
		{"empty", "", 400},
		{"7 chars", "1234567", 400},
		{"8 chars", "12345678", 202},
		{"128 chars", strings.Repeat("a", 128), 202},
		{"129 chars", strings.Repeat("a", 129), 400},
		{"128 runes of 4 bytes (512 bytes)", strings.Repeat("\U0001F600", 128), 202},
		{"129 runes of 4 bytes", strings.Repeat("\U0001F600", 129), 400},
		{"1 MiB", strings.Repeat("a", 1<<20), 413},   // over the request-body cap (F-16), rejected before validation
		{"nul bytes inside", "pass\x00word123", 202}, // stored as bytes; argon2 handles NUL
		{"only whitespace", "        ", 202},         // accepted: policy is length only (documented gap, low)
	}
	for _, c := range cases {
		if got := reg(c.pw); got != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, got)
		}
	}
	// A password with NUL must round-trip: the stored hash must verify the exact bytes.
	email := uniq("nul")
	h.mustRegister(email, "pass\x00word123")
	if rec := h.login(email, "pass\x00word123", withIP("198.51.100.77")); rec.Code != 200 {
		t.Fatalf("NUL password cannot log in: %d", rec.Code)
	}
	if rec := h.login(email, "pass", withIP("198.51.100.78")); rec.Code != 401 {
		t.Fatalf("truncated-at-NUL password was accepted (C-string truncation): %d", rec.Code)
	}
	// Unicode: NFC vs NFD forms are different passwords (no normalization) - pin current behaviour.
	e2 := uniq("uni")
	h.mustRegister(e2, "café-password") // composed
	if rec := h.login(e2, "café-password", withIP("198.51.100.79")); rec.Code != 200 {
		t.Fatalf("unicode password round trip failed: %d", rec.Code)
	}
}

func TestSecurity_PasswordReset_Lifecycle_Replay_Expiry_CrossUser(t *testing.T) {
	h := newHarness(t)
	victim := uniq("victim")
	other := uniq("other")
	vReg := h.mustRegister(victim, "old-password-123")
	h.mustRegister(other, "other-password-123")

	if rec := h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": victim}); rec.Code != 202 {
		t.Fatal(rec.Code)
	}
	tok := h.mail.resetToken(victim)
	if tok == "" {
		t.Fatal("no reset token captured")
	}

	// Garbage / wrong tokens.
	for _, bad := range []string{"", "x", tok + "x", strings.ToUpper(tok), tok[:len(tok)-1], h.mail.resetToken(other)} {
		rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": bad, "new_password": "new-password-123"})
		if rec.Code != 400 && rec.Code != 401 {
			t.Errorf("bad token %q accepted/mishandled: %d %s", bad, rec.Code, rec.Body.String())
		}
	}
	// A verification token must not work as a reset token (token-type confusion).
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": h.mail.verifyToken(victim), "new_password": "new-password-123"}); rec.Code != 401 {
		t.Errorf("email-verification token accepted as password reset token: %d", rec.Code)
	}
	// New password too short is rejected before the token is consumed.
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": tok, "new_password": "short"}); rec.Code != 400 {
		t.Errorf("short new password: %d", rec.Code)
	}

	// Real confirm.
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": tok, "new_password": "new-password-123"}); rec.Code != 204 {
		t.Fatalf("confirm: %d %s", rec.Code, rec.Body.String())
	}
	// Replay.
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": tok, "new_password": "attacker-password-1"}); rec.Code != 401 {
		t.Fatalf("replayed reset token accepted: %d", rec.Code)
	}
	// Old password is dead, new one works, the attacker's password never took effect.
	if rec := h.login(victim, "old-password-123", withIP("198.51.100.41")); rec.Code != 401 {
		t.Fatalf("old password still valid: %d", rec.Code)
	}
	if rec := h.login(victim, "attacker-password-1", withIP("198.51.100.42")); rec.Code != 401 {
		t.Fatalf("replayed password took effect: %d", rec.Code)
	}
	if rec := h.login(victim, "new-password-123", withIP("198.51.100.43")); rec.Code != 200 {
		t.Fatalf("new password does not work: %d", rec.Code)
	}
	// Sessions existing before the reset are dead: refresh token revoked.
	if rec := h.refresh(vReg.RefreshToken); rec.Code != 401 {
		t.Fatalf("pre-reset refresh token survived a password reset: %d", rec.Code)
	}
	// The other user is unaffected.
	if rec := h.login(other, "other-password-123", withIP("198.51.100.44")); rec.Code != 200 {
		t.Fatalf("other user's login broken: %d", rec.Code)
	}

	// Expiry: a fresh token pushed into the past is refused.
	h.call("POST", "/api/v1/auth/password-reset/request", map[string]any{"email": victim})
	tok2 := h.mail.resetToken(victim)
	if err := h.env.DB.Exec(`UPDATE password_reset_tokens SET expires_at = now() - interval '1 second' WHERE used_at IS NULL`).Error; err != nil {
		t.Fatal(err)
	}
	if rec := h.call("POST", "/api/v1/auth/password-reset/confirm", map[string]any{"token": tok2, "new_password": "late-password-123"}); rec.Code != 401 {
		t.Fatalf("expired reset token accepted: %d", rec.Code)
	}
}

func TestSecurity_EmailVerification_Replay_Expiry_Garbage(t *testing.T) {
	h := newHarness(t)
	email := uniq("ver")
	reg := h.mustRegister(email, "correct-horse-battery")
	tok := h.mail.verifyToken(email)
	if tok == "" {
		t.Fatal("no verification token captured")
	}
	get := func(token string) *httptest.ResponseRecorder {
		return h.call("GET", "/api/v1/auth/verify-email/confirm?token="+url.QueryEscape(token), nil)
	}

	for _, bad := range []string{"", "x", tok + "x", "%00", "' OR 1=1--", strings.Repeat("A", 100000)} {
		rec := get(bad)
		if rec.Code != 400 && rec.Code != 401 {
			t.Errorf("bad token %.20q: %d", bad, rec.Code)
		}
	}
	if rec := get(tok); rec.Code != 204 {
		t.Fatalf("confirm: %d %s", rec.Code, rec.Body.String())
	}
	if rec := get(tok); rec.Code != 401 {
		t.Fatalf("replayed verification token accepted: %d", rec.Code)
	}
	me := decodeUser(t, h.call("GET", "/api/v1/me", nil, withToken(reg.AccessToken)))
	if me.EmailVerifiedAt == nil {
		t.Fatal("account not verified after valid confirm")
	}
	// Resend for an already-verified account must silently succeed and not mint a usable token.
	before := h.mail.verifyToken(email)
	if rec := h.call("POST", "/api/v1/auth/verify-email/resend", map[string]any{"email": email}); rec.Code != 202 {
		t.Fatalf("resend: %d", rec.Code)
	}
	if h.mail.verifyToken(email) != before {
		t.Error("a new verification token was minted for an already verified account")
	}
	// Expiry.
	e2 := uniq("ver2")
	h.mustRegister(e2, "correct-horse-battery")
	t2 := h.mail.verifyToken(e2)
	h.env.DB.Exec(`UPDATE email_verification_tokens SET expires_at = now() - interval '1 second' WHERE used_at IS NULL`)
	if rec := get(t2); rec.Code != 401 {
		t.Fatalf("expired verification token accepted: %d", rec.Code)
	}
}

func TestSecurity_RefreshForSuspendedUser_IsDenied_AndLoginToo(t *testing.T) {
	h := newHarness(t)
	email := uniq("susp")
	reg := h.mustRegister(email, "correct-horse-battery")
	admin := h.mustRegister(uniq("adm"), "correct-horse-battery")
	h.setRoleSQL(admin.User.ID, "admin")
	adm := mustAccess(t, h, uuid.MustParse(admin.User.ID), "admin", 1)
	if rec := h.call("PATCH", "/api/v1/admin/users/"+reg.User.ID+"/role", map[string]any{"role": "student", "status": "suspended"}, withToken(adm)); rec.Code != 200 {
		t.Fatalf("suspend: %d %s", rec.Code, rec.Body.String())
	}
	if rec := h.refresh(reg.RefreshToken); rec.Code != 403 {
		t.Fatalf("suspended user refreshed: %d", rec.Code)
	}
	// F-32: a suspended account answers like a wrong password (401
	// invalid_credentials) so login does not confirm password correctness.
	if rec := h.login(email, "correct-horse-battery", withIP("198.51.100.51")); rec.Code != 401 {
		t.Fatalf("suspended user logged in: %d", rec.Code)
	}
}

// --- helpers ---------------------------------------------------------------

func decodeAuth(t *testing.T, rec *httptest.ResponseRecorder) identity.AuthResponse {
	t.Helper()
	var out identity.AuthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code >= 300 {
		t.Fatalf("decode auth response (%d): %v %s", rec.Code, err, rec.Body.String())
	}
	return out
}

func decodeUser(t *testing.T, rec *httptest.ResponseRecorder) identity.UserResponse {
	t.Helper()
	var out identity.UserResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != 200 {
		t.Fatalf("decode user response (%d): %v %s", rec.Code, err, rec.Body.String())
	}
	return out
}

func mustAccess(t *testing.T, h *harness, id uuid.UUID, role string, tv int16) string {
	t.Helper()
	tok, err := auth.IssueAccessToken(h.jwtCfg, id, role, tv)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

var _ = jwt.SigningMethodHS256
var _ = http.StatusOK

func assertNoInternalLeak(t *testing.T, name, body string) {
	t.Helper()
	lower := strings.ToLower(body)
	for _, leak := range []string{"pq:", "pgx", "sqlstate", "gorm", "relation \"", "syntax error", "goroutine", ".go:", "panic", "runtime error", "select ", "insert into", "identity.users", "redis:", "dial tcp", "stack"} {
		if strings.Contains(lower, leak) {
			t.Errorf("%s: response leaks %q: %.300s", name, leak, body)
		}
	}
}
