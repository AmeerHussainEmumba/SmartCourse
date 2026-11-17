// Adversarial JWT tests (security tester, area A). Every case forges or
// mangles a token the way an outside attacker would and asserts RequireAuth
// answers 401 with the clean error body, never 200 and never a panic. No
// database: pure gin + httptest, so this file runs in the default unit pass.
package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func signWith(t *testing.T, method jwt.SigningMethod, key any, claims jwt.Claims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func goodClaims() Claims {
	cfg := testJWTCfg()
	now := time.Now()
	return Claims{
		Role:         "admin",
		TokenVersion: 1,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.NewString(),
			Issuer:    cfg.Issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
		},
	}
}

func TestRequireAuth_ForgedTokens_AllRejected(t *testing.T) {
	cfg := testJWTCfg()
	key := []byte(cfg.SigningKey)
	e := newEngine(RequireAuth(cfg))

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	valid := signWith(t, jwt.SigningMethodHS256, key, goodClaims())
	parts := strings.Split(valid, ".")

	// Re-signed-by-attacker claims with the payload swapped but the original signature kept.
	tamperedClaims := goodClaims()
	tamperedClaims.Role = "admin"
	other := signWith(t, jwt.SigningMethodHS256, key, tamperedClaims)
	otherParts := strings.Split(other, ".")
	swappedPayload := parts[0] + "." + otherParts[1] + "." + parts[2]

	expired := goodClaims()
	expired.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
	notYet := goodClaims()
	notYet.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
	wrongIss := goodClaims()
	wrongIss.Issuer = "evil-issuer"
	noIss := goodClaims()
	noIss.Issuer = ""
	noExp := goodClaims()
	noExp.ExpiresAt = nil
	noSub := goodClaims()
	noSub.Subject = ""
	badSub := goodClaims()
	badSub.Subject = "1' OR '1'='1"
	nilUUIDSub := goodClaims()
	nilUUIDSub.Subject = "not-a-uuid"

	cases := map[string]string{
		"alg none, empty sig":               b64(`{"alg":"none","typ":"JWT"}`) + "." + b64(`{"sub":"`+uuid.NewString()+`","role":"admin","iss":"smartcourse-test","exp":9999999999}`) + ".",
		"alg None capitalised":              b64(`{"alg":"None","typ":"JWT"}`) + "." + b64(`{"sub":"`+uuid.NewString()+`","role":"admin","iss":"smartcourse-test","exp":9999999999}`) + ".",
		"alg none with garbage sig":         b64(`{"alg":"none"}`) + "." + b64(`{"sub":"`+uuid.NewString()+`","role":"admin","iss":"smartcourse-test","exp":9999999999}`) + ".AAAA",
		"alg empty string":                  b64(`{"alg":"","typ":"JWT"}`) + "." + b64(`{"sub":"`+uuid.NewString()+`","role":"admin"}`) + ".AAAA",
		"hs256 signed with wrong key":       signWith(t, jwt.SigningMethodHS256, []byte("an-attacker-controlled-key-0123456789"), goodClaims()),
		"hs256 signed with the issuer name": signWith(t, jwt.SigningMethodHS256, []byte(cfg.Issuer), goodClaims()),
		"hs256 signed with empty key":       b64(`{"alg":"HS256","typ":"JWT"}`) + "." + b64(`{"sub":"`+uuid.NewString()+`","role":"admin","iss":"smartcourse-test","exp":9999999999}`) + ".",
		"rs256 token (alg confusion)":       signWith(t, jwt.SigningMethodRS256, rsaKey, goodClaims()),
		"es256 token":                       signWith(t, jwt.SigningMethodES256, ecKey, goodClaims()),
		"hs256 with RSA public key as hmac": signWith(t, jwt.SigningMethodHS256, []byte(string(rsaKey.N.Bytes())), goodClaims()),
		"payload swapped, sig kept":         swappedPayload,
		"expired":                           signWith(t, jwt.SigningMethodHS256, key, expired),
		"not yet valid (nbf)":               signWith(t, jwt.SigningMethodHS256, key, notYet),
		"wrong issuer":                      signWith(t, jwt.SigningMethodHS256, key, wrongIss),
		"missing issuer":                    signWith(t, jwt.SigningMethodHS256, key, noIss),
		"missing exp (never expires)":       signWith(t, jwt.SigningMethodHS256, key, noExp),
		"missing sub":                       signWith(t, jwt.SigningMethodHS256, key, noSub),
		"sql in sub":                        signWith(t, jwt.SigningMethodHS256, key, badSub),
		"non-uuid sub":                      signWith(t, jwt.SigningMethodHS256, key, nilUUIDSub),
		"two parts only":                    parts[0] + "." + parts[1],
		"four parts":                        valid + ".extra",
		"empty token":                       "",
		"just dots":                         "..",
		"bearer garbage":                    "not.a.jwt",
		"nul bytes":                         "a\x00b.c\x00d.e\x00f",
		"unicode":                           "\u202e.\u202e.\u202e",
		"huge token 1MiB":                   strings.Repeat("A", 1<<20) + "." + strings.Repeat("B", 16) + "." + strings.Repeat("C", 16),
		"huge header segment":               b64(strings.Repeat(`{"alg":"HS256","x":"`+strings.Repeat("y", 4096)+`"}`, 1)) + "." + parts[1] + "." + parts[2],
		"deeply nested payload":             parts[0] + "." + b64(strings.Repeat("[", 5000)+strings.Repeat("]", 5000)) + "." + parts[2],
		"alg header injected hs256 + kid":   b64(`{"alg":"HS256","typ":"JWT","kid":"../../../../dev/null"}`) + "." + parts[1] + "." + parts[2],
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			rec := doRequest(t, e, "Bearer "+tok)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("forged token accepted or mishandled: want 401, got %d: %.200s", rec.Code, rec.Body.String())
			}
			assertCleanErrorBody(t, rec.Body.Bytes())
		})
	}
}

// TestRequireAuth_AuthorizationHeaderShapes covers scheme/spacing tricks.
func TestRequireAuth_AuthorizationHeaderShapes(t *testing.T) {
	cfg := testJWTCfg()
	e := newEngine(RequireAuth(cfg))
	valid := signWith(t, jwt.SigningMethodHS256, []byte(cfg.SigningKey), goodClaims())

	rejected := map[string]string{
		"basic scheme":          "Basic " + valid,
		"no scheme":             valid,
		"bearer lowercase":      "bearer " + valid, // currently rejected; documents the strict prefix
		"double space":          "Bearer  " + valid,
		"trailing space":        "Bearer " + valid + " ",
		"leading space":         " Bearer " + valid,
		"token twice":           "Bearer " + valid + " " + valid,
		"bearer only":           "Bearer ",
		"bearer no space":       "Bearer",
		"newline in token":      "Bearer " + valid[:10] + "\n" + valid[10:],
		"comma separated multi": "Bearer " + valid + ", Bearer " + valid,
	}
	for name, h := range rejected {
		t.Run("reject/"+name, func(t *testing.T) {
			rec := doRequest(t, e, h)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("want 401, got %d", rec.Code)
			}
		})
	}

	if rec := doRequest(t, e, "Bearer "+valid); rec.Code != http.StatusOK {
		t.Fatalf("control: the genuine token must pass, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Other HMAC variants signed with the real key are legitimately ours; this
// pins that HS384/HS512 do not downgrade anything (still need the real key).
func TestParseAccessToken_HMACVariantsNeedRealKey(t *testing.T) {
	cfg := testJWTCfg()
	for _, m := range []jwt.SigningMethod{jwt.SigningMethodHS384, jwt.SigningMethodHS512} {
		bad := signWith(t, m, []byte("wrong-key-wrong-key-wrong-key-wrong"), goodClaims())
		if _, err := ParseAccessToken(cfg, bad); err == nil {
			t.Fatalf("%s with wrong key accepted", m.Alg())
		}
	}
}

// A token whose role claim is empty or odd must never satisfy RequireRole.
func TestRequireRole_OddRoleClaims_Denied(t *testing.T) {
	cfg := testJWTCfg()
	e := newEngine(RequireAuth(cfg), RequireRole("admin"))
	for _, role := range []string{"", "Admin", "ADMIN", "admin ", " admin", "admin\x00", "admin,student", "root", "instructor"} {
		c := goodClaims()
		c.Role = role
		tok := signWith(t, jwt.SigningMethodHS256, []byte(cfg.SigningKey), c)
		rec := doRequest(t, e, "Bearer "+tok)
		if rec.Code != http.StatusForbidden {
			t.Errorf("role %q: want 403, got %d", role, rec.Code)
		}
	}
}

// Claims carrying extra/duplicate "role" keys: encoding/json takes the last
// duplicate; the signature covers the payload so an attacker can't add one
// without the key. Sanity-check that a duplicate in a *validly signed* token
// resolves deterministically and cannot yield an unexpected 200 for a
// non-admin route.
func TestParseAccessToken_DuplicateRoleKey_LastWins(t *testing.T) {
	cfg := testJWTCfg()
	header := b64(`{"alg":"HS256","typ":"JWT"}`)
	payload := `{"sub":"` + uuid.NewString() + `","iss":"smartcourse-test","exp":` + jsonInt(time.Now().Add(time.Minute).Unix()) + `,"role":"admin","role":"student","tv":1}`
	signing := header + "." + b64(payload)
	sig, err := jwt.SigningMethodHS256.Sign(signing, []byte(cfg.SigningKey))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := ParseAccessToken(cfg, signing+"."+base64.RawURLEncoding.EncodeToString(sig))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if claims.Role != "student" {
		t.Fatalf("duplicate role key resolved to %q; want deterministic last-wins 'student'", claims.Role)
	}
}

func jsonInt(n int64) string { b, _ := json.Marshal(n); return string(b) }

func assertCleanErrorBody(t *testing.T, body []byte) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("error body is not JSON: %q", body)
	}
	if m["code"] == nil || m["message"] == nil {
		t.Fatalf("error body missing code/message: %s", body)
	}
	lower := strings.ToLower(string(body))
	for _, leak := range []string{"jwt", "token is", "signature", "hmac", "golang", ".go:", "goroutine", "panic", "key"} {
		if strings.Contains(lower, leak) {
			t.Fatalf("error body leaks %q: %s", leak, body)
		}
	}
}

// FuzzParseAccessToken must never panic and must never accept a token that
// was not signed with the configured key.
func FuzzParseAccessToken(f *testing.F) {
	cfg := testJWTCfg()
	// Deterministic: fuzz workers are separate processes, so the "known good"
	// token must be byte-identical in each (fixed sub/jti/times, far-future exp).
	fixed := Claims{Role: "student", TokenVersion: 1, RegisteredClaims: jwt.RegisteredClaims{
		Subject: "0192f0aa-0000-7000-8000-000000000001", Issuer: cfg.Issuer, ID: "fixed-jti",
		IssuedAt: jwt.NewNumericDate(time.Unix(1700000000, 0)), ExpiresAt: jwt.NewNumericDate(time.Unix(4102444800, 0)),
	}}
	good, err := jwt.NewWithClaims(jwt.SigningMethodHS256, fixed).SignedString([]byte(cfg.SigningKey))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add("")
	f.Add("a.b.c")
	f.Add(b64(`{"alg":"none"}`) + "." + b64(`{}`) + ".")
	f.Add(good + "x")
	f.Fuzz(func(t *testing.T, raw string) {
		claims, err := ParseAccessToken(cfg, raw)
		if err == nil && raw != good {
			// The only other way to a nil error is a token validly signed with our
			// key; a fuzzer cannot forge an HMAC, so any success must be the seed
			// token (possibly with insignificant base64 padding differences, which
			// still decode to the same signed bytes).
			if claims == nil || claims.Issuer != cfg.Issuer {
				t.Fatalf("accepted unexpected token %q", raw)
			}
			gp := strings.Split(good, ".")
			rp := strings.Split(raw, ".")
			if len(rp) != 3 || rp[0] != gp[0] || rp[1] != gp[1] {
				t.Fatalf("accepted a token whose header/payload differ from the only validly signed seed: %q", raw)
			}
		}
	})
}
