// Fuzzing of the identity request DTOs' JSON binding/validation (area D). No
// Docker. Nothing that binds may violate the documented constraints, and
// nothing may panic the binder.
package identity_test

import (
	"testing"
	"unicode/utf8"

	"github.com/gin-gonic/gin/binding"

	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

func FuzzBindIdentityDTOs(f *testing.F) {
	for _, s := range []string{
		`{"email":"a@b.co","password":"12345678","full_name":"N"}`,
		`{"email":"a@b.co","password":"x"}`,
		`{"refresh_token":"abc"}`,
		`{"token":"t","new_password":"12345678"}`,
		`{"full_name":"N","role":"admin"}`,
		`{}`, `null`, `[]`, `{"email":`, `{"email":"\u0000@x.co"}`, "\xef\xbb\xbf{}",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var reg identity.RegisterRequest
		if binding.JSON.BindBody(data, &reg) == nil {
			if n := utf8.RuneCountInString(reg.Password); n < 8 || n > 128 {
				t.Fatalf("register accepted a password of %d runes", n)
			}
			if n := utf8.RuneCountInString(reg.FullName); n < 1 || n > 200 {
				t.Fatalf("register accepted a full_name of %d runes", n)
			}
			if reg.Email == "" {
				t.Fatal("register accepted an empty email")
			}
		}
		var up identity.UpdateProfileRequest
		if binding.JSON.BindBody(data, &up) == nil && (up.FullName == "" || utf8.RuneCountInString(up.FullName) > 200) {
			t.Fatalf("update-profile accepted full_name %q", up.FullName)
		}
		var rc identity.PasswordResetConfirmRequest
		if binding.JSON.BindBody(data, &rc) == nil {
			if n := utf8.RuneCountInString(rc.NewPassword); n < 8 || n > 128 || rc.Token == "" {
				t.Fatalf("reset-confirm accepted token=%q new_password of %d runes", rc.Token, n)
			}
		}
		var lg identity.LoginRequest
		_ = binding.JSON.BindBody(data, &lg)
		var rf identity.RefreshRequest
		if binding.JSON.BindBody(data, &rf) == nil && rf.RefreshToken == "" {
			t.Fatal("refresh accepted an empty token")
		}
	})
}
