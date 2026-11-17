package httpx

import "testing"

func TestValidRequestID(t *testing.T) {
	good := []string{"a", "abc-123_X.y", "550e8400-e29b-41d4-a716-446655440000"}
	bad := []string{"", "has space", "new\nline", `{"a":1}`, string(make([]byte, 65)), "ünï"}
	for _, s := range good {
		if !validRequestID(s) {
			t.Errorf("%q rejected", s)
		}
	}
	for _, s := range bad {
		if validRequestID(s) {
			t.Errorf("%q accepted", s)
		}
	}
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	if validRequestID(string(long)) {
		t.Error("65-char id accepted")
	}
}
