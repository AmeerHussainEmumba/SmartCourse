// What this checks: EncodeCursor/DecodeCursor round-trip exactly (including
// sub-second precision, which JSON time marshaling can silently lose
// depending on format) and reject malformed input as a client error rather
// than panicking or silently truncating; ClampLimit enforces RFC §11.5's
// server-side page-size cap at its boundaries. No database — see
// docs/guides/testing.md, "unit" tier.
package httpx

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestCursor_EncodeDecode_RoundTrip(t *testing.T) {
	want := Cursor{
		// Sub-second precision, deliberately — a lossy time format would
		// still "work" for whole-second timestamps and only fail once two
		// rows share the same second, which is exactly the kind of bug
		// that hides until it doesn't.
		CreatedAt: time.Date(2026, 3, 14, 15, 9, 26, 535897932, time.UTC),
		ID:        "01234567-89ab-cdef-0123-456789abcdef",
	}

	encoded, err := EncodeCursor(want)
	if err != nil {
		t.Fatalf("EncodeCursor: %v", err)
	}

	got, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt: got %s, want %s", got.CreatedAt, want.CreatedAt)
	}
	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
}

func TestEncodeCursor_OutputIsOpaque(t *testing.T) {
	c := Cursor{CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ID: "some-id"}
	encoded, err := EncodeCursor(c)
	if err != nil {
		t.Fatalf("EncodeCursor: %v", err)
	}

	// The whole point of "opaque" is that a client can't read or construct
	// one by hand — it shouldn't be plain JSON, and it shouldn't contain
	// the raw ID string a client could spot and start depending on.
	if encoded == `{"created_at":"2026-01-01T00:00:00Z","id":"some-id"}` {
		t.Error("EncodeCursor returned naive JSON verbatim — not opaque")
	}
	if containsSubstring(encoded, c.ID) {
		t.Error("encoded cursor contains the raw ID as a visible substring — not opaque")
	}
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestDecodeCursor_RejectsInvalidBase64(t *testing.T) {
	if _, err := DecodeCursor("not valid base64!!!"); err == nil {
		t.Fatal("expected an error for a string that isn't valid base64")
	}
}

func TestDecodeCursor_RejectsValidBase64InvalidJSON(t *testing.T) {
	// Valid base64, but the decoded bytes aren't a JSON object at all —
	// this is the "manipulated or stale query parameter" case the
	// function's own doc comment calls out, distinct from garbled base64.
	notJSON := base64.RawURLEncoding.EncodeToString([]byte("this is not json"))
	if _, err := DecodeCursor(notJSON); err == nil {
		t.Fatal("expected an error for valid base64 that doesn't decode to valid JSON")
	}
}

func TestClampLimit(t *testing.T) {
	cases := []struct {
		name      string
		requested int
		want      int
	}{
		{"zero defaults", 0, DefaultPageLimit},
		{"negative defaults", -5, DefaultPageLimit},
		{"in range passes through", 42, 42},
		{"exactly the default", DefaultPageLimit, DefaultPageLimit},
		{"exactly the max", MaxPageLimit, MaxPageLimit},
		{"one over the max clamps down", MaxPageLimit + 1, MaxPageLimit},
		{"far over the max clamps down", 100000, MaxPageLimit},
	}
	for _, c := range cases {
		if got := ClampLimit(c.requested); got != c.want {
			t.Errorf("%s: ClampLimit(%d) = %d, want %d", c.name, c.requested, got, c.want)
		}
	}
}
