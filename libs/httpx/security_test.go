// Adversarial checks for cursors, limits and the error envelope (area D/E).
package httpx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
)

// FuzzDecodeCursor: any input must either decode or error, never panic, and
// whatever decodes must survive a re-encode/decode round trip.
func FuzzDecodeCursor(f *testing.F) {
	good, _ := EncodeCursor(Cursor{CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), ID: "0192f0aa-0000-7000-8000-000000000001"})
	f.Add(good)
	f.Add("")
	f.Add("e30")    // {}
	f.Add("bnVsbA") // null
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":"0000-00-00T00:00:00Z","id":"x"}`)))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":"9999-12-31T23:59:59.999999999Z","id":"\u0000"}`)))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("[", 10000))))
	f.Add("====")
	f.Add("%00%00")
	f.Fuzz(func(t *testing.T, s string) {
		c, err := DecodeCursor(s)
		if err != nil {
			return
		}
		enc, err := EncodeCursor(c)
		if err != nil {
			return // e.g. year out of JSON range after a lossy decode; must not panic
		}
		if _, err := DecodeCursor(enc); err != nil {
			t.Fatalf("re-decode of own encoding failed: %v", err)
		}
	})
}

func TestDecodeCursor_Hostile(t *testing.T) {
	huge := base64.RawURLEncoding.EncodeToString([]byte(`{"id":"` + strings.Repeat("A", 4<<20) + `"}`))
	for name, in := range map[string]string{
		"standard (padded) base64": base64.StdEncoding.EncodeToString([]byte(`{"a":1}`)) + "=",
		"url-unsafe chars":         "+/+/",
		"nul":                      "\x00",
		"empty":                    "",
		"json null":                base64.RawURLEncoding.EncodeToString([]byte("null")),
		"json array":               base64.RawURLEncoding.EncodeToString([]byte("[]")),
		"wrong types":              base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":5,"id":[]}`)),
		"bad time":                 base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":"yesterday","id":"x"}`)),
		"deep nesting":             base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat(`{"a":`, 20000))),
		"4MiB id":                  huge,
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			_, _ = DecodeCursor(in)
		})
	}
}

// DecodeCursor does not itself validate; callers must. Document that an empty
// object decodes "successfully" to the zero cursor (year 1, empty id) so every
// caller has to validate the id (course.ListCatalogue does).
func TestDecodeCursor_EmptyObject_DecodesToZeroValue_CallerMustValidate(t *testing.T) {
	c, err := DecodeCursor(base64.RawURLEncoding.EncodeToString([]byte(`{}`)))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if c.ID != "" || !c.CreatedAt.IsZero() {
		t.Fatalf("zero cursor expected, got %+v", c)
	}
}

func TestClampLimit_ExtremeInputs(t *testing.T) {
	for _, in := range []int{-1 << 62, -1, 0, 1, MaxPageLimit, MaxPageLimit + 1, 1 << 62} {
		got := ClampLimit(in)
		if got < 1 || got > MaxPageLimit {
			t.Errorf("ClampLimit(%d)=%d outside [1,%d]", in, got, MaxPageLimit)
		}
	}
}

func TestFail_NeverLeaksInternalCause(t *testing.T) {
	gin.SetMode(gin.TestMode)
	secret := errors.New(`pq: relation "identity.users" does not exist at /srv/repo.go:99 password=hunter2`)
	for name, err := range map[string]error{
		"wrapped app error": apperrors.ErrInternal.Wrap(secret),
		"validation wrap":   apperrors.ErrValidation.Wrap(secret),
		"plain error":       secret,
		"conflict wrap":     apperrors.ErrConflict.Wrap(secret),
		"unavailable wrap":  apperrors.ErrUnavailable.Wrap(secret),
	} {
		t.Run(name, func(t *testing.T) {
			r := gin.New()
			r.GET("/", func(c *gin.Context) { Fail(c, err) })
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
			var m map[string]any
			if e := json.Unmarshal(rec.Body.Bytes(), &m); e != nil {
				t.Fatalf("body not JSON: %q", rec.Body.String())
			}
			for _, leak := range []string{"pq:", "relation", "identity.users", "repo.go", "hunter2"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Fatalf("response leaks %q: %s", leak, rec.Body.String())
				}
			}
			if rec.Code == http.StatusOK {
				t.Fatal("error rendered as 200")
			}
		})
	}
}

// BindJSON must classify every malformed body as a 400 with the generic
// message, never a 500 and never echo the parser's error text (which names
// Go types and field paths).
func TestBindJSON_HostileBodies_Are400_NoParserText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	type dst struct {
		Name string `json:"name" binding:"required,max=10"`
		N    int    `json:"n"`
	}
	r := gin.New()
	r.POST("/", func(c *gin.Context) {
		var d dst
		if BindJSON(c, &d) {
			c.Status(200)
		}
	})
	bodies := map[string]string{
		"empty":            ``,
		"null":             `null`,
		"array":            `[]`,
		"string":           `"x"`,
		"truncated":        `{"name":"a"`,
		"wrong type":       `{"name":5}`,
		"overflow int":     `{"name":"a","n":99999999999999999999999}`,
		"float for int":    `{"name":"a","n":1.5}`,
		"dup keys":         `{"name":"","name":"ok"}`,
		"invalid utf8":     "{\"name\":\"\xff\xfe\"}",
		"nul":              "{\"name\":\"a\x00b\"}",
		"trailing garbage": `{"name":"a"} {"name":"b"}`,
		"too long":         `{"name":"` + strings.Repeat("a", 11) + `"}`,
		"deep nesting":     strings.Repeat(`{"x":`, 50000),
		"bom":              "\xef\xbb\xbf{\"name\":\"a\"}",
	}
	for name, b := range bodies {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", strings.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code == 200 {
				// only the tolerated shapes: duplicate keys (last wins) and trailing garbage
				if name != "dup keys" && name != "trailing garbage" && name != "invalid utf8" {
					t.Fatalf("hostile body accepted: %s", b)
				}
				return
			}
			if rec.Code != 400 {
				t.Fatalf("want 400, got %d: %.200s", rec.Code, rec.Body.String())
			}
			for _, leak := range []string{"json:", "cannot unmarshal", "Go struct", "Key:", "Field validation", "dst", "unexpected end"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Fatalf("400 body leaks parser/validator text %q: %s", leak, rec.Body.String())
				}
			}
		})
	}
}
