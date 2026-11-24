//go:build integration

package securitytest_test

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// (NUL bytes and deep JSON nesting are reproduced separately in security_regression_integration_test.go.)
// No search/catalogue input may produce a 5xx, a SQL error, or rows that are not public.
func TestSearchAndCatalogue_HostileQueryParams_NeverBreakOrLeak(t *testing.T) {
	w := newWorld(t)
	hidden := w.newDraft(instrA) // a draft that must never surface
	pub := w.newPublished(instrA)
	_ = pub

	payloads := []string{
		"' OR '1'='1", "'; DROP TABLE courses;--", "\" OR \"\"=\"", "1; SELECT pg_sleep(5)--",
		"%", "%%", "_", "\\\\", "[a-z]", "a' UNION SELECT password_hash FROM users--",
		"a & b | !c", "a <-> b", ":*", "(((((", "))))", "!!!!", "'", "\"", "`", "${jndi:ldap://x}", "{{7*7}}",
		"../../etc/passwd", "..%2f..%2f", "‮", "\U0001F600\U0001F600", "ａｄｍｉｎ", "İ", "ß", "�",
		strings.Repeat("a", 100000), strings.Repeat("%", 10000), strings.Repeat("a & ", 5000),
		"<script>alert(1)</script>", "\r\nX-Injected: 1", "null", "undefined", "true", "-1", "0", "9999999999999999999",
	}
	endpoints := []string{"/api/v1/courses/search?q=%s", "/api/v1/courses?q=%s", "/api/v1/courses?category=%s", "/api/v1/courses?level=%s"}
	start := time.Now()
	for _, ep := range endpoints {
		for _, p := range payloads {
			rec := w.do(studentA, "GET", fmt.Sprintf(ep, url.QueryEscape(p)), nil)
			label := fmt.Sprintf("GET %s payload=%.30q", strings.Split(ep, "?")[0], p)
			if rec.Code >= 500 {
				t.Errorf("%s -> %d %.150s", label, rec.Code, rec.Body.String())
			}
			assertNoInternalLeak(t, label, rec)
			if strings.Contains(rec.Body.String(), "password_hash") || strings.Contains(rec.Body.String(), hidden.Title) {
				t.Errorf("%s leaks data: %.200s", label, rec.Body.String())
			}
			if rec.Header().Get("X-Injected") != "" {
				t.Errorf("%s: header injection", label)
			}
		}
	}
	// pg_sleep must not have run: 160 requests in far less than 5s * N.
	if time.Since(start) > 60*time.Second {
		t.Errorf("query payloads took %v: stacked query / sleep executed?", time.Since(start))
	}
	// The courses table is intact.
	if w.scalar(`SELECT count(*)::text FROM courses`) == "0" {
		t.Fatal("courses table emptied")
	}
}

func TestSearchAndCatalogue_LimitParam_Abuse(t *testing.T) {
	w := newWorld(t)
	w.newPublished(instrA)
	for _, l := range []string{"-1", "0", "abc", "1e3", "999999999999999999999", "101", "100000", "%00", "1.5", " 5", "5 ", "+5", "0x10", "", "٣"} {
		for _, p := range []string{"/api/v1/courses?limit=", "/api/v1/courses/search?q=sec&limit="} {
			rec := w.do(studentA, "GET", p+url.QueryEscape(l), nil)
			if rec.Code != 200 {
				t.Errorf("%s%q: %d %.120s", p, l, rec.Code, rec.Body.String())
			}
		}
	}
}

func cursor(t *testing.T, raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// Forged/tampered cursors are rejected cleanly (400) or harmlessly ignored; they never 5xx and never
// surface unpublished rows.
func TestCatalogueCursor_Forgery(t *testing.T) {
	w := newWorld(t)
	draft := w.newDraft(instrA)
	for i := 0; i < 3; i++ {
		w.newPublished(instrA)
	}
	page := w.do(anon, "GET", "/api/v1/courses?limit=1", nil)
	if page.Code != 200 {
		t.Fatalf("page 1: %d", page.Code)
	}
	genuine := ""
	if i := strings.Index(page.Body.String(), `"next_cursor":"`); i >= 0 {
		rest := page.Body.String()[i+len(`"next_cursor":"`):]
		genuine = rest[:strings.Index(rest, `"`)]
	}
	if genuine == "" {
		t.Fatal("no next_cursor returned for limit=1 with 3 published courses")
	}

	forged := map[string]string{
		"garbage":               "!!!not-base64!!!",
		"empty json":            cursor(t, `{}`),
		"null":                  cursor(t, `null`),
		"array":                 cursor(t, `[1,2,3]`),
		"id not uuid":           cursor(t, `{"created_at":"2026-01-01T00:00:00Z","id":"1' OR 1=1--"}`),
		"id empty":              cursor(t, `{"created_at":"2026-01-01T00:00:00Z","id":""}`),
		"time not rfc3339":      cursor(t, `{"created_at":"yesterday","id":"`+uuid.NewString()+`"}`),
		"time month 13":         cursor(t, `{"created_at":"2026-13-45T00:00:00Z","id":"`+uuid.NewString()+`"}`),
		"time as number":        cursor(t, `{"created_at":1,"id":"`+uuid.NewString()+`"}`),
		"extra fields":          cursor(t, `{"created_at":"2026-01-01T00:00:00Z","id":"`+uuid.NewString()+`","limit":9999,"where":"1=1","draft":true}`),
		"id is a draft":         cursor(t, `{"created_at":"2099-01-01T00:00:00Z","id":"`+draft.CourseID.String()+`"}`),
		"year 0001":             cursor(t, `{"created_at":"0001-01-01T00:00:00Z","id":"`+uuid.NewString()+`"}`),
		"year 9999":             cursor(t, `{"created_at":"9999-12-31T23:59:59.999999999Z","id":"`+uuid.NewString()+`"}`),
		"nil uuid":              cursor(t, `{"created_at":"2026-01-01T00:00:00Z","id":"`+uuid.Nil.String()+`"}`),
		"huge":                  cursor(t, `{"created_at":"2026-01-01T00:00:00Z","id":"`+strings.Repeat("a", 3<<20)+`"}`),
		"deep nest":             cursor(t, strings.Repeat(`{"a":`, 5000)),
		"standard base64 (+/=)": base64.StdEncoding.EncodeToString([]byte(`{"created_at":"2026-01-01T00:00:00Z","id":"` + uuid.NewString() + `"}`)),
		"genuine + junk":        genuine + "AAAA",
		"genuine truncated":     genuine[:len(genuine)/2],
		"nul":                   "%00",
	}
	for name, c := range forged {
		rec := w.do(anon, "GET", "/api/v1/courses?cursor="+url.QueryEscape(c), nil)
		if rec.Code >= 500 {
			t.Errorf("%s: %d %.150s", name, rec.Code, rec.Body.String())
		}
		if rec.Code != 200 && rec.Code != 400 {
			t.Errorf("%s: unexpected status %d", name, rec.Code)
		}
		assertNoInternalLeak(t, "cursor "+name, rec)
		if strings.Contains(rec.Body.String(), draft.Slug) || strings.Contains(rec.Body.String(), draft.Title) {
			t.Errorf("%s: forged cursor surfaced a draft: %.200s", name, rec.Body.String())
		}
	}
	// The genuine cursor still works.
	if rec := w.do(anon, "GET", "/api/v1/courses?limit=1&cursor="+url.QueryEscape(genuine), nil); rec.Code != 200 {
		t.Fatalf("genuine cursor: %d", rec.Code)
	}
}

// Every id-taking route rejects non-UUID ids with 4xx, never 5xx.
func TestPathParams_InvalidIDs_AreClientErrors(t *testing.T) {
	w := newWorld(t)
	bad := []string{"abc", "1", "0", uuid.Nil.String(), "00000000-0000-0000-0000-00000000000g", "' OR 1=1", "%00", "../..", "%2e%2e%2f", strings.Repeat("a", 5000), "‮", uuid.NewString() + "x", "{" + uuid.NewString() + "}", "urn:uuid:" + uuid.NewString()}
	routes := []struct {
		method, tmpl string
		actor        int
		body         any
	}{
		{"PATCH", "/api/v1/courses/%s", instrA, map[string]any{"title": "x"}},
		{"POST", "/api/v1/courses/%s/modules", instrA, map[string]any{"title": "x", "position": 1}},
		{"POST", "/api/v1/courses/%s/publish", instrA, nil},
		{"GET", "/api/v1/courses/%s/publish-status", instrA, nil},
		{"POST", "/api/v1/courses/%s/archive", instrA, nil},
		{"POST", "/api/v1/modules/%s/lessons", instrA, map[string]any{"title": "x", "position": 1, "content_type": "text"}},
		{"PATCH", "/api/v1/modules/%s", instrA, map[string]any{"title": "x"}},
		{"DELETE", "/api/v1/modules/%s", instrA, nil},
		{"PATCH", "/api/v1/lessons/%s", instrA, map[string]any{"title": "x", "content_type": "text"}},
		{"DELETE", "/api/v1/lessons/%s", instrA, nil},
		{"GET", "/api/v1/enrollments/%s", studentA, nil},
		{"DELETE", "/api/v1/enrollments/%s", studentA, nil},
		{"GET", "/api/v1/enrollments/%s/progress", studentA, nil},
		{"PUT", "/api/v1/enrollments/%s/lessons/" + uuid.NewString() + "/complete", studentA, nil},
		{"PUT", "/api/v1/enrollments/" + uuid.NewString() + "/lessons/%s/complete", studentA, nil},
	}
	for _, r := range routes {
		for _, b := range bad {
			p := fmt.Sprintf(r.tmpl, url.PathEscape(b))
			rec := w.do(r.actor, r.method, p, r.body)
			if rec.Code >= 500 {
				t.Errorf("%s %.80s -> %d %.120s", r.method, p, rec.Code, rec.Body.String())
			}
			assertNoInternalLeak(t, r.method+" "+p, rec)
		}
	}
	// the public slug route with traversal / unicode / huge slugs
	for _, s := range []string{"..", "../../etc/passwd", "%2e%2e", "a b", "‮", strings.Repeat("a", 10000), "%00", "' OR 1=1--", "CON", "nul"} {
		rec := w.do(anon, "GET", "/api/v1/courses/"+url.PathEscape(s), nil)
		if rec.Code != 404 && rec.Code != 400 {
			t.Errorf("slug %.30q: %d", s, rec.Code)
		}
	}
}

func TestCreateCourse_SlugAndFieldValidation(t *testing.T) {
	w := newWorld(t)
	post := func(m map[string]any) int { return w.do(instrA, "POST", "/api/v1/courses", m).Code }
	good := func() map[string]any {
		return map[string]any{"title": "T", "slug": "ok-" + strings.ToLower(uuid.NewString()[:8])}
	}
	for name, slug := range map[string]string{
		"uppercase": "Upper", "space": "a b", "slash": "a/b", "dotdot": "..", "traversal": "../../x", "leading hyphen": "-a",
		"trailing hyphen": "a-", "double hyphen": "a--b", "underscore": "a_b", "unicode": "café", "fullwidth": "ａｂｃ",
		"cyrillic a": "аbc", "emoji": "a\U0001F600", "nul": "a\x00b", "newline": "a\nb", "sql": "a';--", "percent": "a%2fb",
		"too long": strings.Repeat("a", 201), "empty": "",
	} {
		m := good()
		m["slug"] = slug
		if code := post(m); code != 400 {
			t.Errorf("slug %s (%q): want 400, got %d", name, slug, code)
		}
	}
	for name, mut := range map[string]func(map[string]any){
		"title too long":     func(m map[string]any) { m["title"] = strings.Repeat("a", 201) },
		"title empty":        func(m map[string]any) { m["title"] = "" },
		"description > 10k":  func(m map[string]any) { m["description"] = strings.Repeat("a", 10001) },
		"level bogus":        func(m map[string]any) { m["level"] = "expert" },
		"limit zero":         func(m map[string]any) { m["enrollment_limit"] = 0 },
		"limit negative":     func(m map[string]any) { m["enrollment_limit"] = -5 },
		"title wrong type":   func(m map[string]any) { m["title"] = 5 },
		"title array":        func(m map[string]any) { m["title"] = []string{"a"} },
		"title object":       func(m map[string]any) { m["title"] = map[string]any{"$ne": ""} },
		"limit as string":    func(m map[string]any) { m["enrollment_limit"] = "5" },
		"limit float":        func(m map[string]any) { m["enrollment_limit"] = 1.5 },
		"category wrong typ": func(m map[string]any) { m["category"] = 7 },
	} {
		m := good()
		mut(m)
		if code := post(m); code != 400 {
			t.Errorf("%s: want 400, got %d", name, code)
		}
	}
	// duplicate slug -> 409, not 500
	m := good()
	if code := post(m); code != 201 {
		t.Fatalf("create: %d", code)
	}
	if code := post(m); code != 409 {
		t.Errorf("duplicate slug: want 409, got %d", code)
	}
}

func TestAuthoringBodies_Malformed_NeverServerError(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	c := fx.CourseID.String()
	deepArr := strings.Repeat("[", 20000) + strings.Repeat("]", 20000)
	deepObj := strings.Repeat(`{"a":`, 20000) + "1" + strings.Repeat("}", 20000)
	bodies := map[string]string{
		"empty": "", "null": "null", "array": "[]", "string": `"x"`, "number": "1", "truncated": `{"title":"x"`,
		"trailing comma": `{"title":"x",}`, "single quotes": `{'title':'x'}`, "BOM": "\xef\xbb\xbf{\"title\":\"x\"}",
		"invalid utf8": "{\"title\":\"\xff\xfe\"}", "lone surrogate": `{"title":"\ud800"}`,
		"deep array": deepArr, "deep object": deepObj, "huge number": `{"title":"x","tags":[` + strings.Repeat("1,", 100000) + `1]}`,
		"21 tags":        `{"title":"x","tags":["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21"]}`,
		"duplicate keys": `{"title":"a","title":"b"}`, "big exponent": `{"title":"x","enrollment_limit":1e999}`,
		"xml": `<title>x</title>`, "form": `title=x&slug=y`, "yaml": "title: x",
	}
	targets := []struct{ method, path string }{
		{"POST", "/api/v1/courses"}, {"PATCH", "/api/v1/courses/" + c}, {"POST", "/api/v1/courses/" + c + "/modules"},
		{"POST", "/api/v1/modules/" + fx.ModuleID.String() + "/lessons"}, {"PATCH", "/api/v1/modules/" + fx.ModuleID.String()},
		{"PATCH", "/api/v1/lessons/" + fx.LessonID.String()}, {"POST", "/api/v1/enrollments"},
	}
	for _, tg := range targets {
		actor := instrA
		if tg.path == "/api/v1/enrollments" {
			actor = studentA
		}
		for name, b := range bodies {
			rec := w.do(actor, tg.method, tg.path, b)
			label := tg.method + " " + tg.path[:min(len(tg.path), 30)] + " body=" + name
			if rec.Code >= 500 {
				t.Errorf("%s -> %d %.150s", label, rec.Code, rec.Body.String())
			}
			assertNoInternalLeak(t, label, rec)
			for _, leak := range []string{"cannot unmarshal", "json:", "Go struct", "Key:", "Field validation", "unexpected end", "invalid character"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Errorf("%s: parser/validator text %q reaches the client: %.200s", label, leak, rec.Body.String())
				}
			}
		}
	}
	// Wrong HTTP methods / content types / trailing slashes must not bypass auth or reach handlers.
	for _, m := range []string{"TRACE", "HEAD", "OPTIONS", "PUT", "CONNECT", "FOO"} {
		if rec := w.doRaw("", m, "/api/v1/courses/"+c, nil); okStatus(rec.Code) && rec.Body.Len() > 0 && strings.Contains(rec.Body.String(), fx.Title) {
			t.Errorf("method %s returned draft data", m)
		}
	}
	if rec := w.doRaw("", "POST", "/api/v1/courses/", map[string]any{"title": "x", "slug": "x-x"}); okStatus(rec.Code) {
		t.Errorf("trailing-slash POST /courses/ without a token succeeded: %d", rec.Code)
	}
	// Stats on a (hostile) HTTP method spoof header must not change routing.
	rec := w.do(anon, "GET", "/api/v1/courses/"+fx.Slug+"?_method=DELETE", nil)
	if rec.Code != 404 {
		t.Errorf("draft slug with _method override: %d", rec.Code)
	}
}

func TestLessonContent_SizeAndShape(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	url := "/api/v1/modules/" + fx.ModuleID.String() + "/lessons"
	for i, c := range []map[string]any{
		{"a": strings.Repeat("x", 70000)},               // over the 64 KiB cap
		{"a": strings.Repeat("x", 65535)},               // on the edge
		{"a": strings.Repeat("é", 40000)},               // multibyte over cap
		{"html": "<script>alert(1)</script>", "x": nil}, // stored as data, never executed server-side
	} {
		rec := w.do(instrA, "POST", url, map[string]any{"title": "L", "position": 10 + i, "content_type": "text", "content": c})
		if rec.Code >= 500 {
			t.Errorf("content #%d -> %d %.150s", i, rec.Code, rec.Body.String())
		}
		assertNoInternalLeak(t, fmt.Sprintf("content #%d", i), rec)
	}
}

func nestMap(depth int) any {
	var v any = "leaf"
	for i := 0; i < depth; i++ {
		v = map[string]any{"n": v}
	}
	return v
}
