//go:build integration

package course_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// insertPublished creates a published course directly in SQL with an explicit
// created_at, so tests can force identical sort keys.
func insertPublished(t *testing.T, env *testenv.Env, instructor uuid.UUID, slug string, createdAt time.Time) uuid.UUID {
	t.Helper()
	courseID, versionID := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO courses (id, instructor_id, slug, created_at) VALUES (?, ?, ?, ?)`, courseID, instructor, slug, createdAt).Error; err != nil {
		t.Fatalf("insert course: %v", err)
	}
	if err := env.DB.Exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', ?, '{}')`, versionID, courseID, "Title "+slug).Error; err != nil {
		t.Fatalf("insert version: %v", err)
	}
	if err := env.DB.Exec(`UPDATE courses SET published_version_id = ? WHERE id = ?`, versionID, courseID).Error; err != nil {
		t.Fatalf("publish: %v", err)
	}
	return courseID
}

// walk follows next_cursor to the end and returns slugs in order. onPage runs
// after each page is fetched (to mutate data between pages).
func walk(t *testing.T, svc *course.Service, pageSize int, onPage func(page int, items []course.CatalogueCardResponse)) []string {
	t.Helper()
	var all []string
	cursor := ""
	for page := 1; page < 100; page++ {
		resp, err := svc.ListCatalogue(t.Context(), pageSize, cursor, "", "", "")
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, it := range resp.Items {
			all = append(all, it.Slug)
		}
		if onPage != nil {
			onPage(page, resp.Items)
		}
		if resp.NextCursor == nil {
			return all
		}
		cursor = *resp.NextCursor
	}
	t.Fatal("pagination did not terminate in 100 pages (cursor loop)")
	return nil
}

func assertNoDup(t *testing.T, slugs []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, s := range slugs {
		if seen[s] {
			t.Fatalf("slug %s returned twice: %v", s, slugs)
		}
		seen[s] = true
	}
}

func TestAdvPagination_DuplicateSortKeys_EveryRowExactlyOnce(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	same := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	var want []string
	for i := 0; i < 11; i++ { // 11 rows share ONE created_at
		s := fmt.Sprintf("tie-%02d", i)
		insertPublished(t, env, instr, s, same)
		want = append(want, s)
	}
	for i := 0; i < 4; i++ { // plus two more clusters
		s := fmt.Sprintf("newer-%d", i)
		insertPublished(t, env, instr, s, same.Add(time.Minute))
		want = append(want, s)
	}
	for i := 0; i < 3; i++ {
		s := fmt.Sprintf("older-%d", i)
		insertPublished(t, env, instr, s, same.Add(-time.Minute))
		want = append(want, s)
	}
	sort.Strings(want)
	for _, size := range []int{1, 2, 3, 5, 7, 18, 100} {
		got := walk(t, svc, size, nil)
		assertNoDup(t, got)
		g := append([]string(nil), got...)
		sort.Strings(g)
		if strings.Join(g, ",") != strings.Join(want, ",") {
			t.Fatalf("page size %d: rows missing or extra\n got %v\nwant %v", size, g, want)
		}
		// newer cluster first, then ties, then older
		if !strings.HasPrefix(got[0], "newer-") || !strings.HasPrefix(got[len(got)-1], "older-") {
			t.Fatalf("page size %d: ordering broken: %v", size, got)
		}
	}
}

func TestAdvPagination_ChangesBetweenPages_NoDuplicatesNoSkipsOfStableRows(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	ids := map[string]uuid.UUID{}
	for i := 0; i < 10; i++ { // s09 is the newest, s00 the oldest
		s := fmt.Sprintf("s%02d", i)
		ids[s] = insertPublished(t, env, instr, s, base.Add(time.Duration(i)*time.Minute))
	}

	var firstPage []string
	got := walk(t, svc, 3, func(page int, items []course.CatalogueCardResponse) {
		if page != 1 {
			return
		}
		for _, it := range items {
			firstPage = append(firstPage, it.Slug)
		}
		// page 1 = s09,s08,s07. Between pages:
		//  - archive the cursor row itself (s07) and an unseen row (s04)
		env.DB.Exec(`UPDATE courses SET archived_at = now() WHERE id IN (?, ?)`, ids["s07"], ids["s04"])
		//  - add a NEWER row (must not appear on later pages) and an OLDER row (must)
		insertPublished(t, env, instr, "brand-new", base.Add(time.Hour))
		insertPublished(t, env, instr, "ancient", base.Add(-time.Hour))
	})
	assertNoDup(t, got)
	if fmt.Sprint(firstPage) != "[s09 s08 s07]" {
		t.Fatalf("page 1 = %v", firstPage)
	}
	rest := got[3:]
	if fmt.Sprint(rest) != "[s06 s05 s03 s02 s01 s00 ancient]" {
		t.Fatalf("remaining pages after concurrent changes = %v (archived s04 must vanish, brand-new must not appear, ancient must)", rest)
	}
}

func TestAdvPagination_HTTP_LimitAndCursorEdgeCases(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	r := newTestEngine(svc)
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
	for i := 0; i < 130; i++ {
		insertPublished(t, env, instr, fmt.Sprintf("bulk-%03d", i), base.Add(time.Duration(i)*time.Second))
	}

	items := func(q string) (int, []any, string) {
		t.Helper()
		rec := doJSON(t, r, http.MethodGet, "/api/v1/courses?"+q, "", nil)
		var body struct {
			Items      []any  `json:"items"`
			NextCursor string `json:"next_cursor"`
			Code       string `json:"code"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Items, body.NextCursor + body.Code
	}
	for _, c := range []struct {
		q    string
		want int
	}{
		{"", 20}, {"limit=0", 20}, {"limit=-5", 20}, {"limit=abc", 20}, {"limit=1", 1},
		{"limit=100", 100}, {"limit=101", 100}, {"limit=1000000", 100}, {"limit=99999999999999999999", 20},
	} {
		code, got, _ := items(c.q)
		if code != 200 || len(got) != c.want {
			t.Errorf("?%s -> %d with %d items, want 200 with %d", c.q, code, len(got), c.want)
		}
	}

	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for _, c := range []struct {
		name, cursor string
		wantStatus   int
	}{
		{"not base64", "!!!!", 400},
		{"base64 of non-json", b64("hello"), 400},
		{"empty json object", b64("{}"), 400},
		{"bad uuid", b64(`{"created_at":"2026-01-01T00:00:00Z","id":"nope"}`), 400},
		{"bad time", b64(`{"created_at":"yesterday","id":"` + uuid.NewString() + `"}`), 400},
		{"huge garbage", strings.Repeat("A", 200000), 400},
		{"far future cursor returns everything older", b64(`{"created_at":"9999-12-31T23:59:59Z","id":"` + uuid.NewString() + `"}`), 200},
		{"nil-uuid ancient cursor returns nothing", b64(`{"created_at":"0001-01-01T00:00:00Z","id":"` + uuid.Nil.String() + `"}`), 200},
		{"offset-style numeric cursor", "20", 400},
	} {
		code, got, tail := items("limit=5&cursor=" + url.QueryEscape(c.cursor))
		if code != c.wantStatus {
			t.Errorf("%s: status %d (%s), want %d", c.name, code, tail, c.wantStatus)
			continue
		}
		if c.wantStatus == 400 && !strings.Contains(tail, "validation_failed") {
			t.Errorf("%s: error code %q, want validation_failed", c.name, tail)
		}
		if strings.HasPrefix(c.name, "far future") && len(got) != 5 {
			t.Errorf("%s: %d items", c.name, len(got))
		}
		if strings.HasPrefix(c.name, "nil-uuid") && len(got) != 0 {
			t.Errorf("%s: %d items", c.name, len(got))
		}
	}

	// Walk the full catalogue over HTTP with the cursor the server hands out.
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		q := "limit=50"
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		rec := doJSON(t, r, http.MethodGet, "/api/v1/courses?"+q, "", nil)
		var body course.CatalogueResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 200 {
			t.Fatalf("page %d: %d %s", pages, rec.Code, rec.Body.String())
		}
		for _, it := range body.Items {
			if seen[it.Slug] {
				t.Fatalf("dup %s", it.Slug)
			}
			seen[it.Slug] = true
		}
		pages++
		if body.NextCursor == nil {
			break
		}
		cursor = *body.NextCursor
	}
	if len(seen) != 130 || pages != 3 {
		t.Fatalf("walked %d rows in %d pages, want 130 in 3", len(seen), pages)
	}
}
