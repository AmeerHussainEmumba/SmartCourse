package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const (
	testAdminEmail = "root@example.test"
	testAdminPW    = "fake-admin-pw-for-tests"
)

type fakeUser struct {
	id, email, name, password, role string
}

type fakeCourse struct {
	id, slug, owner string
	state           string // draft, publishing, ready, archived
	limit           int
	modules         [][]string // lesson keys per module
	statusCalls     int
}

type moduleRef struct {
	c   *fakeCourse
	idx int
}

type fakeEnrollment struct {
	id, student, course string
	done                map[string]bool
}

// fake is an in-memory stand-in for identity-api, course-api and analytics-api.
type fake struct {
	mu sync.Mutex
	t  *testing.T

	users       map[string]*fakeUser // by email
	tokens      map[string]*fakeUser
	courses     map[string]*fakeCourse // by slug
	byID        map[string]*fakeCourse
	enrollments []*fakeEnrollment
	moduleOf    map[string]moduleRef
	seq         int

	publish503     bool // publish start answers 503
	pendingPolls   int  // publish-status answers publishing this many times first
	registerLimits int  // the first N register calls answer 429
	counts         map[string]int
	xff            map[string]bool
	seenXFF        int

	identity, course, analytics *httptest.Server
}

func newFake(t *testing.T) *fake {
	f := &fake{t: t, users: map[string]*fakeUser{}, tokens: map[string]*fakeUser{}, courses: map[string]*fakeCourse{},
		byID: map[string]*fakeCourse{}, moduleOf: map[string]moduleRef{}, counts: map[string]int{}, xff: map[string]bool{}}
	f.addUser(testAdminEmail, "Admin", testAdminPW, "admin")
	f.identity = httptest.NewServer(http.HandlerFunc(f.serveIdentity))
	f.course = httptest.NewServer(http.HandlerFunc(f.serveCourse))
	f.analytics = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	t.Cleanup(func() { f.identity.Close(); f.course.Close(); f.analytics.Close() })
	return f
}

func (f *fake) id(prefix string) string { f.seq++; return fmt.Sprintf("%s-%d", prefix, f.seq) }

func (f *fake) addUser(email, name, pw, role string) *fakeUser {
	u := &fakeUser{id: f.id("u"), email: email, name: name, password: pw, role: role}
	f.users[email] = u
	return u
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func apiErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"code": code, "message": code})
}

func decode(r *http.Request) map[string]any {
	var m map[string]any
	_ = json.NewDecoder(r.Body).Decode(&m)
	return m
}

func (f *fake) caller(r *http.Request) *fakeUser {
	return f.tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
}

func (f *fake) note(r *http.Request) {
	f.counts[r.Method+" "+r.URL.Path]++
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		if f.xff[ip] {
			f.t.Errorf("X-Forwarded-For %s reused", ip)
		}
		f.xff[ip] = true
		f.seenXFF++
	}
}

func (f *fake) serveIdentity(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note(r)
	switch {
	case r.URL.Path == "/health/ready":
		w.WriteHeader(200)
	case r.URL.Path == "/api/v1/auth/login":
		b := decode(r)
		u := f.users[fmt.Sprint(b["email"])]
		if u == nil || u.password != b["password"] {
			apiErr(w, 401, "invalid_credentials")
			return
		}
		tok := "tok-" + f.id("t")
		f.tokens[tok] = u
		writeJSON(w, 200, map[string]any{"access_token": tok, "user": map[string]string{"id": u.id, "role": u.role}})
	case r.URL.Path == "/api/v1/auth/register":
		if f.registerLimits > 0 {
			f.registerLimits--
			apiErr(w, 429, "rate_limited")
			return
		}
		b := decode(r)
		email := fmt.Sprint(b["email"])
		if f.users[email] == nil {
			f.addUser(email, fmt.Sprint(b["full_name"]), fmt.Sprint(b["password"]), "student")
		}
		writeJSON(w, 202, map[string]string{"message": "accepted"})
	case strings.HasPrefix(r.URL.Path, "/api/v1/admin/users/") && strings.HasSuffix(r.URL.Path, "/role"):
		if c := f.caller(r); c == nil || c.role != "admin" {
			apiErr(w, 403, "forbidden")
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/users/"), "/role")
		for _, u := range f.users {
			if u.id == id {
				u.role = fmt.Sprint(decode(r)["role"])
				writeJSON(w, 200, map[string]string{"id": id})
				return
			}
		}
		apiErr(w, 404, "not_found")
	default:
		apiErr(w, 404, "not_found")
	}
}

func (f *fake) serveCourse(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note(r)
	p := r.URL.Path
	parts := strings.Split(strings.Trim(p, "/"), "/") // api v1 ...
	caller := f.caller(r)
	switch {
	case p == "/health/ready":
		w.WriteHeader(200)
	case p == "/api/v1/courses" && r.Method == http.MethodPost:
		if caller == nil || caller.role != "instructor" {
			apiErr(w, 403, "forbidden")
			return
		}
		b := decode(r)
		slug := fmt.Sprint(b["slug"])
		if f.courses[slug] != nil {
			apiErr(w, 409, "slug_taken")
			return
		}
		c := &fakeCourse{id: f.id("c"), slug: slug, owner: caller.id, state: "draft"}
		if l, ok := b["enrollment_limit"].(float64); ok {
			c.limit = int(l)
		}
		f.courses[slug], f.byID[c.id] = c, c
		writeJSON(w, 201, map[string]string{"id": c.id})
	case len(parts) == 5 && parts[2] == "courses" && parts[4] == "modules":
		c := f.byID[parts[3]]
		if c == nil || caller == nil || c.owner != caller.id {
			apiErr(w, 404, "not_found")
			return
		}
		c.modules = append(c.modules, nil)
		mid := f.id("m")
		f.moduleOf[mid] = moduleRef{c, len(c.modules) - 1}
		writeJSON(w, 201, map[string]string{"id": mid})
	case len(parts) == 5 && parts[2] == "modules" && parts[4] == "lessons":
		ref, ok := f.moduleOf[parts[3]]
		if !ok || caller == nil || ref.c.owner != caller.id {
			apiErr(w, 404, "not_found")
			return
		}
		key := f.id("key")
		ref.c.modules[ref.idx] = append(ref.c.modules[ref.idx], key)
		writeJSON(w, 201, map[string]string{"id": f.id("l"), "lesson_key": key})
	case len(parts) == 5 && parts[2] == "courses" && parts[4] == "publish":
		c := f.byID[parts[3]]
		if f.publish503 {
			apiErr(w, 503, "temporarily_unavailable")
			return
		}
		c.state = "publishing"
		writeJSON(w, 202, map[string]string{"state": "publishing"})
	case len(parts) == 5 && parts[2] == "courses" && parts[4] == "publish-status":
		c := f.byID[parts[3]]
		c.statusCalls++
		if c.state == "publishing" && c.statusCalls > f.pendingPolls {
			c.state = "ready"
		}
		st := c.state
		if st == "archived" {
			st = "ready"
		}
		writeJSON(w, 200, map[string]string{"state": st})
	case len(parts) == 5 && parts[2] == "courses" && parts[4] == "archive":
		f.byID[parts[3]].state = "archived"
		w.WriteHeader(204)
	case len(parts) == 4 && parts[2] == "courses" && r.Method == http.MethodGet:
		c := f.courses[parts[3]]
		if c == nil || c.state != "ready" {
			apiErr(w, 404, "not_found")
			return
		}
		var mods []map[string]any
		for _, m := range c.modules {
			var ls []map[string]string
			for _, k := range m {
				ls = append(ls, map[string]string{"lesson_key": k})
			}
			mods = append(mods, map[string]any{"lessons": ls})
		}
		writeJSON(w, 200, map[string]any{"course": map[string]string{"id": c.id}, "modules": mods})
	case p == "/api/v1/enrollments" && r.Method == http.MethodPost:
		f.enroll(w, r, caller)
	case p == "/api/v1/enrollments" && r.Method == http.MethodGet:
		var items []map[string]string
		for _, e := range f.enrollments {
			if e.student == caller.id {
				items = append(items, map[string]string{"id": e.id, "course_id": e.course})
			}
		}
		writeJSON(w, 200, map[string]any{"items": items})
	case len(parts) == 7 && parts[2] == "enrollments" && parts[4] == "lessons":
		for _, e := range f.enrollments {
			if e.id == parts[3] && e.student == caller.id {
				e.done[parts[5]] = true
				w.WriteHeader(204)
				return
			}
		}
		apiErr(w, 404, "not_found")
	default:
		apiErr(w, 404, "not_found")
	}
}

func (f *fake) enroll(w http.ResponseWriter, r *http.Request, caller *fakeUser) {
	cid := fmt.Sprint(decode(r)["course_id"])
	c := f.byID[cid]
	if c == nil || c.state != "ready" {
		apiErr(w, 409, "course_not_published")
		return
	}
	n := 0
	for _, e := range f.enrollments {
		if e.course == cid {
			n++
			if e.student == caller.id {
				apiErr(w, 409, "already_enrolled")
				return
			}
		}
	}
	if c.limit > 0 && n >= c.limit {
		apiErr(w, 409, "course_full")
		return
	}
	e := &fakeEnrollment{id: f.id("e"), student: caller.id, course: cid, done: map[string]bool{}}
	f.enrollments = append(f.enrollments, e)
	writeJSON(w, 201, map[string]string{"id": e.id})
}
