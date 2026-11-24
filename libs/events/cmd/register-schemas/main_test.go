package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
)

// fakeRegistry implements just enough of the registry API: per-subject
// versions and compatibility level, global ids by schema text, and a
// compatibility rule that stands in for the real one (a new schema is
// incompatible if it contains the word "BREAKING").
type fakeRegistry struct {
	mu       sync.Mutex
	versions map[string][]string
	level    map[string]string
	ids      map[string]int
	calls    map[string]int
}

func newFake() *fakeRegistry {
	return &fakeRegistry{versions: map[string][]string{}, level: map[string]string{}, ids: map[string]int{}, calls: map[string]int{}}
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var body struct {
		Schema        string `json:"schema"`
		SchemaType    string `json:"schemaType"`
		Compatibility string `json:"compatibility"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	reply := func(status int, v any) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	switch {
	case r.Method == http.MethodPut && parts[0] == "config":
		f.level[parts[1]] = body.Compatibility
		reply(200, map[string]string{"compatibility": body.Compatibility})
	case r.Method == http.MethodGet && parts[0] == "config":
		if l, ok := f.level[parts[1]]; ok {
			reply(200, map[string]string{"compatibilityLevel": l})
			return
		}
		reply(404, map[string]any{"error_code": 40408, "message": "no config"})
	case r.Method == http.MethodPost && parts[0] == "compatibility":
		s := parts[2]
		if len(f.versions[s]) == 0 {
			reply(404, map[string]any{"error_code": 40401, "message": "Subject not found"})
			return
		}
		reply(200, map[string]bool{"is_compatible": !strings.Contains(body.Schema, "BREAKING")})
	case r.Method == http.MethodPost && parts[0] == "subjects":
		s := parts[1]
		if body.SchemaType != "PROTOBUF" {
			reply(422, map[string]any{"error_code": 42201, "message": "bad type"})
			return
		}
		vs := f.versions[s]
		if len(vs) == 0 || vs[len(vs)-1] != body.Schema { // identical text: no new version
			f.versions[s] = append(vs, body.Schema)
			f.calls[s]++
		}
		if _, ok := f.ids[body.Schema]; !ok {
			f.ids[body.Schema] = len(f.ids) + 1
		}
		reply(200, map[string]int{"id": f.ids[body.Schema]})
	case r.Method == http.MethodGet && parts[0] == "subjects" && parts[len(parts)-1] == "latest":
		reply(200, map[string]int{"version": len(f.versions[parts[1]])})
	default:
		reply(404, map[string]any{"error_code": 404, "message": r.Method + " " + r.URL.Path})
	}
}

func TestRun_RegistersEverySubjectIdempotently(t *testing.T) {
	fake := newFake()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	subjects, err := events.Subjects()
	if err != nil {
		t.Fatal(err)
	}
	// 4 envelope subjects (one per topic) + 11 payload records.
	if len(subjects) != 15 {
		t.Fatalf("%d subjects", len(subjects))
	}

	var out bytes.Buffer
	for range 2 { // second run must change nothing
		out.Reset()
		if err := run(context.Background(), &out, registry.New(srv.URL), subjects); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range subjects {
		if got := len(fake.versions[s.Name]); got != 1 {
			t.Errorf("%s has %d versions after two runs, want 1", s.Name, got)
		}
		if fake.level[s.Name] != registry.Backward {
			t.Errorf("%s level %q", s.Name, fake.level[s.Name])
		}
		if !strings.Contains(out.String(), s.Name) {
			t.Errorf("%s missing from output", s.Name)
		}
	}
}

func TestRun_IncompatibleSchemaFails(t *testing.T) {
	fake := newFake()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := registry.New(srv.URL)
	ok := []events.Subject{{Name: "s", Schema: "v1"}}
	if err := run(context.Background(), &bytes.Buffer{}, c, ok); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), &bytes.Buffer{}, c, []events.Subject{{Name: "s", Schema: "v2 BREAKING"}})
	if err == nil || !strings.Contains(err.Error(), "not BACKWARD-compatible") {
		t.Fatalf("got %v", err)
	}
	if len(fake.versions["s"]) != 1 {
		t.Fatal("incompatible schema must not be registered")
	}
}

func TestRun_RegistryDown(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	err := run(context.Background(), &bytes.Buffer{}, registry.New(srv.URL), []events.Subject{{Name: "s", Schema: "x"}})
	if err == nil {
		t.Fatal("expected error")
	}
}
