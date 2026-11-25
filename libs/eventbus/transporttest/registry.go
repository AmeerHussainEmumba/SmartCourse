// Package transporttest holds test doubles for libs/eventbus.
package transporttest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
)

// Registry is a stand-in for the Confluent Schema Registry that answers
// GET /subjects/{subject}/versions/latest for every subject libs/events
// defines, with the ids a real registry would give: one id per distinct schema
// text (so the four envelope subjects share one), version 1. It exists because
// the real registry needs its own Kafka to start; it is NOT the real thing, and
// nothing here shows the real registry accepts or returns these values (the
// libs/events live tests cover registration against the compose registry).
type Registry struct {
	URL string

	mu      sync.Mutex
	subject map[string]events.Subject
	ids     map[string]int
	missing map[string]bool
	srv     *httptest.Server
}

// NewRegistry starts the stand-in and stops it when the test ends.
func NewRegistry(t *testing.T) *Registry {
	t.Helper()
	subs, err := events.Subjects()
	if err != nil {
		t.Fatalf("events.Subjects: %v", err)
	}
	r := &Registry{subject: map[string]events.Subject{}, ids: map[string]int{}, missing: map[string]bool{}}
	next := 100
	byText := map[string]int{}
	for _, s := range subs {
		id, ok := byText[s.Schema]
		if !ok {
			next++
			id = next
			byText[s.Schema] = id
		}
		r.subject[s.Name] = s
		r.ids[s.Name] = id
	}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	r.URL = r.srv.URL
	return r
}

// ID returns the id served for subject.
func (r *Registry) ID(subject string) int { return r.ids[subject] }

// Remove makes subject answer 404, as an unregistered subject does.
func (r *Registry) Remove(subject string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.missing[subject] = true
}

func (r *Registry) serve(w http.ResponseWriter, req *http.Request) {
	const prefix, suffix = "/subjects/", "/versions/latest"
	p := req.URL.Path
	if req.Method != http.MethodGet || !strings.HasPrefix(p, prefix) || !strings.HasSuffix(p, suffix) {
		http.NotFound(w, req)
		return
	}
	name := strings.TrimSuffix(strings.TrimPrefix(p, prefix), suffix)
	r.mu.Lock()
	s, ok := r.subject[name]
	gone := r.missing[name]
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if !ok || gone {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 40401, "message": "Subject '" + name + "' not found."})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"subject": name, "version": 1, "id": r.ids[name], "schema": s.Schema})
}
