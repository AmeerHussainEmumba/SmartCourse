package registry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLatestSchema_ReturnsIDAndVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/subjects/s/versions/latest" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.EscapedPath())
		}
		_, _ = w.Write([]byte(`{"subject":"s","version":3,"id":42,"schema":"x"}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).LatestSchema(context.Background(), "s")
	if err != nil || got.ID != 42 || got.Version != 3 || got.Subject != "s" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestLatestSchema_MissingSubject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error_code":40401,"message":"Subject 'x' not found."}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL).LatestSchema(context.Background(), "x")
	if !errors.Is(err, ErrSubjectNotFound) {
		t.Fatalf("want ErrSubjectNotFound, got %v", err)
	}
}
