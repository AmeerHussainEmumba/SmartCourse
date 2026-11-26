package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegister_RequestAndResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/subjects/a.b%2Fc/versions" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.EscapedPath())
		}
		if ct := r.Header.Get("Content-Type"); ct != contentType {
			t.Errorf("content type %q", ct)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["schemaType"] != "PROTOBUF" || body["schema"] != "syntax" {
			t.Errorf("body %v", body)
		}
		_, _ = w.Write([]byte(`{"id":17}`))
	}))
	defer srv.Close()

	id, err := New(srv.URL+"/").Register(context.Background(), "a.b/c", "syntax")
	if err != nil || id != 17 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestRegister_IncompatibleIsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":409,"message":"Schema being registered is incompatible"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL).Register(context.Background(), "s", "x")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 409 || apiErr.ErrorCode != 409 {
		t.Fatalf("got %v", err)
	}
}

func TestAPIError_NonJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream down"))
	}))
	defer srv.Close()

	_, err := New(srv.URL).SchemaByID(context.Background(), 1)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 502 || apiErr.Message != "upstream down" {
		t.Fatalf("got %v", err)
	}
}

func TestCompatibility_SetAndRead(t *testing.T) {
	level := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/config/s":
			var b map[string]string
			_ = json.NewDecoder(r.Body).Decode(&b)
			level = b["compatibility"]
			_, _ = w.Write([]byte(`{"compatibility":"` + level + `"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/config/s":
			if r.URL.Query().Get("defaultToGlobal") != "false" {
				t.Errorf("must not fall back to the global level")
			}
			if level == "" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error_code":40408,"message":"Subject 's' does not have subject-level compatibility configured"}`))
				return
			}
			_, _ = w.Write([]byte(`{"compatibilityLevel":"` + level + `"}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
		}
	}))
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()

	if _, ok, err := c.Compatibility(ctx, "s"); err != nil || ok {
		t.Fatalf("before set: ok=%v err=%v", ok, err)
	}
	if err := c.SetCompatibility(ctx, "s", Backward); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := c.Compatibility(ctx, "s"); err != nil || !ok || got != Backward {
		t.Fatalf("got %q ok=%v err=%v", got, ok, err)
	}
}

func TestIsCompatible(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
		wantEr bool
	}{
		{"compatible", 200, `{"is_compatible":true}`, true, false},
		{"incompatible", 200, `{"is_compatible":false}`, false, false},
		{"new subject", 404, `{"error_code":40401,"message":"Subject 's' not found."}`, true, false},
		{"other 404", 404, `{"error_code":40402,"message":"Version not found."}`, false, true},
		{"server error", 500, `{"error_code":50001,"message":"boom"}`, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/compatibility/subjects/s/versions/latest" {
					t.Errorf("path %s", r.URL.Path)
				}
				w.WriteHeader(c.status)
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()
			got, err := New(srv.URL).IsCompatible(context.Background(), "s", "x")
			if (err != nil) != c.wantEr || got != c.want {
				t.Fatalf("got %v err=%v", got, err)
			}
		})
	}
}

func TestSchemaByIDAndLatestVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/5":
			_, _ = w.Write([]byte(`{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\";"}`))
		case "/subjects/s/versions/latest":
			_, _ = w.Write([]byte(`{"subject":"s","version":3,"id":5}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":40403,"message":"Schema not found"}`))
		}
	}))
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()

	s, err := c.SchemaByID(ctx, 5)
	if err != nil || s.Type != "PROTOBUF" || s.Schema != `syntax = "proto3";` {
		t.Fatalf("got %+v err=%v", s, err)
	}
	if v, err := c.LatestVersion(ctx, "s"); err != nil || v != 3 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	var apiErr *APIError
	if _, err := c.SchemaByID(ctx, 6); !errors.As(err, &apiErr) || apiErr.ErrorCode != 40403 {
		t.Fatalf("got %v", err)
	}
}

func TestContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(srv.URL).SchemaByID(ctx, 1); err == nil {
		t.Fatal("expected error")
	}
}
