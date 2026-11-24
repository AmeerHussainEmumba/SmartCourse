package config

import (
	"strings"
	"testing"
)

func TestLoad_Mongo_DefaultsAndOptional(t *testing.T) {
	setValidEnv(t)
	t.Setenv("MONGO_URI", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mongo.Configured() || cfg.Mongo.Database != "smartcourse" || cfg.Mongo.OpTimeout <= 0 {
		t.Fatalf("defaults = %+v", cfg.Mongo)
	}
	t.Setenv("MONGO_URI", "mongodb://mongodb:27017")
	t.Setenv("MONGO_DATABASE", "sc_test")
	cfg, err = Load()
	if err != nil || !cfg.Mongo.Configured() || cfg.Mongo.Database != "sc_test" {
		t.Fatalf("configured = %+v, %v", cfg, err)
	}
}

func TestLoad_Mongo_Validation(t *testing.T) {
	cases := []struct{ key, val, want string }{
		{"MONGO_URI", "http://mongodb:27017", "MONGO_URI must start with"},
		{"MONGO_URI", "mongodb://", "MONGO_URI must name a host"},
		{"MONGO_DATABASE", "a.b", "MONGO_DATABASE"},
		{"MONGO_DATABASE", strings.Repeat("d", 64), "MONGO_DATABASE"},
		{"MONGO_OP_TIMEOUT", "0s", "MONGO_OP_TIMEOUT"},
		{"MONGO_CONNECT_TIMEOUT", "-1s", "MONGO_CONNECT_TIMEOUT"},
		{"MONGO_SERVER_SELECTION_TIMEOUT", "1h", "MONGO_SERVER_SELECTION_TIMEOUT"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.val, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(c.key, c.val)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want an error containing %q, got %v", c.want, err)
			}
		})
	}
}

func TestLoad_Mongo_URIWithPasswordIsNeverEchoed(t *testing.T) {
	setValidEnv(t)
	t.Setenv("MONGO_URI", "http://user:s3cr3t-pass@host:1")
	_, err := Load()
	if err == nil || strings.Contains(err.Error(), "s3cr3t-pass") {
		t.Fatalf("want an error that does not echo the password, got %v", err)
	}
}

func TestLoad_Admin_AddrDefaultAndValidation(t *testing.T) {
	setValidEnv(t)
	t.Setenv("ADMIN_HTTP_ADDR", "")
	cfg, err := Load()
	if err != nil || cfg.Admin.Addr != ":8093" {
		t.Fatalf("default admin addr = %+v, %v", cfg.Admin, err)
	}
	for _, ok := range []string{":9000", "127.0.0.1:8093", "worker:8093", "[::1]:8093"} {
		setValidEnv(t)
		t.Setenv("ADMIN_HTTP_ADDR", ok)
		if _, err := Load(); err != nil {
			t.Errorf("Load rejected ADMIN_HTTP_ADDR=%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"8093", ":0", ":65536", ":abc", "host", "bad host:80", "a_b:80"} {
		setValidEnv(t)
		t.Setenv("ADMIN_HTTP_ADDR", bad)
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "ADMIN_HTTP_ADDR") {
			t.Errorf("Load accepted ADMIN_HTTP_ADDR=%s (err %v)", bad, err)
		}
	}
}
