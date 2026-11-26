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

func TestLoadWith_Mongo_RequiredOnlyWhenRequested(t *testing.T) {
	setValidEnv(t)
	t.Setenv("MONGO_URI", "")
	if _, err := Load(); err != nil {
		t.Fatalf("MONGO_URI is optional by default: %v", err)
	}
	_, err := LoadWith(Requirements{Mongo: true})
	if err == nil || !strings.Contains(err.Error(), "MONGO_URI is required") {
		t.Fatalf("want MONGO_URI is required, got %v", err)
	}
	t.Setenv("MONGO_URI", "mongodb://mongodb:27017")
	cfg, err := LoadWith(Requirements{Mongo: true})
	if err != nil || !cfg.Mongo.Configured() {
		t.Fatalf("LoadWith with MONGO_URI: %+v, %v", cfg, err)
	}
	t.Setenv("MONGO_URI", "http://x:1")
	if _, err := LoadWith(Requirements{Mongo: true}); err == nil {
		t.Fatal("a bad MONGO_URI must still fail")
	}
}
