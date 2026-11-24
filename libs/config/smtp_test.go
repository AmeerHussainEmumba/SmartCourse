package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_SMTP_DefaultsAreUnconfiguredButValid(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SMTP.Configured() {
		t.Fatal("SMTP must be unconfigured when SMTP_HOST is unset")
	}
	if cfg.SMTP.Port != 1025 || cfg.SMTP.StartTLS || cfg.SMTP.AllowInsecureAuth ||
		cfg.SMTP.ConnectTimeout != 5*time.Second || cfg.SMTP.SendTimeout != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg.SMTP)
	}
}

func TestLoad_SMTP_MailpitStyleNoAuthNoTLS(t *testing.T) {
	setValidEnv(t)
	t.Setenv("SMTP_HOST", "mailpit")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.SMTP.Configured() || cfg.SMTP.Host != "mailpit" || cfg.SMTP.Username != "" {
		t.Fatalf("unexpected: %+v", cfg.SMTP)
	}
}

func TestLoad_SMTP_Validation(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // substring of the error; "" means Load must succeed
	}{
		{"credentials over cleartext refused", map[string]string{"SMTP_USERNAME": "u", "SMTP_PASSWORD": "hunter2-secret"}, "cleartext"},
		{"credentials with STARTTLS ok", map[string]string{"SMTP_USERNAME": "u", "SMTP_PASSWORD": "p", "SMTP_STARTTLS": "true"}, ""},
		{"credentials cleartext explicitly allowed", map[string]string{"SMTP_USERNAME": "u", "SMTP_PASSWORD": "p", "SMTP_ALLOW_INSECURE_AUTH": "true"}, ""},
		{"username without password", map[string]string{"SMTP_USERNAME": "u", "SMTP_STARTTLS": "true"}, "set together"},
		{"port not a number", map[string]string{"SMTP_PORT": "abc"}, "SMTP_PORT must be an integer"},
		{"port out of range", map[string]string{"SMTP_PORT": "70000"}, "SMTP_PORT must be between"},
		{"from with display name", map[string]string{"SMTP_FROM": "Evil <a@b.c>"}, "SMTP_FROM"},
		{"from with newline", map[string]string{"SMTP_FROM": "a@b.c\r\nBcc: x@y.z"}, "SMTP_FROM"},
		{"zero send timeout", map[string]string{"SMTP_SEND_TIMEOUT": "0s"}, "SMTP_SEND_TIMEOUT"},
		{"bad bool", map[string]string{"SMTP_STARTTLS": "maybe"}, "SMTP_STARTTLS must be a boolean"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("SMTP_HOST", "smtp.example.test")
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if c.want == "" {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
			if strings.Contains(err.Error(), "hunter2-secret") {
				t.Fatalf("error must not echo the password: %v", err)
			}
		})
	}
}
