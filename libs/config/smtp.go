package config

import (
	"fmt"
	"net/mail"
	"os"
	"strconv"
	"time"
)

// SMTPConfig points the notification dispatcher's SMTP provider at a mail
// server. Local development targets the Mailpit sink (docker-compose.yml,
// port 1025): no authentication and no TLS. There is deliberately no real
// mail provider (Week 3 decision D6), but STARTTLS and authentication are
// supported so pointing the same code at one later is configuration, not a
// rewrite.
//
// Like Temporal, this is all-optional at Load: only the worker sends mail,
// and identity-api, course-api and analytics-api must keep booting without
// SMTP_HOST set. Host == "" means "not configured"; the worker decides what
// that implies for it.
type SMTPConfig struct {
	Host string
	Port int
	// From is the envelope sender and the From header (SMTP_FROM).
	From string
	// Username/Password enable PLAIN authentication (SMTP_USERNAME,
	// SMTP_PASSWORD). Both or neither.
	Username string
	Password string
	// StartTLS upgrades the connection with STARTTLS before anything else is
	// sent (SMTP_STARTTLS); the upgrade is mandatory when set, never a
	// best-effort fallback to cleartext.
	StartTLS bool
	// AllowInsecureAuth permits sending credentials over a cleartext link
	// (SMTP_ALLOW_INSECURE_AUTH). Off by default: Load rejects credentials
	// without StartTLS unless this is explicitly true.
	AllowInsecureAuth bool
	// ConnectTimeout bounds the TCP dial (SMTP_CONNECT_TIMEOUT); SendTimeout
	// bounds one whole send, handshake to QUIT (SMTP_SEND_TIMEOUT).
	ConnectTimeout time.Duration
	SendTimeout    time.Duration
}

const (
	defaultSMTPPort           = 1025
	defaultSMTPFrom           = "no-reply@smartcourse.local"
	defaultSMTPConnectTimeout = 5 * time.Second
	defaultSMTPSendTimeout    = 30 * time.Second
	maxSMTPTimeout            = 5 * time.Minute
)

// Configured reports whether an SMTP server was named at all.
func (c SMTPConfig) Configured() bool { return c.Host != "" }

// loadSMTP reads the SMTP_* variables and returns every problem it finds, in
// the style of Load: collected, not fail-on-first.
func loadSMTP() (SMTPConfig, []string) {
	var errs []string
	cfg := SMTPConfig{
		Host:           os.Getenv("SMTP_HOST"),
		Port:           defaultSMTPPort,
		From:           envOr("SMTP_FROM", defaultSMTPFrom),
		Username:       os.Getenv("SMTP_USERNAME"),
		Password:       os.Getenv("SMTP_PASSWORD"),
		ConnectTimeout: defaultSMTPConnectTimeout,
		SendTimeout:    defaultSMTPSendTimeout,
	}
	if v := os.Getenv("SMTP_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("SMTP_PORT must be an integer: %v", err))
		} else {
			cfg.Port = n
		}
	}
	boolVar := func(key string, dst *bool) {
		if v := os.Getenv(key); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s must be a boolean: %v", key, err))
				return
			}
			*dst = b
		}
	}
	durVar := func(key string, dst *time.Duration) {
		if v := os.Getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s must be a duration (e.g. 5s): %v", key, err))
				return
			}
			*dst = d
		}
	}
	boolVar("SMTP_STARTTLS", &cfg.StartTLS)
	boolVar("SMTP_ALLOW_INSECURE_AUTH", &cfg.AllowInsecureAuth)
	durVar("SMTP_CONNECT_TIMEOUT", &cfg.ConnectTimeout)
	durVar("SMTP_SEND_TIMEOUT", &cfg.SendTimeout)

	if cfg.Port < 1 || cfg.Port > 65535 {
		errs = append(errs, "SMTP_PORT must be between 1 and 65535")
	}
	if a, err := mail.ParseAddress(cfg.From); err != nil || a.Address != cfg.From {
		// A bare address only: a display name or a stray CR/LF in the
		// sender has no business in an envelope.
		errs = append(errs, "SMTP_FROM must be a bare email address (user@host)")
	}
	for key, d := range map[string]time.Duration{
		"SMTP_CONNECT_TIMEOUT": cfg.ConnectTimeout, "SMTP_SEND_TIMEOUT": cfg.SendTimeout,
	} {
		if d <= 0 || d > maxSMTPTimeout {
			errs = append(errs, key+" must be greater than zero and at most 5m")
		}
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		errs = append(errs, "SMTP_USERNAME and SMTP_PASSWORD must be set together")
	}
	if cfg.Username != "" && !cfg.StartTLS && !cfg.AllowInsecureAuth {
		errs = append(errs, "SMTP_USERNAME/SMTP_PASSWORD would be sent in cleartext: set SMTP_STARTTLS=true, or SMTP_ALLOW_INSECURE_AUTH=true to accept that explicitly")
	}
	return cfg, errs
}
