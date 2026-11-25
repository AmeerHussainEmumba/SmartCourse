package notification

import (
	"context"
	"crypto/tls"
	"errors"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

func smtpCfg(f *fakeSMTP) config.SMTPConfig {
	h, p := f.hostPort()
	return config.SMTPConfig{
		Host: h, Port: p, From: "no-reply@smartcourse.local",
		ConnectTimeout: time.Second, SendTimeout: 3 * time.Second,
	}
}

func baseMsg() Message {
	return Message{ID: "abc-123", To: "ada@example.com", ToName: "Ada", Subject: "Hello", Text: "Hi Ada\n", HTML: "<p>Hi Ada</p>"}
}

func mustProvider(t *testing.T, cfg config.SMTPConfig, opts ...SMTPOption) *SMTPProvider {
	t.Helper()
	p, err := NewSMTPProvider(cfg, opts...)
	if err != nil {
		t.Fatalf("NewSMTPProvider: %v", err)
	}
	return p
}

func TestSMTP_SendsPlainDevelopmentMessage(t *testing.T) {
	f := newFakeSMTP(t)
	p := mustProvider(t, smtpCfg(f))
	if err := p.Send(context.Background(), baseMsg()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msgs := f.Messages()
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}
	m := msgs[0]
	if m.from != "no-reply@smartcourse.local" || len(m.to) != 1 || m.to[0] != "ada@example.com" {
		t.Fatalf("envelope: %+v", m)
	}
	parsed, err := mail.ReadMessage(strings.NewReader(m.data))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := parsed.Header.Get("Subject"); got != "Hello" {
		t.Fatalf("subject %q", got)
	}
	if got := parsed.Header.Get("Message-Id"); got != "<abc-123@smartcourse.local>" {
		t.Fatalf("message-id %q", got)
	}
	if !strings.Contains(parsed.Header.Get("Content-Type"), "multipart/alternative") || !strings.Contains(m.data, "<p>Hi Ada</p>") {
		t.Fatalf("expected multipart with html part: %s", m.data)
	}
}

func TestSMTP_ServerRejectionClassification(t *testing.T) {
	cases := []struct {
		name      string
		set       func(*fakeSMTP)
		permanent bool
	}{
		{"550 on RCPT is permanent", func(f *fakeSMTP) { f.failRcpt = "550 no such user" }, true},
		{"451 on RCPT is retryable", func(f *fakeSMTP) { f.failRcpt = "451 try later" }, false},
		{"451 on MAIL is retryable", func(f *fakeSMTP) { f.failMail = "451 greylisted" }, false},
		{"554 after DATA is permanent", func(f *fakeSMTP) { f.failData = "554 rejected" }, true},
		{"connection dropped after DATA is retryable", func(f *fakeSMTP) { f.dropAfterData = true }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeSMTP(t)
			c.set(f)
			err := mustProvider(t, smtpCfg(f)).Send(context.Background(), baseMsg())
			if err == nil {
				t.Fatal("want an error")
			}
			if got := errors.Is(err, ErrPermanent); got != c.permanent {
				t.Fatalf("permanent=%v want %v (err: %v)", got, c.permanent, err)
			}
		})
	}
}

func TestSMTP_ConnectionRefusedIsRetryable(t *testing.T) {
	f := newFakeSMTP(t)
	cfg := smtpCfg(f)
	_ = f.ln.Close()
	err := mustProvider(t, cfg).Send(context.Background(), baseMsg())
	if err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("want a retryable error, got %v", err)
	}
}

func TestSMTP_HangingServerHitsSendTimeout(t *testing.T) {
	f := newFakeSMTP(t)
	f.hang = true
	cfg := smtpCfg(f)
	cfg.SendTimeout = 300 * time.Millisecond
	start := time.Now()
	err := mustProvider(t, cfg).Send(context.Background(), baseMsg())
	if err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("want a retryable timeout error, got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("send did not honour its timeout: %v", d)
	}
}

func TestSMTP_ContextCancelStopsSend(t *testing.T) {
	f := newFakeSMTP(t)
	f.hang = true
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	start := time.Now()
	if err := mustProvider(t, smtpCfg(f)).Send(ctx, baseMsg()); err == nil {
		t.Fatal("want an error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancel did not interrupt the send")
	}
}

func TestSMTP_StartTLSAndAuth(t *testing.T) {
	f := newFakeSMTP(t)
	f.advertiseSTARTTLS, f.advertiseAuth, f.requireTLSForMail = true, true, true
	cfg := smtpCfg(f)
	cfg.StartTLS, cfg.Username, cfg.Password = true, "svc", "s3cret"
	p := mustProvider(t, cfg, WithTLSConfig(&tls.Config{RootCAs: f.caPool}))
	if err := p.Send(context.Background(), baseMsg()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msgs := f.Messages()
	if len(msgs) != 1 || !msgs[0].tls {
		t.Fatalf("message must arrive over TLS: %+v", msgs)
	}
	if a := f.Auths(); len(a) != 1 || a[0] != "\x00svc\x00s3cret" {
		t.Fatalf("auth: %q", a)
	}
}

func TestSMTP_StartTLSRequestedButNotOffered_NeverFallsBackToCleartext(t *testing.T) {
	f := newFakeSMTP(t) // advertises no STARTTLS
	cfg := smtpCfg(f)
	cfg.StartTLS = true
	err := mustProvider(t, cfg).Send(context.Background(), baseMsg())
	if err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("want a STARTTLS error, got %v", err)
	}
	if len(f.Messages()) != 0 {
		t.Fatal("a message was sent in cleartext after the TLS upgrade failed")
	}
}

func TestSMTP_StartTLSUntrustedCertificateFails(t *testing.T) {
	f := newFakeSMTP(t)
	f.advertiseSTARTTLS = true
	cfg := smtpCfg(f)
	cfg.StartTLS = true
	// No RootCAs override: the throwaway CA is not trusted.
	if err := mustProvider(t, cfg).Send(context.Background(), baseMsg()); err == nil {
		t.Fatal("want a certificate verification error")
	}
	if len(f.Messages()) != 0 {
		t.Fatal("message sent despite failed verification")
	}
}

func TestSMTP_CredentialsNeverSentOverCleartext(t *testing.T) {
	cfg := config.SMTPConfig{Host: "127.0.0.1", Port: 1025, From: "a@b.co", Username: "u", Password: "p"}
	if _, err := NewSMTPProvider(cfg); err == nil || !strings.Contains(err.Error(), "cleartext") {
		t.Fatalf("constructor must refuse, got %v", err)
	}

	// With AllowInsecureAuth the operator accepted it explicitly.
	f := newFakeSMTP(t)
	f.advertiseAuth = true
	cfg2 := smtpCfg(f)
	cfg2.Username, cfg2.Password, cfg2.AllowInsecureAuth = "u", "p", true
	if err := mustProvider(t, cfg2).Send(context.Background(), baseMsg()); err != nil {
		t.Fatalf("explicitly allowed cleartext auth should work: %v", err)
	}
	if len(f.Auths()) != 1 {
		t.Fatal("credentials were not sent although explicitly allowed")
	}
}

func TestSMTP_MisconfigurationRejected(t *testing.T) {
	for name, cfg := range map[string]config.SMTPConfig{
		"no host":        {Port: 25, From: "a@b.co"},
		"bad from":       {Host: "h", Port: 25, From: "Evil <a@b.co>"},
		"user no pass":   {Host: "h", Port: 25, From: "a@b.co", Username: "u", StartTLS: true},
		"port too large": {Host: "h", Port: 99999, From: "a@b.co"},
	} {
		if _, err := NewSMTPProvider(cfg); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// Header injection: untrusted subject and display name must never add a
// header, a recipient, or end the header block, however the CR/LF arrives.
func TestSMTP_HeaderInjectionIsNeutralised(t *testing.T) {
	attacks := []string{
		"Ada\r\nBcc: victim@evil.test",
		"Ada\nBcc: victim@evil.test",
		"Ada\rBcc: victim@evil.test",
		"Ada Bcc: victim@evil.test",
		"Ada\r\n\r\nInjected body",
		"Ada\x00Bcc: victim@evil.test",
	}
	for _, a := range attacks {
		t.Run(strings.NewReplacer("\r", "CR", "\n", "LF", "\x00", "NUL", " ", "LS").Replace(a), func(t *testing.T) {
			f := newFakeSMTP(t)
			m := baseMsg()
			m.ToName, m.Subject, m.HTML = a, "Welcome "+a, ""
			if err := mustProvider(t, smtpCfg(f)).Send(context.Background(), m); err != nil {
				t.Fatalf("Send: %v", err)
			}
			msgs := f.Messages()
			if len(msgs) != 1 {
				t.Fatalf("want 1 message, got %d", len(msgs))
			}
			if len(msgs[0].to) != 1 {
				t.Fatalf("recipients widened: %v", msgs[0].to)
			}
			parsed, err := mail.ReadMessage(strings.NewReader(msgs[0].data))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			for _, h := range []string{"Bcc", "Cc"} {
				if parsed.Header.Get(h) != "" {
					t.Fatalf("injected header %s present: %q", h, parsed.Header.Get(h))
				}
			}
			head := msgs[0].data[:strings.Index(msgs[0].data, "\r\n\r\n")]
			if n := strings.Count(head, "\r\n"); n != 7 {
				t.Fatalf("header block has %d lines, want 8 (injection added or ended headers): %q", n+1, head)
			}
			if body := msgs[0].data[len(head):]; strings.Contains(body, "Injected body") {
				t.Fatalf("injected text became the body: %q", body)
			}
		})
	}
}

func TestSMTP_RecipientAddressInjectionRefused(t *testing.T) {
	f := newFakeSMTP(t)
	for _, to := range []string{"a@b.co\r\nRCPT TO:<x@y.co>", "a@b.co>\r\nBcc: x@y.co", "a@b.co,x@y.co", ""} {
		m := baseMsg()
		m.To = to
		err := mustProvider(t, smtpCfg(f)).Send(context.Background(), m)
		if !errors.Is(err, ErrPermanent) {
			t.Errorf("to=%q: want ErrPermanent, got %v", to, err)
		}
	}
	if len(f.Commands()) != 0 {
		t.Fatalf("a malformed recipient reached the wire: %v", f.Commands())
	}
}

// A body line that is just "." must not end DATA early (SMTP smuggling).
func TestSMTP_BodyDotLineDoesNotTerminateData(t *testing.T) {
	f := newFakeSMTP(t)
	m := baseMsg()
	m.HTML = ""
	m.Text = "line one\r\n.\r\nQUIT\r\nMAIL FROM:<evil@x.co>\r\n"
	if err := mustProvider(t, smtpCfg(f)).Send(context.Background(), m); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := len(f.Messages()); n != 1 {
		t.Fatalf("want exactly 1 message, got %d", n)
	}
	for _, c := range f.Commands() {
		if strings.Contains(c, "evil@x.co") {
			t.Fatalf("body content was interpreted as an SMTP command: %q", c)
		}
	}
}
