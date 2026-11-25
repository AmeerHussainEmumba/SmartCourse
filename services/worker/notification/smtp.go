package notification

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

// SMTPProvider sends mail with the standard library's net/smtp. It targets
// the Mailpit sink in development (no auth, no TLS) and supports mandatory
// STARTTLS and PLAIN authentication when configured; there is deliberately
// no real mail provider (Week 3 decision D6). One connection per message:
// delivery volume here does not justify pooling, and it keeps a send's
// failure modes independent.
type SMTPProvider struct {
	cfg       config.SMTPConfig
	tlsConfig *tls.Config
	helo      string
}

// SMTPOption customises an SMTPProvider.
type SMTPOption func(*SMTPProvider)

// WithTLSConfig overrides the TLS settings used for STARTTLS (tests use it
// to trust a throwaway CA). ServerName defaults to the configured host and
// the minimum version to TLS 1.2.
func WithTLSConfig(c *tls.Config) SMTPOption { return func(p *SMTPProvider) { p.tlsConfig = c } }

// WithHelloName sets the name sent in EHLO (default "localhost").
func WithHelloName(name string) SMTPOption { return func(p *SMTPProvider) { p.helo = name } }

// NewSMTPProvider validates cfg the way config.Load does (so a hand-built
// config cannot bypass the cleartext-credentials rule) and returns a
// provider.
func NewSMTPProvider(cfg config.SMTPConfig, opts ...SMTPOption) (*SMTPProvider, error) {
	if !cfg.Configured() {
		return nil, errors.New("smtp: host is not configured")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, errors.New("smtp: port out of range")
	}
	a, err := mail.ParseAddress(cfg.From)
	if err != nil || a.Address != cfg.From {
		return nil, errors.New("smtp: from must be a bare email address")
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		return nil, errors.New("smtp: username and password must be set together")
	}
	if cfg.Username != "" && !cfg.StartTLS && !cfg.AllowInsecureAuth {
		return nil, errors.New("smtp: refusing to send credentials over a cleartext link (enable STARTTLS or AllowInsecureAuth)")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = 30 * time.Second
	}
	p := &SMTPProvider{cfg: cfg, helo: "localhost"}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

// Send delivers msg. SMTP 5xx replies and invalid messages wrap
// ErrPermanent; connection problems, timeouts and 4xx replies are plain
// errors the caller should retry.
//
// If the connection dies after the final "." of DATA was sent but before the
// reply arrived, the server may have accepted the message: that ambiguity is
// inherent to SMTP and is one source of the rare duplicate (README).
func (p *SMTPProvider) Send(ctx context.Context, msg Message) error {
	if err := msg.validate(); err != nil {
		return err
	}
	body, err := p.build(msg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.SendTimeout)
	defer cancel()
	addr := net.JoinHostPort(p.cfg.Host, strconv.Itoa(p.cfg.Port))
	d := net.Dialer{Timeout: p.cfg.ConnectTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	// net/smtp has no context support: closing the connection is what
	// unblocks it when ctx ends.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := p.converse(conn, msg, body); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("smtp send: %w", ctx.Err())
		}
		return classify(err)
	}
	return nil
}

func (p *SMTPProvider) converse(conn net.Conn, msg Message, body []byte) error {
	c, err := smtp.NewClient(conn, p.cfg.Host)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if err := c.Hello(p.helo); err != nil {
		return err
	}
	secure := false
	if p.cfg.StartTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			// Never fall back to cleartext when TLS was asked for.
			return errors.New("smtp: server does not offer STARTTLS")
		}
		tc := &tls.Config{}
		if p.tlsConfig != nil {
			tc = p.tlsConfig.Clone()
		}
		if tc.ServerName == "" {
			tc.ServerName = p.cfg.Host
		}
		if tc.MinVersion == 0 {
			tc.MinVersion = tls.VersionTLS12
		}
		if err := c.StartTLS(tc); err != nil {
			return err
		}
		secure = true
	}
	if p.cfg.Username != "" {
		if !secure && !p.cfg.AllowInsecureAuth {
			return errors.New("smtp: refusing to send credentials over a cleartext link")
		}
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("smtp: server does not offer AUTH")
		}
		// net/smtp's own PlainAuth refuses cleartext to non-localhost
		// hosts even when the operator allowed it, so this one trusts the
		// decision made above.
		if err := c.Auth(plainAuth{user: p.cfg.Username, pass: p.cfg.Password}); err != nil {
			return err
		}
	}
	if err := c.Mail(p.cfg.From); err != nil {
		return err
	}
	if err := c.Rcpt(msg.To); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	// The Data writer normalises line endings and dot-stuffs, so a body line
	// of "." cannot end the message early.
	if _, err := w.Write(body); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

// classify wraps 5xx replies as permanent.
func classify(err error) error {
	var te *textproto.Error
	if errors.As(err, &te) && te.Code >= 500 {
		return fmt.Errorf("%w: smtp %d %s", ErrPermanent, te.Code, te.Msg)
	}
	return fmt.Errorf("smtp send: %w", err)
}

type plainAuth struct{ user, pass string }

func (a plainAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "PLAIN", []byte("\x00" + a.user + "\x00" + a.pass), nil
}

func (a plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, errors.New("smtp: unexpected server challenge")
	}
	return nil, nil
}

// build renders the RFC 5322 message. Every header value that carries
// untrusted text is stripped of control characters first (cleanLine) and
// then encoded, so no input can add a header or end the header block.
// Header injection is the classic bug here; message_test.go pins it.
func (p *SMTPProvider) build(m Message) ([]byte, error) {
	// The display name is user-controlled and shows in the recipient's To line:
	// the same allowlist as the body (S-10).
	to := mail.Address{Name: DisplayName(m.ToName), Address: m.To}
	var b strings.Builder
	h := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	h("From", p.cfg.From)
	h("To", to.String())
	h("Subject", mime.QEncoding.Encode("utf-8", cleanLine(m.Subject)))
	h("Date", time.Now().UTC().Format(time.RFC1123Z))
	if m.ID != "" {
		domain := "smartcourse.local"
		if i := strings.LastIndexByte(p.cfg.From, '@'); i >= 0 {
			domain = p.cfg.From[i+1:]
		}
		h("Message-ID", "<"+m.ID+"@"+domain+">")
	}
	h("MIME-Version", "1.0")
	if m.HTML == "" {
		h("Content-Type", `text/plain; charset="utf-8"`)
		h("Content-Transfer-Encoding", "8bit")
		b.WriteString("\r\n" + m.Text)
		return []byte(b.String()), nil
	}
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("smtp: boundary: %w", err)
	}
	boundary := "sc-" + hex.EncodeToString(raw[:])
	h("Content-Type", `multipart/alternative; boundary="`+boundary+`"`)
	b.WriteString("\r\n")
	part := func(ctype, content string) {
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString("Content-Type: " + ctype + "; charset=\"utf-8\"\r\n")
		b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
		b.WriteString(content + "\r\n")
	}
	part("text/plain", m.Text)
	part("text/html", m.HTML)
	b.WriteString("--" + boundary + "--\r\n")
	return []byte(b.String()), nil
}
