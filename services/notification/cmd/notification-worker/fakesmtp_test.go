//go:build integration

package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSMTP is a minimal in-process SMTP server with failure injection. It
// speaks just enough of RFC 5321 for net/smtp: EHLO, STARTTLS, AUTH PLAIN,
// MAIL, RCPT, DATA, QUIT.
type fakeSMTP struct {
	ln       net.Listener
	tlsConf  *tls.Config
	caPool   *x509.CertPool
	once     sync.Once
	mu       sync.Mutex
	messages []fakeMessage
	auths    []string // decoded AUTH PLAIN payloads, "authzid\x00user\x00pass"
	commands []string

	// Knobs (set before traffic).
	advertiseSTARTTLS bool
	advertiseAuth     bool
	failMail          string // reply to MAIL, e.g. "451 try later"
	failRcpt          string
	failData          string // reply after the final "."
	dropAfterData     bool   // close the connection instead of replying after the final "."
	hang              bool   // accept, then never speak
	requireTLSForMail bool
}

type fakeMessage struct {
	from string
	to   []string
	data string
	tls  bool
}

func newFakeSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeSMTP{ln: ln}
	f.tlsConf, f.caPool = selfSignedTLS(t)
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

// hostPort also starts serving: the knobs are set before the first call, and
// starting the goroutine here gives the race detector a happens-before edge.
func (f *fakeSMTP) hostPort() (string, int) {
	f.once.Do(func() { go f.serve() })
	a := f.ln.Addr().(*net.TCPAddr)
	return a.IP.String(), a.Port
}

func (f *fakeSMTP) Messages() []fakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeMessage(nil), f.messages...)
}

func (f *fakeSMTP) Auths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.auths...)
}

func (f *fakeSMTP) Commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commands...)
}

func (f *fakeSMTP) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.session(c)
	}
}

func (f *fakeSMTP) session(c net.Conn) {
	defer func() { _ = c.Close() }()
	if f.hang {
		time.Sleep(10 * time.Second)
		return
	}
	var isTLS bool
	r := bufio.NewReader(c)
	say := func(s string) { _, _ = fmt.Fprintf(c, "%s\r\n", s) }
	say("220 fake ESMTP")
	var cur fakeMessage
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		f.mu.Lock()
		f.commands = append(f.commands, line)
		f.mu.Unlock()
		verb := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(verb, "EHLO"), strings.HasPrefix(verb, "HELO"):
			_, _ = fmt.Fprintf(c, "250-fake\r\n")
			if f.advertiseSTARTTLS && !isTLS {
				_, _ = fmt.Fprintf(c, "250-STARTTLS\r\n")
			}
			if f.advertiseAuth {
				_, _ = fmt.Fprintf(c, "250-AUTH PLAIN\r\n")
			}
			_, _ = fmt.Fprintf(c, "250 8BITMIME\r\n")
		case verb == "STARTTLS":
			say("220 go ahead")
			tc := tls.Server(c, f.tlsConf)
			if err := tc.Handshake(); err != nil {
				return
			}
			c, isTLS = tc, true
			r = bufio.NewReader(tc)
		case strings.HasPrefix(verb, "AUTH PLAIN"):
			raw, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(line[len("AUTH PLAIN"):]))
			f.mu.Lock()
			f.auths = append(f.auths, string(raw))
			f.mu.Unlock()
			say("235 ok")
		case strings.HasPrefix(verb, "MAIL FROM:"):
			if f.requireTLSForMail && !isTLS {
				say("530 must issue STARTTLS first")
				continue
			}
			if f.failMail != "" {
				say(f.failMail)
				continue
			}
			cur = fakeMessage{from: angle(line[len("MAIL FROM:"):]), tls: isTLS}
			say("250 ok")
		case strings.HasPrefix(verb, "RCPT TO:"):
			if f.failRcpt != "" {
				say(f.failRcpt)
				continue
			}
			cur.to = append(cur.to, angle(line[len("RCPT TO:"):]))
			say("250 ok")
		case verb == "DATA":
			say("354 end with .")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(strings.TrimPrefix(l, ".")) // undo dot-stuffing
			}
			if f.dropAfterData {
				return
			}
			if f.failData != "" {
				say(f.failData)
				continue
			}
			cur.data = b.String()
			f.mu.Lock()
			f.messages = append(f.messages, cur)
			f.mu.Unlock()
			say("250 queued")
		case verb == "QUIT":
			say("221 bye")
			return
		default:
			say("502 not implemented")
		}
	}
}

// selfSignedTLS returns a server config for 127.0.0.1 and a pool trusting it.
func selfSignedTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fake-smtp"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}, pool
}

// angle extracts the address from "<addr> PARAMS".
func angle(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '>'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimPrefix(s, "<")
}
