// Adversarial security tests for notification templates and the SMTP provider
// (security tester, Week 3). The S-10 reproducer became an ordinary regression
// test when the fix landed. Run:
//
//	cd services/notification && go test -run 'Welcome|Adversarial' ./notification/
package notification

import (
	"net/mail"
	"strings"
	"testing"
)

// S-10 (fixed): the Welcome mail is sent to any address that registers (open
// registration, the address is never verified first) and greets the recipient
// with the attacker's free-text full_name, up to 200 characters, inside a
// message from the platform's own sender and domain. An unauthenticated caller
// can therefore make SmartCourse send "Hi <anything, incl. a phishing line and
// URL>" to any third-party mailbox, at the register limit (5/min/IP). The test
// asserts the rendered mail carries no URL from the name field.
func TestWelcomeMail_NeverCarriesAUserControlledURL(t *testing.T) {
	_, text, html, err := Render(KindWelcome, TemplateData{Name: "Your account is locked, sign in at https://evil.example/login within 24h"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "https://evil.example") || strings.Contains(html, "https://evil.example") {
		t.Fatalf("a stranger-controlled URL is delivered inside the platform's own Welcome mail:\n%s", text)
	}
}

// HELD: every untrusted field, in every kind, reaches the wire only as an
// encoded header or an escaped HTML value. The built message is parsed back and
// must have exactly the headers the provider wrote.
func TestAdversarial_Message_NoHeaderOrMIMEInjectionFromAnyField(t *testing.T) {
	p := &SMTPProvider{}
	p.cfg.From = "SmartCourse <noreply@smartcourse.local>"
	evil := []string{
		"x\r\nBcc: attacker@evil.example", "x\nBcc: attacker@evil.example", "x Bcc: a@b.c", "x\u0085Bcc: a@b.c",
		"x\r\n\r\n<script>alert(1)</script>", "\"><img src=x onerror=alert(1)>", "--sc-deadbeef\r\nContent-Type: text/html",
		"=?utf-8?B?Qg==?=\r\nBcc: a@b.c", "x\x00y", "<b>bold</b>", "{{.Name}}", "{{template \"x\"}}",
	}
	for _, kind := range []string{KindWelcome, KindEnrollmentConfirm, KindCourseCompleted, KindCoursePublished, KindCertificateIssued, KindPublishFailed} {
		for _, e := range evil {
			subj, text, html, err := Render(kind, TemplateData{Name: e, CourseTitle: e, FailedActivity: e, Reason: e})
			if err != nil {
				t.Fatalf("%s %q: %v", kind, e, err)
			}
			raw, err := p.build(Message{ID: "0b0e9c6a-0000-4000-8000-000000000000", To: "victim@example.com", ToName: e, Subject: subj, Text: text, HTML: html})
			if err != nil {
				t.Fatalf("%s %q build: %v", kind, e, err)
			}
			msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatalf("%s %q: message does not parse: %v", kind, e, err)
			}
			for h := range msg.Header {
				switch h {
				case "From", "To", "Subject", "Date", "Message-Id", "Mime-Version", "Content-Type", "Content-Transfer-Encoding":
				default:
					t.Errorf("%s %q: injected header %q", kind, e, h)
				}
			}
			if strings.Contains(html, "<script") || strings.Contains(html, "<img") || strings.Contains(html, "<b>") {
				t.Errorf("%s %q: unescaped markup in the HTML part: %s", kind, e, html)
			}
			if strings.Contains("\r\n"+string(raw), "\r\n--sc-deadbeef") || strings.Contains(string(raw), "\n--sc-deadbeef") {
				t.Errorf("%s %q: attacker text appears as a MIME boundary line", kind, e)
			}
		}
	}
}
