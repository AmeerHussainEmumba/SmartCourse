package notification

import (
	"regexp"
	"strings"
	"testing"
)

func TestDisplayName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Ada Lovelace", "Ada Lovelace"},
		{"Zoë Ångström-Müller", "Zoë Ångström-Müller"},
		{"李小龙", "李小龙"},
		{"José   O'Brien Jr.", "José O'Brien Jr."},
		{"Anne-Marie", "Anne-Marie"},
		{"Ada\r\nBcc: v@evil.test", "Ada Bcc"}, // the address word is dropped whole
		{"<b>Ada</b>", "bAdab"},
		{"Ada 123", "Ada"},
		{"Your account is locked, sign in at https://evil.example/login within 24h", "Your account is locked sign in at within h"},
		{"visit evil.example now", "visit evil example now"},
		{"www.evil.example", ""},
		{"click HTTP://EVIL.EXAMPLE", "click"},
		{"user@evil.example", ""},
		{"j.r.r. tolkien", "j r r. tolkien"},
		{"---", ""},
		{"...", ""},
		{"", ""},
		{"   \t\n", ""},
		{"\x00\x01", ""},
	} {
		if got := DisplayName(tc.in); got != tc.want {
			t.Errorf("DisplayName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDisplayName_CapsAtSixtyCharacters(t *testing.T) {
	got := DisplayName(strings.Repeat("é", 500))
	if n := len([]rune(got)); n != 60 {
		t.Errorf("length = %d, want 60", n)
	}
	if got := DisplayName(strings.Repeat("ab ", 100)); len([]rune(got)) > 60 || strings.HasSuffix(got, " ") {
		t.Errorf("not capped or not trimmed: %q", got)
	}
}

func TestSafeCourseTitle(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Go 101: Basics & Beyond (2026)", "Go 101: Basics & Beyond (2026)"},
		{"CI/CD for 24/7 teams", "CI/CD for 24/7 teams"},
		{"Python 3.12 in 10 days!", "Python 3.12 in 10 days!"},
		{"Node.js basics", "Node js basics"},
		{"Free course at https://evil.example/x now", "Free course at now"},
		{"Free course at evil.example/x now", "Free course at evil example/x now"},
		{"Mail me: a@b.c today", "Mail me: today"},
		{"Go\r\nBcc: victim@evil.test", "Go Bcc:"},
		{"<img src=x onerror=alert(2)>", "img srcx onerroralert(2)"},
		{"", ""},
	} {
		if got := SafeCourseTitle(tc.in); got != tc.want {
			t.Errorf("SafeCourseTitle(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Whatever the input, the output never contains something a mail client could
// linkify or an attacker could use to deliver a destination.
func TestSanitizers_NeverEmitURLLikeText(t *testing.T) {
	domainLike := regexp.MustCompile(`[\p{L}\p{N}]\.[\p{L}\p{N}]`)
	inputs := []string{
		"https://evil.example/login", "HTTPS://EVIL.EXAMPLE", "http://1.2.3.4/x", "ftp://x.y", "//evil.example/a",
		"evil.example", "evil.example/path?q=1", "a.b.c.d", "www.evil.example", "WWW.EVIL.EXAMPLE", "x@evil.example",
		"mailto:x@evil.example", "evil。example", "evil．example", "evil․example", "ev<b>il</b>.example",
		"evil.<>example", "h\u200bttps://evil.example", "bit.ly/3abc", "1.2.3.4", "javascript:alert(1)//evil.example",
		strings.Repeat("a.", 100), "ex ample.com", "evil.example.", ".evil.example", "https∶//evil.example",
	}
	for name, f := range map[string]func(string) string{
		"DisplayName": DisplayName, "SafeCourseTitle": SafeCourseTitle, "safeReason": safeReason, "safeActivity": safeActivity,
	} {
		for _, in := range inputs {
			out := f(in)
			l := strings.ToLower(out)
			switch {
			case strings.Contains(l, "://"), strings.Contains(l, "@"), strings.HasPrefix(l, "www."), strings.Contains(l, " www."):
				t.Errorf("%s(%q) = %q looks like a URL or address", name, in, out)
			}
			// Digits on both sides of a dot are a version number, not a host.
			for _, m := range domainLike.FindAllString(out, -1) {
				r := []rune(m)
				digits := r[0] >= '0' && r[0] <= '9' && r[2] >= '0' && r[2] <= '9'
				if !digits || name == "DisplayName" {
					t.Errorf("%s(%q) = %q keeps domain-like text %q", name, in, out, m)
				}
			}
		}
	}
}

// S-10 end to end through Render: the phishing line and URL do not reach any
// kind's text or HTML part, and the greeting keeps working.
func TestRender_UserControlledTextNeverCarriesAURL(t *testing.T) {
	evil := "Your account is locked, sign in at https://evil.example/login or evil.example within 24h"
	for _, kind := range []string{KindWelcome, KindEnrollmentConfirm, KindCourseCompleted, KindCoursePublished, KindCertificateIssued, KindPublishFailed} {
		subject, text, html, err := Render(kind, TemplateData{Name: evil, CourseTitle: evil, FailedActivity: evil, Reason: evil})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		for _, part := range []string{subject, text, html} {
			if strings.Contains(part, "evil.example") || strings.Contains(part, "://") || strings.Contains(part, "https") {
				t.Errorf("%s: a user-controlled URL reached the mail: %q", kind, part)
			}
		}
	}
	_, text, _, _ := Render(KindWelcome, TemplateData{Name: "https://evil.example/login"})
	if !strings.Contains(text, "Hi there,") {
		t.Errorf("a name that is only a URL must fall back to the neutral greeting: %q", text)
	}
}

// The recipient's display name is also shown in the To line; it gets the same
// treatment in the SMTP provider.
func TestSMTPBuild_ToDisplayNameIsSanitised(t *testing.T) {
	p := &SMTPProvider{}
	p.cfg.From = "SmartCourse <noreply@smartcourse.local>"
	raw, err := p.build(Message{ID: "0b0e9c6a-0000-4000-8000-000000000000", To: "victim@example.com", ToName: "Sign in at https://evil.example now", Subject: "s", Text: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "evil") {
		t.Errorf("the To header carries the attacker's URL:\n%s", raw)
	}
}
