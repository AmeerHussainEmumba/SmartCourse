package notification

import (
	"errors"
	"strings"
	"testing"
)

func TestRender_EveryKindHasTextAndHTML(t *testing.T) {
	for _, kind := range []string{KindWelcome, KindEnrollmentConfirm, KindCourseCompleted, KindCoursePublished, KindCertificateIssued, KindPublishFailed} {
		subject, text, html, err := Render(kind, TemplateData{Name: "Ada", CourseTitle: "Go 101"})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if subject == "" || !strings.Contains(text, "Ada") || !strings.Contains(html, "<p>") {
			t.Errorf("%s: incomplete render: %q %q %q", kind, subject, text, html)
		}
	}
}

func TestRender_HTMLPartEscapesUserControlledValues(t *testing.T) {
	_, _, html, err := Render(KindEnrollmentConfirm, TemplateData{
		Name:        `<script>alert(1)</script>`,
		CourseTitle: `<img src=x onerror=alert(2)>`,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"<script>", "<img", "onerror=alert(2)>"} {
		if strings.Contains(html, bad) {
			t.Fatalf("unescaped %q in HTML: %s", bad, html)
		}
	}
	// The allowlist strips the markup characters before html/template ever
	// sees them, so neither the raw nor the escaped form is present.
	if strings.Contains(html, "&lt;") || strings.Contains(html, "&gt;") {
		t.Fatalf("markup characters survived the allowlist: %s", html)
	}
}

func TestRender_SubjectCannotCarryLineBreaks(t *testing.T) {
	subject, text, _, err := Render(KindCourseCompleted, TemplateData{
		Name: "Ada", CourseTitle: "Go\r\nBcc: victim@evil.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(subject, "\r\n") {
		t.Fatalf("subject has a line break: %q", subject)
	}
	if strings.Contains(text, "Go\r\nBcc") || strings.Contains(text, "Go\nBcc") {
		t.Fatalf("title kept its line break in the text part: %q", text)
	}
}

func TestRender_FallbacksAndLengthCap(t *testing.T) {
	_, text, _, err := Render(KindEnrollmentConfirm, TemplateData{Name: "  \r\n ", CourseTitle: ""})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "Hi there,") || !strings.Contains(text, "your course") {
		t.Fatalf("missing fallbacks: %q", text)
	}
	_, long, _, _ := Render(KindWelcome, TemplateData{Name: strings.Repeat("x", 5000)})
	if len(long) > 1000 {
		t.Fatalf("name was not capped: %d bytes", len(long))
	}
}

func TestRender_PublishFailed_ReasonIsSanitisedClippedAndNeverInTheSubject(t *testing.T) {
	subject, text, html, err := Render(KindPublishFailed, TemplateData{
		Name: "Grace", CourseTitle: "Go", FailedActivity: "VerifyAssets",
		Reason: "<script>x</script>\r\nBcc: victim@evil.test " + strings.Repeat("y", 2000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(subject, "script") || strings.ContainsAny(subject, "\r\n") {
		t.Fatalf("subject carries the reason or a line break: %q", subject)
	}
	if strings.Contains(html, "<script") || strings.Contains(html, "&lt;") {
		t.Fatalf("reason carries markup: %s", html)
	}
	if strings.Contains(text, "@") {
		t.Fatalf("reason carries an address: %q", text)
	}
	if strings.Contains(text, "\nBcc:") || len(text) > 1500 {
		t.Fatalf("reason kept a line break or was not clipped (%d bytes): %q", len(text), text)
	}
	_, bare, _, err := Render(KindPublishFailed, TemplateData{Name: "Grace", CourseTitle: "Go"})
	if err != nil || strings.Contains(bare, "at step") || strings.Contains(bare, ": .") {
		t.Fatalf("no activity or reason must render cleanly: %q (%v)", bare, err)
	}
}

func TestRender_UnknownKindIsPermanent(t *testing.T) {
	if _, _, _, err := Render("nope", TemplateData{}); !errors.Is(err, ErrPermanent) {
		t.Fatalf("want ErrPermanent, got %v", err)
	}
}
