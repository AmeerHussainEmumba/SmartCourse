package notification

import (
	"errors"
	"strings"
	"testing"
)

func TestRender_EveryKindHasTextAndHTML(t *testing.T) {
	for _, kind := range []string{KindWelcome, KindEnrollmentConfirm, KindCourseCompleted, KindCoursePublished} {
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
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Fatalf("expected escaped markup: %s", html)
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

func TestRender_UnknownKindIsPermanent(t *testing.T) {
	if _, _, _, err := Render("nope", TemplateData{}); !errors.Is(err, ErrPermanent) {
		t.Fatalf("want ErrPermanent, got %v", err)
	}
}
