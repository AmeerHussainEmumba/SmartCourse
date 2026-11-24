package notification

import (
	"bytes"
	"fmt"
	htmltemplate "html/template"
	"strings"
	texttemplate "text/template"
)

// Delivery kinds. A kind picks the template; together with the event id and
// the recipient it is the delivery's identity (UNIQUE, migration 000001).
const (
	KindWelcome            = "welcome"
	KindEnrollmentConfirm  = "enrollment_confirmation"
	KindCourseCompleted    = "course_completed"
	KindCoursePublished    = "course_published"
	fallbackRecipientName  = "there"
	fallbackCourseTitle    = "your course"
	maxTemplateFieldLength = 200
)

// TemplateData is everything a template may interpolate. Both fields are
// untrusted (a user chose the name, an instructor chose the title).
type TemplateData struct {
	Name        string
	CourseTitle string
}

type tmpl struct {
	subject string // plain text; cleaned and encoded by the provider
	text    *texttemplate.Template
	html    *htmltemplate.Template
}

func newTmpl(subject, text, html string) tmpl {
	return tmpl{
		subject: subject,
		text:    texttemplate.Must(texttemplate.New("text").Parse(text)),
		html:    htmltemplate.Must(htmltemplate.New("html").Parse(html)),
	}
}

// The text templates use text/template and the HTML ones html/template, which
// escapes every interpolated value for its HTML context: a name like
// "<script>" is rendered inert in the HTML part. The plain-text part is not
// HTML, so it is left as typed; the provider is what guarantees header and
// line-ending safety (smtp.go).
var templates = map[string]tmpl{
	KindWelcome: newTmpl(
		"Welcome to SmartCourse",
		"Hi {{.Name}},\n\nWelcome to SmartCourse. Your account is ready.\n",
		"<p>Hi {{.Name}},</p><p>Welcome to SmartCourse. Your account is ready.</p>",
	),
	KindEnrollmentConfirm: newTmpl(
		"You are enrolled in {{SUBJECT_COURSE}}",
		"Hi {{.Name}},\n\nYou are now enrolled in {{.CourseTitle}}. Happy learning!\n",
		"<p>Hi {{.Name}},</p><p>You are now enrolled in <strong>{{.CourseTitle}}</strong>. Happy learning!</p>",
	),
	KindCourseCompleted: newTmpl(
		"Congratulations on completing {{SUBJECT_COURSE}}",
		"Hi {{.Name}},\n\nCongratulations, you completed {{.CourseTitle}}.\n",
		"<p>Hi {{.Name}},</p><p>Congratulations, you completed <strong>{{.CourseTitle}}</strong>.</p>",
	),
	KindCoursePublished: newTmpl(
		"{{SUBJECT_COURSE}} is now published",
		"Hi {{.Name}},\n\nYour course {{.CourseTitle}} is now published and open for enrollment.\n",
		"<p>Hi {{.Name}},</p><p>Your course <strong>{{.CourseTitle}}</strong> is now published and open for enrollment.</p>",
	),
}

// Render builds the message content for kind. An unknown kind is a
// permanent error: retrying cannot make a template appear.
func Render(kind string, d TemplateData) (subject, text, html string, err error) {
	t, ok := templates[kind]
	if !ok {
		return "", "", "", fmt.Errorf("%w: no template for kind %q", ErrPermanent, kind)
	}
	d.Name = clip(cleanLine(d.Name), maxTemplateFieldLength)
	if d.Name == "" {
		d.Name = fallbackRecipientName
	}
	d.CourseTitle = clip(cleanLine(d.CourseTitle), maxTemplateFieldLength)
	if d.CourseTitle == "" {
		d.CourseTitle = fallbackCourseTitle
	}
	var tb, hb bytes.Buffer
	if err := t.text.Execute(&tb, d); err != nil {
		return "", "", "", fmt.Errorf("%w: render text: %v", ErrPermanent, err)
	}
	if err := t.html.Execute(&hb, d); err != nil {
		return "", "", "", fmt.Errorf("%w: render html: %v", ErrPermanent, err)
	}
	return strings.ReplaceAll(t.subject, "{{SUBJECT_COURSE}}", d.CourseTitle), tb.String(), hb.String(), nil
}

func clip(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
