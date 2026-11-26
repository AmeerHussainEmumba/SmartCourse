package eventstest

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	const ok = `{"enrollment_id":"6f1b6a3e-0000-4000-8000-000000000001","student_id":"6f1b6a3e-0000-4000-8000-000000000002","completed_at":"2026-11-26T10:00:00Z"}`
	cases := []struct {
		name, typ, payload, wantErr string
	}{
		{"conforming", "progress.course_completed", ok, ""},
		{"renamed field", "progress.course_completed", strings.Replace(ok, "student_id", "studentId", 1), "missing [student_id]"},
		{"extra field", "progress.course_completed", strings.Replace(ok, "{", `{"extra":1,`, 1), "not in the contract [extra]"},
		{"missing field", "progress.course_completed", `{"enrollment_id":"x","student_id":"y"}`, "missing [completed_at]"},
		{"wrong type", "progress.course_completed", strings.Replace(ok, `"2026-11-26T10:00:00Z"`, `12`, 1), "protojson rejects"},
		{"unknown event", "progress.nope", ok, "not in the libs/events catalogue"},
		{"not an object", "progress.course_completed", `[]`, "not a JSON object"},
	}
	for _, c := range cases {
		err := Check(c.typ, []byte(c.payload))
		if c.wantErr == "" && err != nil || c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}
