package events

import "testing"

// The catalogue is pinned by name so adding or dropping an event is a
// deliberate edit here, next to the RFC §6.5 comparison in the README.
func TestTypes_Catalogue(t *testing.T) {
	want := map[string]string{
		"identity.user_registered":      TopicIdentity,
		"identity.user_profile_updated": TopicIdentity,
		"identity.user_role_changed":    TopicIdentity,
		"course.course_published":       TopicCourse,
		"course.course_archived":        TopicCourse,
		"course.publish_failed":         TopicCourse,
		"enrollment.student_enrolled":   TopicEnrollment,
		"enrollment.student_withdrawn":  TopicEnrollment,
		"progress.lesson_completed":     TopicProgress,
		"progress.course_completed":     TopicProgress,
		"progress.certificate_issued":   TopicProgress,
	}
	got := map[string]string{}
	for _, ty := range Types() {
		if _, dup := got[ty.EventType]; dup {
			t.Errorf("%s listed twice", ty.EventType)
		}
		got[ty.EventType] = ty.Topic
	}
	for name, topic := range want {
		if got[name] != topic {
			t.Errorf("%s: topic %q, want %q (missing from Types()?)", name, got[name], topic)
		}
	}
	if len(got) != len(want) {
		t.Errorf("Types() has %d events, want %d", len(got), len(want))
	}
}
