// Fuzzing of the request DTOs' JSON binding + validation (area D). No Docker:
// the point is that arbitrary bytes never panic the binder and that anything
// that binds satisfies the documented constraints.
package securitytest_test

import (
	"testing"

	"github.com/gin-gonic/gin/binding"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

func FuzzBindCourseDTOs(f *testing.F) {
	for _, s := range []string{
		`{"title":"T","slug":"a-b"}`,
		`{"title":"T","slug":"a-b","level":"beginner","enrollment_limit":5,"category":"x"}`,
		`{"title":"x","tags":["a","b"]}`,
		`{"title":"L","position":1,"content_type":"text","content":{"a":{"b":[1,2,3]}},"duration_s":3}`,
		`{"course_id":"0192f0aa-0000-7000-8000-000000000001"}`,
		`{}`, `null`, `[]`, `{"title":`, "\xef\xbb\xbf{}", `{"title":"\u0000"}`, `{"enrollment_limit":1e999}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var cc course.CreateCourseRequest
		if err := binding.JSON.BindBody(data, &cc); err == nil {
			if cc.Title == "" || len(cc.Title) > 200*4 || len(cc.Slug) == 0 || len(cc.Description) > 10000*4 {
				t.Fatalf("CreateCourseRequest accepted out-of-policy value: %+v", cc)
			}
			if cc.Level != nil && *cc.Level != "beginner" && *cc.Level != "intermediate" && *cc.Level != "advanced" {
				t.Fatalf("bad level accepted: %q", *cc.Level)
			}
			if cc.EnrollmentLimit != nil && *cc.EnrollmentLimit < 1 {
				t.Fatalf("non-positive enrollment_limit accepted: %d", *cc.EnrollmentLimit)
			}
		}
		var ud course.UpdateDraftRequest
		if err := binding.JSON.BindBody(data, &ud); err == nil && len(ud.Tags) > 20 {
			t.Fatalf("more than 20 tags accepted: %d", len(ud.Tags))
		}
		var cl course.CreateLessonRequest
		if err := binding.JSON.BindBody(data, &cl); err == nil {
			switch cl.ContentType {
			case "video", "text", "pdf", "link":
			default:
				t.Fatalf("content_type %q accepted", cl.ContentType)
			}
			if cl.Position < 1 || cl.DurationS < 0 {
				t.Fatalf("position/duration out of policy: %+v", cl)
			}
		}
		var cm course.CreateModuleRequest
		if err := binding.JSON.BindBody(data, &cm); err == nil && cm.Position < 1 {
			t.Fatalf("module position %d accepted", cm.Position)
		}
		var er enrollment.EnrollRequest
		if err := binding.JSON.BindBody(data, &er); err == nil && len(er.CourseID) != 36 {
			t.Fatalf("course_id %q is not uuid-shaped", er.CourseID)
		}
	})
}
