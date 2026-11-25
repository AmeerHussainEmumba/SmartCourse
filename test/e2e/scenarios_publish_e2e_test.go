//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// Operational scenarios: E12 to E14, on the full stack of test/e2e/harness.
// See the header of scenarios_pipeline_e2e_test.go.

// liveCourse is what the public GET /courses/{slug} serves.
type liveCourse struct {
	VersionID string
	Title     string
	Modules   int
	Lessons   int
}

func (l liveCourse) String() string {
	return fmt.Sprintf("version %s %q with %d modules and %d lessons", l.VersionID, l.Title, l.Modules, l.Lessons)
}

func fetchLive(courseURL, slug string) (liveCourse, error) {
	code, raw, err := harness.Do("GET", courseURL+"/api/v1/courses/"+slug, "", nil)
	return parseLive(code, raw, err)
}

func parseLive(code int, raw []byte, err error) (liveCourse, error) {
	if err != nil {
		return liveCourse{}, err
	}
	if code != http.StatusOK {
		return liveCourse{}, fmt.Errorf("HTTP %d %s", code, raw)
	}
	var d struct {
		Version struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		} `json:"version"`
		Modules []struct {
			Lessons []json.RawMessage `json:"lessons"`
		} `json:"modules"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return liveCourse{}, err
	}
	l := liveCourse{VersionID: d.Version.ID, Title: d.Version.Title, Modules: len(d.Modules)}
	for _, m := range d.Modules {
		l.Lessons += len(m.Lessons)
	}
	return l, nil
}

// TestE2E_E12_PublishUnderFault is E12, matrix row E12 (Phase 1 lease
// reconciliation; Phase 3 for the event side).
//
// Steps:
//  1. Real Temporal (the shared container), the worker a killable subprocess.
//     The instructor publishes version 1 (two lessons) to "ready". Note the live
//     version and fetch GET /courses/{slug}.
//  2. A version 2 draft is made. The API has no route that creates a draft of a
//     course that has a published version (PATCH, add-module and add-lesson
//     answer "no draft" once the draft pointer is cleared by the first
//     publish), so the test creates version 2 with SQL, copying version 1's
//     modules, as a later API would (PRODUCT GAP, reported; asserted below
//     as a log line, not as a failure of this scenario).
//  3. Fault A, an activity fault with no hook in the product: version 2 is
//     given modules but no lessons, so the ValidateMetadata activity (no retry)
//     fails. POST /courses/{id}/publish (202); wait for publish-status.
//  4. Fault B, a killed worker: the draft is repaired through the API (a
//     lesson is added), the worker is kill -9'd, publish is requested (202,
//     the workflow starts on Temporal with nobody to run it), an immediate
//     second publish is refused (409 publish_in_progress), and the workflow is
//     terminated in Temporal as a dead process leaves it. A new worker starts.
//  5. Fault C, retry completes it: a second course; the worker is killed, a
//     publish is requested, the worker is started again, and the workflow
//     that waited on Temporal runs to completion.
//
// Assertions:
//   - After Fault A: publish-status is "publish_failed" with the activity's own
//     message (not Temporal's wrapped text); GET /courses/{slug} still serves
//     version 1 unchanged (same version id and outline); the outbox holds no
//     course.published event for version 2.
//   - After Fault B: the claim does not block a republish for 15 minutes:
//     within the reconciliation interval plus the one-minute grace the stale
//     claim is released (publish-status publish_failed, the workflow not
//     running in Temporal) and a new POST /publish returns 202, not 409;
//     publish-status ends "ready" and GET /courses/{slug} now serves version 2.
//   - After Fault C: the course reaches "ready" without a second request.
//   - Throughout, the live version was never absent or half-promoted: the
//     public GET is polled every 100 ms and each response is a complete
//     version 1 or a complete version 2 (the right module and lesson counts).
//
// Deviation: the spec's worker "fault hook" and "reconciliation interval
// configured short for the test" do not exist (no test code in the product;
// the cron scheduler's minimum is one minute and the stale-claim grace is one
// minute), so the faults are the ones above and the wait for the release is
// up to about three minutes.
func TestE2E_E12_PublishUnderFault(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	inst := s.NewInstructor(t)
	course := s.NewCourse(t, inst, 2, 100)
	s.StartPublish(t, inst, course)
	s.WaitPublished(t, inst, course)
	s.WaitSettled(t, time.Minute)

	v1, err := fetchLive(s.CourseURL, course.Slug)
	if err != nil || v1.Lessons != 2 || v1.Modules != 1 {
		t.Fatalf("live course after the first publish: %v (err %v), want one module and two lessons", v1, err)
	}
	if _, err := fetchLive(s.CourseURL, course.Slug); err != nil {
		t.Fatal(err)
	}
	publishedEvents := func() int64 {
		return s.MustCount(t, `SELECT count(*) FROM platform.outbox WHERE event_type = 'course.course_published'
			AND convert_from(payload, 'UTF8')::jsonb ->> 'course_id' = ?`, course.ID.String())
	}
	if n := publishedEvents(); n != 1 {
		t.Fatalf("course.course_published events after the first publish: %d, want 1", n)
	}

	// Step 2: the API cannot make a version 2 draft.
	if code, body, _ := harness.Do("PATCH", s.CourseURL+"/api/v1/courses/"+course.ID.String(), inst.Token, map[string]any{"title": "v2"}); code == http.StatusOK {
		t.Logf("PATCH on a published course created or edited a draft (%s): the API can now start version 2, switch the test to it", body)
	} else {
		t.Logf("PRODUCT GAP confirmed: PATCH /courses/{id} on a published course answers HTTP %d %s: no API route makes a version 2 draft", code, strings.TrimSpace(string(body)))
	}
	v2Module := makeDraftV2(t, s, course, "Version 2")

	// The poller: the live version is always a complete v1 or a complete v2.
	var mu sync.Mutex
	var seen = map[string]int{}
	var violations []string
	var v2Lessons = -1 // the lesson count v2 will have once published
	stopPoll := make(chan struct{})
	var poll sync.WaitGroup
	poll.Add(1)
	go func() {
		defer poll.Done()
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(100 * time.Millisecond):
			}
			l, err := fetchLive(s.CourseURL, course.Slug)
			mu.Lock()
			switch {
			case err != nil:
				violations = append(violations, "the live course was not served: "+err.Error())
			case l.VersionID == v1.VersionID:
				seen["v1"]++
				if l.Modules != v1.Modules || l.Lessons != v1.Lessons || l.Title != v1.Title {
					violations = append(violations, "half-changed version 1: "+l.String())
				}
			default:
				seen["v2"]++
				if l.Modules != 1 || l.Lessons != v2Lessons || !strings.HasSuffix(l.Title, "Version 2") {
					violations = append(violations, fmt.Sprintf("incomplete version 2 (want 1 module, %d lessons): %s", v2Lessons, l))
				}
			}
			mu.Unlock()
		}
	}()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopPoll)
			poll.Wait()
		})
	}
	defer stop()

	// Step 3: Fault A, no lessons in the draft.
	s.StartPublish(t, inst, course)
	var status struct {
		State string  `json:"state"`
		Error *string `json:"error"`
	}
	harness.Eventually(t, 2*time.Minute, "publish-status publish_failed for the empty draft", func() error {
		status.State, status.Error = "", nil
		raw := mustGet(t, s.CourseURL+"/api/v1/courses/"+course.ID.String()+"/publish-status", inst.Token)
		if err := json.Unmarshal(raw, &status); err != nil {
			return err
		}
		if status.State != "publish_failed" {
			return harness.Errf("publish-status %q", status.State)
		}
		return nil
	})
	if status.Error == nil || !strings.Contains(*status.Error, "at least one lesson is required") {
		t.Errorf("publish-status error %v, want the validation activity's own message (at least one lesson is required)", status.Error)
	}
	if status.Error != nil {
		for _, noise := range []string{"activity error", "ActivityError", "scheduledEventID", "ApplicationError", "(type:"} {
			if strings.Contains(*status.Error, noise) {
				t.Errorf("publish-status error carries Temporal's wrapper text %q: %s", noise, *status.Error)
			}
		}
	}
	if l, err := fetchLive(s.CourseURL, course.Slug); err != nil || l != v1 {
		t.Errorf("after Fault A the live course is %v (err %v), want version 1 unchanged: %v", l, err, v1)
	}
	if n := publishedEvents(); n != 1 {
		t.Errorf("course.course_published events after Fault A: %d, want 1 (none for version 2)", n)
	}

	// Step 4: Fault B. Repair the draft through the API, then lose the worker.
	harness.Eventually(t, 10*time.Second, "a lesson to be added to the version 2 draft through the API", func() error {
		code, body, err := harness.Do("POST", s.CourseURL+"/api/v1/modules/"+v2Module.String()+"/lessons", inst.Token, map[string]any{
			"title": "Lesson 1 (v2)", "position": 1, "content_type": "text", "content": map[string]string{"body": "v2"}, "duration_s": 30,
		})
		if err != nil || code != http.StatusCreated {
			return harness.Errf("add lesson: HTTP %d %s (err %v)", code, body, err)
		}
		return nil
	})
	mu.Lock()
	v2Lessons = 1
	mu.Unlock()

	s.KillWorker()
	workflowID := publishRaw(t, s, inst, course)
	if st := s.PublishState(t, inst, course); st != "publishing" {
		t.Fatalf("publish-status %q right after the claim with the worker dead, want publishing", st)
	}
	if code, body, _ := harness.Do("POST", s.CourseURL+"/api/v1/courses/"+course.ID.String()+"/publish", inst.Token, nil); code != http.StatusConflict {
		t.Errorf("a second publish while the claim is fresh: HTTP %d %s, want 409", code, body)
	}
	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: s.TemporalAddr})
	if err != nil {
		t.Fatalf("dial temporal: %v", err)
	}
	defer tc.Close()
	if err := tc.TerminateWorkflow(context.Background(), workflowID, "", "e2e: the worker died mid-publish"); err != nil {
		t.Fatalf("terminate workflow %s: %v", workflowID, err)
	}
	s.StartWorker(t)
	claimed := time.Now()
	harness.Eventually(t, 4*time.Minute, "the reconciler to release the stale claim (publish_failed)", func() error {
		if st := s.PublishState(t, inst, course); st != "publish_failed" {
			return harness.Errf("publish-status %q", st)
		}
		return nil
	})
	t.Logf("the stale claim was released %s after it was taken (the lease is 15 minutes)", time.Since(claimed).Round(time.Second))
	if n := s.MustCount(t, `SELECT count(*) FROM course.course_versions WHERE course_id = ? AND state = 'publishing'`, course.ID); n != 0 {
		t.Errorf("%d versions are still publishing after the release", n)
	}
	harness.Eventually(t, time.Minute, "a republish to be accepted (202) after the release", func() error {
		code, body, err := harness.Do("POST", s.CourseURL+"/api/v1/courses/"+course.ID.String()+"/publish", inst.Token, nil)
		if err != nil || code != http.StatusAccepted {
			return harness.Errf("POST publish: HTTP %d %s (err %v), want 202", code, body, err)
		}
		return nil
	})
	s.WaitPublished(t, inst, course)
	harness.Eventually(t, 30*time.Second, "the live course to be version 2", func() error {
		l, err := fetchLive(s.CourseURL, course.Slug)
		if err != nil || l.VersionID == v1.VersionID || l.Lessons != 1 {
			return harness.Errf("live course %v (err %v), want the complete version 2", l, err)
		}
		return nil
	})
	if n := publishedEvents(); n != 2 {
		t.Errorf("course.course_published events after version 2: %d, want 2", n)
	}

	harness.Eventually(t, 15*time.Second, "the 100 ms poller to observe version 2 a few times", func() error {
		mu.Lock()
		defer mu.Unlock()
		if seen["v2"] < 5 {
			return harness.Errf("%d version 2 responses seen", seen["v2"])
		}
		return nil
	})
	stop()
	mu.Lock()
	t.Logf("live course polled every 100 ms: %d complete version 1 responses, %d complete version 2 responses", seen["v1"], seen["v2"])
	if len(violations) > 0 {
		t.Errorf("the live course was absent or half-promoted %d times; first: %s", len(violations), violations[0])
	}
	if seen["v1"] == 0 || seen["v2"] == 0 {
		t.Errorf("the poller saw %v: it must see both versions to prove the switch was whole", seen)
	}
	mu.Unlock()

	// Step 5: Fault C, the worker is down when the publish is requested and
	// returns before the claim goes stale: the workflow waited on Temporal.
	other := s.NewCourse(t, inst, 1, 100)
	s.StartPublish(t, inst, other)
	s.WaitPublished(t, inst, other)
	otherModule := makeDraftV2(t, s, other, "Retry")
	if code, body, err := harness.Do("POST", s.CourseURL+"/api/v1/modules/"+otherModule.String()+"/lessons", inst.Token, map[string]any{
		"title": "Lesson 1 (v2)", "position": 1, "content_type": "text", "content": map[string]string{"body": "v2"}, "duration_s": 30,
	}); err != nil || code != http.StatusCreated {
		t.Fatalf("add a lesson to the second course's draft: HTTP %d %s (err %v)", code, body, err)
	}
	s.KillWorker()
	publishRaw(t, s, inst, other)
	harness.Never(t, 3*time.Second, "the course staying in publishing while no worker runs", func() error {
		if st := s.PublishState(t, inst, other); st != "publishing" {
			return harness.Errf("publish-status %q with the worker dead, want publishing", st)
		}
		return nil
	})
	s.StartWorker(t)
	s.WaitPublished(t, inst, other)
	if l, err := fetchLive(s.CourseURL, other.Slug); err != nil || !strings.HasSuffix(l.Title, "Retry") || l.Lessons != 1 {
		t.Errorf("after Fault C the live course is %v (err %v), want the complete version 2", l, err)
	}
}

// makeDraftV2 creates the version 2 draft the API cannot create: a copy of the
// published version's modules (without lessons), the title suffixed, and the
// course's draft pointer set to it. It returns the id of the new module.
func makeDraftV2(t *testing.T, s *harness.Stack, c *harness.Course, titleSuffix string) uuid.UUID {
	t.Helper()
	var v1 uuid.UUID
	if err := s.DB().Raw(`SELECT published_version_id FROM course.courses WHERE id = ?`, c.ID).Row().Scan(&v1); err != nil {
		t.Fatalf("read the published version: %v", err)
	}
	v2, module := uuid.New(), uuid.New()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO course.course_versions (id, course_id, version_number, state, title, description, tags)
			SELECT ?, course_id, version_number + 1, 'draft', title || ' ' || ?, description, tags FROM course.course_versions WHERE id = ?`, []any{v2, titleSuffix, v1}},
		{`INSERT INTO course.modules (id, course_version_id, module_key, position, title)
			SELECT ?, ?, module_key, position, title FROM course.modules WHERE course_version_id = ? ORDER BY position LIMIT 1`, []any{module, v2, v1}},
		{`UPDATE course.courses SET draft_version_id = ? WHERE id = ?`, []any{v2, c.ID}},
	} {
		if err := s.DB().Exec(q.sql, q.args...).Error; err != nil {
			t.Fatalf("make the version 2 draft: %v", err)
		}
	}
	return module
}

// publishRaw requests a publish (202) and returns the workflow id.
func publishRaw(t *testing.T, s *harness.Stack, inst *harness.User, c *harness.Course) string {
	t.Helper()
	code, body, err := harness.Do("POST", s.CourseURL+"/api/v1/courses/"+c.ID.String()+"/publish", inst.Token, nil)
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("POST publish: HTTP %d %s (err %v), want 202", code, body, err)
	}
	var r struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal(body, &r); err != nil || r.WorkflowID == "" {
		t.Fatalf("publish response %s: no workflow id (%v)", body, err)
	}
	return r.WorkflowID
}

func mustGet(t *testing.T, url, token string) []byte {
	t.Helper()
	code, raw, err := harness.Do("GET", url, token, nil)
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET %s: HTTP %d %s (err %v)", url, code, raw, err)
	}
	return raw
}
