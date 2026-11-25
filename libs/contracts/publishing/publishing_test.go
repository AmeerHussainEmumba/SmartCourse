package publishing_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

// The names are wire format (workflow history, queue routing): pin them.
func TestContractValues(t *testing.T) {
	if publishing.WorkflowName != "PublishCourseWorkflow" {
		t.Errorf("WorkflowName = %q", publishing.WorkflowName)
	}
	if publishing.WorkflowQueue != "publish-workflows" || publishing.ActivityQueue != "course-activities" {
		t.Errorf("queues = %q, %q", publishing.WorkflowQueue, publishing.ActivityQueue)
	}
	if publishing.FailureType != "PublishFailure" {
		t.Errorf("FailureType = %q", publishing.FailureType)
	}
	want := []string{"ValidateMetadata", "VerifyAssets", "ComputeMetadata", "BuildOutline", "IndexSearch", "WarmCache", "PromoteAndPublish", "MarkPublishFailed"}
	if got := publishing.ActivityNames(); !slices.Equal(got, want) {
		t.Errorf("ActivityNames = %v, want %v", got, want)
	}
}

func TestActivityQueueOrDefault(t *testing.T) {
	if got := (publishing.PublishWorkflowInput{}).ActivityQueueOrDefault(); got != publishing.ActivityQueue {
		t.Errorf("empty input queue = %q", got)
	}
	if got := (publishing.PublishWorkflowInput{ActivityQueue: "other"}).ActivityQueueOrDefault(); got != "other" {
		t.Errorf("explicit queue = %q", got)
	}
}

// An input written before ActivityQueue existed (a workflow already running)
// must still decode, and the new field must not appear in what a default
// starter writes.
func TestWorkflowInputJSONCompatibility(t *testing.T) {
	id := uuid.New()
	old := `{"CourseID":"` + id.String() + `","VersionID":"` + id.String() + `","OldVersionID":null,"Title":"T"}`
	var in publishing.PublishWorkflowInput
	if err := json.Unmarshal([]byte(old), &in); err != nil {
		t.Fatal(err)
	}
	if in.CourseID != id || in.Title != "T" || in.ActivityQueue != "" {
		t.Fatalf("decoded %+v", in)
	}
	b, _ := json.Marshal(publishing.PublishWorkflowInput{CourseID: id, VersionID: id, Title: "T"})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, ok := m["ActivityQueue"]; ok {
		t.Errorf("default input serialises ActivityQueue: %s", b)
	}
}

func TestOutlineRoundTrip(t *testing.T) {
	cat := "dev"
	o := publishing.PublishOutline{CourseID: uuid.New(), VersionID: uuid.New(), VersionNumber: 2, Slug: "s", Title: "T",
		Category: &cat, Tags: []string{"a"}, EnrollmentCount: 3, DetailJSON: []byte(`{"x":1}`)}
	b, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	var got publishing.PublishOutline
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(o, got) {
		t.Fatalf("round trip changed the outline:\n%+v\n%+v", o, got)
	}
}
