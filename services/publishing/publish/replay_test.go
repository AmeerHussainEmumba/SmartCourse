// Replay (determinism) test. Temporal's test environment does not expose the
// history it generates and no Temporal server is available to this module's
// tests, so the history below is BUILT here, event by event, the way a server
// records a successful publish on the wire: the workflow type, the input
// payload, every ActivityTaskScheduled with its activity type NAME and task
// queue. The SDK's replayer then re-executes PublishCourseWorkflow against it
// and fails on any nondeterminism: a renamed or reordered activity, a changed
// task queue, an added or removed step, a changed input encoding. That is the
// guarantee a deploy needs for workflows already running. It is a hand-built
// (not recorded) history: replace it with a captured one from a real server
// when the E2E suite (Wave C) can export one.
package publish

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

func timeProto(t time.Time) *timestamppb.Timestamp       { return timestamppb.New(t) }
func durationProto(d time.Duration) *durationpb.Duration { return durationpb.New(d) }

// historyBuilder appends events with consecutive ids.
type historyBuilder struct {
	t      *testing.T
	events []*historypb.HistoryEvent
	now    time.Time
	dc     converter.DataConverter
}

func (b *historyBuilder) add(typ enumspb.EventType, attrs func(*historypb.HistoryEvent)) int64 {
	b.now = b.now.Add(time.Millisecond)
	ev := &historypb.HistoryEvent{EventId: int64(len(b.events) + 1), EventTime: timeProto(b.now), EventType: typ}
	attrs(ev)
	b.events = append(b.events, ev)
	return ev.EventId
}

func (b *historyBuilder) payloads(args ...any) *commonpb.Payloads {
	p, err := b.dc.ToPayloads(args...)
	if err != nil {
		b.t.Fatalf("encode payloads: %v", err)
	}
	return p
}

// workflowTask records the scheduled/started/completed trio and returns the
// completed event's id (the one commands refer to).
func (b *historyBuilder) workflowTask() int64 {
	sched := b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue: &taskqueuepb.TaskQueue{Name: publishing.WorkflowQueue}, StartToCloseTimeout: durationProto(10 * time.Second), Attempt: 1,
		}}
	})
	started := b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: sched}}
	})
	return b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: sched, StartedEventId: started}}
	})
}

// activity records a scheduled activity; completeActivity records its result.
func (b *historyBuilder) schedule(wftCompleted int64, name, queue string, args ...any) int64 {
	return b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId:                   strconv.Itoa(len(b.events) + 1),
			ActivityType:                 &commonpb.ActivityType{Name: name},
			TaskQueue:                    &taskqueuepb.TaskQueue{Name: queue},
			Input:                        b.payloads(args...),
			StartToCloseTimeout:          durationProto(30 * time.Second),
			WorkflowTaskCompletedEventId: wftCompleted,
		}}
	})
}

func (b *historyBuilder) complete(scheduled int64, result *commonpb.Payloads) {
	started := b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: scheduled, Attempt: 1}}
	})
	b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: scheduled, StartedEventId: started, Result: result}}
	})
}

// successHistory is the full history of a publish that succeeds.
func successHistory(t *testing.T, in publishing.PublishWorkflowInput) *historypb.History {
	b := &historyBuilder{t: t, now: time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC), dc: converter.GetDefaultDataConverter()}
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: publishing.WorkflowName},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: publishing.WorkflowQueue},
			Input:        b.payloads(in),
			Attempt:      1,
		}}
	})
	q := in.ActivityQueueOrDefault()
	outline := publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1, Slug: "intro", Title: in.Title}

	w1 := b.workflowTask()
	s := b.schedule(w1, publishing.ActivityValidateMetadata, q, in.VersionID)
	b.complete(s, nil)

	w2 := b.workflowTask()
	sv := b.schedule(w2, publishing.ActivityVerifyAssets, q, in.VersionID)
	sc := b.schedule(w2, publishing.ActivityComputeMetadata, q, in.VersionID)
	b.complete(sv, nil)
	b.complete(sc, nil)

	w3 := b.workflowTask()
	so := b.schedule(w3, publishing.ActivityBuildOutline, q, in.CourseID, in.VersionID)
	b.complete(so, b.payloads(&outline))

	w4 := b.workflowTask()
	si := b.schedule(w4, publishing.ActivityIndexSearch, q, outline)
	sw := b.schedule(w4, publishing.ActivityWarmCache, q, outline)
	b.complete(si, nil)
	b.complete(sw, nil)

	w5 := b.workflowTask()
	sp := b.schedule(w5, publishing.ActivityPromoteAndPublish, q, in.CourseID, in.VersionID, in.OldVersionID, in.Title)
	b.complete(sp, nil)

	w6 := b.workflowTask()
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{WorkflowTaskCompletedEventId: w6}}
	})
	return &historypb.History{Events: b.events}
}

func replay(t *testing.T, h *historypb.History) error {
	t.Helper()
	r := worker.NewWorkflowReplayer()
	Register(r)
	return r.ReplayWorkflowHistory(nil, h)
}

func TestReplay_SuccessHistory_IsDeterministic(t *testing.T) {
	in := publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "Intro to Go"}
	if err := replay(t, successHistory(t, in)); err != nil {
		t.Fatalf("replay of the success history: %v", err)
	}
}

func TestReplay_StarterQueueOverride_IsDeterministic(t *testing.T) {
	old := uuid.New()
	in := publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), OldVersionID: &old, Title: "T", ActivityQueue: "custom-activities"}
	if err := replay(t, successHistory(t, in)); err != nil {
		t.Fatalf("replay with an activity queue override: %v", err)
	}
}

// The replayer must actually be able to fail: a history recorded for a
// different activity name (what a rename would look like) is rejected.
func TestReplay_RenamedActivityInHistory_IsRejected(t *testing.T) {
	in := publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "T"}
	h := successHistory(t, in)
	for _, e := range h.Events {
		if a := e.GetActivityTaskScheduledEventAttributes(); a != nil && a.ActivityType.Name == publishing.ActivityWarmCache {
			a.ActivityType.Name = "WarmTheCache"
		}
	}
	if err := replay(t, h); err == nil {
		t.Fatal("replay accepted a history whose activity name differs from the workflow's")
	}
}
