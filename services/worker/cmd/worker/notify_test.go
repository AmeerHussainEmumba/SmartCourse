package main

import (
	"testing"
	"time"
)

func TestDeliveryTaskID_SameWithinABucketNewAfterIt(t *testing.T) {
	payload := []byte(`{"delivery_id":"11111111-1111-1111-1111-111111111111"}`)
	start := time.Unix(1_700_000_100, 0) // inside a bucket
	same := deliveryTaskID("notification:deliver", payload, start.Add(time.Minute))
	if deliveryTaskID("notification:deliver", payload, start) != same {
		t.Error("two enqueues in one bucket must share a task ID so the second is a duplicate")
	}
	if deliveryTaskID("notification:deliver", payload, start.Add(taskIDBucket)) == same {
		t.Error("the next bucket must get a new task ID so a row whose task was archived can be enqueued again")
	}
	other := []byte(`{"delivery_id":"22222222-2222-2222-2222-222222222222"}`)
	if deliveryTaskID("notification:deliver", other, start) == deliveryTaskID("notification:deliver", payload, start) {
		t.Error("different deliveries must never share a task ID")
	}
}
