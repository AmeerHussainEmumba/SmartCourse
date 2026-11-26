package config

import "testing"

// The workflow and the activities have their own queues (publishing contract):
// TEMPORAL_TASK_QUEUE is the workflow queue, TEMPORAL_ACTIVITY_QUEUE the
// activity queue, and each can be overridden on its own.
func TestLoad_Temporal_QueueDefaultsAndOverrides(t *testing.T) {
	setValidEnv(t)
	t.Setenv("TEMPORAL_TASK_QUEUE", "")
	t.Setenv("TEMPORAL_ACTIVITY_QUEUE", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Temporal.TaskQueue != "publish-workflows" || cfg.Temporal.ActivityQueue != "course-activities" {
		t.Fatalf("defaults = %q, %q", cfg.Temporal.TaskQueue, cfg.Temporal.ActivityQueue)
	}

	t.Setenv("TEMPORAL_TASK_QUEUE", "wf")
	t.Setenv("TEMPORAL_ACTIVITY_QUEUE", "act")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Temporal.TaskQueue != "wf" || cfg.Temporal.ActivityQueue != "act" {
		t.Fatalf("overrides = %q, %q", cfg.Temporal.TaskQueue, cfg.Temporal.ActivityQueue)
	}
}
