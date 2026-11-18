//go:build integration

package e2e_test

import "testing"

// Operational scenarios: E12 to E14. See the header of
// scenarios_pipeline_test.go for the rules every spec follows.

// TestE2E_E12_PublishUnderFault is E12, matrix row E12 (Phase 1 lease
// reconciliation; Phase 3 for the event side).
//
// Steps:
//  1. Real Temporal (testcontainer or dev server), worker as a killable
//     subprocess. Instructor publishes course version 1 (reaches "ready").
//     Note the live version id and fetch GET /courses/{slug} for later.
//  2. Instructor edits the draft (new lesson) so a version 2 draft exists.
//  3. Fault A, an activity fault: configure a test-only fault in the
//     PromoteAndPublish activity's predecessor (for example IndexSearch
//     returns a retryable error N+1 times, N being its retry budget).
//     POST /courses/{id}/publish (202). Wait for publish-status.
//  4. Fault B, a killed worker: remove the fault, publish again, and kill -9
//     the worker right after the claim and before the workflow's first
//     activity completes. Start a new worker.
//
// Assertions:
//   - After Fault A: publish-status is "publish_failed" with the activity's own
//     message (not Temporal's wrapped text); GET /courses/{slug} still serves
//     version 1 unchanged (same version id, same outline); no course.published
//     event for version 2 exists in the outbox.
//   - After Fault B: the claim does not block a republish for 15 minutes: within
//     one reconciliation interval (configured short for the test) the claim is
//     released (the workflow id is not running in Temporal) and a new POST
//     /publish returns 202, not 409; publish-status ends "ready" and GET
//     /courses/{slug} now serves version 2.
//   - Throughout, the live version was never absent or half-promoted (poll the
//     public GET every 100 ms and assert each response is a complete version 1
//     or a complete version 2).
//
// Helper needed: worker subprocess with a fault hook, real Temporal, the
// Phase 1 reconciliation (describe workflow, release stale claim).
func TestE2E_E12_PublishUnderFault(t *testing.T) {
	t.Skip("pending: Phase 1 - publish lease reconciliation using Temporal describe; fault hook for activities")
}

// TestE2E_E13_ArchiveAndAudit is E13, matrix row E13 (Phase 4).
//
// Steps:
//  1. Stack as E01 including MongoDB and the audit-archiver consumer.
//  2. Drive one example of every event type in RFC 6.5: UserRegistered,
//     UserRoleChanged, CourseCreated, CourseVersionCreated,
//     CoursePublishRequested, CoursePublished, CoursePublishFailed (a course
//     that fails validation), CourseArchived, StudentEnrolled,
//     StudentWithdrawn, LessonCompleted, CourseCompleted, CertificateIssued.
//  3. Wait for the archiver's consumer lag to reach 0. Note the document count
//     of the events collection.
//  4. Replay (admin) a handled event by id, and re-produce one record twice.
//  5. Restart the worker (graceful) and again drive one more event.
//
// Assertions:
//   - MongoDB events collection has exactly one document per event id for all
//     thirteen types, each with its schema id (so it stays interpretable,
//     RFC 6.8), the topic, key, partition and offset, and the actor id.
//   - audit_log has an entry for each administrative action driven (role
//     change, replay).
//   - After step 4 the events document count is unchanged (replay and
//     duplicate delivery do not duplicate the archive; unique index on event
//     id).
//   - After the restart the archive is complete and contains the new event
//     once.
//   - With MongoDB stopped, enrollments and analytics keep working and only the
//     archiver's lag grows; after MongoDB returns the archive catches up.
//
// Helper needed: MongoDB client and container control (Phase 4).
func TestE2E_E13_ArchiveAndAudit(t *testing.T) {
	t.Skip("pending: Phase 4 - libs/mongostore and the audit-archiver consumer")
}

// TestE2E_E14_AuthAndAccessOnNewSurface is E14, matrix row E14 (Phase 4 for
// failed-events and replay, Phase 6 for most popular and failed-events metrics).
//
// Steps:
//  1. Stack as E01 with one parked failed event (as in E04) and one published
//     course with an enrolled student.
//  2. Create tokens for: no token, a student enrolled in the course, a student
//     not enrolled, an instructor who owns the course, an instructor who does
//     not own it, an admin, and a token made stale by a role change.
//  3. Call each new endpoint with each token: list failed events, replay a
//     failed event by id, most-popular-courses, the failed-events metric, and
//     per-course analytics for the course.
//
// Assertions (the status codes below are the intended policy; settle them in
// the Phase 4/6 design and update this comment before implementing):
//   - Failed-events list and replay: admin 200 (replay 202 or 200); everyone
//     else (student, instructor, stale admin token) 403, no token 401. A non-admin
//     replay changes nothing (the event's effect does not appear, attempts
//     counter unchanged).
//   - Most popular courses and platform-level failed-events metric follow the
//     existing analytics rule for platform data (any authenticated user may
//     read platform aggregates; no token 401), unless the design says
//     admin only; assert whichever is chosen.
//   - Per-course analytics: instructor who owns the course 200, instructor who
//     does not own it 404 (ownership enforced through instructor_id on
//     course_published, closing the assessment gap), enrolled student 200,
//     other student 404, admin 200.
//   - Error responses use the standard {code, message} shape; none is a 5xx; a
//     malformed event id on replay is 400.
//   - Replay path with a role changed after the token was issued: the request
//     is refused as token_stale, not accepted.
//
// Helper needed: token factory for all roles (reuse stack.register, login,
// setRole from redis_flush_e2e_test.go), failed-events fixture from E04.
func TestE2E_E14_AuthAndAccessOnNewSurface(t *testing.T) {
	t.Skip("pending: Phase 4/6 - failed-events, replay, most-popular endpoints; instructor ownership via instructor_id")
}
