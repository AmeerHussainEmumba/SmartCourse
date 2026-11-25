//go:build e2e_legacy

package e2e_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// asMap reads a nested BSON document whichever Go type the driver gave it.
func asMap(v any) map[string]any {
	switch d := v.(type) {
	case bson.M:
		return d
	case map[string]any:
		return d
	case bson.D:
		m := map[string]any{}
		for _, e := range d {
			m[e.Key] = e.Value
		}
		return m
	}
	return nil
}

// TestE2E_E13_ArchiveAndAudit is E13, matrix row E13 (Phase 4).
//
// Steps:
//  1. Stack as E01 including MongoDB and the audit-archiver consumer, with a
//     retry budget of 12 attempts and 200 ms to 2 s backoff (about 19 s, long
//     enough to outlast the MongoDB outage in step 6 and short enough to park
//     the poison event of step 4 quickly).
//  2. Drive one example of every event type the services emit: UserRegistered,
//     UserProfileUpdated, UserRoleChanged, CoursePublished, CourseArchived,
//     StudentEnrolled, StudentWithdrawn, CourseCompleted. (The spec's other
//     types (CourseCreated, CourseVersionCreated, CoursePublishRequested,
//     CoursePublishFailed, LessonCompleted, CertificateIssued) are not in the
//     catalogue of libs/events and no service emits them: the subtest "types
//     the spec lists that no service emits" records that and asserts nothing.)
//  3. Wait for every consumer group's lag to reach 0 and note the document
//     count of the events collection.
//  4. Replay by id (admin) a handled event (not parked: 404, nothing changes)
//     and a parked one (a poison event fixed and replayed: 200), and
//     re-produce one record twice.
//  5. Restart the worker gracefully (SIGTERM) and drive one more event.
//  6. Stop MongoDB, enroll a student, start MongoDB.
//
// Assertions:
//   - The MongoDB events collection has exactly one document per event id for
//     every outbox event (all eight types are present), each with its schema id
//     (equal to the id in the Kafka record's frame, so it stays interpretable,
//     RFC 6.8), the aggregate id, the schema subject, and the actor id when the
//     event had one. The topic, key, partition and offset of the record are
//     asserted in their own subtest.
//   - audit_log has an entry, with the acting admin, for the role change, the
//     publish and the archive; and one for the admin replay (own subtest).
//   - After step 4 the events document count is unchanged (replay and duplicate
//     delivery do not duplicate the archive).
//   - After the restart the archive is complete and holds the new event once.
//   - With MongoDB stopped, the enrollment is 201, analytics counts it, its
//     mail is delivered and only the audit-archiver's lag grows; after MongoDB
//     returns the archive catches up and holds the event once.
func TestE2E_E13_ArchiveAndAudit(t *testing.T) {
	const budget = 12
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"KAFKA_CONSUMER_MAX_ATTEMPTS": fmt.Sprint(budget),
		"KAFKA_CONSUMER_BACKOFF_BASE": "200ms",
		"KAFKA_CONSUMER_BACKOFF_MAX":  "2s",
	}})
	admin := s.Login(t, s.AdminEmail())
	inst := s.NewInstructor(t) // user_registered, user_role_changed (by the admin)
	a, b, c := s.NewStudentNamed(t, "Alice"), s.NewStudentNamed(t, "Bob"), s.NewStudentNamed(t, "Carol")
	if code, body, err := harness.Do("PATCH", s.IdentityURL+"/api/v1/me", a.Token, map[string]string{"full_name": "Alice Renamed"}); err != nil || code != http.StatusOK {
		t.Fatalf("update profile: HTTP %d %s (err %v)", code, body, err)
	}
	course1 := s.PublishedCourse(t, inst, 1) // course_published
	course2 := s.PublishedCourse(t, inst, 1)
	s.WaitSettled(t, time.Minute)
	enrA := s.Enroll(t, a, course1)
	s.CompleteAll(t, a, enrA) // course_completed
	enrB := s.Enroll(t, b, course1)
	s.Withdraw(t, b, enrB) // student_withdrawn
	s.Enroll(t, c, course2)
	if code, body, err := harness.Do("POST", s.CourseURL+"/api/v1/courses/"+course2.ID.String()+"/archive", inst.Token, nil); err != nil || code/100 != 2 {
		t.Fatalf("archive course: HTTP %d %s (err %v)", code, body, err) // course_archived, then the archival consumer withdraws C
	}
	s.WaitSettled(t, time.Minute)
	// The archival consumer writes StudentWithdrawn for the archived course's
	// student while it consumes; a second settle covers that follow-on event.
	harness.Eventually(t, time.Minute, "the withdrawal caused by the archive", func() error {
		if n := s.MustCount(t, `SELECT count(*) FROM course.enrollments WHERE course_id = ? AND status = 'withdrawn'`, course2.ID); n != 1 {
			return harness.Errf("%d withdrawn enrollments in the archived course, want 1", n)
		}
		return nil
	})
	s.WaitSettled(t, time.Minute)

	type outboxRow struct {
		EventID, EventType, AggregateID string
		ActorID                         *string
	}
	readOutbox := func() []outboxRow {
		var rows []outboxRow
		if err := s.DB().Raw(`SELECT event_id::text AS event_id, event_type, aggregate_id::text AS aggregate_id, actor_id::text AS actor_id FROM platform.outbox ORDER BY id`).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	rows := readOutbox()
	outboxIDs := func() []string {
		var ids []string
		for _, r := range readOutbox() {
			ids = append(ids, r.EventID)
		}
		return ids
	}
	harness.Eventually(t, time.Minute, "every event archived exactly once", archiveCompleteWith(s, outboxIDs))

	t.Run("one document per event with its schema id", func(t *testing.T) {
		recs, err := s.ReadTopics(harness.EventTopics...)
		if err != nil {
			t.Fatal(err)
		}
		byEvent := map[string]harness.KRecord{}
		for _, r := range recs {
			byEvent[r.EventID()] = r
		}
		types := map[string]int{}
		for _, r := range rows {
			types[r.EventType]++
			docs, err := s.Mongo.Find("events", bson.M{"_id": r.EventID})
			if err != nil || len(docs) != 1 {
				t.Errorf("event %s (%s): %d archive documents (err %v), want 1", r.EventID, r.EventType, len(docs), err)
				continue
			}
			doc := docs[0]
			if doc["event_type"] != r.EventType {
				t.Errorf("event %s archived as %v, want %s", r.EventID, doc["event_type"], r.EventType)
			}
			if doc["aggregate_id"] != r.AggregateID {
				t.Errorf("event %s: aggregate_id %v, want %s", r.EventID, doc["aggregate_id"], r.AggregateID)
			}
			_, frameID, err := byEvent[r.EventID].Envelope()
			if err != nil {
				t.Errorf("event %s: %v", r.EventID, err)
				continue
			}
			if schemaID, _ := doc["schema_id"].(int32); schemaID == 0 || uint32(schemaID) != frameID {
				t.Errorf("event %s (%s): archived schema_id %v, the record's frame carries %d", r.EventID, r.EventType, doc["schema_id"], frameID)
			}
			h := asMap(doc["headers"])
			if want, ok := events.Lookup(r.EventType); ok && h["schema_subject"] != want.Subject() {
				t.Errorf("event %s: schema_subject %v, want %s", r.EventID, h["schema_subject"], want.Subject())
			}
			if r.ActorID != nil && h["actor_id"] != *r.ActorID {
				t.Errorf("event %s (%s): actor_id %v, want %s", r.EventID, r.EventType, h["actor_id"], *r.ActorID)
			}
		}
		for _, ty := range events.Types() {
			if _, emitted := types[ty.EventType]; !emitted {
				t.Logf("the catalogue has %s but this scenario did not drive it", ty.EventType)
			}
		}
		for _, want := range []string{
			"identity.user_registered", "identity.user_profile_updated", "identity.user_role_changed",
			"course.course_published", "course.course_archived",
			"enrollment.student_enrolled", "enrollment.student_withdrawn", "progress.course_completed",
		} {
			if types[want] == 0 {
				t.Errorf("no %s event was driven or archived", want)
			}
		}
	})

	t.Run("kafka coordinates", func(t *testing.T) {
		docs, err := s.Mongo.Find("events", bson.M{"_id": rows[0].EventID})
		if err != nil || len(docs) != 1 {
			t.Fatalf("archive document: %d (err %v)", len(docs), err)
		}
		h := asMap(docs[0]["headers"])
		var missing []string
		for _, f := range []string{"topic", "key", "partition", "offset"} {
			_, top := docs[0][f]
			_, hdr := h[f]
			if !top && !hdr {
				missing = append(missing, f)
			}
		}
		if len(missing) > 0 {
			t.Errorf("PRODUCT DEFECT: the archive document has no %s of the Kafka record (services/worker/archive/archive.go eventDoc stores only the event envelope fields and the headers payload_encoding, schema_subject, span_id, actor_id, outbox_id); E13's spec and RFC 6.8 ask for the topic, key, partition and offset. Document keys: %v, headers: %v", strings.Join(missing, ", "), keysOf(docs[0]), keysOf(h))
		}
	})

	t.Run("audit_log", func(t *testing.T) {
		for _, want := range []struct{ action, resource string }{
			{"identity.user_role_changed", inst.ID.String()},
			{"course.course_published", course1.ID.String()},
			{"course.course_archived", course2.ID.String()},
		} {
			harness.Eventually(t, 30*time.Second, "an audit_log entry for "+want.action, func() error {
				docs, err := s.Mongo.Find("audit_log", bson.M{"action": want.action, "resource_id": want.resource})
				if err != nil || len(docs) != 1 {
					return harness.Errf("%d audit entries (err %v), want 1", len(docs), err)
				}
				if got := docs[0]["actor_id"]; want.action == "identity.user_role_changed" && got != admin.ID.String() {
					return harness.Errf("role change audited with actor %v, want the admin %s", got, admin.ID)
				}
				return nil
			})
		}
		// Each administrative event has exactly one audit entry.
		auditable := 0
		for _, r := range rows {
			switch r.EventType {
			case "identity.user_role_changed", "course.course_published", "course.course_archived":
				auditable++
			}
		}
		if n, err := s.Mongo.Count("audit_log", nil); err != nil || n != int64(auditable) {
			t.Errorf("audit_log holds %d entries (err %v), want %d (one per role change, publish and archive)", n, err, auditable)
		}
	})

	t.Run("types the spec lists that no service emits", func(t *testing.T) {
		var missing []string
		for _, name := range []string{
			"CourseCreated", "CourseVersionCreated", "CoursePublishRequested", "CoursePublishFailed", "LessonCompleted", "CertificateIssued",
		} {
			found := false
			for _, ty := range events.Types() {
				found = found || strings.HasSuffix(ty.Subject(), "."+name)
			}
			if !found {
				missing = append(missing, name)
			}
		}
		t.Skipf("known gap: %s are not in the libs/events catalogue (the spec lists thirteen types, the services emit eight); another change is adding some of them, so nothing is asserted here", strings.Join(missing, ", "))
	})

	// Step 4: replay and duplicate delivery change nothing in the archive.
	ghostEnrollment, ghostStudent := uuid.New(), uuid.New()
	poisonPayload := fmt.Sprintf(`{"enrollment_id":%q,"student_id":%q,"completed_at":%q}`, ghostEnrollment, ghostStudent, time.Now().UTC().Format(time.RFC3339))
	producer := s.NewEventProducer(t)
	poisonID := producer.Produce(t, "progress.course_completed", enrA, poisonPayload, 1<<40)
	harness.Eventually(t, time.Minute, "the poison event parked", func() error {
		if docs, err := s.Mongo.FailedEvents(poisonID); err != nil || len(docs) != 1 {
			return harness.Errf("%d failed_events documents (err %v), want 1", len(docs), err)
		}
		return nil
	})
	s.WaitSettled(t, time.Minute)
	rows = readOutbox() // the poison event bypasses the outbox: it is archived too
	harness.Eventually(t, time.Minute, "the poison event archived once", func() error {
		if n, err := s.Mongo.ArchivedEvents(poisonID); err != nil || n != 1 {
			return harness.Errf("poison event archived %d times (err %v), want 1", n, err)
		}
		return nil
	})
	before, err := s.Mongo.Count("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if code, _, raw := s.ReplayFailedEvent(t, rows[0].EventID); code != http.StatusNotFound {
		t.Errorf("replay of a handled (never parked) event: HTTP %d %s, want 404", code, raw)
	}
	if err := s.DB().Exec(`INSERT INTO analytics.enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at, last_event_id)
		VALUES (?, ?, ?, 'active', now(), 0)`, ghostEnrollment, course1.ID, ghostStudent).Error; err != nil {
		t.Fatalf("fix the cause of the poison event: %v", err)
	}
	if code, res, raw := s.ReplayFailedEvent(t, poisonID); code != 200 || res == nil || !res.Replayed {
		t.Fatalf("replay of the parked event: HTTP %d %s, want 200 replayed", code, raw)
	}
	recs, err := s.ReadTopic("enrollment.events")
	if err != nil || len(recs) == 0 {
		t.Fatalf("read enrollment.events: %d records (err %v)", len(recs), err)
	}
	producer.Redeliver(t, recs[0])
	producer.Redeliver(t, recs[0])
	s.WaitSettled(t, time.Minute)
	harness.Never(t, 3*time.Second, "the archive count after replay and duplicate delivery", func() error {
		if n, err := s.Mongo.Count("events", nil); err != nil || n != before {
			return harness.Errf("events collection holds %d documents (err %v), was %d before the replay and the duplicates", n, err, before)
		}
		return nil
	})
	t.Run("the admin replay is audited", func(t *testing.T) {
		docs, err := s.Mongo.Find("audit_log", bson.M{"action": bson.M{"$regex": "replay", "$options": "i"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(docs) == 0 {
			t.Errorf("PRODUCT DEFECT: the admin replay of %s left no audit_log entry (the archiver audits only domain events, services/worker/archive/archive.go auditSpec; services/worker/admin does not call AppendAudit). E13 asks for an entry for each administrative action driven, role change and replay", poisonID)
		}
	})

	// Step 5: a graceful restart, then one more event.
	s.Worker.Stop(30 * time.Second)
	if s.Worker.Running() {
		t.Fatal("the worker did not stop on SIGTERM")
	}
	s.StartWorker(t)
	d := s.NewStudentNamed(t, "Dora")
	enrD := s.Enroll(t, d, course1)
	harness.Eventually(t, 5*time.Minute, "the restarted worker to archive the new events", func() error {
		return archiveCompleteWith(s, outboxIDs, poisonID)()
	})
	evD, err := s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrD.String())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Mongo.ArchivedEvents(evD); err != nil || n != 1 {
		t.Errorf("the event after the restart is archived %d times (err %v), want 1", n, err)
	}
	s.WaitSettled(t, 2*time.Minute)

	// Step 6: MongoDB down, then up.
	e := s.NewStudentNamed(t, "Erin")
	s.WaitSettled(t, time.Minute)
	statsBefore := s.CourseStatsFor(t, inst, course1)
	s.StopMongo(t)
	enrE := s.Enroll(t, e, course1) // 201 with MongoDB down
	var evE string
	harness.Eventually(t, time.Minute, "the StudentEnrolled event in the outbox", func() error {
		var err error
		evE, err = s.EventIDFor("enrollment.student_enrolled", "enrollment_id", enrE.String())
		return err
	})
	harness.Eventually(t, time.Minute, "analytics and Mailpit to process the enrollment with MongoDB down", func() error {
		if cs := s.CourseStatsFor(t, inst, course1); cs.TotalEnrollments != statsBefore.TotalEnrollments+1 {
			return harness.Errf("course stats %+v, want %d enrollments", cs, statsBefore.TotalEnrollments+1)
		}
		if msgs, err := s.Mailpit.MessagesToWithSubject(e.Email, "enrolled"); err != nil || len(msgs) != 1 {
			return harness.Errf("%d enrollment mails (err %v), want 1", len(msgs), err)
		}
		return nil
	})
	harness.Eventually(t, time.Minute, "the audit-archiver's lag to grow while the others are at 0", func() error {
		lag, err := s.ConsumerLag("audit-archiver")
		if err != nil || lag < 1 {
			return harness.Errf("audit-archiver lag %d (err %v), want at least 1", lag, err)
		}
		for _, g := range []string{"analytics-aggregator", "notification-dispatcher"} {
			if l, err := s.ConsumerLag(g); err != nil || l != 0 {
				return harness.Errf("group %s lag %d (err %v), want 0", g, l, err)
			}
		}
		return nil
	})
	s.StartMongo(t)
	harness.Eventually(t, 3*time.Minute, "the archive to catch up after MongoDB returned", func() error {
		if n, err := s.Mongo.ArchivedEvents(evE); err != nil || n != 1 {
			return harness.Errf("the event from the outage is archived %d times (err %v), want 1", n, err)
		}
		return nil
	})
	s.WaitSettled(t, 2*time.Minute)
	harness.Eventually(t, time.Minute, "the archive to be complete and single", archiveCompleteWith(s, outboxIDs, poisonID))
}

// archiveCompleteWith asserts that every outbox event, and the extra ids (events
// produced past the outbox), is archived once and that the collection holds
// nothing else.
func archiveCompleteWith(s *harness.Stack, outboxIDs func() []string, extra ...string) func() error {
	return func() error {
		ids := append(outboxIDs(), extra...)
		for _, id := range ids {
			if n, err := s.Mongo.ArchivedEvents(id); err != nil || n != 1 {
				return harness.Errf("event %s archived %d times (err %v), want 1", id, n, err)
			}
		}
		if total, err := s.Mongo.Count("events", nil); err != nil || total != int64(len(ids)) {
			return harness.Errf("events collection holds %d documents (err %v), want %d", total, err, len(ids))
		}
		return nil
	}
}
