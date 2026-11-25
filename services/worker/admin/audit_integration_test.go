//go:build integration

package admin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/admin"
)

// The E10 defect: after a Redis flush a version-1 token of an admin who was
// demoted (database version 2) has no cached version to compare with. A miss
// must refuse, not read as version 1.
func TestAdmin_TokenVersionCacheMiss_FailsClosed(t *testing.T) {
	e := setup(t)
	adminID := uuid.New()
	tok, _ := auth.IssueAccessToken(e.cfg.JWT, adminID, "admin", 1) // nothing cached for this admin
	for _, p := range []struct{ m, path string }{{"GET", base}, {"GET", base + "/" + uuid.NewString()}, {"POST", base + "/" + uuid.NewString() + "/replay"}} {
		wantStatus(t, e.do(p.m, p.path, tok, nil), 401, "token_stale")
	}
	// Once the key is back (identity's re-seeder, or a login) the same token works.
	if err := e.pg.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 1, 0).Err(); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.do("GET", base, tok, nil), 200, "")
}

func auditDocs(t *testing.T, e *env, filter bson.D) []bson.M {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(e.mongo.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	cur, err := c.Database(mongostore.DefaultDatabase).Collection(mongostore.CollAuditLog).Find(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	var docs []bson.M
	if err := cur.All(context.Background(), &docs); err != nil {
		t.Fatal(err)
	}
	return docs
}

// E13: every replay attempt is an audit_log entry (who, what, which consumer,
// the outcome), including a repeat attempt on the same event and a failed one.
func TestAdmin_Replay_IsAudited_EveryAttempt(t *testing.T) {
	e := setup(t)
	tok, adminID := e.tokenFor("admin")
	user := uuid.New()
	e.pg.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'Old Name', 0)`, user)
	id := e.parkProfile(user, "Replayed", 5)
	path := base + "/" + id.String() + "/replay"

	wantStatus(t, e.do("POST", path, tok, nil), 200, "")
	wantStatus(t, e.do("POST", path, tok, nil), 200, "") // already replayed: still an attempt

	docs := auditDocs(t, e, bson.D{{Key: "action", Value: admin.ActionReplayed}, {Key: "resource_id", Value: id.String()}})
	if len(docs) != 2 {
		t.Fatalf("%d audit entries for two replay attempts, want 2: %v", len(docs), docs)
	}
	outcomes := map[string]int{}
	for _, d := range docs {
		if d["actor_id"] != adminID.String() || d["actor_role"] != "admin" || d["resource_type"] != "failed_event" {
			t.Errorf("entry %v: want actor %s (admin), resource_type failed_event", d, adminID)
		}
		if d["at"] == nil {
			t.Errorf("entry %v has no timestamp", d)
		}
		after := asMap(d["after"])
		if after["consumer"] != "course-instructor-mirror" {
			t.Errorf("entry %v: consumer = %v", d, after["consumer"])
		}
		outcomes[after["outcome"].(string)]++
	}
	if outcomes["replayed"] != 1 || outcomes["already_replayed"] != 1 {
		t.Errorf("outcomes = %v, want one replayed and one already_replayed", outcomes)
	}
}

func TestAdmin_Replay_FailedAttemptIsAuditedWithTheOutcome(t *testing.T) {
	failing := relay.NewHandler("course-instructor-mirror", []string{"identity.user_profile_updated"},
		func(context.Context, *gorm.DB, relay.DecodedEvent) error { return errors.New("still broken") })
	e := setup(t, failing)
	tok, adminID := e.tokenFor("admin")
	id := e.parkProfile(uuid.New(), "N", 1)
	wantStatus(t, e.do("POST", base+"/"+id.String()+"/replay", tok, nil), 422, "replay_failed")
	docs := auditDocs(t, e, bson.D{{Key: "action", Value: admin.ActionReplayed}, {Key: "resource_id", Value: id.String()}})
	if len(docs) != 1 {
		t.Fatalf("%d audit entries, want 1", len(docs))
	}
	after := asMap(docs[0]["after"])
	if docs[0]["actor_id"] != adminID.String() || after["outcome"] != "failed" || after["error"] == nil {
		t.Errorf("entry %v: want actor %s, outcome failed and the error", docs[0], adminID)
	}
	// Not found and bad ids are not replays: nothing is audited for them.
	e.do("POST", base+"/"+uuid.NewString()+"/replay", tok, nil)
	if n := len(auditDocs(t, e, bson.D{{Key: "action", Value: admin.ActionReplayed}})); n != 1 {
		t.Errorf("%d replay audit entries after a not-found replay, want still 1", n)
	}
}

type failingAuditor struct{ calls int }

func (f *failingAuditor) AppendAudit(context.Context, mongostore.AuditEntry) (string, error) {
	f.calls++
	return "", errors.New("mongo down")
}

// The replay already happened: a failure to write its audit entry must not
// turn the response into an error, but it is counted (and logged at error level).
func TestAdmin_Replay_AuditFailureDoesNotUndoTheReplay(t *testing.T) {
	aud := &failingAuditor{}
	e := setupWith(t, aud)
	user := uuid.New()
	e.pg.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'Old Name', 0)`, user)
	id := e.parkProfile(user, "Replayed", 5)
	before := admin.AuditFailures()
	rec := e.do("POST", base+"/"+id.String()+"/replay", e.admin(), nil)
	wantStatus(t, rec, 200, "")
	if body := decode[replayBody](t, rec); !body.Replayed {
		t.Errorf("replay = %+v, want replayed", body)
	}
	var name string
	e.pg.DB.Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, user).Row().Scan(&name)
	if name != "Replayed" {
		t.Errorf("mirror = %q: the replay was undone", name)
	}
	if aud.calls != 1 || admin.AuditFailures() != before+1 {
		t.Errorf("auditor calls %d, audit failures %d (was %d): want 1 call and the failure counted", aud.calls, admin.AuditFailures(), before)
	}
}

// asMap reads a nested document, which the driver returns as bson.D or bson.M.
func asMap(v any) bson.M {
	switch m := v.(type) {
	case bson.M:
		return m
	case bson.D:
		out := bson.M{}
		for _, e := range m {
			out[e.Key] = e.Value
		}
		return out
	}
	return bson.M{}
}
