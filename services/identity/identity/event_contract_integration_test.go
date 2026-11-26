//go:build integration

// What this file checks, against real Postgres: every identity event the
// service writes (user_registered, user_profile_updated, user_role_changed),
// produced by the real service methods, conforms to the Protobuf contract in
// libs/events. The payloads are written from maps, so the real outbox row is
// the only place a renamed key shows up. The producer module does not import
// libs/events; a test may (a _test import is not linked into a binary).
package identity_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/eventstest"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

func TestEventContract_IdentityOutboxRowsConformToLibsEvents(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// user_registered.
	reg := h.mustRegister(uniq("contract"), "CorrectPass123!")
	userID := uuid.MustParse(reg.User.ID)
	// user_profile_updated.
	if _, err := h.svc.UpdateMe(ctx, userID, identity.UpdateProfileRequest{FullName: "New Name"}); err != nil {
		t.Fatal(err)
	}
	// user_role_changed.
	if _, err := h.svc.UpdateUserRole(ctx, userID, identity.RoleInstructor, identity.StatusActive); err != nil {
		t.Fatal(err)
	}

	rows, err := h.env.DB.Raw(`SELECT event_type, payload FROM outbox ORDER BY id`).Rows()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	seen := map[string]int{}
	for rows.Next() {
		var typ string
		var raw []byte
		if err := rows.Scan(&typ, &raw); err != nil {
			t.Fatal(err)
		}
		eventstest.RequireConforms(t, typ, raw)
		seen[typ]++
	}
	for _, typ := range []string{"identity.user_registered", "identity.user_profile_updated", "identity.user_role_changed"} {
		if seen[typ] == 0 {
			t.Errorf("no %s row was written, so its contract was not checked (saw %v)", typ, seen)
		}
	}
}
