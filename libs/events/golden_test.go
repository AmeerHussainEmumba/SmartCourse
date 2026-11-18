package events

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	coursev1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/course/v1"
	enrollmentv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/enrollment/v1"
	identityv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/identity/v1"
	progressv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/progress/v1"
)

// The golden files are the CI guard for BACKWARD compatibility (ADR-0008,
// ADR-0030). For every event they pin three things:
//
//	<type>.bin     a framed envelope as written today. A later version of the code
//	               must still decode it ("old payloads still decode").
//	<type>.json    the payload decoded from that .bin, with field names. A renamed or
//	               removed field changes (or drops) a key, so the test fails.
//	<type>.schema  name, number, kind and cardinality of every field.
//
// Wire bytes alone cannot catch a rename (only field numbers are encoded),
// which is why the JSON and schema files exist. After an intentional,
// additive change run: go test -run Golden -update
var update = flag.Bool("update", false, "rewrite golden files")

const goldenSchemaID = 7

var (
	uid  = [...]string{"0192f0c0-0000-7000-8000-000000000001", "0192f0c0-0000-7000-8000-000000000002", "0192f0c0-0000-7000-8000-000000000003", "0192f0c0-0000-7000-8000-000000000004"}
	when = timestamppb.New(time.Date(2026, 11, 18, 9, 30, 0, 123000000, time.UTC))
)

// samples sets every field of every payload to a non-zero value, so removing
// a field is visible in the output.
func samples() map[string]proto.Message {
	return map[string]proto.Message{
		"identity.user_registered":      &identityv1.UserRegistered{UserId: uid[0], Email: "a@example.com", Role: "student", FullName: "Ada Lovelace"},
		"identity.user_profile_updated": &identityv1.UserProfileUpdated{UserId: uid[0], FullName: "Ada King"},
		"identity.user_role_changed":    &identityv1.UserRoleChanged{UserId: uid[0], Role: "instructor", Status: "active"},
		"course.course_published":       &coursev1.CoursePublished{CourseId: uid[1], VersionId: uid[2], Title: "Go in Depth", InstructorId: uid[3]},
		"course.course_archived":        &coursev1.CourseArchived{CourseId: uid[1]},
		"enrollment.student_enrolled":   &enrollmentv1.StudentEnrolled{EnrollmentId: uid[2], StudentId: uid[0], CourseId: uid[1], EnrolledAt: when},
		"enrollment.student_withdrawn":  &enrollmentv1.StudentWithdrawn{EnrollmentId: uid[2], StudentId: uid[0], CourseId: uid[1]},
		"progress.course_completed":     &progressv1.CourseCompleted{EnrollmentId: uid[2], StudentId: uid[0], CompletedAt: when},
	}
}

func meta(t Type) *Envelope {
	return &Envelope{
		EventId:       uid[3],
		SchemaVersion: 1,
		AggregateType: strings.SplitN(t.EventType, ".", 2)[0],
		AggregateId:   uid[1],
		OccurredAt:    when,
		TraceId:       "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanId:        "00f067aa0ba902b7",
		ActorId:       uid[0],
	}
}

func schemaSignature(md protoreflect.MessageDescriptor) string {
	var lines []string
	fs := md.Fields()
	for i := range fs.Len() {
		f := fs.Get(i)
		lines = append(lines, fmt.Sprintf("%s %d %s %s", f.Name(), f.Number(), f.Kind(), f.Cardinality()))
	}
	sort.Strings(lines)
	return string(md.FullName()) + "\n" + strings.Join(lines, "\n") + "\n"
}

func normJSON(t *testing.T, m proto.Message) string {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.MarshalIndent(v, "", "  ") // map keys are sorted
	if err != nil {
		t.Fatal(err)
	}
	return string(out) + "\n"
}

func readOrUpdate(t *testing.T, name, got string) string {
	t.Helper()
	path := filepath.Join("testdata", "golden", name)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden file %s (run go test -run Golden -update): %v", path, err)
	}
	return string(want)
}

func TestGolden_EveryEventHasASample(t *testing.T) {
	s := samples()
	for _, ty := range Types() {
		if _, ok := s[ty.EventType]; !ok {
			t.Errorf("no sample for %s", ty.EventType)
		}
	}
	if len(s) != len(Types()) {
		t.Errorf("%d samples for %d event types", len(s), len(Types()))
	}
}

func TestGolden_Events(t *testing.T) {
	for _, ty := range Types() {
		t.Run(ty.EventType, func(t *testing.T) {
			sample := samples()[ty.EventType]
			env, err := NewEnvelope(ty, meta(ty), sample)
			if err != nil {
				t.Fatal(err)
			}
			framed, err := Encode(goldenSchemaID, env)
			if err != nil {
				t.Fatal(err)
			}

			// Today's encoding must equal the committed bytes.
			wantBin := readOrUpdate(t, ty.EventType+".bin", string(framed))
			if string(framed) != wantBin {
				t.Errorf("encoding of %s changed; golden bytes differ", ty.EventType)
			}

			// The committed (old) bytes must decode with today's code, cleanly.
			var got Envelope
			id, err := DecodeInto([]byte(wantBin), &got)
			if err != nil {
				t.Fatalf("golden bytes no longer decode: %v", err)
			}
			if id != goldenSchemaID {
				t.Errorf("schema id %d, want %d", id, goldenSchemaID)
			}
			if len(got.ProtoReflect().GetUnknown()) != 0 {
				t.Errorf("envelope has unknown fields: a field was removed or renumbered")
			}
			payload, err := OpenPayload(&got)
			if err != nil {
				t.Fatal(err)
			}
			if len(payload.ProtoReflect().GetUnknown()) != 0 {
				t.Errorf("payload has unknown fields: a field was removed or renumbered")
			}
			if !proto.Equal(payload, sample) {
				t.Errorf("decoded payload differs from the sample:\n got %v\nwant %v", payload, sample)
			}

			// Names: a rename changes a JSON key, a removal drops one.
			if want := readOrUpdate(t, ty.EventType+".json", normJSON(t, payload)); normJSON(t, payload) != want {
				t.Errorf("payload JSON changed (field renamed or removed?)\n got:\n%s\nwant:\n%s", normJSON(t, payload), want)
			}
			if want := readOrUpdate(t, ty.EventType+".schema", schemaSignature(ty.Message())); schemaSignature(ty.Message()) != want {
				t.Errorf("payload schema changed\n got:\n%s\nwant:\n%s", schemaSignature(ty.Message()), want)
			}
		})
	}
}

func TestGolden_Envelope(t *testing.T) {
	md := (&Envelope{}).ProtoReflect().Descriptor()
	got := schemaSignature(md)
	if want := readOrUpdate(t, "envelope.schema", got); got != want {
		t.Errorf("envelope schema changed\n got:\n%s\nwant:\n%s", got, want)
	}
}
