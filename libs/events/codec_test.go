package events

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	coursev1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/course/v1"
)

func TestEncode_HeaderLayout(t *testing.T) {
	// CourseArchived is the second message in course/v1/events.proto: indexes
	// [1], written as count 1 and index 1, each zigzag-encoded (0x02, 0x02).
	msg := &coursev1.CourseArchived{CourseId: "c"}
	got, err := Encode(0x01020304, msg)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(msg)
	want := append([]byte{0, 1, 2, 3, 4, 0x02, 0x02}, body...) // magic, big-endian id, indexes, body
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x\nwant % x", got, want)
	}
}

func TestEncode_FirstMessageUsesShortcut(t *testing.T) {
	msg := &coursev1.CoursePublished{CourseId: "c"} // first message in its file: indexes [0]
	got, err := Encode(5, msg)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(msg)
	want := append([]byte{0, 0, 0, 0, 5, 0}, body...) // single 0 byte, not 02 00
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x\nwant % x", got, want)
	}
	f, err := Decode(got)
	if err != nil || len(f.Indexes) != 1 || f.Indexes[0] != 0 || f.SchemaID != 5 {
		t.Fatalf("decode: %+v %v", f, err)
	}
}

func TestAppendHeader_NestedIndexesAreZigzag(t *testing.T) {
	// [1, 0] is written as count 2, 1, 0 each zigzag-varint: 04 02 00.
	got := AppendHeader(nil, 1, []int{1, 0}, []byte{0xAA})
	want := []byte{0, 0, 0, 0, 1, 0x04, 0x02, 0x00, 0xAA}
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x want % x", got, want)
	}
	f, err := Decode(got)
	if err != nil || len(f.Indexes) != 2 || f.Indexes[0] != 1 || f.Indexes[1] != 0 || !bytes.Equal(f.Body, []byte{0xAA}) {
		t.Fatalf("decode: %+v %v", f, err)
	}
}

func TestMessageIndexes_Nested(t *testing.T) {
	// DescriptorProto.ReservedRange is nested in DescriptorProto (index 2 in descriptor.proto's
	// top-level list is stable only for that file; assert the path shape instead).
	md := (&descriptorpb.DescriptorProto_ReservedRange{}).ProtoReflect().Descriptor()
	idx := MessageIndexes(md)
	if len(idx) != 2 {
		t.Fatalf("nested message should have 2 indexes, got %v", idx)
	}
	if idx[0] != md.Parent().Index() || idx[1] != md.Index() {
		t.Fatalf("indexes %v", idx)
	}
}

func TestRoundTrip_EveryEvent(t *testing.T) {
	for _, ty := range Types() {
		t.Run(ty.EventType, func(t *testing.T) {
			sample := samples()[ty.EventType]
			env, err := NewEnvelope(ty, meta(ty), sample)
			if err != nil {
				t.Fatal(err)
			}
			data, err := Encode(42, env)
			if err != nil {
				t.Fatal(err)
			}
			var back Envelope
			id, err := DecodeInto(data, &back)
			if err != nil || id != 42 {
				t.Fatalf("id=%d err=%v", id, err)
			}
			if !proto.Equal(env, &back) {
				t.Fatalf("envelope differs: %v vs %v", env, &back)
			}
			payload, err := OpenPayload(&back)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(payload, sample) {
				t.Fatalf("payload differs: %v vs %v", payload, sample)
			}
		})
	}
}

func TestDecode_Malformed(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"nil", nil, ErrTruncated},
		{"empty", []byte{}, ErrTruncated},
		{"only magic", []byte{0}, ErrTruncated},
		{"short id", []byte{0, 0, 0, 1}, ErrTruncated},
		{"no index byte", []byte{0, 0, 0, 0, 1}, ErrTruncated},
		{"wrong magic", []byte{1, 0, 0, 0, 1, 0, 0}, ErrBadMagic},
		{"json looking", []byte(`{"a":1}`), ErrBadMagic},
		{"count promises more than present", []byte{0, 0, 0, 0, 1, 0x04, 0x02}, ErrTruncated},
		{"negative count", []byte{0, 0, 0, 0, 1, 0x01}, ErrBadIndexes},
		{"huge count", []byte{0, 0, 0, 0, 1, 0xfe, 0xff, 0xff, 0xff, 0x0f}, ErrBadIndexes},
		{"negative index", []byte{0, 0, 0, 0, 1, 0x02, 0x01}, ErrBadIndexes},
		{"unterminated varint", []byte{0, 0, 0, 0, 1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, ErrBadIndexes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Decode(c.in); !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

func TestDecodeInto_WrongMessageAndBadBody(t *testing.T) {
	data, err := Encode(1, &coursev1.CoursePublished{CourseId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeInto(data, &coursev1.CourseArchived{}); err == nil {
		t.Fatal("expected message mismatch error")
	}
	// Valid header, garbage body.
	bad := AppendHeader(nil, 1, []int{0}, []byte{0xff, 0xff, 0xff})
	if _, err := DecodeInto(bad, &coursev1.CoursePublished{}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestOpenPayload_UnknownType(t *testing.T) {
	if _, err := OpenPayload(&Envelope{EventType: "nope.nope"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestNewEnvelope_RejectsWrongPayload(t *testing.T) {
	ty, _ := Lookup("course.course_published")
	if _, err := NewEnvelope(ty, &Envelope{}, &coursev1.CourseArchived{}); err == nil {
		t.Fatal("expected error")
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 1, 0, 0x0a, 0x01, 'x'})
	f.Add([]byte{0, 0, 0, 0, 1, 0x04, 0x02, 0x00})
	f.Add([]byte{1, 2, 3})
	for _, ty := range Types() {
		env, _ := NewEnvelope(ty, meta(ty), samples()[ty.EventType])
		data, _ := Encode(9, env)
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fr, err := Decode(data)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode to the same bytes, except that
		// [0] has two spellings (single 0, or 1,0) and decodes to one.
		again := AppendHeader(nil, fr.SchemaID, fr.Indexes, fr.Body)
		fr2, err := Decode(again)
		if err != nil || fr2.SchemaID != fr.SchemaID || !equalInts(fr2.Indexes, fr.Indexes) || !bytes.Equal(fr2.Body, fr.Body) {
			t.Fatalf("re-encode round trip failed: %+v vs %+v (%v)", fr, fr2, err)
		}
		// And the envelope decoder must not panic on arbitrary bodies.
		var env Envelope
		_ = proto.Unmarshal(fr.Body, &env)
	})
}
