package events

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The Confluent Schema Registry wire format for Protobuf (verified against
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html):
//
//	byte 0        magic byte, 0
//	bytes 1..4    schema id, 4 bytes, big-endian
//	bytes 5..     message indexes: a zigzag-varint count followed by that many
//	              zigzag-varint indexes locating the message inside the schema
//	              (top-level index, then nested indexes). The common case [0]
//	              is written as the single byte 0 instead of 1,0.
//	rest          the Protobuf-encoded message
const magicByte = 0

// maxIndexDepth bounds the message-index list a decoder will accept; real
// schemas nest a handful of levels, so a larger count is malformed input.
const maxIndexDepth = 32

var (
	ErrTruncated  = errors.New("events: truncated registry frame")
	ErrBadMagic   = errors.New("events: bad magic byte, not a registry frame")
	ErrBadIndexes = errors.New("events: malformed message index list")
)

// Frame is a decoded registry frame; Body is the raw Protobuf message.
type Frame struct {
	SchemaID uint32
	Indexes  []int
	Body     []byte
}

// Encode marshals msg and prefixes it with the registry header for schemaID.
// The message indexes are derived from msg's descriptor.
func Encode(schemaID uint32, msg proto.Message) ([]byte, error) {
	body, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", msg.ProtoReflect().Descriptor().FullName(), err)
	}
	return AppendHeader(nil, schemaID, MessageIndexes(msg.ProtoReflect().Descriptor()), body), nil
}

// AppendHeader appends magic byte, schema id, message indexes and body to dst.
func AppendHeader(dst []byte, schemaID uint32, indexes []int, body []byte) []byte {
	dst = append(dst, magicByte)
	dst = binary.BigEndian.AppendUint32(dst, schemaID)
	if len(indexes) == 1 && indexes[0] == 0 {
		dst = append(dst, 0)
	} else {
		dst = binary.AppendVarint(dst, int64(len(indexes)))
		for _, i := range indexes {
			dst = binary.AppendVarint(dst, int64(i))
		}
	}
	return append(dst, body...)
}

// Decode splits data into its header fields and body. It never panics: any
// malformed input returns ErrTruncated, ErrBadMagic or ErrBadIndexes.
func Decode(data []byte) (Frame, error) {
	if len(data) < 6 {
		return Frame{}, ErrTruncated
	}
	if data[0] != magicByte {
		return Frame{}, ErrBadMagic
	}
	f := Frame{SchemaID: binary.BigEndian.Uint32(data[1:5])}
	rest := data[5:]

	count, n := binary.Varint(rest)
	if n == 0 {
		return Frame{}, ErrTruncated
	}
	if n < 0 || count < 0 || count > maxIndexDepth {
		return Frame{}, ErrBadIndexes
	}
	rest = rest[n:]
	if count == 0 {
		f.Indexes = []int{0} // the single-byte shortcut for [0]
	} else {
		f.Indexes = make([]int, 0, count)
		for range count {
			v, n := binary.Varint(rest)
			if n == 0 {
				return Frame{}, ErrTruncated
			}
			if n < 0 || v < 0 || v > int64(^uint32(0)) {
				return Frame{}, ErrBadIndexes
			}
			f.Indexes = append(f.Indexes, int(v))
			rest = rest[n:]
		}
	}
	f.Body = rest
	return f, nil
}

// DecodeInto decodes data into msg and returns the schema id. It rejects a
// frame whose message indexes point at a different message than msg.
func DecodeInto(data []byte, msg proto.Message) (uint32, error) {
	f, err := Decode(data)
	if err != nil {
		return 0, err
	}
	want := MessageIndexes(msg.ProtoReflect().Descriptor())
	if !equalInts(f.Indexes, want) {
		return 0, fmt.Errorf("events: frame holds message %v, want %v (%s)", f.Indexes, want, msg.ProtoReflect().Descriptor().FullName())
	}
	if err := proto.Unmarshal(f.Body, msg); err != nil {
		return 0, fmt.Errorf("unmarshal %s: %w", msg.ProtoReflect().Descriptor().FullName(), err)
	}
	return f.SchemaID, nil
}

// MessageIndexes returns the registry message-index path of md: its position
// among its file's top-level messages, then among each parent's nested ones.
func MessageIndexes(md protoreflect.MessageDescriptor) []int {
	var rev []int
	for d := protoreflect.Descriptor(md); d != nil; d = d.Parent() {
		if _, ok := d.(protoreflect.FileDescriptor); ok {
			break
		}
		rev = append(rev, d.Index())
	}
	out := make([]int, len(rev))
	for i, v := range rev {
		out[len(rev)-1-i] = v
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
