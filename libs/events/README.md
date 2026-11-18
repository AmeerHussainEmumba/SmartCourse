# libs/events

Event contracts (ADR-0030, RFC §6.4, §6.5, §6.8): the `.proto` files, the generated Go code (committed), the envelope, a codec for the Confluent Schema Registry wire format, a small registry client, and the bootstrap command that registers every subject. Pure Go (`CGO_ENABLED=0` builds); the only dependency is `google.golang.org/protobuf`. It imports no service and no Kafka client.

## What is defined

The eight events the services write to the outbox today. Each payload's fields were derived from the `outbox.Write` call sites, not from the RFC's wider list; events the RFC names but nothing emits (`CourseCreated`, `CertificateIssued`, ...) get a message when a service starts writing them.

| event_type | Topic (key) | Payload message | Emitted in |
|---|---|---|---|
| `identity.user_registered` | `identity.events` (user_id) | `identity.v1.UserRegistered` | `services/identity/identity/service.go` |
| `identity.user_profile_updated` | `identity.events` | `identity.v1.UserProfileUpdated` | same |
| `identity.user_role_changed` | `identity.events` | `identity.v1.UserRoleChanged` | same |
| `course.course_published` | `course.events` (course_id) | `course.v1.CoursePublished` (includes `instructor_id`) | `services/course/course/service.go` |
| `course.course_archived` | `course.events` | `course.v1.CourseArchived` | same |
| `enrollment.student_enrolled` | `enrollment.events` (course_id) | `enrollment.v1.StudentEnrolled` | `services/course/enrollment/service.go` |
| `enrollment.student_withdrawn` | `enrollment.events` | `enrollment.v1.StudentWithdrawn` | same |
| `progress.course_completed` | `progress.events` (enrollment_id) | `progress.v1.CourseCompleted` | `services/course/progress/service.go` |

The topic follows the event type's prefix, not the outbox `aggregate_type` (which is `user`, `course` or `enrollment`; enrollment events use `course`). `events.Types()` is the machine-readable form of this table.

## Layout

- `proto/` - sources (`buf.yaml`, `buf.gen.yaml`, `smartcourse/<domain>/v1/*.proto`) and `embed.go`, which embeds them so the bootstrap registers the exact text the code was generated from.
- `gen/` - generated Go, committed so a contributor can build without the toolchain (RFC §6.8). Do not edit.
- `codec.go` - `Encode`, `Decode`, `DecodeInto`, `MessageIndexes`.
- `catalog.go`, `envelope.go` - `Types()`, `Subjects()`, `NewEnvelope`, `OpenPayload`.
- `registry/` - REST client: `Register`, `SetCompatibility`, `Compatibility`, `IsCompatible`, `SchemaByID`, `LatestVersion`.
- `cmd/register-schemas/` - idempotent bootstrap.
- `testdata/golden/` - the compatibility guard (below).

## Wire format

The Kafka value is a registry frame around an `Envelope` (RFC §6.4). `Envelope.payload` holds the plain Protobuf encoding of the payload message named by `event_type`.

```
byte 0       0 (magic)
bytes 1-4    schema id, big-endian
bytes 5-     message indexes: zigzag-varint count, then that many zigzag-varint indexes; [0] is the single byte 0
rest         Protobuf-encoded Envelope
```

Verified on 2026-11-18 against Confluent's "Schema Registry serializer and formatter" page (`docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html`). Note that page writes `[1, 0]` as "2,1,0"; those are the logical values, the bytes are zigzag-encoded (`04 02 00`), which `TestAppendHeader_NestedIndexesAreZigzag` pins. Not verified: round-trip with a real Confluent client (Java or Go) reading our bytes; do that when Phase 3 has a consumer.

## Subjects and schema_version

12 subjects, all `BACKWARD`:

- `<topic>-value` for each of the 4 topics: the envelope schema (what the Kafka value is).
- the payload message's full name (for example `smartcourse.course.v1.CoursePublished`) for each event: the registry's record-name strategy. The registered text is the whole `events.proto` file for that domain.

`Envelope.schema_version` is meant to be the registry version of the payload's subject, so an archived event can be resolved to its schema. This is a convention, not enforced by the registry; the producer (Phase 3) must fill it from `Client.LatestVersion` or the bootstrap output. This is the open design decision recorded in the report for the project owner. The registry de-duplicates identical schema text, so envelope subjects share one id, as do payloads from the same file; the message index in the frame says which message is meant.

## The compatibility guard

Wire bytes cannot catch a field rename (only numbers are encoded), so `testdata/golden/` pins, for every event: a framed envelope (`.bin`), the decoded payload as JSON with field names (`.json`), and each field's name, number, kind and cardinality (`.schema`). `go test` fails if a field is renamed, removed, renumbered or retyped, if the committed bytes stop decoding, or if decoding leaves unknown fields. After an intentional additive change: `go test -run Golden -update`, then review the diff. A removed or renamed field is a breaking change under ADR-0008 and needs the discussion, not an `-update`.

## Regenerating

```
./generate.sh            # installs pinned buf and protoc-gen-go into $TMPDIR, lints, generates
```

Pinned: buf v1.75.0 (Apache-2.0), protoc-gen-go v1.36.11 (BSD-3-Clause), kept equal to the `google.golang.org/protobuf` version in `go.mod`. buf v1.75.0 needs Go 1.26.7+; the Go toolchain switch downloads it automatically. Re-running must produce no diff in `gen/`.

## Registering schemas

```
docker compose --profile events up -d kafka schema-registry
go run ./cmd/register-schemas -registry http://localhost:8081   # or SCHEMA_REGISTRY_URL
```

For each subject it sets `BACKWARD`, confirms it, asks the registry whether the schema is compatible, registers it, and prints id and version. A second run changes nothing. An incompatible schema fails the run with a non-zero exit.

Tests against a real registry sit behind the `live` build tag, so normal CI does not need Kafka:

```
GOWORK=off go test -tags live -run Live -v ./cmd/register-schemas
```

## Not done here

The relay does not use this module yet: `libs/outbox` still writes JSON, and the outbox has no `trace_id`/`span_id`/`actor_id` columns (Phase 2 migration, ADR-0032). Converting the outbox JSON to these messages, and the producer, belong to Phase 3.
