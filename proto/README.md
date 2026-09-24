# proto/

Will hold Protobuf schemas for every domain event (`StudentEnrolled`, `CoursePublished`, `LessonCompleted`, etc.) and the generated Go code produced from them — committed to the repo so a contributor can build without running the protobuf toolchain (RFC §6.8, ADR-0008).

## Currently empty

Events, the outbox, and Kafka are **M3 scope** (`docs/rfc/smartcourse-rfc.md` §21.0). Right now, `internal/platform/outbox` writes events as JSON (see that package's doc comment) — a deliberate, documented stand-in until these schemas exist. The event *shape* (envelope fields: `event_id`, `event_type`, `schema_version`, `aggregate_type`/`aggregate_id`, `occurred_at`, `payload`) is already fixed by RFC §6.4, so writing the `.proto` files is mechanical once M3 starts; what doesn't exist yet is the Schema Registry, the compatibility-check tooling, and the consumers that would decode them.

## Why Protobuf + a committed codegen step, not JSON forever

Multiple independent consumers read the same event (analytics, notifications, audit — see RFC §6.5's consumer table). Without a registry-enforced schema, a producer-side field rename breaks all three silently, discovered later as wrong dashboard numbers — exactly the class of bug this project exists to eliminate. Full reasoning: ADR-0008.
