# Changelog — libs/events

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Week 3 plan, ADR-0026).
- Protobuf contracts for the eight events services emit today (identity, course, enrollment, progress) and the `Envelope` (RFC §6.4); `course_published` carries `instructor_id`. Generated Go committed under `gen/`; `generate.sh` pins buf v1.75.0 and protoc-gen-go v1.36.11.
- Codec for the Confluent Schema Registry Protobuf wire format (`Encode`, `Decode`, `DecodeInto`), with a fuzz target for the decoder.
- `registry` package: net/http Schema Registry client (register, compatibility set/check, fetch by id).
- `cmd/register-schemas`: idempotent bootstrap of 12 subjects at BACKWARD compatibility.
- `registry.Client.LatestSchema` and `registry.ErrSubjectNotFound`: the latest version of a subject with its global id, so a producer can frame messages and fill `Envelope.schema_version` without registering (used by the worker at start, ADR-0029). Additive.
- Golden files under `testdata/golden` as the CI guard for BACKWARD compatibility; `live`-tagged tests against a real registry.
