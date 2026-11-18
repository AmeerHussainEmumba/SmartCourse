# proto/

The event schemas moved into the module that owns them: **`libs/events`** (ADR-0030). The `.proto` files, the committed generated Go code, the Confluent wire-format codec and the Schema Registry bootstrap are all there; start with `libs/events/README.md`.

This directory is kept only so old links do not 404. Nothing is generated here.

Why Protobuf with a registry rather than JSON: several independent consumers read the same event (RFC §6.5), and a registry-enforced schema turns a silent field rename into a failed CI check. Full reasoning: ADR-0008.
