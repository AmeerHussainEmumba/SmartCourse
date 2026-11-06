# internal/notification

Empty — not started. Reserved for notification preferences and delivery records (RFC §21, M3), Asynq-backed delivery.

A straightforward addition when it lands: register a new handler with the outbox relay (`internal/platform/relay`), the same pattern `internal/analytics` and `internal/course` already use — no new infrastructure required, since the relay and the outbox it drains already exist and already carry every event this module would need to react to (enrollment, course publication, progress completion, etc.).

See `internal/README.md`'s module status table and `docs/rfc/smartcourse-rfc.md` §21.0 for what's deferred and why across the whole codebase, not just this module.
