# internal/audit

Empty — not started. Reserved for the event archive and audit log (RFC §21, M3), backed by MongoDB (ADR-0004).

Blocked specifically on MongoDB, which nothing in this codebase touches yet — unlike `notification`, this isn't just a matter of registering a new relay handler against existing infrastructure; the storage layer itself doesn't exist. Also the eventual home for the dead-letter store (a poison event past the relay's `maxAttempts` is logged at `ERROR` today but not parked anywhere recoverable — RFC §6.9).

See `internal/README.md`'s module status table and `docs/rfc/smartcourse-rfc.md` §21.0 for what's deferred and why across the whole codebase, not just this module.
