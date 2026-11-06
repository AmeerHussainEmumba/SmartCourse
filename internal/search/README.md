# internal/search

Empty — not started. Reserved for a standalone search module: its own Postgres schema (ADR-0024's pattern — see how `internal/analytics` got its own schema), its own relay handler, and a real ranking strategy, as scoped by RFC §21 (M3).

**This is not the same as "search doesn't work yet."** `course_search_index`'s write (`course.Service.IndexSearch`, run as part of the M2 publish workflow) and read (`GET /courses/search`) are both real and live today — built inside `internal/course` rather than waiting for this module to exist, per ADR-0025. See `internal/course/README.md` and ADR-0025 for the full reasoning, and that ADR's reversal trigger for what happens to `IndexSearch`'s write path if this module is ever actually built: superseded, not duplicated alongside it, the same way `internal/analytics` was rebuilt rather than merely relocated when ADR-0024 landed.

See `internal/README.md`'s module status table and `docs/rfc/smartcourse-rfc.md` §21.0 for what's deferred and why across the whole codebase, not just this module.
