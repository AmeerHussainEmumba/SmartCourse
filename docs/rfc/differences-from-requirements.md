# RFC — Differences from Requirements

| Field | Value |
|---|---|
| Status | Living document |
| Last updated | 2026-11-06 |
| Related | `smartcourse-rfc.md`, `smartcourse-adr.md`, `docs/baseline/SmartCourse.md`, `docs/baseline/Execution Guidelines.md` |

## Purpose

`docs/baseline/SmartCourse.md` and `docs/baseline/Execution Guidelines.md` are frozen. They are not edited as the design evolves, and they are the documents this project is judged against. Whenever our design adds something they don't ask for, interprets something they leave silent, or deliberately reads a section of theirs differently from its literal wording, that decision is logged here — what changed, why, and why it was justified rather than simply avoided.

An entry here is not an admission of a mistake. It's the record that lets a reviewer — or a future contributor — tell a deliberate, reasoned divergence apart from an accidental one, the same distinction `smartcourse-adr.md` draws for internal design decisions. If an entry's reasoning turns out to be wrong, the fix is a new entry that supersedes it, not a silent edit.

**Rules for this document**, mirroring the ADR register:

- An entry is never edited to reflect a reversal. It is marked `Superseded` and a new entry is appended.
- Entries are categorized: **Addition** (we added something the baseline doesn't mention), **Interpretation** (the baseline is silent or ambiguous and we made a judgment call), or **Correction** (we found and fixed an error in our own prior documentation, not in the baseline).

---

## Index

| # | Title | Category | Status |
|---|---|---|---|
| [DR-001](#dr-001--minio-for-object-storage) | MinIO for object storage | Addition | Accepted |
| [DR-002](#dr-002--authentication-mechanism-is-unspecified-in-the-brief) | Authentication mechanism is unspecified in the brief | Interpretation | Accepted |
| [DR-003](#dr-003--mongodbs-purpose-is-unstated-in-the-brief) | MongoDB's purpose is unstated in the brief | Interpretation | Accepted |
| [DR-004](#dr-004--gorm-is-bounded-to-simple-crud-raw-sql-elsewhere) | GORM is bounded to simple CRUD; raw SQL elsewhere | Interpretation | Accepted |
| [DR-005](#dr-005--a-formal-prd-is-authored-as-a-new-document) | A formal PRD is authored as a new document | Addition | Accepted |
| [DR-006](#dr-006--weekly-milestones-are-review-checkpoints-not-a-build-order) | Weekly milestones are review checkpoints, not a build order | Interpretation | Accepted |
| [DR-007](#dr-007--celery-citation-correction) | Celery citation correction | Correction | Accepted |
| [DR-008](#dr-008--password-reset-and-email-verification) | Password reset and email verification | Addition | Accepted |
| [DR-009](#dr-009--separate-api-and-worker-binaries-per-supervisor-review) | Separate API and worker binaries, per supervisor review | Addition | Accepted |
| [DR-010](#dr-010--per-service-table-ownership-and-liquibase-deferred-pending-discussion) | Per-service table ownership and Liquibase — deferred pending discussion | Interpretation | Proposed (tables half resolved — see entry's update) |
| [DR-011](#dr-011--further-splitting-cmdapi-into-per-domain-deployable-services) | Further splitting cmd/api into per-domain deployable services | Interpretation | Accepted |
| [DR-012](#dr-012--seed-data-created-through-the-service-layer-not-a-sql-script) | Seed data created through the service layer, not a SQL script | Interpretation | Proposed |
| [DR-013](#dr-013--schema-per-service-reverses-dr-010s-shared-schema-position) | Schema-per-service reverses DR-010's shared-schema position | Interpretation | Accepted |
| [DR-014](#dr-014--search-indexing-and-course-detail-caching-pulled-forward-into-m2) | Search indexing and course-detail caching pulled forward into M2 | Addition | Accepted |
| [DR-015](#dr-015--dr-008s-existing-asynq-notification-path-was-inaccurate) | DR-008's "existing Asynq notification path" was inaccurate | Correction | Accepted |
| [DR-016](#dr-016--syncrwmutex-required-by-the-tech-stack-not-used-not-previously-logged) | sync.RWMutex: required by the tech stack, not used, not previously logged | Interpretation | Accepted |
| [DR-017](#dr-017--course_stats-and-daily_enrollments-were-computed-but-unreadable) | course_stats and daily_enrollments were computed but unreadable | Correction | Accepted |

---

## DR-001 — MinIO for object storage

**Category:** Addition · **Date:** 2026-09-24 · **Related:** ADR-0019

### What differs

The SmartCourse.md tech stack lists no object/blob storage technology. We added **MinIO**, an S3-compatible self-hosted store, as a new Compose service.

### Why

SmartCourse.md requires instructors to "upload learning materials" and requires asset verification as a publishing step. There is no functional path to satisfying that requirement with the listed stack — PostgreSQL, MongoDB, and Redis are all poor fits for arbitrary-size binary assets, for reasons detailed in ADR-0019. Some store for asset bytes is not optional; the brief simply doesn't name one.

MinIO is chosen over a real cloud provider's object storage to keep local development and CI free of external credentials and network dependencies, and over storing bytes in PostgreSQL or on the API's local filesystem for the reasons ADR-0019 details in full.

**This addition is called out explicitly, twice: once here, once in ADR-0019, and a third time in the RFC's technology allocation table (§4.7) — because it is the one component in this system that is not permission granted by the tech stack, but a gap the tech stack left for us to fill.** We want it kept, not quietly folded into the stack list as if it had always been there.

### If this turns out to be wrong

Reversal trigger matches ADR-0019: a real cloud deployment target is chosen, at which point MinIO is swapped for the provider's object storage via configuration, not a rewrite, because the client speaks the S3 API either way.

---

## DR-002 — Authentication mechanism is unspecified in the brief

**Category:** Interpretation · **Date:** 2026-09-24 · **Related:** ADR-0015

### What differs

Neither SmartCourse.md nor the Execution Guidelines specifies an authentication mechanism, despite both documents assuming distinct user roles (student/instructor/admin) throughout. We chose JWT access tokens with rotating, hash-stored refresh tokens.

### Why

Some authentication scheme is required for role-based access to function at all; the brief is silent on which. ADR-0015 gives the full reasoning (statelessness at the stated concurrency target, argon2id over bcrypt, rotation with reuse detection). This entry exists so the *absence* of a specified mechanism — and the fact that a real choice was made to fill it — is visible in one place, rather than only discoverable by reading ADR-0015 in isolation.

### If this turns out to be wrong

Matches ADR-0015: a requirement for immediate global token revocation would force session-based auth or a denylist instead.

---

## DR-003 — MongoDB's purpose is unstated in the brief

**Category:** Interpretation · **Date:** 2026-09-24 · **Related:** ADR-0004

### What differs

SmartCourse.md lists MongoDB in the tech stack with no stated purpose or role. We scoped it narrowly to event archive, audit log, and failure forensics — and explicitly excluded it from the synchronous request path and from analytics aggregates.

### Why

"Presence in the stack is permission, not obligation" (ADR-0004) is the governing principle applied here: MongoDB being listed does not by itself justify any particular use of it. The one textual anchor in SmartCourse.md is a single passing phrase — operational data "not being efficiently processed for reporting, **auditing**, or platform insights." That is real but thin, so this is logged as an interpretation rather than treated as if the brief had specified an audit subsystem in detail. Confirmed as intended scope by the product owner on 2026-09-24 (see RFC assumption A10).

### If this turns out to be wrong

Matches ADR-0004: audit ceasing to be a requirement, or event volume proving low enough that a partitioned PostgreSQL table suffices.

---

## DR-004 — GORM is bounded to simple CRUD; raw SQL elsewhere

**Category:** Interpretation · **Date:** 2026-09-24 · **Related:** RFC §5.9

### What differs

SmartCourse.md's tech stack names GORM as the data-access technology. Our design uses GORM only for single-entity CRUD and simple associations, and uses raw parameterised SQL for the catalogue query, search, analytics aggregation, and any query with a non-trivial join or window function.

### Why

This is not a rejection of GORM — it is used, as specified, for what it does well. The boundary exists because GORM's `Preload` issues a second query per association, which is both a common source of N+1 regressions and, more importantly, hides the generated SQL on exactly the paths (§13.2's keyset-pagination and no-N+1 rules) where the generated SQL must be known, not inferred. Repository methods return the same domain types regardless of which path served them, so the boundary is invisible outside `repository.go` files. Logged here because "GORM" in the stack list could be read as "GORM everywhere," and that reading is deliberately not what we built.

### If this turns out to be wrong

No reversal trigger anticipated; this is a code-organization boundary, not a bet on future information.

---

## DR-005 — A formal PRD is authored as a new document

**Category:** Addition · **Date:** 2026-09-24 · **Related:** `docs/prd/smartcourse-prd.md`

### What differs

`docs/baseline/SmartCourse.md` is the commissioning brief — problem statement, business goals, functional requirements, tech stack. It is not itself structured as a PRD (no formal use-cases, no requirements traceability matrix, no explicit non-functional requirements section), yet its own "Expected Outcomes" section lists a "PRD Requirement" with exactly those sub-items, and RFC Appendix A already points to `docs/prd/smartcourse-prd.md` as a deliverable. We are authoring that document as new work, derived from the frozen brief.

### Why

The brief asks for a PRD-shaped deliverable without being one itself. Rather than retrofit PRD structure into a document we're not permitted to edit, we write a separate PRD that cites `SmartCourse.md` as its source of truth for scope. Confirmed with the product owner on 2026-09-24: `SmartCourse.md` and `Execution Guidelines.md` stay frozen; the PRD is new.

### If this turns out to be wrong

Not applicable — this is fulfilling a stated deliverable, not a bet.

---

## DR-006 — Weekly milestones are review checkpoints, not a build order

**Category:** Interpretation · **Date:** 2026-09-24 · **Related:** RFC §21, §21.1

### What differs

The Execution Guidelines lay out a three-week, module-by-module execution plan (Week 1: foundation and CRUD; Week 2: enrollment and publishing; Week 3: events and observability) and frame it as the assignment's structure. Our actual delivery plan (RFC §21, milestones M0–M4) does not build the system in that order — most notably, the complete database schema (including the outbox, idempotency tables, and `lesson_key`) is created in M0, and enrollment is implemented in M1 alongside course CRUD, both ahead of where the weekly plan would introduce them.

### Why

Retrofitting the outbox, idempotency keys, and `lesson_key` after other write paths already exist means touching every one of those paths a second time — which is exactly the class of inconsistency this project exists to eliminate, reintroduced by our own build order. The weekly plan is a review and feedback cadence ("share progress with your mentor," Execution Guidelines §2), which we follow; it is not, on our reading, a claim that these specific tables or flows must not exist before a given week. Per direction from the product owner: the weekly checkpoints are honored as checkpoints, but do not dictate the technical build sequence. RFC §21.1 states this mapping explicitly and shows what is additionally delivered ahead of schedule at each checkpoint, so a reviewer comparing our progress against the weekly plan can see the mapping rather than a mismatch.

### If this turns out to be wrong

If the mentor/reviewer intends the weekly scope as a hard technical gate rather than a review cadence, this entry is superseded and the delivery plan is resequenced to match.

---

## DR-007 — Celery citation correction

**Category:** Correction · **Date:** 2026-09-24 · **Related:** RFC assumption A11, ADR-0013

### What differs

An earlier draft of RFC-001 (assumption A11) and ADR-0013 asserted that the *current* Execution Guidelines document references Celery — a Python library — as carry-over evidence from a Python variant of the brief. On audit, that claim did not match `docs/baseline/Execution Guidelines.md` as provided: no occurrence of "Celery" exists in the current baseline. Confirmed with the document owner: an earlier draft did mention Celery, and it was removed by the owner before this baseline was finalized, because it didn't fit a Go-only stack.

### Why this is logged

Not a deviation from the baseline — a correction to our own prior documentation, which had cited the baseline inaccurately. It's logged here rather than silently fixed because an ADR asserting something false about a source document, however minor, is a document-integrity issue worth a visible trail. The underlying decision (Asynq over RabbitMQ) is unaffected — it was never dependent on the Celery reference and stands on the reasoning in ADR-0013 and RFC §19.3.

### If this turns out to be wrong

Not applicable — this entry itself is the correction.

---

## DR-008 — Password reset and email verification

**Category:** Addition · **Date:** 2026-09-24 · **Related:** RFC §5.3.1, §15.2

### What differs

Neither `SmartCourse.md` nor the Execution Guidelines mentions password reset or email verification. Since authentication itself was already an interpretation (DR-002), the original identity endpoint list (register, login, refresh, logout, profile) simply carried that silence forward — a real gap, not a deliberate scoping choice. We added both flows: `POST /auth/password-reset/request`, `POST /auth/password-reset/confirm`, `POST /auth/verify-email/resend`, `GET /auth/verify-email/confirm`, backed by single-use hashed tokens (`password_reset_tokens`, `email_verification_tokens`), delivered by email through the existing Asynq notification path.

### Why

An authentication system without a password-reset path isn't a smaller version of a real auth system — it's an incomplete one; a locked-out user has no recovery route. Confirmed with the product owner on 2026-09-24 as in-scope. No new infrastructure: both flows reuse the token-hashing pattern already established for refresh tokens (ADR-0015) and the notification delivery path already established for other emails (ADR-0014).

### If this turns out to be wrong

Not applicable — this closes a gap rather than making a bet; no condition would make removing it correct short of dropping password-based auth entirely.

---

## DR-009 — Separate API and worker binaries, per supervisor review

**Category:** Addition · **Date:** 2026-10-19 · **Related:** ADR-0022

### What differs

Supervisor review of PR #1 asked for independently deployable API and worker services, each with its own `main`, Dockerfile, and Compose entry — not what the original brief or RFC specified (one binary, `--mode` flag, RFC §4.2).

### Why

A fair request on its own terms: one binary with a mode flag genuinely doesn't give independent build/deploy/restart/scale, which "separate services" reasonably implies. Implemented as `cmd/api` and `cmd/worker`, both sharing the existing modular-monolith codebase and database (ADR-0001 unaffected) via a new `internal/platform/bootstrap` composition root. Full reasoning: ADR-0022.

### If this turns out to be wrong

Not applicable — this is a deployment-artifact change with no architectural downside identified; reversing it would mean returning to one binary, which no one has asked for.

---

## DR-010 — Per-service table ownership and Liquibase — deferred pending discussion

**Category:** Interpretation · **Date:** 2026-10-19 · **Related:** `docs/for-the-devs/` pushback document (hosted on Claude — see that doc for the full case), ADR-0001, ADR-0012

### What differs

The same review asked for two further changes we have not implemented: (1) each service owning its own database tables, with no cross-service reads/writes — cited specifically against `enrollment` locking the `courses` row during enrollment; and (2) replacing `golang-migrate` with Liquibase, changelog-per-service.

### Why this is logged as "proposed," not "accepted"

Both reverse specific, reasoned ADRs rather than filling a gap the brief left open — (1) reverses ADR-0001's modular-monolith decision and breaks ADR-0012's row-lock mechanism for the enrollment seat-limit guarantee (proven by `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`); (2) adds a JVM dependency to an all-Go stack for a capability (`golang-migrate` already has tested down-migrations for every change) we don't have a confirmed gap in. Neither the original brief nor the RFC names a technical problem these solve that our current design doesn't already address — see the pushback document for the point-by-point case, built from `docs/baseline/SmartCourse.md` and `docs/baseline/Execution Guidelines.md` directly. This is deliberately NOT resolved unilaterally: both are flagged for direct discussion with the supervisor before any implementation decision, which is why this entry's status is `Proposed`, not `Accepted` — unlike every other entry in this register, this one doesn't yet represent a settled position.

### If this turns out to be wrong

If the supervisor confirms either requirement after discussion (e.g., an org-wide Liquibase standard we're not aware of, or a grading rubric that specifically requires per-service databases), this entry is superseded by a new one recording the implementation, with a new ADR superseding ADR-0001/ADR-0012 as applicable.

### Update — 2026-10-26: per-service tables half resolved, in our favor

The direct conversation this entry was waiting on happened. The supervisor's per-service table ask was about matching Emumba's own company convention for a later review, not a rejection of the reasoning above — she explicitly understands and accepts the shared-database case. The Liquibase half of this entry wasn't revisited and remains open as originally written. See DR-011 for what the conversation did ask for instead, and ADR-0023 for the resulting decision.

### Update — 2026-11-03: Liquibase half now resolved too

A later written review asked for Liquibase again ("own Liquibase changelog" per service). Asked directly, the supervisor confirmed: "Liquibase not a hard requirement for you. You're on golang-migrate just we need to make sure each service has its own migrations folder." Both halves of this entry are now resolved — neither in the direction the original review asked for, but both by direct confirmation rather than this project's own unilateral case. `golang-migrate` stays (`internal/platform/db`, `Makefile`), reorganized into one independent migration sequence per service (`migrations/{platform,identity,course,analytics}/`) rather than Liquibase changelogs. See DR-013 and ADR-0024 for what that same conversation asked for on data ownership, which this register's original per-service-table reasoning did not survive unchanged.

---

## DR-011 — Further splitting cmd/api into per-domain deployable services

**Category:** Interpretation · **Date:** 2026-10-26 · **Related:** ADR-0022, ADR-0023, DR-010

### What differs

The brief doesn't ask for more than an API service and a worker service (the ADR-0022/DR-009 request). A follow-up conversation with the supervisor, after she clarified the DR-010 per-service-table ask was about company convention rather than a correctness concern, raised a further request: split the application into 4-5 separately deployable Docker images while keeping the core architecture (shared database, shared transactions) intact — her own example was "separating out the APIs into a different service."

### Why this is logged as "accepted," not "proposed"

Unlike DR-010, this doesn't reverse any existing ADR — it's an extension of ADR-0022's own reasoning, applied with finer granularity, and the supervisor's own framing already excludes the part (per-service data ownership) that would have reversed ADR-0001/ADR-0012. `cmd/api` split three ways — `cmd/identity-api`, `cmd/course-api` (bundling `course`, `enrollment`, `progress` to preserve ADR-0012's same-transaction seat-limit lock), `cmd/analytics-api` — alongside the unchanged `cmd/worker`, for four deployable images total. See ADR-0023 for the full decision record, including the options considered and rejected (one binary per module, which would have broken the seat-limit transaction).

### If this turns out to be wrong

If a five-image split (splitting `enrollment` out of `course-api` too) turns out to be what's actually expected, that requires accepting a real architectural cost — the seat-limit lock becoming a network call or distributed transaction — not just another `cmd/` directory. That tradeoff isn't decided here and would need its own ADR superseding ADR-0023's grouping decision specifically, not a silent code change.

---

## DR-012 — Seed data created through the service layer, not a SQL script

**Category:** Interpretation · **Date:** 2026-10-26 · **Related:** `cmd/seed/main.go`, DR-004

### What differs

Feedback received was that `cmd/seed/main.go` — the `make seed` dev-dataset generator — "needs to be done" as a SQL script. What exists instead is a Go program that creates instructors, courses, students, and progress by calling the real `identity`/`course`/`enrollment`/`progress` service layer, not raw `INSERT` statements.

### Why this is logged as "proposed," not "accepted"

This wasn't received as a specific, detailed ask (unlike DR-009/DR-011) — it's a short note whose exact intent (and whether it's a hard requirement, a convention preference, or a misreading of what `cmd/seed` does) isn't yet confirmed. The case for the current approach: going through the service layer means every seeded row passes the same validation and transactional invariants a real request would (e.g., Argon2 password hashing, the enrollment seat-limit check), and the sentinel-admin idempotency check makes `make seed` safe to re-run — properties a hand-written `.sql` file would have to reimplement and keep in sync by hand as the schema evolves. It also means the seed script doubles as a lightweight end-to-end smoke test of the four core modules wired together. No technical gap in the current approach has been identified; this entry exists to flag the feedback rather than resolve it unilaterally, pending direct confirmation of what was meant.

### If this turns out to be wrong

If a plain SQL seed script is actually required (e.g., to match a company convention, or because the service-layer approach is unacceptable for some reason not yet stated), `cmd/seed/main.go` would be replaced by a `.sql` file (or a `golang-migrate`-style seed migration) populating the same dataset directly, accepting the loss of validation-by-construction and the idempotency behavior unless reimplemented separately.

---

## DR-013 — Schema-per-service reverses DR-010's shared-schema position

**Category:** Interpretation · **Date:** 2026-11-03 · **Related:** DR-010, DR-011, ADR-0001, ADR-0012, ADR-0023, ADR-0024

### What differs

DR-010 logged this project's reasoned case against per-service data ownership, and recorded the supervisor's agreement with it ("she explicitly accepts the shared-database reasoning," per that entry's 2026-10-26 update). A later written review asked again, more specifically: "The direction is independent services that each own their code and their data... no foreign keys or joins across services." Asked directly, the supervisor confirmed and narrowed it: identity and analytics each get their own schema, course/enrollment/progress stay together (the seat-limit transaction case was accepted again), and it's "not one schema per table, it's one schema per service." Each service now has its own Postgres schema, with no foreign key crossing a schema boundary, implemented as ADR-0024.

### Why this is logged as "accepted," not treated as settling DR-010

This is a genuine reversal of DR-010's shared-*schema* position, not an extension of it the way DR-011 extended ADR-0022 — DR-010's own case (built from `docs/baseline/SmartCourse.md` and `docs/baseline/Execution Guidelines.md` directly) is **not retracted**: nothing in the original brief required this, and no new technical finding makes it necessary for correctness, performance, or anything the brief asks for. It's implemented because it was given as a direct, confirmed requirement, which is a different and sufficient reason on its own — matching the supervisor's stated direction for this engagement takes precedence over the team's own architectural preference here. DR-010's shared-*database* case (one Postgres instance, one set of credentials, transactions that can still span what are now schema boundaries) is unaffected and remains accepted; only the "one schema" half of DR-010 is reversed. The seat-limit transaction reasoning DR-010 led with is explicitly still intact — course, enrollment, and progress share one schema specifically so that guarantee never had to be touched.

### If this turns out to be wrong

If the schema split ever needs reversing (e.g., it turns out to add operational cost — migration-tooling surface, `search_path` misconfiguration risk — without a corresponding benefit the supervisor confirms she still wants), the fix is a new entry superseding this one and ADR-0024, collapsing back toward DR-010's original shared-schema position, not a silent revert.

---

## DR-014 — Search indexing and course-detail caching pulled forward into M2

**Category:** Addition · **Date:** 2026-11-05 · **Related:** DR-006, ADR-0021, ADR-0025

### What differs

RFC §21.1 maps "Week 2" to M2 (the publishing workflow) and scopes search indexing and caching to M3 ("Kafka events, background workers, analytics, observability"). M2's actual implementation includes a working `IndexSearch` activity (a real `course_search_index` upsert plus a live `GET /courses/search` endpoint) and a working `WarmCache` activity (a real version-scoped Redis cache, read through by `GET /courses/{id}`) — both nominally M3 scope, built as real, working pieces rather than left for later or stubbed out to match the RFC's §7.4 diagram literally.

### Why

The same reasoning DR-006 already established and ADR-0021 already acted on: the weekly/milestone breakdown is a review checkpoint, not a build order, and a design audit that finds a gap between the documented plan and what's actually buildable right now should close that gap rather than wait on a calendar date. Here, the alternative to building these for real was a workflow with two of its seven activities as literal no-ops (rejected directly — see ADR-0025's Option B) or silently dropped (leaves RFC §7.4's own diagram unfulfilled for no forcing reason, when `course_search_index`'s schema and `internal/platform/cache`'s Redis client both already existed from M0/M1). Full reasoning and the real bugs found building this: ADR-0025.

### If this turns out to be wrong

If `internal/search` lands in M3 as its own module with its own schema and ranking strategy (the same way `internal/analytics` was rebuilt rather than merely relocated when ADR-0024 landed), this entry's `IndexSearch` write path is superseded by that module's own relay-driven handler — not a reversal, an upgrade path already anticipated in ADR-0025's reversal trigger.

---

## DR-015 — DR-008's "existing Asynq notification path" was inaccurate

**Category:** Correction · **Date:** 2026-11-06 · **Related:** DR-008, DR-007, `internal/identity/email.go`

### What was wrongly asserted

DR-008 (password reset and email verification) states both flows are "delivered by email through the existing Asynq notification path." No such path exists, today or at any point this register can verify: Asynq is not a dependency (absent from `go.mod`), and email delivery is `identity.LoggingEmailSender` — a stand-in that logs `"would send email verification email"` and sends nothing (its own doc comment: "the M0/M1 placeholder... until it does," swappable behind the `EmailSender` interface it's declared against). A baseline-compliance audit comparing claims across this project's own documentation against actual running code found the mismatch.

### Why this is logged

Same reason as DR-007: not a deviation from the baseline, a correction to our own prior documentation, which asserted something false about implementation state. DR-008's underlying decision (add password reset/email verification, reusing the token-hashing pattern already built for refresh tokens) is unaffected and stands — only the "through the existing Asynq notification path" clause was wrong; it should have said "through `LoggingEmailSender`, a stand-in pending real delivery (Asynq, M3)." Left uncorrected, a reviewer reading DR-008 alone would believe background email delivery is built; it isn't.

### If this turns out to be wrong

Not applicable — this entry itself is the correction, the same way DR-007 is. DR-008 is not edited, per this register's own rule against editing an entry to reflect a reversal.

---

## DR-016 — `sync.RWMutex`: required by the tech stack, not used, not previously logged

**Category:** Interpretation · **Date:** 2026-11-06 · **Related:** RFC §21.0, ADR-0021

### What differs

`SmartCourse.md`'s Tech Stack section lists `sync.RWMutex` by name among the concurrency primitives "the implementation must demonstrate practical usage of" — the same list `sync.Mutex`, `sync/atomic`, `sync.WaitGroup`, and `context.Context` appear in, all of which are used in real production code. `sync.RWMutex` has zero uses anywhere in this codebase outside test files, confirmed by a baseline-compliance audit.

### Why

RFC §21.0 already states this plainly in its own status prose: "`sync.RWMutex` has no forced-fit use — the one candidate pattern RFC §10.3 names (a read-heavy/write-rare in-process config cache) doesn't exist as a real need yet, and adding one only to use the primitive would be exactly the anti-pattern ADR-0021 was written to avoid." That's a defensible reading of an ambiguous instruction — "demonstrate practical usage" read as "use it where a real need exists," not "manufacture one" — but it was never logged in this register the way every other baseline departure was, so a reviewer checking this register specifically for unmet tech-stack items wouldn't find it. Logged now for exactly that reason: the gap itself isn't new; its absence from this document was.

### If this turns out to be wrong

If a genuine read-heavy/write-rare shared-state need emerges (the in-process config cache RFC §10.3 names is still the most likely candidate), this is superseded by a new entry recording where `sync.RWMutex` was added and why that specific case earned it. If the reviewer intends literal, forced demonstration of every named primitive regardless of organic need, this entry is superseded and one is added for its own sake, with that tradeoff stated plainly rather than hidden.

---

## DR-017 — `course_stats` and `daily_enrollments` were computed but unreadable

**Category:** Correction · **Date:** 2026-11-06 · **Related:** ADR-0024, DR-014, `internal/analytics`

### What was wrongly asserted

RFC §21.0 and `internal/analytics/README.md` both claimed analytics closed "5 of 10" SmartCourse.md FR-5 metrics, counting per-course completion rate, average time to complete, and new enrollments over time among them. `course_stats` and `daily_enrollments` were genuinely written by the relay's handlers (since ADR-0024) — but neither had a repository read method, a `Service` method, or an HTTP route. "Computed and stored" was reported as "live"; a baseline-compliance audit comparing the brief's metric list against what's actually reachable by a client found the three metrics above were not.

### Why this is logged, and what changed

Same category as DR-007/DR-015: a correction to our own status claims, not a baseline deviation. Fixed in the same pass this was found, rather than left open: `Repository.GetCourseStats`/`ListDailyEnrollments`, `Service` wrappers, and two new routes (`GET /analytics/courses/{id}`, `GET /analytics/courses/{id}/daily-enrollments`) now give both tables a real read path, with integration and HTTP-level test coverage (`service_integration_test.go`, `analytics_http_test.go`). FR-5 is now honestly 7 of 10 — the remaining two ("Most Popular Courses," "Failed Events / Workflow Issues") are genuinely not built yet, not merely unexposed.

### If this turns out to be wrong

Not applicable — this closes a gap between documentation and reality rather than making a bet.

---

*End of differences register.*
