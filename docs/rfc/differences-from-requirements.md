# RFC — Differences from Requirements

| Field | Value |
|---|---|
| Status | Living document |
| Last updated | 2026-09-24 |
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

*End of differences register.*
