# PRD-001 — SmartCourse Backend

| Field | Value |
|---|---|
| Status | Draft — open for review |
| Version | 0.1 |
| Last updated | 2026-09-24 |
| Source of truth for scope | `docs/baseline/SmartCourse.md` (frozen — this PRD may not contradict it, only elaborate it) |
| Source of truth for architecture | `docs/rfc/smartcourse-rfc.md`, `docs/adr/smartcourse-adr.md` |
| Divergences from the baseline | `docs/rfc/differences-from-requirements.md` |

> This document did not exist in the original brief. `docs/baseline/SmartCourse.md` is the commissioning problem statement; it lists "PRD Requirement — key use-cases, functional and non-functional requirements, timeline/milestones, traceability" as an expected outcome without itself being structured that way. This PRD fills that gap (logged as DR-005). It restates and elaborates the frozen brief; it does not add scope the brief didn't ask for. Where a requirement here needs architectural detail, it links to the RFC rather than duplicating it — the RFC is authoritative on *how*, this document is authoritative on *what* and *why it matters to the product*.

---

## 1. Problem statement

EduCorp's SmartCourse platform is growing faster than its current backend can support. Four symptoms, as stated in the brief:

1. Publishing a course is slow and manual.
2. Students can't find relevant courses.
3. Course, enrollment, and analytics data disagree with each other.
4. The system doesn't hold up under traffic spikes, and background processing isn't reliable.

This PRD scopes the backend rebuild that addresses those four symptoms. See `docs/baseline/SmartCourse.md` for EduCorp's full framing.

## 2. Goals

Restated from the brief in outcome terms — each goal is something a reviewer can check for, not just an intention:

| Goal | What "done" looks like |
|---|---|
| G1 | An instructor can publish a course and, without manual intervention, see it become searchable, cached, and analytics-tracked |
| G2 | Two students racing for the last seat in a capped course never both get in, and never both get rejected when a seat is free |
| G3 | A student's enrollment, progress, and certificate are never lost, even if the process crashes mid-request |
| G4 | Dashboard numbers (enrollments, completions, popular courses) always agree with what the database actually contains, within a stated staleness window |
| G5 | Any request or background job's outcome can be explained by looking at traces and metrics, not by reading logs line by line |
| G6 | A new contributor can run the system locally and make a safe first change without asking the original author |

G6 exists because of a decision made explicit during scoping: **the team is one person today and is expected to grow.** Documentation and test coverage are treated as first-class deliverables, not polish, specifically so growth doesn't create a bottleneck on the original author. See §7 (Non-functional requirements) and §9 (Documentation and test requirements).

## 3. Actors

| Actor | Description |
|---|---|
| **Student** | Browses, enrolls, tracks progress, completes lessons, earns certificates |
| **Instructor** | Authors courses, modules, and lessons; publishes and updates them; views their course roster and stats |
| **Admin** | Manages user roles, investigates audit and failure data, has platform-wide visibility |

A user holds exactly one role (RFC assumption A6). Reversal trigger, per the RFC: if a user ever needs more than one role, the enum becomes a role/permission table.

## 4. Key use cases

Each use case names the actor, the trigger, the expected outcome, and the requirement(s) it exercises. These are the scenarios integration and end-to-end tests should be built around (see §9).

### UC-1 — Instructor authors and publishes a course

An instructor creates a course, adds modules and lessons (including uploaded assets), and requests publication. The system validates metadata, verifies assets, builds a search index entry, warms the outline cache, and initializes analytics — all without the instructor needing to trigger or babysit each step individually. The course becomes visible to students only once every step succeeds; if any step fails, the course they already published (if any) is untouched, and the instructor is told what went wrong. *(FR-1, FR-2)*

### UC-2 — Instructor updates an already-published course

An instructor edits a live course (adds a lesson, fixes a typo) and republishes. Currently-enrolled students see no interruption and no broken state while the update processes. Their existing progress is not lost, even though the update creates new content rows internally. *(FR-2)*

### UC-3 — Student discovers and enrolls in a course

A student searches or browses the catalogue, filters by category/level, and enrolls in a course. If the course has a seat limit, enrollment is rejected once the limit is reached — correctly, even when many students attempt to enroll in the same near-full course at the same moment. A student cannot enroll in the same course twice while already enrolled, but can re-enroll after withdrawing. *(FR-1, FR-3, FR-7)*

### UC-4 — Student progresses through a course and earns a certificate

A student marks lessons complete as they go. Progress is visible immediately. Once every lesson in the currently published version is complete, the course is marked complete and a certificate is issued. The certificate is independently verifiable by a third party without authentication. *(FR-1, FR-3)*

### UC-5 — Platform reacts to activity without blocking the user

Enrolling, publishing, and completing a lesson all trigger downstream work — analytics updates, notifications, cache/search updates. None of that work makes the user wait, none of it is lost if a downstream system is temporarily unavailable, and none of it double-processes if the same event is delivered twice. *(FR-3, FR-4)*

### UC-6 — Admin diagnoses a problem

An admin (or, operationally, the on-call engineer) needs to know why a course failed to publish, why a notification wasn't delivered, or why a dashboard number looks wrong — and needs to answer that from traces, metrics, and an audit/event trail, not by asking the original author or reading raw logs end to end. *(FR-6)*

### UC-7 — Platform reports on its own health

Anyone with access to the analytics endpoints can see total students, instructors, published courses, enrollment trends, completion rates, average time to complete, popular courses, average courses per student, and failed-event counts — and those numbers are trustworthy because they self-correct on a schedule rather than drifting silently. *(FR-5)*

## 5. Functional requirements

Numbered to match the brief's own FR groupings (`docs/baseline/SmartCourse.md` §"Core Functional Requirements").

### FR-1 — Course & user management

- Users register with a role (student/instructor/admin).
- Instructors create and update courses, modules, and learning assets (uploaded, not just referenced).
- Students enroll in courses, subject to: no duplicate active enrollment, an optional seat limit, and optional prerequisites.
- Enrollment history is retained, including past withdrawals.
- Users can reset a forgotten password and verify their email address, via single-use, expiring, hashed tokens delivered by email (DR-008).
- **Acceptance criteria:** covered by UC-1, UC-3; enforced at the database layer (partial unique indexes, check constraints — RFC §5.3), not only in application code.

### FR-2 — Content publishing workflow

- Publishing validates metadata, verifies assets, updates the search index, refreshes caches, and initializes analytics.
- The course is marked Ready only once every step above has succeeded.
- A partial failure never corrupts the already-published version — students see either the old version or the new one, never a broken in-between state.
- **Acceptance criteria:** covered by UC-1, UC-2; the "no broken in-between state" requirement is specifically what blue-green version promotion (ADR-0010) exists to guarantee, and is verified by workflow failure-injection tests (RFC §18.6).

### FR-3 — Enrollment workflow

- Enrollment records the student, initializes their progress, updates analytics, and may trigger a notification.
- Must handle high volume, must be idempotent (retrying an enrollment request never double-enrolls), must apply backpressure rather than fail under load, must recover from mid-operation failures, and must stay correct under concurrent access.
- **Acceptance criteria:** covered by UC-3; concurrency correctness is verified by an explicit multi-goroutine test asserting a seat limit is never exceeded (RFC §18.3) — this is called out because it is the single test in the suite that catches a class of bug invisible to any functional/manual test.

### FR-4 — Distributed & event-driven behaviors

- Publishing, analytics updates, notification delivery, cache sync, and search indexing all execute asynchronously, independent of the request that triggered them.
- These flows are traceable and recoverable, handle failure gracefully, avoid duplicate processing, and support workload spikes and concurrent execution.
- **Acceptance criteria:** covered by UC-5; "avoid duplicate processing" is verified by a replay test asserting counters are unchanged when the same event is processed twice (RFC §18.3, §6.6).

### FR-5 — Analytics metrics

All ten metrics from the brief, each with a stated store and freshness target (RFC §9.1): total students, total instructors, total courses published, new enrollments over time, course completion rate, average time to complete, most popular courses, average courses per student, and failed events/workflow issues.

- **Acceptance criteria:** covered by UC-7; each metric has both an event-driven update path and a scheduled reconciliation path, and a test asserting they converge (RFC §9.2).

### FR-6 — System observability & reliability

- Clear separation of responsibilities between components.
- Monitoring and logging across all key flows.
- Ability to diagnose failures in publishing, enrollment, and background tasks specifically — not just "something went wrong somewhere."
- High consistency and accuracy across all data models.
- **Acceptance criteria:** covered by UC-6; a single trace ID must be traceable from an HTTP request through the outbox, Kafka, and any resulting Asynq job (RFC §12.2) — verified manually in Jaeger during M4, and is the specific capability that makes this requirement checkable rather than aspirational.

### FR-7 — Concurrent processing & high-performance backend

- Course publishing runs independent steps concurrently where dependencies allow, demonstrating goroutines, WaitGroups/errgroups, channels, context propagation, and graceful error handling.
- High-volume background work (notifications, analytics aggregation, event processing, scheduled jobs, cache sync) runs through bounded worker pools with job queues, graceful shutdown, retries, and backpressure.
- Shared resources (caches, counters, popularity stats, rate-limiter state) are synchronized correctly — `sync.Mutex`/`RWMutex` for process-local state, Redis for state shared across replicas (this distinction is a named risk in RFC §10.3, because getting it backwards compiles, passes the race detector, and is still wrong).
- No race conditions, deadlocks, or goroutine leaks — enforced continuously via `-race` and `goleak` in CI (RFC §10.6), not verified once and assumed to hold.
- **Acceptance criteria:** covered by UC-1 (concurrent publishing activities), UC-3 (worker-pool-backed background processing), and the concurrency test suite in RFC §18.3.

## 6. Non-functional requirements

Full detail lives in the RFC; this section states the product-level commitment each one exists to keep.

| NFR | Commitment | RFC detail |
|---|---|---|
| Consistency | Anything a user can assert as fact about their own account is never wrong or missing | §3 |
| Latency | Users never wait for work that doesn't affect what they're told | §3.4, §13.1 |
| Scalability | Design carries no known failure mode at tens of thousands of concurrent learners, even though that load isn't generated in this phase | §13.3, G6 |
| Availability & degradation | Every dependency's failure mode is known in advance and is a degradation, not silent data loss, with PostgreSQL as the sole accepted exception | §13.4 |
| Security | Credential theft, token replay, privilege escalation, horizontal data access, mass assignment, injection, enumeration, and unauthorized asset access are all in scope and mitigated | §11 |
| Observability | Any request or background task is diagnosable end to end via one trace | §12 |
| Contributor accessibility | A competent engineer new to Go or this domain can run the system and make a safe change without the original author (G5/G6) | §22 |

## 7. Non-goals

Restated from RFC §2.2 for visibility in the product document. Explicitly out of scope: payments/billing, video hosting/transcoding, quizzes/grading, live sessions/chat/forums, multi-tenancy, i18n/l10n, a recommendation engine, mobile/offline sync, production Kubernetes/IaC deployment, and measured load testing. If any of these become required later, that's new scope requiring a new PRD revision, not a silent expansion of this one.

## 8. Assumptions

Full list with impact and reversal conditions: RFC §2.3. Two are called out here because they were confirmed directly with the product owner rather than inferred:

- **A10 — Audit/event history is a genuine product requirement**, queried by administrators. Confirmed 2026-09-24.
- **A9 — The team is solo today, plural tomorrow.** Confirmed 2026-09-24; this is why §6's "contributor accessibility" row and §9 below are treated as requirements, not nice-to-haves.

## 9. Documentation and test requirements

Elevated to their own section because of A9: documentation and tests are how a one-person project stops being a one-person dependency.

**Documentation.** Every module has a README explaining what it owns and why. Every non-trivial decision gets an ADR (`docs/adr/`). Every place the design adds to or reinterprets the frozen brief gets an entry in `docs/rfc/differences-from-requirements.md`. The RFC's onboarding path (§22) is kept current as the system grows, not written once and abandoned.

**Testing.** Per RFC §18, with an explicit addition here: every test file states, in a comment or accompanying doc, *what it verifies and why that verification matters* — not just what it calls. A test suite that passes but doesn't explain what failure it would have caught is not useful to a future contributor deciding whether it's safe to change the code it covers. Coverage spans:

- Unit tests — domain logic, no database
- Integration tests — real Postgres via testcontainers, because mocks can't verify constraint or transaction behavior
- Concurrency tests — the highest-value tier; see FR-3 and FR-7 acceptance criteria above
- Security tests — auth bypass attempts, IDOR checks (a student fetching another student's resource by ID), mass-assignment attempts, injection attempts against raw-SQL paths, rate-limit and lockout behavior on login
- Contract tests — event schema compatibility against the registry
- Query-shape tests — mechanical N+1 and offset-pagination regressions

A `docs/guides/testing.md` (to be authored alongside the test suite as it's built) will index every test category: what it checks, how to run it, and what a failure means — the "how to run them" and "what they check" the product owner asked to be maintained.

## 10. Traceability matrix

Maps each functional requirement to its architecture section, its governing ADR(s), and its test tier. This is the product-facing companion to RFC Appendix A, which maps requirements to architecture only; this table adds the test-coverage column the Execution Guidelines' deliverables checklist asks for.

| Requirement | RFC section | Key ADR(s) | Test tier |
|---|---|---|---|
| FR-1 Course & user management | §5.3.1–5.3.4, §8 | ADR-0002, 0005, 0012 | Unit, Integration |
| FR-1 Duplicate/limit/prerequisite enrollment rules | §8.1 | ADR-0012 | Concurrency, Integration |
| FR-2 Publishing workflow, partial-failure safety | §7 | ADR-0009, 0010, 0011 | Workflow, Integration |
| FR-3 Enrollment idempotency & recovery | §5.3.5, §6.6, §8.1 | ADR-0006, 0012 | Concurrency, Integration |
| FR-4 Event-driven, no duplicate processing | §6 | ADR-0006, 0007, 0008 | Concurrency, Contract |
| FR-5 Analytics (all ten metrics) | §9 | ADR-0017 | Integration |
| FR-6 Observability & diagnosability | §12 | — | Manual (Jaeger/Grafana walkthrough), Integration |
| FR-7 Concurrency primitives, no races/leaks/deadlocks | §10 | — | Concurrency (`-race`, `goleak`), Unit |
| Security (§11 threat model) | §11 | ADR-0015 | Security, Unit, Integration |
| Asset upload/storage | §7.4, §11.7 | ADR-0019 | Integration |

## 11. Milestones

Delivery sequencing (M0–M4) and its mapping to the Execution Guidelines' weekly review cadence are owned by the RFC (§21, §21.1) to avoid two documents drifting out of sync. See `docs/rfc/differences-from-requirements.md` (DR-006) for why our build order departs from a literal weekly reading of the Execution Guidelines, and why that departure is deliberate rather than a scheduling slip.

## 12. Open questions

Tracked as they arise; resolved items move to §8 (Assumptions) with a confirmation date, or trigger a new entry in `differences-from-requirements.md` if resolving them changes scope.

---

*End of PRD-001.*
