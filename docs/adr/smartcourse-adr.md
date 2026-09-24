# SmartCourse — Architecture Decision Records

| Field | Value |
|---|---|
| Status | Living document |
| Last updated | 2026-09-24 |
| Related | `smartcourse-rfc.md` |

## Review status (2026-09-24)

ADR-0001 through ADR-0018 were drafted and marked `Accepted` the same day, with no review cycle, and were reset to `Proposed` pending a joint technical review. That review is now complete. Six concrete issues were raised and resolved: MongoDB's role (ADR-0004, confirmed as designed — see note below), JWT revocation on privilege changes (ADR-0015 addendum), a missing password-reset/email-verification surface (RFC §5.3.1, §15.2; `differences-from-requirements.md` DR-008), an unstated CSRF/CORS posture (RFC §11.9), fixed-window rate limiting's boundary-burst flaw (ADR-0020, new), and an unaudited lock-ordering claim in ADR-0012 (addendum + RFC §8.5). The remaining ADRs were reviewed with no issues found. All are now `Accepted`. Ratification isn't permanent, though — any ADR can still be reopened and superseded as implementation surfaces new information; `Accepted` here means "reviewed and current," not "closed to discussion."

**On ADR-0001 and team size:** its context section mentions solo delivery as a feasibility factor, but the decision's actual determining factor is stated as enrollment's need for a local transaction — independent of headcount. Per direction from the product owner, team size is deliberately not a load-bearing input to architecture decisions here; the modular monolith stands on transactional integrity and avoiding unnecessary distributed-system complexity, which holds regardless of whether this is built by one engineer or ten.

## How to use this document

An ADR records a decision **as a point-in-time event, with its context intact**. The RFC describes the system as it currently stands; this document describes how it came to stand that way.

The distinction matters when something changes. Six months from now, a contributor looking at an unexpected design choice needs to know whether it was reasoned or accidental, and what was already tried. An RFC edited in place loses that; an ADR preserves it.

**Rules for this document:**

- A decision is never edited to reflect a reversal. It is marked `Superseded by ADR-NNNN` and a new record is appended.
- `Reversal trigger` states the observable condition that should prompt revisiting. A decision without one is an assertion; a decision with one is a position.
- Options that were considered and rejected stay in the record. The rejected option is often the more useful half.
- New decisions during implementation get a new ADR. The register is expected to grow.

**Statuses:** `Accepted` · `Superseded by ADR-NNNN` · `Deprecated` · `Proposed`

### Index

| ADR | Title | Status | RFC |
|---|---|---|---|
| [0001](#adr-0001--modular-monolith-over-microservices) | Modular monolith over microservices | Accepted | §4.1 |
| [0002](#adr-0002--postgresql-as-the-single-system-of-record) | PostgreSQL as the single system of record | Accepted | §5.1 |
| [0003](#adr-0003--course-content-in-postgresql-with-jsonb) | Course content in PostgreSQL with JSONB | Accepted | §5.5 |
| [0004](#adr-0004--mongodb-for-event-archive-and-audit-only) | MongoDB for event archive and audit only | Accepted | §5.4 |
| [0005](#adr-0005--uuidv7-for-primary-keys) | UUIDv7 for primary keys | Accepted | §5.2 |
| [0006](#adr-0006--transactional-outbox-for-event-publication) | Transactional outbox for event publication | Accepted | §6.2 |
| [0007](#adr-0007--topic-per-aggregate-partition-by-aggregate-id) | Topic per aggregate, partition by aggregate ID | Accepted | §6.5 |
| [0008](#adr-0008--protobuf-with-schema-registry-backward-compatibility) | Protobuf with Schema Registry, BACKWARD compatibility | Accepted | §6.8 |
| [0009](#adr-0009--temporal-for-publishing-only) | Temporal for publishing only | Accepted | §7.3 |
| [0010](#adr-0010--immutable-course-versions-with-blue-green-promotion) | Immutable course versions with blue-green promotion | Accepted | §7.2 |
| [0011](#adr-0011--lesson_key-stable-across-versions) | `lesson_key` stable across versions | Accepted | §5.3.3 |
| [0012](#adr-0012--row-level-locking-for-enrollment-limits) | Row-level locking for enrollment limits | Accepted | §8.1 |
| [0013](#adr-0013--asynq-over-rabbitmq) | Asynq over RabbitMQ | Accepted | §19.3 |
| [0014](#adr-0014--kafka-to-asynq-handoff-for-notifications-only) | Kafka to Asynq handoff for notifications only | Accepted | §6.7 |
| [0015](#adr-0015--jwt-access-tokens-with-rotating-refresh-tokens) | JWT access tokens with rotating refresh tokens | Accepted | §11.2 |
| [0016](#adr-0016--postgresql-full-text-search-over-a-dedicated-engine) | PostgreSQL full-text search over a dedicated engine | Accepted | §19.2 |
| [0017](#adr-0017--analytics-aggregates-in-postgresql-with-scheduled-reconciliation) | Analytics aggregates in PostgreSQL with scheduled reconciliation | Accepted | §9.3 |
| [0018](#adr-0018--docker-compose-profiles-for-tiered-local-development) | Docker Compose profiles for tiered local development | Accepted | §17.1 |
| [0019](#adr-0019--minio-for-learning-asset-object-storage) | MinIO for learning-asset object storage | Accepted | §2.3 (A1), §4.7 |
| [0020](#adr-0020--gcra-rate-limiting-over-fixed-window-counters) | GCRA rate limiting over fixed-window counters | Accepted | §5.7, §11.3 |
| [0021](#adr-0021--in-process-outbox-relay-with-a-worker-pool-ahead-of-kafka) | In-process outbox relay with a worker pool, ahead of Kafka | Accepted | §6.3, §21.0 |

---

## ADR-0001 — Modular monolith over microservices

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §4.1

### Context

SmartCourse must support course management, publishing, enrollment, progress, analytics, notifications, search, and audit. The brief emphasises scalable architecture and clear separation of responsibilities, which is often read as a mandate for microservices. Delivery is by one engineer over roughly three weeks onto Docker Compose, with the infrastructure alone (Postgres, Redis, Kafka, Schema Registry, Temporal, MongoDB, and the observability stack) already approaching ten containers.

### Options

**A. Modular monolith.** One binary, enforced internal boundaries, horizontally scalable as stateless replicas.

**B. Microservices.** Separate deployables per domain, communicating over the network.

**C. Monolith with no internal boundaries.** Fastest to write, and the thing both other options exist to avoid.

### Decision

**Option A.** Eight domain modules and a shared kernel in one binary, with two run modes (`--mode=api`, `--mode=worker`). Boundaries enforced by consumer-declared interfaces and a `depguard` lint rule that fails the build on violation.

The determining factor is enrollment. It writes an enrollment, initialises progress, updates a counter, and records an event — atomically. In a monolith that is one transaction. Split across services it becomes a distributed saga with compensation, which is a large amount of machinery to make a naturally atomic operation behave as though it were. Paying that cost requires a benefit, and neither independent scaling nor team autonomy applies here.

Option C is rejected because "clear separation of responsibilities" is an explicit evaluation criterion, and because a monolith without boundaries cannot be extracted later at any reasonable cost.

### Consequences

**Positive.** Transactional integrity where it matters. One build and one image, so API and workers cannot drift. Feasible for solo delivery. Local development is tractable. Extraction later means swapping an in-process implementation for an RPC client behind an interface that already exists.

**Negative.** No failure isolation — a runaway goroutine degrades the whole process. Mitigated by bounded concurrency (RFC §10.2), not eliminated. All modules scale together on the API tier. Boundary discipline depends on the lint rule holding; if it is disabled, erosion is silent.

### Reversal trigger

Multiple teams requiring independent deploy cadence, or one module's resource profile diverging sharply enough that co-scaling becomes wasteful.

---

## ADR-0002 — PostgreSQL as the single system of record

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.1

### Context

The stack offers PostgreSQL, MongoDB, and Redis. The brief demands high consistency and accuracy across all data models while also requiring an event-driven architecture. Without a designated authority, "which store is right?" becomes unanswerable during an incident — which is precisely the condition the brief describes as its existing problem.

### Options

**A. PostgreSQL authoritative; other stores derived.**

**B. Polyglot persistence by domain** — each store owns different entities outright.

**C. Event log as the source of truth**, all stores as projections (full event sourcing).

### Decision

**Option A.** PostgreSQL holds all transactional state. Redis and MongoDB hold derived or append-only data and are never consulted to answer "what is true".

Option B is what produces the inconsistency the project exists to fix: with no single authority, two stores disagreeing has no resolution procedure. Option C is intellectually attractive and disproportionate here — event sourcing imposes projection rebuilds, snapshotting, and versioned event replay on a team of one, to solve problems a relational database with an outbox already solves.

### Consequences

**Positive.** An unambiguous answer to "what is true". Every derived store is rebuildable from Postgres. Constraints enforce invariants at the storage layer rather than in application code that races.

**Negative.** Single point of failure — Postgres down is a full outage, accepted for this phase. Single primary is the scaling ceiling; read replicas are the documented next step (RFC §13.3).

### Reversal trigger

Write throughput exceeding a single primary, at which point sharding or a different topology is required — a much larger change than this record anticipates.

---

## ADR-0003 — Course content in PostgreSQL with JSONB

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.5

### Context

A course is a nested tree: course → modules → lessons, with lesson payloads that vary by type (video, text, PDF, link). This is the canonical shape for a document store, and MongoDB is in the stack. The question is whether the content tree should live there.

### Options

**A. PostgreSQL relational tables with a JSONB column** for type-specific lesson payloads.

**B. MongoDB** for the content tree; PostgreSQL for everything else.

**C. PostgreSQL, one JSONB blob per course version** holding the whole tree.

### Decision

**Option A.** Modules and lessons as relational rows; `lessons.content` as JSONB with a 64 KB check constraint.

The decision is made on access patterns rather than data shape. The course detail page — the most-requested authenticated page in a learning product — renders the outline **joined against this student's per-lesson completion and enrollment status**. Under Option B that becomes a cross-store join stitched in application code: two round trips, no referential integrity between lesson and progress, and a partial-failure mode on the hottest page in the product.

Option C loses per-lesson addressability, which progress tracking and the lesson player both need.

JSONB supplies the flexibility that motivated MongoDB — differing payloads per lesson type — while `lesson_key` stays joinable to `lesson_progress` and enforceable by constraints.

### Consequences

**Positive.** Content and progress join in one indexed query. Referential integrity across the tree. One store to back up and reason about. Flexible payloads retained.

**Negative.** Deeply nested or structurally queried content would be awkward. Copy-on-write versioning duplicates the tree per version — acceptable at tens of lessons per course, and bounded by version pruning (RFC §16).

### Reversal trigger

Lesson payloads becoming deeply nested and queried by internal structure, or content volume where version duplication becomes a storage problem.

---

## ADR-0004 — MongoDB for event archive and audit only

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.4, §19.9

### Context

MongoDB appears in the specified stack with no stated purpose. The governing principle for this project is that a technology is included only if some requirement is best served by it — presence in the stack is permission, not obligation. Audit and event-history querying were subsequently confirmed as genuine product requirements (RFC assumption A10).

### Options

**A. MongoDB for event archive, audit log, and failure forensics.**

**B. MongoDB for analytics read models** (the initial proposal in design discussion).

**C. Omit MongoDB**; keep events in a partitioned PostgreSQL table.

### Decision

**Option A**, with the constraint that MongoDB is write-only from the application's perspective and read-only from the operator's. No synchronous user-facing request path reads from it.

Option B was proposed and then withdrawn on examination. The ten required metrics are small, structured, and relational; PostgreSQL serves them with ordinary indexed queries and can recompute them from source tables in the same database, which is what makes reconciliation possible (ADR-0017). Putting them in MongoDB would have been reaching for a tool because it was available — the reasoning this project's constraints explicitly rule out.

Option C is defensible and was seriously considered. It loses on two counts: events across all topics have structurally different payloads, so in PostgreSQL they become JSONB blobs in one wide table, which is MongoDB's model with extra steps; and lesson-completion events are the highest-volume stream in the system, so archiving them plus running audit and forensic queries against the OLTP primary puts analytical and archival load on the database serving enrollment transactions.

### Consequences

**Positive.** Archival and audit load isolated from the transactional primary. Heterogeneous documents stored naturally. Unbounded growth confined to a store where it costs nothing operationally. `failed_events` holds the payload and failure history, making the "Failed Events" metric actionable rather than a bare count.

**Negative.** A fourth datastore to run, back up, and understand. Cross-store consistency between the archive and Postgres is eventual. A contributor must learn where each kind of data lives.

### Reversal trigger

Audit ceasing to be a requirement, or observed event volume low enough that a partitioned PostgreSQL table demonstrably suffices.

---

## ADR-0005 — UUIDv7 for primary keys

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.2

### Context

Every table needs a primary key strategy. The system must not leak business information through identifiers, must support IDs generated before insert (needed for the outbox, where an event references an entity created in the same transaction), and must not degrade index performance as tables grow.

### Options

**A. `bigserial`.** **B. UUIDv4.** **C. UUIDv7.**

### Decision

**Option C**, stored as native `uuid` (16 bytes) rather than `varchar` (36 bytes with incorrect ordering semantics).

`bigserial` leaks record counts, is trivially enumerable, and requires a database round trip before the application knows the ID. UUIDv4 solves both but is randomly distributed, so inserts scatter across the B-tree causing page splits and index bloat at volume. UUIDv7 embeds a timestamp prefix, so inserts append and index locality is preserved, while remaining non-enumerable and application-generated.

The exception is `outbox`, which uses `BIGSERIAL` because the relay reads in insertion order and a monotonic integer is the natural cursor.

### Consequences

**Positive.** No enumeration. No round trip before ID is known. Index locality comparable to a sequence. Natural rough time-ordering aids debugging.

**Negative.** 16 bytes against 8, which compounds across foreign keys and indexes. UUIDv7 requires a library (`google/uuid` v1.6+) rather than being native. Creation time is inferable from the ID — irrelevant for these entities.

### Reversal trigger

None anticipated. Changing this after data exists is a migration of every table and foreign key.

---

## ADR-0006 — Transactional outbox for event publication

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §6.2

### Context

The commissioning brief's central complaint is that course, enrollment, and analytics data disagree. The architectural cause is the **dual write**: a handler commits a state change to PostgreSQL and then publishes an event to Kafka. If the process dies between the two, the database records an enrollment that no consumer ever hears about. Analytics are then permanently wrong, with no mechanism to detect it. Retries cannot fix this — the retry state dies with the process.

### Options

**A. Transactional outbox** — event written in the same transaction, relayed asynchronously.

**B. Publish-then-commit** — publish to Kafka first, then commit.

**C. Change data capture (Debezium)** reading the PostgreSQL WAL.

**D. Best-effort publish after commit**, with application-level retry.

### Decision

**Option A.** Events are inserted into `outbox` inside the same transaction as the state change. A relay in the worker binary polls unpublished rows every 200 ms using `FOR UPDATE SKIP LOCKED` and publishes to Kafka.

No module publishes to Kafka directly. A Kafka producer imported outside `internal/platform/outbox` is a defect, and that rule is greppable in review.

Option B inverts the failure: events published for state changes that never committed, which is worse. Option D is the status quo the project exists to replace. Option C is genuinely good and rejected for operational cost — a connector to run and a replication slot that, if it stops being consumed, silently accumulates WAL until the disk fills. Polling at 200 ms meets the staleness targets without that hazard.

### Consequences

**Positive.** Events cannot be lost. Kafka being unavailable means the outbox accumulates and drains on recovery, with no data loss. `outbox_backlog` and `outbox_oldest_unpublished_seconds` give early warning. Event ordering per aggregate follows insertion order.

**Negative.** Latency of up to one poll interval. An extra table with its own growth and pruning policy. Delivery is at-least-once, which makes consumer idempotency mandatory rather than optional (ADR-0007 and RFC §6.6). Two rows written per state change.

### Reversal trigger

Outbox backlog metrics showing the poll loop cannot keep pace, at which point CDC becomes worth its operational cost.

---

## ADR-0007 — Topic per aggregate, partition by aggregate ID

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §6.5

### Context

Kafka topic and partition design determines ordering guarantees and consumer scalability. It is difficult to change after consumers exist and offsets are committed.

### Options

**A. Topic per event type** — `student-enrolled`, `course-published`, and so on.

**B. Topic per aggregate** — `enrollment.events`, `course.events`.

**C. One topic for everything.**

### Decision

**Option B.** Four topics — `identity.events`, `course.events`, `enrollment.events`, `progress.events` — partitioned by aggregate ID.

Option A destroys ordering between related facts: `CourseCompleted` could be processed before the `LessonCompleted` that caused it, because they are on different topics with independent consumer progress. It also multiplies partitions and consumer group management. Option C creates a single scaling bottleneck and forces every consumer to filter events it does not care about.

Partitioning by aggregate ID guarantees ordering **per aggregate** — all events for one course arrive in order. No global ordering is assumed anywhere in the design, and consumers must tolerate seeing events for different aggregates out of relative order.

`enrollment.events` is keyed on `course_id` rather than `student_id`, so all enrollment activity for one course lands on one partition and per-course counter updates stay ordered.

### Consequences

**Positive.** Per-aggregate ordering where it matters. Consumers subscribe only to relevant topics. Partition count scales consumer parallelism. Replay of one aggregate type is possible independently.

**Negative.** A hot aggregate (a popular course during launch) concentrates on one partition. Cross-aggregate ordering must never be assumed — a constraint contributors need to know. Changing partition count later rehashes keys and breaks ordering during the transition.

### Reversal trigger

A single course generating enough enrollment volume to saturate one partition, which would require a composite key and the loss of strict per-course ordering.

---

## ADR-0008 — Protobuf with Schema Registry, BACKWARD compatibility

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §6.8

### Context

Confluent Schema Registry is in the stack. It is the heaviest optional component, and a versioned JSON envelope would function at this scale. Under the governing principle, it must be justified by a requirement or omitted.

### Options

**A. Protobuf + Schema Registry, `BACKWARD` compatibility.**

**B. Avro + Schema Registry.**

**C. JSON with a hand-rolled version field.**

### Decision

**Option A.**

Two requirements force it. First, **multiple independent consumers of the same event**: `StudentEnrolled` is read by analytics, notifications, and audit. Without registry enforcement, a producer-side field rename breaks three consumers silently at runtime, discovered later as wrong dashboard numbers — exactly the class of inconsistency this project exists to eliminate. The registry rejects an incompatible schema at publish time, in CI.

Second, **the archive is permanent** (ADR-0004). Events are retained in MongoDB indefinitely for audit. In several years those documents must still be interpretable, which requires the schema they were written against to be recorded and resolvable. Option C provides a version integer and no way to resolve it to a schema.

`BACKWARD` compatibility means new consumers can read old events — precisely what replay and archive reading require. Adding optional fields is permitted; removing or renaming required fields is not.

Protobuf over Avro for better Go code generation ergonomics, smaller payloads, and native registry support without Avro's tooling overhead. Generated code is committed so a contributor can build without running the toolchain.

### Consequences

**Positive.** Breaking changes fail in CI rather than in production. Compact payloads. Generated Go types give compile-time safety at both ends. The permanent archive stays interpretable. Schema evolution becomes an explicit, reviewed act.

**Negative.** An additional container. A code generation step in the build. Events are not human-readable in transit, so debugging needs a decoder. Contributors must learn the compatibility rules.

### Reversal trigger

None anticipated. Removing the registry after events are archived would make the archive progressively uninterpretable.

---

## ADR-0009 — Temporal for publishing only

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §7.3

### Context

Temporal is in the stack. The system has two candidate multi-step processes: course publishing and enrollment. Applying Temporal to both would look thorough; applying it to neither would waste a capability that fits one of them well.

### Options

**A. Temporal for publishing only.**

**B. Temporal for publishing and enrollment.**

**C. No Temporal** — publishing as a chain of queue messages.

### Decision

**Option A.**

Publishing has every property durable execution exists for: multiple steps with external side effects, a requirement to reach a terminal state, per-step retry, recovery after process death mid-sequence, and a mandate that partial failure must not corrupt the result (brief FR-2). Option C means hand-rolling step state, retry bookkeeping, and crash recovery — which is the argument *for* Temporal, restated.

Enrollment has none of those properties. It is one short transaction with no external calls, atomic by construction. A workflow would add latency and machinery while removing the transactional guarantee that currently makes it correct. Option B would be a worse design, not a more thorough one.

The restraint is deliberate and is expected to be challenged. Being able to explain why two superficially similar flows get different treatment is the point.

### Consequences

**Positive.** Publishing survives worker restarts mid-sequence. Per-activity retry with independent policies. Temporal UI gives free visibility into stuck publishes. Enrollment stays fast and transactionally simple.

**Negative.** A significant component used in one place, which may read as under-utilisation. Contributors must learn workflow determinism rules. Temporal down means publishing unavailable — though nothing else is affected.

### Reversal trigger

Enrollment acquiring external steps — payment, approval, external roster synchronisation — at which point it becomes a saga and Temporal becomes the right tool.

---

## ADR-0010 — Immutable course versions with blue-green promotion

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §7.2

### Context

When an instructor updates an already-published course, the publishing workflow must run again. The naive model — a `status` column on `courses` moving to `publishing` — takes the course offline while validation runs, breaking students mid-lesson. The brief requires that partial failures must not corrupt the publishing workflow.

### Options

**A. Status on the course**, mutated in place during publish.

**B. Immutable versions** with an atomic pointer swap on success.

**C. Draft and published as two rows** with a boolean flag.

### Decision

**Option B.** `course_versions` holds immutable snapshots. `courses.published_version_id` is what students see; `courses.draft_version_id` is what is being edited. Publishing operates entirely on the draft. Only on full success does a single transaction swap the published pointer, mark the previous version `superseded`, and clear the draft pointer.

This is blue-green deployment applied to content. A failed publish leaves the live version untouched — the instructor sees an error, students see nothing. `PromoteVersion` is the final activity and the only one whose effect students can observe.

A partial unique index (`idx_one_draft_per_course`) makes concurrent publish attempts on the same course impossible at the storage layer rather than relying on an application check that races.

### Consequences

**Positive.** Zero-downtime updates. Partial failure is structurally harmless rather than handled. Version history for audit. Rollback is a pointer swap. Concurrent publishes prevented by the database.

**Negative.** Content is duplicated per version (bounded by pruning, RFC §16). Two pointer columns to reason about. Progress must be keyed on something stable across versions, which is ADR-0011 — a consequence that is easy to miss and expensive to discover late.

### Reversal trigger

Courses large enough that per-version duplication becomes a storage problem, which would push toward shared content rows with version ranges.

---

## ADR-0011 — `lesson_key` stable across versions

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.3.3

### Context

A direct consequence of ADR-0010. Because versions are immutable, republishing a course creates new `modules` and `lessons` rows with new primary keys. If `lesson_progress` referenced `lessons.id`, every student's progress would be orphaned the moment an instructor republished.

This failure surfaces only on the second publish of a course that already has enrolled students — which is to say, in production rather than in testing.

### Options

**A. Progress references `lessons.id`.**

**B. Progress references a stable `lesson_key`** carried forward when a version is cloned.

**C. Migrate progress rows** to the new lesson IDs during promotion.

### Decision

**Option B.** Every lesson carries a `lesson_key` (UUID) that is stable across versions, unique within its module. Cloning a version copies the key. `lesson_progress` is keyed on `(enrollment_id, lesson_key)`.

New lessons get a new key. Deleted lessons leave orphaned progress rows, which are simply not displayed — cheaper and safer than deleting student data on a content edit.

Option C is a migration inside the promotion transaction, scaling with enrollment count. For a popular course that makes promotion slow and lock-heavy at exactly the wrong moment.

### Consequences

**Positive.** Progress survives republication. Promotion stays O(1) regardless of enrollment count. Renaming or reordering a lesson preserves its completion state.

**Negative.** Two identifiers per lesson, which needs explaining to contributors. Orphaned progress rows accumulate for deleted lessons. Completion percentage is computed against the current version's lesson set, so adding lessons can move a completed student back to incomplete — correct behaviour, but visible.

### Reversal trigger

None. This is a correctness requirement of ADR-0010, not a preference.

---

## ADR-0012 — Row-level locking for enrollment limits

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §8.1

### Context

Courses may have an enrollment limit. Under PostgreSQL's default `READ COMMITTED` isolation, two concurrent transactions both reading `enrollment_count = 49` against a limit of 50 will both pass the check and both insert. The limit is silently violated, with no error raised anywhere.

This passes every manual test and fails under any real concurrency. The brief explicitly requires concurrent processing without data inconsistency.

### Options

**A. Application-level check** before insert.

**B. `SELECT … FOR UPDATE`** on the course row inside the transaction.

**C. `CHECK` constraint** with an atomic increment.

**D. `SERIALIZABLE` isolation** with retry.

### Decision

**Option B.** The enrollment transaction locks the course row before reading the count, so concurrent enrollments serialise on that row. The lock is held for microseconds.

Option A is the bug. Option C works and produces an opaque constraint violation rather than a meaningful "course is full" error, and cannot express prerequisite checks in the same breath. Option D is correct but imposes serialization-failure retry handling on every transaction in the system to solve one contended row — too broad an instrument.

Verified by an explicit concurrency test: N goroutines enrolling simultaneously, asserting the count never exceeds the limit. This is the single most valuable test in the suite, because the failure it catches is invisible to functional testing.

### Consequences

**Positive.** Correct under any concurrency level. Clear, mappable errors. The same lock covers the count check, prerequisite validation, and counter increment atomically.

**Negative.** Enrollments to the same course serialise, so a launch spike on one course is rate-limited by lock contention on one row. Acceptable — a few milliseconds per enrollment, and correctness is not negotiable here. Lock ordering must stay consistent across transactions to avoid deadlock; the course row is always locked first.

### Reversal trigger

Lock contention on a single course becoming a measured bottleneck, which would push toward a reservation pattern with a separate seat-allocation table.

### Addendum — lock ordering made explicit (2026-09-24)

"The course row is always locked first," above, was true in intent but hadn't been checked against every transaction that touches both `courses` and `enrollments`/`lesson_progress`. On audit, withdrawal (RFC §8.2) updated `enrollments` and then decremented `courses.enrollment_count` via a bare `UPDATE` — safe from a lost-update bug on its own (a single `UPDATE` statement takes its row lock atomically), but not visibly following the same explicit ordering as enrollment, which matters once a third transaction of this shape is added by someone who greps for the pattern to copy rather than rereads this ADR.

Fixed: withdrawal now opens with the same explicit `SELECT … FOR UPDATE` on `courses` as enrollment (RFC §8.2). RFC §8.5 adds a table auditing every current transaction against the rule, and is the place to extend — not just this paragraph — when a new transaction touches both tables (the flagged case being a future admin enrollment-override endpoint, not yet designed).

---

## ADR-0013 — Asynq over RabbitMQ

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §19.3

### Context

SmartCourse.md offers "Asynq (Redis-based) **or** RabbitMQ + native Go workers". An earlier draft of the Execution Guidelines also referenced Celery — a Python library with no Go client — for background workers; that reference did not fit a Go-only stack and was removed from the document by its owner before this baseline was set (RFC assumption A11). It is noted here only so a future reader does not wonder why Celery is absent from a document about background processing. The decision below stands on the SmartCourse.md options alone. The intended capability is queued background jobs with retry and scheduling.

### Options

**A. Asynq.** **B. RabbitMQ + native workers.** **C. Neither** — Kafka consumers only.

### Decision

**Option A.**

Redis is already deployed for caching, rate limiting, and counters, so Asynq adds no infrastructure. RabbitMQ's principal strength is flexible exchange and routing topologies — which is the one capability Kafka already provides here, so it would duplicate an existing capability while adding a broker to operate and monitor.

Asynq supplies precisely what Kafka lacks: delayed and periodic jobs (the capability Celery Beat would have provided), per-job exponential backoff, priority queues, and unique-task deduplication. It is Go-native, which fits the concurrency learning goals, and ships a monitoring UI that supports the observability deliverable.

Option C fails because Kafka has no delayed delivery, no scheduling, and no per-message retry without head-of-line blocking (ADR-0014).

**Honest counter-argument, recorded:** RabbitMQ is more common in polyglot enterprise environments and its dead-letter and routing model is richer. If the objective were breadth of exposure to message brokers rather than a lean design, RabbitMQ would be the stronger choice.

### Consequences

**Positive.** No new infrastructure. Go-native with typed handlers. Scheduling, retry, priorities, and deduplication out of the box. Monitoring UI included.

**Negative.** Redis becomes load-bearing for job durability as well as caching — a Redis loss now costs queued jobs, not just cache. Asynq is a smaller project than RabbitMQ with a narrower community. Non-Go consumers could not easily share the queue.

### Reversal trigger

Non-Go services needing to consume the same jobs, or routing requirements exceeding Asynq's model.

---

## ADR-0014 — Kafka to Asynq handoff for notifications only

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §6.7

### Context

With both Kafka and Asynq present, a Kafka consumer that enqueues an Asynq job looks like a redundant hop. Applied everywhere it would be; the question is whether there is a case where it earns its place.

### Options

**A. Consumers always enqueue jobs.** **B. Consumers never enqueue** — always act directly. **C. Consumers act directly, except notification delivery.**

### Decision

**Option C.**

Notification delivery calls a third-party provider that can be slow, rate-limited, or down. Retrying that inside a Kafka consumer leaves two bad choices: block the partition — one stuck notification stalls every event behind it, including analytics and audit — or drop to a dead-letter topic and lose the retry entirely. A task queue provides per-job exponential backoff and rate limiting without stalling the stream.

So `notification-dispatcher` does one thing: translate the event into a queued job and commit its offset immediately. Delivery reliability moves to the queue, where per-item retry belongs.

Every other consumer writes to PostgreSQL or MongoDB — fast, local, reliable — and acts directly. Option A would add latency and a failure point to all of them for no benefit.

### Consequences

**Positive.** Provider outages never stall event processing. Per-notification retry with backoff. Rate limiting against provider quotas. Consumer lag stays a signal of consumer health rather than provider health.

**Negative.** Two hops for notifications, so end-to-end delivery is harder to trace — mitigated by trace context propagating through both (RFC §12.2). The exception needs explaining, or contributors will copy the pattern by default.

### Reversal trigger

Notification delivery becoming a local, reliable operation, at which point the hop collapses into the consumer.

---

## ADR-0015 — JWT access tokens with rotating refresh tokens

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §11.2

### Context

Authentication is not specified anywhere in the brief, despite roles and an implied admin tier. The design target of tens of thousands of concurrent learners makes per-request authentication cost an architectural concern rather than an implementation detail.

### Options

**A. Stateless JWT access token + DB-stored rotating refresh token.**

**B. Redis-backed sessions.**

**C. External IdP (Keycloak).**

### Decision

**Option A.** 15-minute access tokens verified by signature; 7-day refresh tokens stored as SHA-256 hashes with rotation and reuse detection.

Option B requires a Redis round trip on every authenticated request, making read availability dependent on Redis at the stated concurrency. Option C is correct in production and here is an additional container to solve a problem that is not what this project is about.

Passwords use argon2id (64 MB, 3 iterations, parallelism 2) rather than bcrypt, which truncates input at 72 bytes and is no longer the OWASP first recommendation.

Refresh rotation: each refresh issues a new token and marks the old `replaced_by`. Presenting an already-replaced token indicates theft, and the entire token family is revoked. JWT validation explicitly asserts the signing method, since accepting the `alg` header unchecked permits the `alg: none` attack.

### Consequences

**Positive.** No datastore round trip on authenticated requests. Refresh tokens revocable. Stolen refresh tokens detected on reuse. A database disclosure yields no usable tokens.

**Negative.** Access tokens are not revocable within their 15-minute lifetime — the accepted cost of statelessness, documented rather than hidden. A compromised signing key compromises all tokens. Refresh rotation adds client-side complexity.

### Reversal trigger

A requirement for immediate global revocation, which would force Option B or a token denylist.

### Addendum — privilege-sensitive revocation check (2026-09-24)

**Problem.** Statelessness has one specific dangerous instance, not just a general one: an admin demotes a user, or suspends an account, and the demoted/suspended user's existing access token keeps working — for up to 15 minutes — on the exact class of endpoint where that matters (further admin actions, role-gated operations). The blanket answer ("accepted cost, 15 minutes") is fine for browsing a catalogue and wrong for that specific case.

**Decision.** `users` gains a `token_version SMALLINT NOT NULL DEFAULT 1`. It is embedded in the JWT as a `tv` claim at issuance. It is incremented, in the same transaction as the triggering change, whenever a user's role changes, their account is suspended, or an admin forces logout — and the new value is written to Redis (`user:tv:{user_id}`, no TTL, overwritten on next change) in the same request, so the check below never has to fall through to Postgres on the common path.

A new middleware, `RequireFreshPrivilege`, wraps only the routes where staleness is dangerous: `PATCH /admin/users/{id}/role`, every other `/admin/*` route, and any endpoint that mutates another user's access (suspension, enrollment overrides). It does one Redis `GET`, compares against the token's `tv` claim, and returns `401` with a "session outdated, please refresh" code on mismatch — cheap because it is a single in-memory lookup, and narrow because it runs on a small, named set of routes, not on every request. `RequireRole` (route-level, coarse) and ownership checks (service-level, §11.4) are unchanged; this is a third, deliberately minimal layer, not a replacement for either.

**Why this and not more.** A per-request Postgres or Redis check on *every* authenticated call is exactly what Option A was chosen to avoid (this ADR's own Option B). Scoping the check to the handful of routes where a stale token is actually dangerous keeps the original trade-off intact everywhere else. A Redis outage fails the check open or closed by explicit config (default: closed — `RequireFreshPrivilege` routes degrade to `503` rather than silently trusting a token that can't be checked; this is the one place in the system where failing closed is correct, because the whole point of the route is that trust must be current).

**Consequences.** Positive: privilege changes take effect within one Redis lookup, not 15 minutes, on exactly the routes where that matters. Negative: one more claim in the JWT, one more write path (role/status changes now touch Redis, not just Postgres), and `RequireFreshPrivilege` routes are no longer purely stateless — a deliberate, bounded exception to this ADR's core trade-off, not an erosion of it.

---

## ADR-0016 — PostgreSQL full-text search over a dedicated engine

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §19.2

### Context

The brief requires that search indexes be updated as part of publishing and names poor course discovery as a core problem. No search engine appears in the stack.

### Options

**A. PostgreSQL `tsvector` + `pg_trgm`** in a dedicated index table.

**B. Add OpenSearch or Meilisearch.**

**C. Naive `LIKE` queries** against `course_versions`.

### Decision

**Option A**, in a **separate `course_search_index` table** populated asynchronously — deliberately not a generated column or trigger on `course_versions`.

That structural detail is the important part. A trigger would make index maintenance invisible and synchronous, coupling it to the write transaction and effectively deleting a required publishing step (FR-2). A separate table keeps indexing asynchronous, replayable, and observable, and means substituting OpenSearch later replaces one consumer rather than restructuring the write path.

`tsvector` with a GIN index handles weighted full-text across title, description, tags, and instructor. `pg_trgm` handles fuzzy matching for typos. Faceting on category and level is an ordinary indexed filter. This is sufficient for a catalogue of this scale, and Option B adds a container for capability not yet needed.

### Consequences

**Positive.** No additional infrastructure. The index is transactionally consistent with itself and rebuildable from PostgreSQL. Faceting and sorting compose naturally with SQL. Denormalised, so search queries never touch the content tables.

**Negative.** Relevance ranking is cruder than a dedicated engine. No fuzzy matching across multiple fields simultaneously, no synonyms, no learning-to-rank. Index maintenance is application code rather than a managed service.

### Reversal trigger

Relevance ranking across many weighted fields becoming a requirement, or the catalogue exceeding roughly 100,000 courses.

---

## ADR-0017 — Analytics aggregates in PostgreSQL with scheduled reconciliation

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §9

### Context

Ten metrics are required. The brief names analytics inconsistency — dashboards disagreeing with platform state — as a core problem, so the reliability of these numbers matters more than their freshness.

### Options

**A. Event-driven increments only.**

**B. Query source tables on every dashboard read.**

**C. Event-driven increments plus scheduled reconciliation from source.**

### Decision

**Option C.** The aggregator consumer increments counters as events arrive, with the idempotency marker written in the same transaction so replay does not double-count. Separately, an Asynq periodic job recomputes aggregates from PostgreSQL every 60 seconds.

Option A drifts: a bug, a replay, or a manual correction leaves counters wrong with no self-correction — reproducing the exact problem the project exists to fix. Option B is always correct and does not scale; a completion-rate query scanning all enrollments on every dashboard load competes with transactional traffic.

Reconciliation makes drift self-healing and is the practical expression of the rebuildability rule. Averages (completion time, courses per student) are computed only by the scheduled path, since maintaining a running average incrementally is error-prone and nobody needs it current to the second.

Aggregates stay in PostgreSQL rather than MongoDB (ADR-0004) so reconciliation is a local query rather than a cross-store operation.

### Consequences

**Positive.** Low-latency counters with a self-healing correctness guarantee. Dashboard reads are single indexed queries. Drift is bounded by the reconciliation interval. Replay is safe.

**Negative.** Two code paths computing the same numbers, which must agree — a divergence between them is a bug class that only reconciliation reveals. Reconciliation load scales with data volume. Up to 60 seconds of staleness on aggregate metrics.

### Reversal trigger

Reconciliation queries becoming expensive enough to affect transactional performance, which would push toward incremental-only with periodic sampled verification.

---

## ADR-0018 — Docker Compose profiles for tiered local development

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §17.1

### Context

The full stack is roughly a dozen containers: Postgres, Redis, Kafka, Zookeeper or KRaft, Schema Registry, MongoDB, Temporal, Temporal UI, Prometheus, Grafana, Jaeger, and the OTel collector. Starting all of them to work on an HTTP handler wastes minutes per iteration and makes the project hostile to new contributors — which conflicts directly with the requirement that anyone should be able to contribute later.

### Options

**A. One Compose file, everything always.**

**B. Compose profiles** — `core`, `workflow`, `events`, `observability`, `full`.

**C. Separate Compose files per tier.**

### Decision

**Option B**, exposed through `make dev-core`, `make dev-events`, `make dev-full`.

A contributor working on enrollment logic starts two containers. Someone debugging a consumer starts six. Only integration testing needs all of them.

Option C duplicates service definitions across files, which drift. Profiles keep one definition per service with tier membership as metadata.

`make seed` populates a realistic dataset — instructors, courses at each lifecycle state, students with partial progress — so a contributor gets a working system with data in one command.

### Consequences

**Positive.** Fast iteration on the common path. Low barrier to first contribution. Resource usage matched to the task. One service definition, no drift.

**Negative.** A contributor may forget which profile a feature needs and see confusing failures — mitigated by readiness checks that name the missing dependency. Profile membership must be maintained as services are added.

### Reversal trigger

None. If container count shrinks enough that profiles stop earning their complexity, they can be collapsed.

---

## ADR-0019 — MinIO for learning-asset object storage

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §2.3 (A1), §4.7, §11.7, §17.1

### Context

SmartCourse.md requires instructors to "upload learning materials" and lists asset verification as a mandatory publishing step. Neither `SmartCourse.md` nor the Execution Guidelines names a storage technology for this. The listed tech stack covers relational data (PostgreSQL), documents (MongoDB), cache (Redis), and messaging — nothing suited to storing and serving binary files (video, PDF, slides) of arbitrary size.

**This is flagged explicitly: object storage is not part of the specified tech stack.** It is added because the functional requirement cannot be satisfied without it, not because a gap in the stack list is licence to add whatever is convenient. Every other ADR in this register justifies a choice *among* stack options; this one justifies adding a component the stack omitted. That distinction is the reason it is also logged in `docs/rfc/differences-from-requirements.md` rather than treated as an ordinary stack allocation.

### Options

**A. MinIO** — self-hosted, S3-compatible object storage, run as a Compose service.

**B. A real cloud provider's S3** (AWS S3, or equivalent) — even for local development.

**C. Store asset bytes in PostgreSQL** (`bytea` or large objects).

**D. Store assets on the local filesystem**, served by the API process.

### Decision

**Option A.** MinIO, exposed through Compose, with the application coded against the standard S3 API (`aws-sdk-go-v2`'s S3 client, pointed at MinIO's endpoint locally and swappable to real S3 in a later deployment without an application-code change).

Option B is rejected for this phase: it requires cloud credentials and network access for local development and CI, which contradicts the project's stated goal of low-friction contribution (G5) and the "build it ourselves, from scratch" working style. It remains the natural target for a real deployment later — MinIO's S3-API compatibility is what makes that swap free.

Option C is rejected on the same grounds ADR-0003 rejects oversized JSONB: large binary payloads in the OLTP primary degrade buffer cache efficiency for every other query and bloat backups with data that changes rarely and isn't relational.

Option D is rejected because it couples asset storage to a specific API replica's filesystem, which breaks the moment there is more than one API replica (§13.3 horizontal scaling) and gives the API process large binary transfers to proxy, which §11.7 and A1 specifically avoid via pre-signed URLs.

### Consequences

**Positive.** Satisfies a real functional requirement (asset upload) that the specified stack had no answer for. Pre-signed upload/download URLs keep asset bytes off the API's memory and connection budget entirely (§11.7). S3-API compatibility means production deployment later is a configuration change (endpoint, credentials, TLS), not a rewrite. Self-hosted, so no external dependency or cost during development, consistent with avoiding unnecessary SaaS.

**Negative.** One more container in the Compose stack, added to the `core` profile membership question addressed in ADR-0018 — asset upload is exercised early enough (course authoring, Week 1 scope) that it belongs in `core`, not a later tier. A component outside the commissioned stack, which must stay visible as a deliberate, justified addition rather than silently blending in — hence the differences-log entry. Bucket lifecycle, versioning, and retention policy are not yet decided (tracked as a new deferred decision).

### Reversal trigger

A real cloud deployment target being chosen, at which point MinIO is replaced by the provider's object storage — the S3-compatible client makes this a configuration change, which is the reason Option A was chosen over anything MinIO-specific.

---

## ADR-0020 — GCRA rate limiting over fixed-window counters

**Status:** Accepted · **Date:** 2026-09-24 · **RFC:** §5.7, §11.3

### Context

RFC §5.7 and §11.3 call for Redis-backed rate limiting (login attempts per IP/account, and general API throttling) without specifying an algorithm. The naive implementation — `INCR key; EXPIRE key window` — is a **fixed window counter**, and it has a well-known boundary flaw: a client can send the full limit in the last second of one window and the full limit again in the first second of the next, achieving roughly 2× the intended rate for traffic that happens to straddle a window edge. For login-attempt limiting specifically, that's a real gap, not a theoretical one — it directly weakens the credential-stuffing mitigation §11.3 exists to provide.

### Options

**A. Fixed window counter** (`INCR` + `EXPIRE`) — the naive default.

**B. Sliding window log** — a Redis sorted set of timestamps, trimmed on each check.

**C. Sliding window counter** — weighted average of the current and previous fixed windows.

**D. GCRA (Generic Cell Rate Algorithm) / leaky bucket**, via `go-redis/redis_rate`.

### Decision

**Option D.** `go-redis/redis_rate` implements GCRA as a single Lua script per check — one round trip, atomic, no separate counter-plus-expiry race to reason about.

Option A is the flawed status quo described above. Option B is exactly correct but stores one sorted-set entry per request and trims it on every check — precise, but the heaviest of the four options for a check that runs on every login attempt and potentially every API request. Option C is a good, cheap approximation (two integer keys, weighted math) and would have been a reasonable choice; Option D is preferred over it because the arithmetic and the race-safety are already solved and tested in a maintained library, rather than hand-rolled and then needing its own test suite to prove it doesn't have an off-by-one at the window boundary it was built to fix.

GCRA also naturally expresses burst allowance (a small burst above the steady rate, then smooth throttling) which is a better fit for legitimate API usage than a hard wall at exactly N requests.

### Consequences

**Positive.** No boundary-burst flaw. One atomic Redis call per check. Burst allowance is a configuration parameter, not extra code. Applies uniformly to login-attempt limiting (§11.3) and general per-route throttling (`ratelimit:{scope}:{identity}`, §5.7) with the same mechanism.

**Negative.** A new dependency (`go-redis/redis_rate`), narrow and single-purpose. GCRA's parameters (rate, burst) are slightly less intuitive to reason about at a glance than "N per minute," and need documenting wherever they're configured. Redis is already load-bearing for caching, counters, and Asynq (ADR-0013); this adds no new failure mode, only new keyspace on an existing dependency. Consistent with §13.4, a Redis outage fails rate limiting open (logged), not closed — throttling degrades before availability does.

### Reversal trigger

None anticipated. If Redis itself is ever removed from the stack, rate limiting moves with it regardless of algorithm.

---

## ADR-0021 — In-process outbox relay with a worker pool, ahead of Kafka

**Status:** Accepted · **Date:** 2026-09-25 · **RFC:** §6.3, §21.0

### Context

After M0/M1, every write path correctly wrote to the `outbox` table, but nothing read it — no relay, no Kafka, no consumers. Events accumulated, unpublished, forever. This was flagged directly during an audit against `docs/baseline/SmartCourse.md`: §7's concurrency requirements (goroutines, channels, worker pools, `sync.WaitGroup`) were, at that point, demonstrated nowhere in production code — only in a test harness — and FR-4/FR-5 (async event processing, analytics metrics) were 0% implemented. Kafka, Schema Registry, and Protobuf remain genuinely M3 scope; the question is whether something real and non-Kafka-shaped can close part of this gap honestly, without resorting to the exact anti-pattern RFC §10 warns against: adding concurrency primitives with no real independent work behind them, purely to tick a box.

### Options

**A. Wait for M3.** Leave the gap as documented and correctly scoped, add nothing now.

**B. Add contrived concurrency examples** — a toy goroutine pool processing fake work, just to demonstrate the primitives.

**C. Build the outbox relay now, with a bounded worker pool, dispatching to in-process Go handlers instead of Kafka** — real consumption of the real events already being written, with Kafka slotted in later at exactly the point where "publish" happens today.

### Decision

**Option C.** `internal/platform/relay` polls `outbox` with `SELECT ... FOR UPDATE SKIP LOCKED` (unchanged from the original ADR-0006 design), dispatches claimed rows over a buffered Go channel to a fixed-size worker pool, and each worker runs every registered `Handler` for that event type inside its own transaction, checking and writing `processed_events` in the same transaction as the handler's effect (RFC §6.6, unchanged). `internal/analytics` is the first handler set, registered in `cmd/smartcourse/modules.go` — the composition root, per RFC §4.4.

This is not a toy. Every primitive the brief names is present because the work genuinely needs it: goroutines and a channel for the worker pool itself; `sync.WaitGroup` to drain in-flight workers on shutdown; `sync.Mutex` guarding an in-process "claimed by this instance" set (a legitimately process-local concern, not one requiring Redis — no other replica needs to see it, unlike the rate limiter or the token-version cache); `sync/atomic` for processed/failed/given-up counters; `context.Context` for cancellation, with a specific and non-obvious subtlety documented below.

Option A was rejected because the gap it accepts is large and the brief weights it heavily — and because a real fix was available at reasonable cost. Option B was rejected on RFC §10's own terms: contrived concurrency is worse than none, because it looks like coverage without being evidence of anything.

**Two bugs found building this, kept here rather than quietly fixed:**

1. Handing a worker's in-flight job the *outer* (shutdown) context meant that once cancellation began, an already-running handler could finish its Go code successfully but have its final database commit fail with `context canceled` — silently losing a result that looked like it had drained gracefully. Fixed by detaching the per-job context (`context.WithoutCancel`, bounded by its own timeout) from the shutdown signal, so in-flight work's *commit*, not just its *function return*, survives shutdown. Caught by `TestRelay_GracefulShutdown_DrainsInFlightWork` asserting on the database row, not just an in-memory flag — the first version of that test only checked the flag and would have passed against the bug.
2. `course.Service.validateMetadata` (unrelated to the relay, found during the same audit pass) called `context.Background()` internally instead of accepting the caller's context — meaning a client disconnecting mid-publish couldn't cancel that query. Fixed alongside adding the concurrent publish-validation path.

### Consequences

**Positive.** FR-4's async processing and FR-5's analytics metrics have real, working implementations today, not just schema tables waiting for M3. Every worker-pool/channel/goroutine claim in this codebase is now backed by a passing concurrency test (`internal/platform/relay/relay_integration_test.go`) that would fail if the pooling, idempotency, retry, or shutdown behavior regressed. The migration path to Kafka is a genuinely small, well-understood change: the poll loop's "mark published" step becomes "publish to Kafka," and today's in-process `Handler` functions become Kafka consumer handlers with the same signature.

**Negative.** No real dead-letter store yet — a poison event past `maxAttempts` is marked published anyway (logged at `ERROR`), losing at-least-once delivery for that one event, because MongoDB's `failed_events` collection (RFC §6.9) is still M3 scope. Analytics recompute-from-source on every relevant event rather than incrementally, which is correct and simple but means a hot course publishing/enrolling rapidly does more work per event than a pure counter increment would — acceptable at this scale, and consistent with ADR-0017's own preference for correctness over incremental-update cleverness. No scheduled reconciliation job exists yet (ADR-0017's other half); drift, if any bug ever causes it, has no self-healing mechanism today. Single relay instance only — the design tolerates multiple replicas at the SQL level (`SKIP LOCKED`), but the in-process `inFlight` set means only one process's own re-polling is protected against; running two relay instances today would rely on `SKIP LOCKED` alone for cross-instance safety, which is sufficient but untested here.

### Reversal trigger

Kafka, Schema Registry, and Protobuf landing (M3) — at which point this package's poll-and-dispatch loop is replaced by a Kafka producer/consumer pair, and today's `Handler` functions are rehomed as Kafka consumer handlers largely unchanged.

---

## Appendix — Template for new records

```markdown
## ADR-NNNN — <Title>

**Status:** Proposed | Accepted | Superseded by ADR-NNNN · **Date:** YYYY-MM-DD · **RFC:** §N

### Context
What situation forces a decision. Constraints and requirements in play.
State the problem, not the preferred answer.

### Options
Each genuinely considered option, with its merits. A rejected option
described fairly is what makes this record useful later.

### Decision
What was chosen and why. Address the strongest counter-argument directly.

### Consequences
**Positive.** What this buys.
**Negative.** What it costs. An ADR with no negative consequences is incomplete.

### Reversal trigger
The observable condition that should prompt revisiting this.
"None anticipated" is acceptable when true, with a reason.
```

---

*End of decision register.*
