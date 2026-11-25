# services/notification/notification

The notification-dispatcher (RFC §6.5, §6.7, ADR-0014): turns events into email without ever blocking the event stream on a mail server. This package is the library; `cmd/notification-worker` wires it (see the service README and "What the worker wires"). Moved here from the retired `services/worker` in the microservices split.

There is deliberately **no real mail provider** (Week 3 decision D6, DR-008, DR-015): the SMTP provider points at the self-hosted Mailpit sink (`docker-compose.yml`, SMTP 1025, UI/API 8025). The `Provider` interface is the seam if that ever changes.

## How it works

```
Kafka (source topics + retry topic) -> eventbus consumers -> Handlers()  --(rows only)-->  notification.deliveries
                                                       |
                       EnqueuePending (sweep) <--------+   pending rows
                              |
                          Enqueuer (Asynq)  ->  NewDeliveryHandler  ->  Provider (SMTP)  -> mark sent
```

1. **Handlers** (`handlers.go`) run in the consumer's per-event transaction (`relay.DeliverEvent`), the same one that writes the `notification.processed_events` marker. They write mirror rows and one `deliveries` row, `UNIQUE (event_id, kind, recipient_id)`, so a redelivered event, a replay or two concurrent handlers produce one delivery. They never call the queue.
2. **`EnqueuePending`** (`dispatcher.go`) is the only thing that enqueues. The consumer commits the handler's transaction after the handler returns and has no after-commit hook, so enqueueing from inside a handler could fire for a transaction that rolls back, or before its own commit. This design is not atomic and does not pretend to be: the delivery row is the source of truth, and a crash between commit and enqueue (or Redis being down) leaves a `pending` row that the next sweep enqueues. A row may be enqueued more than once; step 3 makes that harmless.
3. **`NewDeliveryHandler`** (`delivery.go`) is the task body. It claims the row (`pending` to `sending`, a lease), renders, sends, marks `sent`. Already sent, failed, suppressed, parked or being sent by a live claim: it returns nil without touching the provider.

Delivery statuses: `pending`, `sending`, `sent`, `parked` (recipient unknown, promoted when it arrives, never dropped), `suppressed` (recipient status not `active`), `failed` (provider 5xx, bad template, or attempts exhausted).

## Which events notify

| Event | Mail | Recipient |
|---|---|---|
| `identity.user_registered` | welcome | the user |
| `enrollment.student_enrolled` | enrollment confirmation | the student |
| `progress.course_completed` | congratulation | the student |
| `course.course_published` | "is now published" | the instructor (`instructor_id`; older events without it notify nobody) |
| `progress.certificate_issued` | "your certificate for ... has been issued" (no verification code) | the student |
| `course.publish_failed` | "publishing ... failed", naming the failed step and a bounded reason | the instructor (`instructor_id`; an event without it notifies nobody) |

`course.course_archived` is not consumed: the RFC's event table has no notification column for it and the event carries no recipient. `identity.user_profile_updated` only refreshes the recipient mirror. The RFC rows for `CoursePublishFailed` (instructor job) and `CertificateIssued` (delivery job) are built (DR-026, 2026-11-24). Both templates escape every interpolated value in the HTML part, the subject carries only the course title (cleaned to one line and capped), and the publish-failed reason appears only in the body, flattened to one line and capped at 500 characters. One delivery per event, as for the other kinds; an unknown recipient is parked. Events written before this change were not mailed.

`student_enrolled` and `course_completed` carry ids, not emails, so the module keeps its own mirror (`migrations/notification`, schema `notification`): `recipients` (id, email, name, status, `last_event_id`), `course_titles`, `enrollment_courses`. It never reads `identity` or `course` tables (ADR-0024). The name is only overwritten by an event with a higher outbox id, the same out-of-order guard as `analytics.user_role_mirror`; a profile update that beats the registration creates the row without an email, and the email is filled in when the registration arrives. SQL is qualified `notification.`. The binary connects as `svc_notification` with `search_path` `notification,public`, so the qualification is belt and braces here; keep it, because tests and tools that use the owner connection (search path `public` only) rely on it.

`recipients.status` is never fed by the events consumed today (it stays `active`); `identity.user_role_changed` carries a status and is the natural future feed.

## Failure handling

| Situation | Behaviour |
|---|---|
| Provider down / SMTP 4xx / timeout | row back to `pending` with `last_error`, error returned: the queue retries with backoff |
| SMTP 5xx, unknown template, malformed payload | error wraps `ErrPermanent`: the row becomes `failed` where it exists; the queue adapter should map `ErrPermanent` to `asynq.SkipRetry` |
| Recipient not in the mirror yet | `parked`; the registration handler (or the sweep) promotes it |
| Task lost (Redis flushed, archived by Asynq) | the sweep re-enqueues a `pending` row after `RequeueAfter` (default 5m) |
| Worker died mid-send | `sending` lease (default 5m) expires, the sweep re-enqueues, the task takes the row over |
| Attempts exhausted (`MaxAttempts`, default 10) | the sweep marks the row `failed` and logs at ERROR |

### The duplicate-send window that remains

Exactly-once delivery to an external system is impossible and is not claimed (RFC §6.6). The delivery row makes a duplicate unlikely, not impossible. A second send can happen when:

- the provider accepted the mail but marking the row `sent` failed (database error, process killed): the row stays `sending` and is claimed again once the lease expires;
- a send outlives its lease (keep `Lease` longer than the SMTP send timeout plus the queue's task timeout);
- an SMTP connection drops after the final `.` of DATA but before the server's reply: the server may have accepted it, and the retry sends it again.

The accepted trade-off is a rare duplicate rather than a lost mail. `Message-ID` is the delivery id (`<delivery-id@from-domain>`), so a duplicate is recognisable in the mailbox. After a successful send the handler retries the `sent` mark three times before giving up.

## Trace continuation (ADR-0032)

The sweeper enqueues a delivery seconds after the handler wrote its row, from a loop that is in no trace, so the trace has to travel on the row. `InsertDelivery` stores the traceparent of the span the handler runs under (the Kafka consume span, a descendant of the originating request) in `deliveries.traceparent` (migration `000002`, nullable: NULL for older rows and when tracing is off). `EnqueuePending` enqueues each task under `telemetry.ContinueFromEvent(ctx, row.traceparent)`; `libs/asynqx`'s client then starts the enqueue span and puts the traceparent in the task headers, and the delivery job span is its child. Without the column the job would be the root of a new trace and "one trace across the pipeline" would stop at the sweeper. The queue payload (`DeliveryPayload(id)`) is unchanged, so `NewDeliveryHandler`, tasks already in Redis and the deterministic task id are unaffected.

## Safety of the rendered message

- HTML parts use `html/template` (every value escaped); text and subject values have control characters (CR, LF, NUL, U+2028/2029) turned into spaces and are length-capped.
- The SMTP provider builds headers from encoded values (`mime.QEncoding` for the subject, `net/mail` for the display name), validates the recipient address (no CR/LF, no `< > , ;`, no spaces), and uses `net/smtp`'s dot-stuffing writer so a body line of `.` cannot end DATA early. Tested in `smtp_test.go`.
- The SMTP config never logs or echoes the password; `libs/config` refuses credentials over a cleartext link unless `SMTP_ALLOW_INSECURE_AUTH=true`, and `NewSMTPProvider` re-checks it for hand-built configs. When `SMTP_STARTTLS=true` the upgrade is mandatory: no STARTTLS offer means no send, never a cleartext fallback. Certificates are verified (TLS 1.2 minimum).

## What the worker wires (`cmd/notification-worker`)

1. **Consumers:** `eventbus.NewConsumers(cfg.Kafka, notification.Handlers(), db, ...)`: one group, `notification-dispatcher`, on the union of the handlers' topics plus `retry.notification-dispatcher`. Exhausted events are parked in MongoDB `failed_events` through `libs/mongostore` only when `MONGO_URI` is set (park before drop); with no Mongo the event is logged in full and dropped.
2. **Provider:** `notification.NewSMTPProvider(cfg.SMTP)` when `cfg.SMTP.Configured()`. Without it nothing is registered or swept, deliveries stay `pending` and a warning says so.
3. **Asynq adapter:** `notification.TaskTypeDeliver` (`"notification:deliver"`) on the `critical` queue with `notification.NewDeliveryHandler(db, provider)`; `ErrPermanent` becomes `asynqx.SkipRetry` (archived, not retried). Task timeout 2 minutes, below the 5 minute lease.
4. **Enqueuer:** an adapter over `libs/asynqx`; the task id is `DeterministicID(type, payload, 5-minute time bucket)`, so a second enqueue inside the bucket is a duplicate and one after it gets a fresh id (an archived task keeps its id, a fixed id would strand the row).
5. **Sweep:** `notification.RunSweeper` every 2 seconds, the single enqueue path.
6. **Jobs:** `PruneProcessedEvents` through `libs/jobs` on the scheduler.

## Retry timing (E07)

A failed send puts the row back to `pending` and returns the error; the queue retries it with its backoff. The sweeper does not re-enqueue it early (`TestFailedSend_IsNotReEnqueuedBySweepBeforeRequeueAfter`: a pending row enqueued within `RequeueAfter`, 5 minutes, is left alone). The retry gaps the E2E run saw (about 2 s, then about 6 s, flat instead of doubling) came from Asynq's 5-second delayed-task check, fixed in `libs/asynqx` (`DefaultDelayedTaskCheckInterval`, 1 s). Not covered here: a task that exhausts its retries and is archived keeps its task id, so the sweeper's later re-enqueue of the still-pending row is reported as a duplicate and does not run it again (inferred from Asynq's id-conflict behaviour, not tested).

## Tests

- `smtp_test.go`, `templates_test.go` (no Docker): SMTP provider against an in-process fake server (`fakesmtp_test.go`, `net.Listen`) with failure injection (4xx/5xx at MAIL, RCPT and after DATA, dropped connection, hang/timeout, refused connection, STARTTLS offered or not, untrusted certificate, cleartext credentials), header injection, SMTP smuggling via a `.` line, templates.
- `dispatcher_integration_test.go` (`-tags=integration`, Postgres via testcontainers; tests run as `svc_notification` through `setup(t)` in `setup_integration_test.go`, the migration down/up tests use the owner because they run DDL): one delivery per event under repeated and 16-way concurrent delivery; rollback leaves nothing; crash between commit and enqueue is recovered by the sweep; provider down then up gives exactly one send; concurrent tasks send once; lease takeover; permanent failures; out-of-order profile updates; unknown recipient parked then delivered; end to end from events (through `relay.DeliverEvent`, no outbox: this service has none) and the real SMTP provider with a header-injection name, including the `notification.processed_events` marker and a recognised redelivery; migration down/up round trip.
- `mailpit_integration_test.go` (`-tags=integration`): the SMTP provider against the real `axllent/mailpit` container (version from `docker-compose.yml`), asserted through Mailpit's HTTP API.

## Not built

Notification preferences (RFC §21), push notifications, a real mail provider, unsubscribe handling, per-recipient rate limiting beyond the queue's own, an admin view of failed deliveries, a status feed from `identity.user_role_changed`.

See `services/README.md`'s module status table and `docs/rfc/smartcourse-rfc.md` §21.0 for what is deferred across the codebase.

## Mutation note: `cleanLine`

`cleanLine` (provider.go) is the control-character stripper for subjects and display names. Until the split it had no direct test: replacing it with the identity function left every test green, because the MIME Q encoder and `net/mail` also neutralise CR and LF, so the indirect tests proved the pipeline, not this function. `TestCleanLine_RemovesControlCharactersAndCollapsesWhitespace` now pins it and fails under that mutation (verified by hand, not automated). The other layers (Q-encoding, `net/mail`, address validation) remain the defence in depth.
