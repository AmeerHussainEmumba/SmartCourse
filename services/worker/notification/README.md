# services/worker/notification

The notification-dispatcher (RFC §6.5, §6.7, ADR-0014): turns events into email without ever blocking the event stream on a mail server. Status: built as a library package; **wiring into `cmd/worker` and the Asynq adapter belong to other pieces of Week 3 Phase 5** (see "What the worker wires").

There is deliberately **no real mail provider** (Week 3 decision D6, DR-008, DR-015): the SMTP provider points at the self-hosted Mailpit sink (`docker-compose.yml`, SMTP 1025, UI/API 8025). The `Provider` interface is the seam if that ever changes.

## How it works

```
outbox -> relay -> Handlers()  --(rows only)-->  notification.deliveries
                                                       |
                       EnqueuePending (sweep) <--------+   pending rows
                              |
                          Enqueuer (Asynq)  ->  NewDeliveryHandler  ->  Provider (SMTP)  -> mark sent
```

1. **Handlers** (`handlers.go`) run in the relay's per-event transaction, the same one that writes the `processed_events` marker. They write mirror rows and one `deliveries` row, `UNIQUE (event_id, kind, recipient_id)`, so a redelivered event, a replay or two concurrent handlers produce one delivery. They never call the queue.
2. **`EnqueuePending`** (`dispatcher.go`) is the only thing that enqueues. The relay commits the handler's transaction after the handler returns and has no after-commit hook, so enqueueing from inside a handler could fire for a transaction that rolls back, or before its own commit. This design is not atomic and does not pretend to be: the delivery row is the source of truth, and a crash between commit and enqueue (or Redis being down) leaves a `pending` row that the next sweep enqueues. A row may be enqueued more than once; step 3 makes that harmless.
3. **`NewDeliveryHandler`** (`delivery.go`) is the task body. It claims the row (`pending` to `sending`, a lease), renders, sends, marks `sent`. Already sent, failed, suppressed, parked or being sent by a live claim: it returns nil without touching the provider.

Delivery statuses: `pending`, `sending`, `sent`, `parked` (recipient unknown, promoted when it arrives, never dropped), `suppressed` (recipient status not `active`), `failed` (provider 5xx, bad template, or attempts exhausted).

## Which events notify

| Event | Mail | Recipient |
|---|---|---|
| `identity.user_registered` | welcome | the user |
| `enrollment.student_enrolled` | enrollment confirmation | the student |
| `progress.course_completed` | congratulation | the student |
| `course.course_published` | "is now published" | the instructor (`instructor_id`; older events without it notify nobody) |

`course.course_archived` is not consumed: the RFC's event table has no notification column for it and the event carries no recipient. `identity.user_profile_updated` only refreshes the recipient mirror. RFC rows for `CoursePublishFailed` and `CertificateIssued` have no event in `libs/events` yet and are not built.

`student_enrolled` and `course_completed` carry ids, not emails, so the module keeps its own mirror (`migrations/notification`, schema `notification`): `recipients` (id, email, name, status, `last_event_id`), `course_titles`, `enrollment_courses`. It never reads `identity` or `course` tables (ADR-0024). The name is only overwritten by an event with a higher outbox id, the same out-of-order guard as `analytics.user_role_mirror`; a profile update that beats the registration creates the row without an email, and the email is filled in when the registration arrives. SQL is qualified `notification.` because the worker's `search_path` also contains `analytics` and `course`.

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

## Safety of the rendered message

- HTML parts use `html/template` (every value escaped); text and subject values have control characters (CR, LF, NUL, U+2028/2029) turned into spaces and are length-capped.
- The SMTP provider builds headers from encoded values (`mime.QEncoding` for the subject, `net/mail` for the display name), validates the recipient address (no CR/LF, no `< > , ;`, no spaces), and uses `net/smtp`'s dot-stuffing writer so a body line of `.` cannot end DATA early. Tested in `smtp_test.go`.
- The SMTP config never logs or echoes the password; `libs/config` refuses credentials over a cleartext link unless `SMTP_ALLOW_INSECURE_AUTH=true`, and `NewSMTPProvider` re-checks it for hand-built configs. When `SMTP_STARTTLS=true` the upgrade is mandatory: no STARTTLS offer means no send, never a cleartext fallback. Certificates are verified (TLS 1.2 minimum).

## What the worker wires (`cmd/worker`, not done here)

1. **Relay:** `for _, h := range notification.Handlers() { rl.Register(h) }`.
2. **Provider:** `notification.NewSMTPProvider(cfg.SMTP)` when `cfg.SMTP.Configured()`.
3. **Asynq adapter:** register `notification.TaskTypeDeliver` (`"notification:deliver"`) on the `critical` queue with a handler that calls `notification.NewDeliveryHandler(db, provider)`; wrap errors that satisfy `errors.Is(err, notification.ErrPermanent)` with `asynq.SkipRetry`. Give the task a timeout shorter than the lease (default 5m).
4. **Enqueuer:** an adapter implementing `Enqueue(ctx, taskType, payload)` over `libs/asynqx`; it must treat an already-queued duplicate as success.
5. **Sweep:** `go notification.RunSweeper(ctx, db, enqueuer, 2*time.Second, notification.SweepOptions{})` (it calls `EnqueuePending` on a ticker, logs failures and keeps going), started once at worker boot so rows committed before a restart are picked up immediately.
6. **Compose (worker service):** `SMTP_HOST=mailpit`, `SMTP_PORT=1025`; optional `SMTP_FROM`. No credentials are needed for Mailpit.

## Tests

- `smtp_test.go`, `templates_test.go` (no Docker): SMTP provider against an in-process fake server (`fakesmtp_test.go`, `net.Listen`) with failure injection (4xx/5xx at MAIL, RCPT and after DATA, dropped connection, hang/timeout, refused connection, STARTTLS offered or not, untrusted certificate, cleartext credentials), header injection, SMTP smuggling via a `.` line, templates.
- `dispatcher_integration_test.go` (`-tags=integration`, Postgres via testcontainers): one delivery per event under repeated and 16-way concurrent delivery; rollback leaves nothing; crash between commit and enqueue is recovered by the sweep; provider down then up gives exactly one send; concurrent tasks send once; lease takeover; permanent failures; out-of-order profile updates; unknown recipient parked then delivered; end to end from the outbox through the real relay and SMTP provider with a header-injection name; migration down/up round trip.
- `mailpit_integration_test.go` (`-tags=integration`): the SMTP provider against the real `axllent/mailpit` container (version from `docker-compose.yml`), asserted through Mailpit's HTTP API.

## Not built

Notification preferences (RFC §21), push notifications, a real mail provider, unsubscribe handling, per-recipient rate limiting beyond the queue's own, an admin view of failed deliveries, a status feed from `identity.user_role_changed`.

See `services/README.md`'s module status table and `docs/rfc/smartcourse-rfc.md` §21.0 for what is deferred across the codebase.
