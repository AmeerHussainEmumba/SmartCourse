# Changelog — services/worker

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added the `notification` package (notification-dispatcher, ADR-0014): relay handlers for `identity.user_registered`, `identity.user_profile_updated`, `enrollment.student_enrolled`, `progress.course_completed` and `course.course_published` that write a recipient mirror and one delivery record per (event, kind, recipient) in the new `notification` schema (`migrations/notification`); `EnqueuePending`/`RunSweeper`, which hand pending deliveries to an `Enqueuer` and recover a crash between commit and enqueue; `NewDeliveryHandler` and `TaskTypeDeliver` (`notification:deliver`) for the queue task; a `Provider` interface with an SMTP implementation (net/smtp, STARTTLS and auth supported, aimed at Mailpit); escaped text and HTML templates. Not wired into `cmd/worker` yet: see the package README. No change to existing exports. The rare duplicate send (sent but not marked) is documented there, not denied.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
