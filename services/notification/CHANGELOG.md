# Changelog — services/notification

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Asynq queues are namespaced per service: `notification-worker` prefixes its queues with `notification:` (`libs/asynqx` `Namespace`), so the workers can share one Redis database without taking each other's tasks (a worker used to archive another service's task as `handler not found`).
- `PROCESSED_EVENTS_RETENTION` (default 336h) is now a field of `libs/config` (`cfg.Jobs.ProcessedEventsRetention`; zero, negative or unparsable stops startup). The earlier entries below that say the binary reads it with `os.Getenv` describe the first version of the split.
- `services/worker` no longer exists; every concern it ran now lives in the owning service's binaries (microservices split, ADR-0033). The copy of `notification/` the entries below mention is gone from the old location.
- Added: the module, split out of `services/worker` (microservices split, wave B4). `notification/` is a copy of `services/worker/notification` (the old package stays until wave C deletes `services/worker`); its README and tests moved with it.
- Added: `cmd/notification-worker`, built with `libs/runtime`: consumers over `notification.Handlers()` through `libs/eventbus` (group `notification-dispatcher` and its retry topic), the dead-letter sink over `libs/mongostore` only when `MONGO_URI` is set, the Asynq pools, the delivery task on the critical queue, the pending-delivery sweeper, the SMTP provider from `SMTP_*`, and the `PruneProcessedEvents` job. Readiness: Postgres and Redis fatal; Kafka, Mongo and SMTP non-fatal.
- Added: `PROCESSED_EVENTS_RETENTION` (default 336h), read in the binary because `libs/config` has no field for it yet.
- Changed (tests): the package's integration tests run as `svc_notification` (`setup` helper over `env.ServiceDB`); the migration down/up tests keep the owner connection for DDL. The end-to-end mail test drives events through `relay.DeliverEvent` instead of an outbox and relay (this service has none) and asserts the `notification.processed_events` marker and a recognised redelivery.
- Added (test): `TestCleanLine_RemovesControlCharactersAndCollapsesWhitespace`. Making `cleanLine` the identity function used to leave every test green.
- Removed (tests): the cross-service tests of `services/worker/cmd/worker` (events to certificate and mail, one trace across the pipeline, metrics under a Kafka outage) are not copied; they become container E2E tests in wave C.
