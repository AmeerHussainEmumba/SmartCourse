# Changelog — libs/jobs

## [0.1.0] - not yet tagged

- Created for the microservice split (docs/plans/microservices-split.md): the generic part of the old services/worker/jobs (Job, Register, Runner, PruneIdempotencyKeys, PruneOutbox) plus PruneProcessedEvents. Service-specific reconciliations stay with their owners.
