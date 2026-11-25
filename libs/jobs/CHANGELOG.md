# Changelog — libs/jobs

## [0.1.0] - not yet tagged

- `Register` uses the logical low queue of the scheduler and registry it is given (their asynqx namespace applies) and returns `ErrNamespaceMismatch` when the two report different namespaces. The three shared prune job names no longer collide across services once each worker sets its namespace.

- Created for the microservice split (docs/plans/microservices-split.md): the generic part of the old services/worker/jobs (Job, Register, Runner, PruneIdempotencyKeys, PruneOutbox) plus PruneProcessedEvents. Service-specific reconciliations stay with their owners.
