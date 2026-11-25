# libs/jobs

The shared plumbing for a service worker's scheduled jobs. A `Job` is a name, a cron spec and a function; `Register` schedules it on the asynqx scheduler (one task per tick across all replicas) and installs its handler on the low queue.

Housekeeping every service worker needs is here: `PruneOutbox`, `PruneIdempotencyKeys` and `PruneProcessedEvents`. Each prunes the table in the schema of the connection it is given, so a service passes its own connection (its own role and search path). Reconciliations that belong to one service are built by that service with `Job` and `RunnerJob`.

Every job must be safe to run twice or from two replicas at once.
