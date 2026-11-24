# libs/idempotency

`Idempotency-Key` handling (RFC §15.1, §8.1 step 2) for the non-idempotent write routes. A request carrying the header runs at most once per `(key, user, endpoint)`; a retry gets the stored response back.

## Behaviour

- **No header:** the request passes straight through.
- **First request with a key:** claims the key with one atomic `INSERT ... ON CONFLICT` into `platform.idempotency_keys`, runs the handler, and stores the response if it was a 2xx with a JSON body.
- **Retry after completion:** the stored status and body are returned with `Idempotent-Replayed: true`. The body is stored as `JSONB`, so it is equal in content to the first response but whitespace and key order may differ.
- **Retry while the first is still running:** `409 idempotency_request_in_progress` with `Retry-After: 1`.
- **Same key, different request** (method, path, query or body): `422 idempotency_key_reused`.
- **4xx, 5xx, panics, non-JSON or over-1-MiB responses:** the key is released, so the retry runs the handler again, exactly as it would without a key. This relies on the existing database constraints for duplicate protection, as before.
- **Abandoned keys:** an `in_progress` row older than the lease (3 minutes by default, longer than the 120 s request timeout) can be claimed by a new request. Completed rows expire after 24 hours (RFC §15.2) and can be claimed again; `PruneExpired` deletes them and is meant to run as a scheduled worker job.
- **Key format:** 1 to 255 printable ASCII characters, one header value; otherwise `400 idempotency_key_invalid`.
- **Scope:** keys are per authenticated user. A request with no user (`Options.UserID` returns false) is not subject to idempotency handling.

## Use

Install after authentication, on the routes that need it only (not on `GET`, and not on `PATCH`/`DELETE`/`PUT`, which are idempotent by construction):

```go
idem := idempotency.Middleware(db, idempotency.Options{UserID: userFromRequest})
r.POST("/enrollments", idem, h.enroll)
```

## Testing

`idempotency_integration_test.go` runs against real Postgres: replay, same key with a different body, per-user scoping, anonymous passthrough, ten concurrent requests (the handler runs once), release on 4xx/5xx/panic, invalid keys, takeover of an abandoned key, expiry and pruning. `services/course/enrollment` has HTTP-level tests of `POST /enrollments`.

## Not covered

- Whether a client that gets a 5xx and retries sees a duplicate-protection 409 from the database constraint instead of the original success. That is the pre-existing behaviour for failed requests and is deliberate (see above).
