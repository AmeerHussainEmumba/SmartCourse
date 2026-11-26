# Changelog — libs/idempotency

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [0.1.0] - not yet tagged

- Fixed (behaviour B-02, B-03; security S-03, S-04, S-05): the response is stored byte for byte in new columns `response_raw BYTEA` and `response_headers JSONB` (migration `course/000009`, additive) instead of `JSONB`. A 2xx with an empty or non-JSON body, or with a `\u0000` escape, is now stored and replayed (before, the key was released and the handler ran again); a replay is byte-identical to the first response and carries `Content-Type` and `Location`. Rows from before the migration replay from `response_body` as before. Requires migration `course/000009` before the new binary; an old binary running against the new schema still works (it ignores the new columns) but replays of rows the new binary wrote would have an empty body.

- Module created for Week 3 (RFC §15.1, §8.1 step 2). `Middleware` honours `Idempotency-Key` on the routes it is installed on; `PruneExpired` deletes rows past `expires_at`. Nothing has been tagged; consumers reference it through a `replace` directive.
