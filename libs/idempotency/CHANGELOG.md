# Changelog — libs/idempotency

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [0.1.0] - not yet tagged

- Module created for Week 3 (RFC §15.1, §8.1 step 2). `Middleware` honours `Idempotency-Key` on the routes it is installed on; `PruneExpired` deletes rows past `expires_at`. Nothing has been tagged; consumers reference it through a `replace` directive.
