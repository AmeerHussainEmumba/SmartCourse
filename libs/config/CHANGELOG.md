# Changelog — libs/config

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added optional `MinIOConfig.PublicEndpoint` (`MINIO_PUBLIC_ENDPOINT`): the endpoint written into pre-signed URLs for browsers, when it differs from the internal `MINIO_ENDPOINT`. Additive; empty means the same as `Endpoint`.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
