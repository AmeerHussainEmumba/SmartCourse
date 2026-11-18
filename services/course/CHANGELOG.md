# Changelog — services/course

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- `POST /courses/{id}/publish` claims the draft (state `publishing`) before starting the workflow. A duplicate while a publish is running now returns **409 `publish_in_progress`** (was 202 attaching to the running workflow); a failed workflow start releases the claim; a claim older than 15 minutes may be reclaimed. Fixes a race where a fast-failing workflow's `publish_failed` could be overwritten with `publishing` (F-37, ADR-0028). Needs migration `migrations/course/000006_publish_started_at`.

- Fixed (F-01, F-06, F-07, F-08, F-10, F-11, F-17, F-20, F-21, F-22, F-23, F-25, F-27, F-36): see `docs/plans/module-split-findings.md`. Behaviour changes: non-owner access to an existing course now returns 404 `not_found` (was 403 `not_owner`); publishing an archived course returns 409 `course_archived`; `ValidateMetadata` requires a lesson; deleting a module/lesson of a non-draft version returns 409 `no_draft`; `MarkComplete` for a lesson not in the published version returns 404 `lesson_not_found`; instructor write routes enforce the token-version check; NUL bytes, deeply nested content and out-of-range integers return 400.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
