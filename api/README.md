# api/

The API contract — `openapi.yaml` (OpenAPI 3.0). This is the source of truth for what the HTTP API looks like (RFC §15.1); the root README's endpoint table and each module's README describe things in prose, but this file is what a client generator, a linter, or Postman would actually read.

## What's in here

- **`openapi.yaml`** — every endpoint implemented so far (identity, course, enrollment, progress), with request/response schemas and error shapes. An endpoint not listed here isn't built yet — see `docs/rfc/smartcourse-rfc.md` §21.0 for what's still pending and why.

## Keeping it current

This file is grown alongside the code, module by module — not written once and left to drift. When you add or change an endpoint in a module's `handler.go`, update the matching path here in the same change. A contract that doesn't match the code is worse than no contract, because it actively misleads.

## Why it's not generated from code

Some frameworks generate OpenAPI from route annotations automatically. This project writes it by hand deliberately: the contract is a design artifact worth thinking about explicitly (what's optional, what error codes exist, what a client actually needs), not a byproduct of whatever the code happens to do.
