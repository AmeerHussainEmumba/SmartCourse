# api/

The API contract — `openapi.yaml` (OpenAPI 3.0). This is the source of truth for what the HTTP API looks like (RFC §15.1); the root README's endpoint table and each module's README describe things in prose, but this file is what a client generator, a linter, or Postman would actually read.

## What's in here

- **`openapi.yaml`** — every endpoint implemented so far (identity, course, enrollment, progress, analytics), with request/response schemas and error shapes. An endpoint not listed here isn't built yet — see `docs/rfc/smartcourse-rfc.md` §21.0 for what's still pending and why. One spec covers all three HTTP services (`identity-api`, `course-api`, `analytics-api` — ADR-0023); the route paths and shapes didn't change when they split out of `cmd/api`, only which process answers them.

## Keeping it current

This file is grown alongside the code, module by module — not written once and left to drift. When you add or change an endpoint in a module's `handler.go`, update the matching path here in the same change. A contract that doesn't match the code is worse than no contract, because it actively misleads.

## Why it's not generated from code

Some frameworks generate OpenAPI from route annotations automatically. This project writes it by hand deliberately: the contract is a design artifact worth thinking about explicitly (what's optional, what error codes exist, what a client actually needs), not a byproduct of whatever the code happens to do.

## Four `servers` entries, on purpose

Three absolute URLs — `http://localhost:8090/api/v1` (identity-api), `:8091` (course-api), `:8092` (analytics-api) — plus one relative, `/api/v1`. A browser-hosted tool reading this file from its own origin — Swagger UI, specifically — has no "relative to here" to resolve a relative URL against the way a same-origin docs page would; it needs an absolute one, and since ADR-0023 split what used to be one `cmd/api` into three separate processes on three separate ports, there are three to pick from depending on which endpoint you're trying — the server dropdown at the top of Swagger UI is how you switch between them. The relative entry stays for when this spec is ever served from the same origin as the API itself, which isn't true of any local dev tool today.

## Actually calling the API from this file, not just reading it

`docker-compose.yml`'s `core` profile includes a Swagger UI container (`make dev-core` starts it) reading this file live — **http://localhost:8089**. Its "Execute" button fires real requests at whichever server is selected in the dropdown. That only works cross-origin because `env.example`'s `CORS_ALLOWED_ORIGINS` default includes `http://localhost:8089`, and all three API services read that same config value — change that port here and in `docker-compose.yml`'s `swagger-ui` service together, or the browser silently blocks every "Execute" click with no indication why (RFC §11.9's deny-by-default CORS policy doing exactly what it's for). Postman doesn't have this problem — it isn't a browser, so CORS doesn't apply — just File → Import this file directly.
