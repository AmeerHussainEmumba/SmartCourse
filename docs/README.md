# docs/

**New here? Start at [`for-the-devs/GETTING_STARTED.md`](for-the-devs/GETTING_STARTED.md), not this index.** It's the onboarding path — laptop setup, how to run things, the architecture in brief, and what to read next. Everything below is the reference material it points into.

## Layout

| Folder | What's in it | Edited how |
|---|---|---|
| [`baseline/`](baseline/) | The original commissioning brief and execution guidelines EduCorp/the assignment gave us. **Frozen.** | Never edited — this project is judged against it as-is |
| [`prd/`](prd/) | The product requirements: use cases, functional/non-functional requirements, traceability | Living — revised as scope is clarified |
| [`rfc/`](rfc/) | The architecture: `smartcourse-rfc.md` (the design, in full) and `differences-from-requirements.md` (every place the design adds to or reinterprets the frozen baseline, and why) | Living |
| [`adr/`](adr/) | Every individual architecture decision, recorded as a point-in-time event with its context intact — including decisions that were later reconsidered | Append-only — a reversed decision gets a new entry marked `Superseded`, the old one is never deleted |
| [`guides/`](guides/) | How-to references, e.g. `testing.md` (what's tested, why, how to run it) | Living |
| [`for-the-devs/`](for-the-devs/) | Onboarding — the thing to read before any of the above | Living |

## How these relate to each other

`baseline/` is what we're judged against. `prd/` and `rfc/` describe, respectively, *what* we're building and *how* — both derived from and consistent with `baseline/`, elaborating it without contradicting it. `differences-from-requirements.md` is the honesty mechanism: anywhere the design adds something the baseline didn't ask for, or reads an ambiguous section a particular way, that choice is logged there with its reasoning, rather than silently blended in as if the baseline had said it all along. `adr/` is the *history* of how the RFC got to look the way it does — the RFC describes the system as it stands today, the ADRs describe how it came to stand that way and what else was considered.

If you're trying to understand *why* something is built a certain way: check the relevant module's own README first (e.g. `internal/enrollment/README.md`), then the ADR it links to, then the RFC section for full context.
