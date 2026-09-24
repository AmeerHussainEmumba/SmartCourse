# cmd/

Entry points. By Go convention, everything under `cmd/` is a `package main` that produces a runnable binary; all the actual logic lives in `internal/`. There's one entry point in this project:

- **`cmd/smartcourse/`** — the SmartCourse binary. See its own README for what it does.

If a second binary is ever needed (a one-off migration tool, a CLI seed script bigger than a single command, etc.), it gets its own directory here, following the same pattern — a thin `main.go` that wires up and calls into `internal/`, not a place where real logic lives.
