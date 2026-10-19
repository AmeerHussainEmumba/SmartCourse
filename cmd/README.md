# cmd/

Entry points. By Go convention, everything under `cmd/` is a `package main` that produces a runnable binary; all the actual logic lives in `internal/`. Two entry points live here:

- **`cmd/smartcourse/`** — the SmartCourse binary. See its own README for what it does.
- **`cmd/seed/`** — `make seed`: populates a local database with a realistic dataset through the real service layer. See its own README.

If another binary is ever needed (a one-off migration tool, etc.), it gets its own directory here, following the same pattern — a thin `main.go` that wires up and calls into `internal/`, not a place where real logic lives.
