# Agent Instructions

## Shared Instructions

Follow shared instructions in `../agent-instructions/AGENTS.md`.
Follow Modulo workspace instructions in `../agent-instructions/workspaces/modulo.md`.

These shared instruction files live in the sibling
[`modulo-org/agent-instructions`](https://github.com/modulo-org/agent-instructions)
repository when this repo is checked out as part of the Modulo workspace. For a
standalone checkout without that sibling repository, follow the local
instructions below and treat the shared references as unavailable.

Local instructions in this file override shared and workspace instructions.

## Repository Role

`fletch` is the Rust telemetry logging, local catalog, Parquet storage, and
sensor-fusion library for HIL and test-engineering data.

- `fletch_schema!` generates strongly typed ingestion streams backed by Apache Arrow builders.
- `FletchRun` and `FletchWorkspace` own run setup, metadata, local workspace paths, and storage layout.
- The SQLite catalog tracks runs, run metadata, and written Parquet files.
- The optional `view` feature enables Polars-backed analytical views and time-aligned joins.

## Data Rules

- Keep `timestamp_ns` and `run_id` as stable leading columns in generated telemetry schemas.
- Keep stream names, schema hashes, storage paths, catalog rows, row counts, timestamp bounds, file sizes, and content hashes aligned with the written Parquet files.
- Preserve sparse-row semantics: same-timestamp field writes coalesce into one row, missing fields remain null, and duplicate field writes keep the latest value.
- Keep local workspace roots represented as `file://` URIs at public boundaries and convert to filesystem paths at storage boundaries.
- Keep the Polars view layer behind the `view` feature.

## Commands

Run commands from the repository root unless a task is scoped to a specific file or example.

- Format: `cargo fmt`
- Test: `cargo test`
- Test views: `cargo test --features view`
- Lint: `cargo clippy --all-targets --all-features`
- Run examples as needed, for example `cargo run --example accelerometer --features view`

## Branching and Pull Requests

- Follow `../agent-instructions/gitflow.md` and `../agent-instructions/todoist.md` when available.
- If this standalone repository does not have a `staging` branch, open pull requests against its configured default branch.

## Knowledge Base

- Code conventions: `.steering/CODE_CONVENTIONS.md`
- Project architecture: `.steering/PROJECT_ARCHITECTURE.md`
