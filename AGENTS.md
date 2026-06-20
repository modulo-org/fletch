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

`fletch` is the Rust telemetry logging, Iceberg-backed catalog, Parquet storage, and
sensor-fusion library for HIL and test-engineering data.

- `fletch_schema!` generates strongly typed ingestion streams backed by Apache Arrow builders.
- `FletchWorkspace` owns workspace URIs, Iceberg namespaces, and catalog naming.
- The embedded SQL Iceberg catalog tracks tables and appended Parquet data files.
- `BackgroundSink` writes Parquet files and commits Iceberg fast-append transactions.
- `FletchViewBuilder` uses Polars for analytical views and time-aligned joins.

## Data Rules

- Keep `timestamp_ns` and `run_id` as stable leading columns in generated telemetry schemas.
- Keep stream names, Iceberg table schemas, storage paths, row counts, file sizes, and Parquet metrics aligned with the written files.
- Preserve pending-row semantics: field writes for the current timestamp coalesce into one row, missing fields remain null, and duplicate field writes for that pending timestamp keep the latest value.
- Keep local workspace roots represented as `file://` URIs at public boundaries and convert to filesystem paths at storage boundaries.
- Do not present `s3://` storage as complete until catalog initialization and tests exist.

## Commands

Run commands from the repository root unless a task is scoped to a specific file or example.

- Format: `cargo fmt`
- Test: `cargo test`
- Lint: `cargo clippy --all-targets --all-features`
- Run examples as needed, for example `cargo run --example accelerometer`

## Branching and Pull Requests

- Follow `../agent-instructions/gitflow.md` and `../agent-instructions/todoist.md` when available.
- If this standalone repository does not have a `staging` branch, open pull requests against its configured default branch.

## Knowledge Base

- Code conventions: `.steering/CODE_CONVENTIONS.md`
- Project architecture: `.steering/PROJECT_ARCHITECTURE.md`
