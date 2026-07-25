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

`fletch` is the Rust telemetry logging, Parquet storage, and sensor-fusion
library for HIL and test-engineering data.

- `FletchStreamBuilder` builds dynamic ingestion streams backed by Apache Arrow builders.
- `Stream<T>` and `#[derive(FletchSchema)]` provide a typed facade for static schemas.
- `FletchWorkspace` owns the local root folder and storage layout.
- Parquet file-level key/value metadata stores run and user-provided metadata.
- The optional `view` feature enables Polars-backed analytical views and time-aligned joins.

## Data Rules

- Keep `timestamp_ns` as the stable leading column in generated telemetry schemas.
- Keep stream names, storage paths, and file-level metadata aligned with the written Parquet files.
- Keep Fletch-owned `fletch.*` metadata authoritative over user-provided metadata.
- Preserve sparse-row semantics: same-timestamp field writes coalesce into one row, missing fields remain null, and duplicate field writes keep the latest value.
- Keep local workspace roots as filesystem paths at public boundaries.
- Keep the Polars view layer behind the `view` feature.

## Commands

Run commands from the repository root unless a task is scoped to a specific file or example.

- Format: `cargo fmt`
- Test: `cargo test`
- Test views: `cargo test --features view`
- Lint: `cargo clippy --all-targets --all-features`
- Run examples as needed, for example `cargo run --example accelerometer --features view`

## Branching and Pull Requests

- If this standalone repository does not have a `staging` branch, open pull requests against its configured default branch.

## Knowledge Base

- Code conventions: `.steering/CODE_CONVENTIONS.md`
- Project architecture: `.steering/PROJECT_ARCHITECTURE.md`
