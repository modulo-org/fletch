# Fletch Code Conventions

## Rust Style

- Use `cargo fmt` formatting.
- Keep the public API builder-oriented and ergonomic for test-engineering workflows.
- Prefer explicit domain names for workspaces, runs, streams, storage layout, timestamps, file metadata, and analytical views.
- Use `anyhow::Result` consistently for the current library surface unless a typed error boundary is introduced across the crate.
- Keep module boundaries narrow: workspace setup, storage config, dynamic stream ingestion, background sink, file metadata, and optional views should stay separately owned.
- Add concise comments only for non-obvious Arrow, Parquet, or Polars join behavior.

## Data And Storage Practices

- Treat `timestamp_ns` as the stable leading column in generated telemetry schemas.
- Preserve stream names from dynamic builder names or `FletchSchema` struct identifiers unless a deliberate migration path is added.
- Keep local workspace roots represented as filesystem paths at public boundaries.
- Preserve the default layout semantics: run, stream, and unique Parquet file names.
- Sanitize path components before rendering storage layout values.
- Store user metadata as Parquet file-level string key/value pairs, but keep `fletch.*` metadata owned by Fletch.
- Avoid adding sidecar database/catalog dependencies unless the storage contract is intentionally expanded.
- For schema or storage layout changes, include a migration path that preserves existing local workspaces where practical.

## Ingestion And View Practices

- Keep dynamic streams typed through `FletchType` instead of ad hoc values.
- Maintain sparse row behavior: multiple field writes at the same timestamp produce one row, missing fields remain null, and duplicate field writes at a timestamp use the latest value.
- Keep batches sorted by `timestamp_ns` before writing.
- Ensure empty streams can close cleanly without writing Parquet files.
- Keep the Polars-backed view layer behind the `view` feature.
- Preserve run filtering by `run_id` so views do not accidentally cross runs.
- Materialize `run_id` in views from storage layout or metadata.
- Require an explicit `run_id` for multi-source views, then use ASOF joins intentionally for time-aligned streams inside that run.

## Verification

- Run `cargo fmt`.
- Run `cargo test`.
- Run `cargo check --features view` when changing view behavior or public examples that use `FletchViewBuilder`.
- Run `cargo clippy --all-targets --all-features` when practical.
- Use temporary directories in tests for filesystem behavior.
