# Fletch Code Conventions

## Rust Style

- Use `cargo fmt` formatting.
- Keep the public API builder-oriented and ergonomic for test-engineering workflows.
- Prefer explicit domain names for workspaces, runs, streams, catalog records, storage layout, timestamps, and analytical views.
- Use `anyhow::Result` consistently for the current library surface unless a typed error boundary is introduced across the crate.
- Keep module boundaries narrow: workspace/run setup, storage config, macro-generated ingestion, background sink, catalog metadata, and optional views should stay separately owned.
- Add concise comments only for non-obvious Arrow, Parquet, SQLite migration, or Polars join behavior.

## Data And Storage Practices

- Treat `timestamp_ns` and `run_id` as stable leading columns in generated telemetry schemas.
- Preserve stream names from `fletch_schema!` struct identifiers unless a deliberate migration path is added.
- Keep local workspace paths represented as `file://` URIs at public boundaries and convert to paths at filesystem boundaries.
- Preserve the default partition layout semantics: project, date, run, stream, and unique Parquet file names.
- Sanitize path components before rendering storage layout values.
- Keep catalog updates aligned with file writes: runs, run metadata, file rows, row counts, timestamp bounds, file sizes, schema hashes, and content hashes should describe the stored Parquet files accurately.
- Avoid exposing SQLite internals in the public API unless the catalog contract is intentionally expanded.
- For schema or catalog changes, include a migration path that preserves existing local workspaces where practical.

## Ingestion And View Practices

- Keep `fletch_schema!` generated streams strongly typed through `FletchType` instead of ad hoc dynamic values.
- Maintain sparse row behavior: multiple field writes at the same timestamp produce one row, missing fields remain null, and duplicate field writes at a timestamp use the latest value.
- Keep batches sorted by `timestamp_ns` before writing.
- Respect `FlushPolicy`: `flush_rows` must remain positive, and interval-based flushing should not emit empty batches.
- Ensure empty streams can close cleanly without writing Parquet files.
- Keep the Polars-backed view layer behind the `view` feature.
- Preserve run filtering by `project_id`, `run_id`, and metadata filters so views do not accidentally cross projects.
- Use ASOF joins intentionally for time-aligned multi-stream views and keep join-by-run semantics explicit.

## Verification

- Run `cargo fmt`.
- Run `cargo test`.
- Run `cargo test --features view` when changing view, catalog filtering, or public examples that use `FletchViewBuilder`.
- Run `cargo clippy --all-targets --all-features` when practical.
- Use temporary directories in tests for filesystem/catalog behavior.
