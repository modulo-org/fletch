# Fletch Code Conventions

## Rust Style

- Use `cargo fmt` formatting.
- Keep APIs builder-oriented and explicit for test-engineering workflows.
- Use domain names consistently: workspace, namespace, catalog, table, stream, run ID, timestamp, source, and view.
- Use `anyhow::Result` across the current public API unless the crate introduces a typed error boundary.
- Keep module ownership narrow: workspace setup, Iceberg config, macro-generated ingestion, type mapping, background sink, and Polars views should stay separately owned.
- Add comments only where Arrow, Parquet, Iceberg transaction, object-store, or Polars join behavior is not obvious from the code.

## Schema And Type Mapping

- Generated schemas must keep `timestamp_ns` and dictionary-encoded `run_id` as the first two columns.
- Keep telemetry fields nullable so sparse rows can represent missing sensor values.
- Add supported telemetry types through `FletchType` in `src/types.rs` and update `FletchConfig::arrow_to_iceberg` in `src/config.rs` at the same time.
- Preserve stream table names from `fletch_schema!` struct identifiers unless a migration path is intentionally added.
- Treat schema evolution as incomplete in the current code; do not rely on the placeholder in `evolve_schema_if_needed` to add missing Iceberg fields.

## Ingestion And Storage

- Preserve pending-row semantics in `fletch_schema!`: writes for the current timestamp coalesce into one row, missing fields remain null, and duplicate field writes for that pending timestamp keep the latest value.
- Keep batches sorted by `timestamp_ns` before sending them to `BackgroundSink`.
- Keep the default batch threshold at `100_000` rows unless performance testing supports a change.
- Ensure empty streams close cleanly without writing Parquet files or committing empty Iceberg appends.
- Keep file writes atomic from the caller's perspective: write to a temporary spool file, upload through the configured `ObjectStore`, then commit an Iceberg fast-append transaction.
- Keep Parquet data-file metrics aligned with the file written: record count, file size, column sizes, value counts, and null counts.
- Local workspace roots should remain `file://` URIs at public boundaries and convert to filesystem paths only at storage and view boundaries.
- S3 support is not complete; avoid presenting `s3://` as working until catalog initialization and tests exist.

## View Practices

- `FletchViewBuilder` currently depends on Polars unconditionally; do not refer to a `view` feature unless `Cargo.toml` adds one.
- Resolve view inputs through Iceberg table scans rather than direct directory walking.
- Preserve `run_id` filtering before selecting source columns so views do not accidentally combine runs.
- Select only `timestamp_ns`, `run_id`, and requested telemetry columns for each source.
- Use ASOF joins intentionally for multi-source sensor fusion, joining by `timestamp_ns` and grouping by `run_id`.
- Keep relative timestamps derived per `run_id` using the minimum `timestamp_ns` for that run.

## Verification

- Run `cargo fmt`.
- Run `cargo test`.
- Run `cargo run --example accelerometer` when changing ingestion, Iceberg commits, or view behavior.
- Run `cargo clippy --all-targets --all-features` when practical.
- Use temporary directories for filesystem, catalog, and Parquet tests.
