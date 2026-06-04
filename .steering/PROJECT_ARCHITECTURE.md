# Fletch Project Architecture

## Crate

- Root `Cargo.toml` defines the `fletch` Rust crate.
- The crate uses Rust 2024 and exposes a library API for telemetry ingestion, local cataloging, Parquet storage, and optional analytical views.
- The default feature set keeps Polars out of the dependency graph.
- The `view` feature enables Polars-backed exploration through `FletchViewBuilder` and `FletchView`.

## Key Areas

- `src/lib.rs`: public exports and feature-gated view module.
- `src/workspace.rs`: `FletchWorkspace`, workspace builders, local `file://` URI handling, partition layout rendering, and path-value sanitization.
- `src/run.rs`: `FletchRun`, run metadata, flush policy, run creation, and catalog initialization.
- `src/config.rs`: storage config derived from a run, schema fingerprinting, and relative Parquet path generation.
- `src/macros.rs`: `fletch_schema!` macro that generates strongly typed stream structs and per-field logging methods.
- `src/types.rs`: `FletchType` trait and supported Rust-to-Arrow field mappings.
- `src/sink.rs`: background Arrow-to-Parquet writer, temporary spool files, content hashing, file catalog updates, and run completion.
- `src/catalog.rs`: SQLite catalog creation, lightweight migrations, run/file/metadata records, and query indexes.
- `src/view.rs`: optional Polars LazyFrame scans, metadata/run filtering, source selection, ASOF joins, relative timestamps, and CSV/Parquet exports.
- `tests/`: integration tests for ingestion ordering, sparse rows, duplicate timestamps, empty streams, catalog metadata, file integrity, and feature-gated views.
- `examples/`: runnable examples for telemetry capture and optional view construction.

## Data Model

- A workspace is a local `file://` root plus a `project_id` and storage layout.
- A run has a UUID `run_id`, project ID, start/end timestamps, optional repeated metadata key/value pairs, and a flush policy.
- A stream maps to one macro-generated struct and one stream name.
- Generated Arrow schemas start with `timestamp_ns` and dictionary-encoded `run_id`, followed by nullable telemetry fields.
- Parquet files are written under a rendered partition path and tracked by the SQLite catalog.
- The catalog stores runs, files, and run metadata in `fletch_catalog.sqlite`.

## Ingestion Model

- Callers create a `FletchRun`, construct one or more macro-generated streams with `try_new`, write field values by timestamp, and call `close`.
- Field writes sharing the same timestamp are coalesced into one pending row.
- When the timestamp changes, the pending row is committed and may be flushed according to the run's flush policy.
- Before each batch is sent to the sink, rows are sorted by `timestamp_ns`.
- `BackgroundSink` writes batches on a worker thread through a Tokio runtime and commits catalog metadata after a successful non-empty file write.
- Empty streams close without creating Parquet files, while still finishing the run record.

## View Model

- `FletchViewBuilder` is available only with the `view` feature.
- Views resolve eligible Parquet files through the catalog using the workspace project, explicit run ID, or metadata filters.
- Each source selects `timestamp_ns`, `run_id`, and requested telemetry columns.
- Multiple sources are joined with Polars ASOF joins using `timestamp_ns` and `run_id`.
- Optional relative timestamps are derived per run.
- Views can be collected into a DataFrame or exported to CSV/Parquet.

## Modulo Workspace Role

- Fletch owns telemetry-library behavior and local analytical data storage.
- It should remain decoupled from `modulo-sdk` protocol definitions and `modulo-firmware` hardware execution unless a task explicitly connects hardware telemetry to Fletch ingestion.
- Bitshift or other UI/data consumers should treat Fletch outputs as cataloged Parquet/data-view artifacts rather than duplicating Fletch storage rules.
