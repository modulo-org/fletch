# Fletch Project Architecture

## Crate

- Root `Cargo.toml` defines the `fletch` Rust crate.
- The crate uses Rust 2024 and exposes a library API for telemetry ingestion, local Parquet storage, and optional analytical views.
- The default feature set keeps Polars out of the dependency graph.
- The `view` feature enables Polars-backed exploration through `FletchViewBuilder` and `FletchView`.

## Key Areas

- `src/lib.rs`: public exports and feature-gated view module.
- `src/workspace.rs`: `FletchWorkspace`, workspace builders, and local root path handling.
- `src/config.rs`: storage config, output Parquet path generation, and file-level metadata assembly.
- `src/stream.rs`: `FletchStreamBuilder`, dynamic `FletchStream`, typed `Stream<T>`, and sparse-row batching.
- `src/types.rs`: `FletchType` trait and supported Rust-to-Arrow field mappings.
- `src/sink.rs`: background Arrow-to-Parquet writer and Parquet file metadata writing.
- `fletch-derive/`: derive macro for static `Stream<T>` schemas.
- `src/view.rs`: optional Polars LazyFrame scans, metadata/run filtering, source selection, ASOF joins, relative timestamps, and CSV/Parquet exports.
- `tests/`: integration tests for ingestion ordering, sparse rows, duplicate timestamps, empty streams, file metadata, and feature-gated views.
- `examples/`: runnable examples for telemetry capture and optional view construction.

## Data Model

- A workspace is a local root folder.
- A run is identified by a caller-provided `run_id`.
- A stream maps to one stream name and a dynamic list of registered channels.
- Arrow schemas start with `timestamp_ns`, followed by nullable telemetry fields.
- Parquet files are written under `runs/{run_id}/{stream_name}/{uuid}.parquet`.
- Run and user metadata is stored as Parquet file-level key/value metadata.

## Ingestion Model

- Callers create one or more dynamic streams with `FletchStreamBuilder`, or static streams with `Stream<T>`, write channel values by timestamp, and call `close`.
- Field writes sharing the same timestamp are coalesced into one pending row.
- When the timestamp changes, the pending row is committed and may be flushed according to the batch capacity.
- Before each batch is sent to the sink, rows are sorted by `timestamp_ns`.
- `BackgroundSink` writes batches on a worker thread and writes file-level metadata into the Parquet footer.
- Empty streams close without creating Parquet files.

## View Model

- `FletchViewBuilder` is available only with the `view` feature.
- Views resolve eligible Parquet files from the local root using an explicit run ID, or all runs for single-source views.
- Each source materializes `run_id` from the storage layout and selects `timestamp_ns`, `run_id`, and requested telemetry columns.
- Multiple sources require an explicit run ID and are joined with Polars ASOF joins using `timestamp_ns`.
- Optional relative timestamps are derived per run from the selected rows.
- Views can be collected into a DataFrame or exported to CSV/Parquet.

## Modulo Workspace Role

- Fletch owns telemetry-library behavior and local analytical data storage.
- It should remain decoupled from `modulo-sdk` protocol definitions and `modulo-firmware` hardware execution unless a task explicitly connects hardware telemetry to Fletch ingestion.
- Bitshift or other UI/data consumers should treat Fletch outputs as plain Parquet/data-view artifacts rather than duplicating Fletch storage rules.
