# Fletch Project Architecture

## Crate Shape

- Root `Cargo.toml` defines the `fletch` Rust crate on Rust 2024.
- The crate exposes a library API for macro-generated telemetry ingestion, Apache Arrow batching, Parquet file writes, Iceberg catalog commits, and Polars analytical views.
- Polars and `FletchViewBuilder` are currently compiled unconditionally; there is no `view` feature in `Cargo.toml`.
- Public exports are centralized in `src/lib.rs`.

## Modules

- `src/workspace.rs`: `FletchWorkspace`, `FletchWorkspaceBuilder`, required `file://` or future object-store URI, Iceberg namespace, and catalog name.
- `src/config.rs`: `FletchConfig`, environment inference, local object-store setup, SQL Iceberg catalog initialization, namespace/table creation, Arrow-to-Iceberg schema mapping, and per-stream Parquet object paths.
- `src/macros.rs`: `fletch_schema!`, generated stream structs, generated field logging methods, pending-row coalescing, batch creation, timestamp sorting, and stream close behavior.
- `src/types.rs`: `FletchType` trait and supported Rust-to-Arrow builders. Current telemetry field types are `f64` and `i32`.
- `src/sink.rs`: `BackgroundSink`, bounded channel, worker thread, Tokio runtime, temporary Parquet spool file, object-store upload, Iceberg `DataFile` metrics, and fast-append commit.
- `src/view.rs`: `FletchViewBuilder`, Iceberg table scans, Polars `LazyFrame` construction, optional `run_id` filtering, source selection, ASOF joins, relative timestamps, and CSV/Parquet exports.
- `tests/telemetry_sorting.rs`: integration coverage for sorted writes, sparse null rows, duplicate pending timestamp overwrite, and empty stream close.
- `examples/accelerometer.rs`: end-to-end ingestion plus Polars view construction for accelerometer and power-supply streams.

## Data Model

- A workspace is a storage URI, an Iceberg namespace, and a catalog name.
- A stream maps to one macro-generated Rust struct and one Iceberg table named after that struct.
- A caller-provided `run_id` is stored in every row as a dictionary-encoded Arrow column.
- Generated Arrow schemas start with non-null `timestamp_ns`, non-null dictionary `run_id`, then nullable telemetry fields.
- Local workspaces store an Iceberg SQL catalog at `iceberg_catalog.db`.
- Parquet data files are written under `{StreamName}/data/{uuid}.parquet` below the workspace root.
- Iceberg table metadata tracks appended Parquet files; there is no separate Fletch run metadata table in the current implementation.

## Ingestion Flow

1. A caller builds a `FletchWorkspace` with `.uri(...)`, `.namespace(...)`, and optional `.catalog(...)`.
2. A generated stream calls `FletchConfig::init`, which creates or loads the namespace and table for that stream.
3. Generated field methods update the pending row for the current timestamp or commit it when the timestamp changes.
4. `flush_batch` finishes Arrow builders, creates a `RecordBatch`, sorts by `timestamp_ns`, and sends the batch to `BackgroundSink`.
5. `BackgroundSink` writes all received batches for that stream to one Parquet spool file, uploads it through `ObjectStore`, and commits an Iceberg fast append.
6. `close` flushes the final pending row and joins the worker thread. Empty streams close without writing a data file.

## View Flow

1. `FletchViewBuilder::new(&workspace)` collects one or more stream table sources.
2. `build` loads the local SQL Iceberg catalog and resolves data files through table scans.
3. Each source becomes a sorted Polars `LazyFrame` filtered by `run_id` when requested.
4. Multiple sources are joined with backward ASOF joins on `timestamp_ns` and grouped by `run_id`.
5. `with_relative_timestamp` adds `relative_time_ns` from each row's timestamp minus the per-run minimum timestamp.
6. `FletchView` can be collected as a `DataFrame`, converted into a `LazyFrame`, or exported to CSV/Parquet.

## Modulo Workspace Role

- Fletch owns telemetry-library behavior, Iceberg-backed cataloging, Parquet data files, and Polars analytical views.
- Keep Fletch decoupled from `modulo-sdk` protocol definitions and `modulo-firmware` hardware execution unless a task explicitly connects hardware telemetry to Fletch ingestion.
- Bitshift and other UI/data consumers should treat Fletch outputs as cataloged Parquet and view artifacts rather than duplicating Fletch storage or join rules.
