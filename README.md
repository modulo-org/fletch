# Fletch 🏹

[<img alt="github" src="https://img.shields.io/badge/github-ruben1729/fletch-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/Ruben1729/fletch)
[<img alt="crates.io" src="https://img.shields.io/crates/v/fletch.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/fletch)


**High-throughput, zero-config telemetry logging and sensor fusion for Hardware-in-the-Loop (HIL) platforms.**

Fletch is a Rust library designed to completely eliminate the data engineering boilerplate from hardware validation, DFMEA testing, and embedded systems monitoring.

> [!WARNING]
> **Status: Experimental.** This library is in early development. While the core architecture is functional, it has primarily been validated in controlled test environments (`tempdir`). Use in production local directories or mission-critical HIL rigs with caution. APIs are subject to change.

It provides a strongly-typed, macro-driven ingestion engine that writes millions of samples per second directly to Apache Parquet, backed by an embedded Apache Iceberg catalog. When you are ready to analyze your runs, Fletch uses Polars to instantly perform time-aligned sensor fusion (ASOF joins) across different telemetry streams.

## Key Features

* **Zero-Config Iceberg:** No servers or JVMs required. Fletch manages an embedded SQLite Iceberg catalog locally or directly on S3.
* **Strongly-Typed Ingestion:** Use the `fletch_schema!` macro to generate highly optimized Apache Arrow builders for your exact sensor loadout.
* **Blazing Fast:** Dictionary-encoded partition keys and Arrow's columnar memory model allow for microsecond ingestion latency.
* **Native Sensor Fusion:** The `FletchViewBuilder` uses Polars to instantly time-align sensors operating at completely different frequencies (e.g., matching a 1000Hz accelerometer to a 10Hz power supply).
* **One-Click Exports:** Dump fused analytical views directly to CSV or Parquet for plotting.

## Quick Start

### 1. Define Your Sensors
Generate strongly-typed telemetry streams using the `fletch_schema!` macro.

```rust
use fletch::fletch_schema;

fletch_schema! {
    AccelerometerTelemetry {
        accel_x: f64,
        accel_y: f64,
        accel_z: f64,
    }
}

fletch_schema! {
    PowerSupplyTelemetry {
        voltage: f64,
        current_consumption: f64,
    }
}
```

### 2. High-Speed Ingestion
Group your tests dynamically using namespaces and log data at the speed of RAM. Out-of-order timestamps are handled automatically.

```rust
use fletch::FletchWorkspace;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Define the storage location and logical test suite
    let workspace = FletchWorkspace::builder()
        .uri("file:///C:/fletch_warehouse")
        .namespace(&["environmental_testing", "vibration_profile"])
        .build()?;

    let run_id = "run_001";
    let mut accel_stream = AccelerometerTelemetry::try_new(&workspace, run_id).await?;
    let mut pwr_stream = PowerSupplyTelemetry::try_new(&workspace, run_id).await?;

    // Log data (Fletch automatically batches into Arrow arrays)
    let ts_ns = 1_718_000_000_000;
    accel_stream.accel_z(ts_ns, 9.81)?;
    pwr_stream.voltage(ts_ns, 3.3)?;

    // Flushes remaining batches to Parquet and commits Iceberg transactions
    accel_stream.close()?;
    pwr_stream.close()?;

    Ok(())
}
```

### 3. Time-Aligned Exploration (Sensor Fusion)
Stop manually wrangling arrays in Python. Extract the exact columns you need across multiple runs, and Fletch will time-align them instantly.

```rust
use fletch::{FletchWorkspace, FletchViewBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .uri("file:///C:/fletch_warehouse")
        .namespace(&["environmental_testing", "vibration_profile"])
        .build()?;

    // Fetch the data and align the 1000Hz accelerometer with the 10Hz power supply
    FletchViewBuilder::new(&workspace)
        .run_id("run_001")
        .add_source("AccelerometerTelemetry", &["accel_z"])
        .add_source("PowerSupplyTelemetry", &["voltage"])
        .build()
        .await?
        .to_csv("aligned_fusion_report.csv")?;

    Ok(())
}
```

## Architecture
1. Ingestion: fletch_schema! → Apache Arrow RecordBatch → AsyncArrowWriter → Apache Parquet
2. Metadata: iceberg-rs handles ACID transactions, schema evolution, and file-level min/max statistics.
3. Exploration: polars directly scans the Iceberg manifests and performs zero-copy LazyFrame execution and ASOF joins.
