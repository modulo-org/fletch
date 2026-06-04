# Fletch

High-throughput telemetry logging for test engineering and HIL platforms.

Fletch writes typed telemetry streams to local Apache Parquet files using Apache
Arrow builders. A caller provides a root folder, a run id, channel definitions,
and optional string key/value metadata. The metadata is written into the Parquet
file footer so the data remains self-contained.

> Status: experimental. APIs are still expected to change while Modulo hardware
> telemetry workflows settle.

## Storage Layout

Fletch stores files under the workspace root:

```text
root/
  runs/
    run_001/
      DigitalTelemetry/
        <uuid>.parquet
```

The `timestamp_ns` column is always first. Channel columns follow in the order
they were registered. Run metadata such as `fletch.run_id`,
`fletch.stream_name`, `fletch.created_at_ns`, and user-provided metadata is
stored as Parquet file-level key/value metadata.

## Dynamic Streams

Dynamic stream configuration is the lowest-level API and is intended for
hardware telemetry paths where channels are discovered at runtime.

```rust
use fletch::{FletchStreamBuilder, FletchWorkspace};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let mut stream = FletchStreamBuilder::new(&workspace, "DigitalTelemetry")
        .channel::<u8>("channel")?
        .channel::<bool>("rising")?
        .metadata("modulo.test_id", "digital-edge-test")
        .build_dynamic("run_001")
        .await?;

    stream.write(1_718_000_000_000, "channel", 7_u8)?;
    stream.write(1_718_000_000_000, "rising", true)?;
    stream.close()?;

    Ok(())
}
```

## Derived Streams

For statically known schemas, derive `FletchSchema` and use `Stream<T>`.

```rust
use fletch::{FletchSchema, FletchWorkspace, Stream};

#[derive(FletchSchema)]
struct AccelerometerTelemetry {
    accel_x: f64,
    accel_y: f64,
    accel_z: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let mut stream = Stream::<AccelerometerTelemetry>::try_new(&workspace, "run_001").await?;
    stream.accel_x(1_718_000_000_000, 0.1)?;
    stream.accel_y(1_718_000_000_000, 0.2)?;
    stream.accel_z(1_718_000_000_000, 9.81)?;
    stream.close()?;

    Ok(())
}
```

## Querying with DuckDB

DuckDB can query the Parquet files directly:

```sql
SELECT *
FROM read_parquet('C:/fletch_warehouse/runs/*/DigitalTelemetry/*.parquet');
```

File-level metadata is preserved in the Parquet footer. Use columns for
telemetry values that need normal SQL filtering, and metadata for contextual
run/test/device values that should travel with the file without requiring a
sidecar database.
