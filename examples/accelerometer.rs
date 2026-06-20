use fletch::{FletchSchema, FletchViewBuilder, FletchWorkspace, Stream};
use tempfile::tempdir;

#[derive(FletchSchema)]
struct AccelerometerTelemetry {
    accel_x: f64,
    accel_y: f64,
    accel_z: f64,
}

#[derive(FletchSchema)]
struct PowerSupplyTelemetry {
    voltage: f64,
    current_consumption: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let run_id = "run_001";

    println!(
        "Initializing zero-config HIL logging to: {}",
        dir.path().display()
    );

    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut accel_stream = Stream::<AccelerometerTelemetry>::try_new(&workspace, run_id).await?;
    let mut pwr_stream = Stream::<PowerSupplyTelemetry>::try_new(&workspace, run_id).await?;

    println!("Generating 100,000 samples at mixed rates...");
    let start_ts: i64 = 1_718_000_000_000;
    let num_samples = 100_000;

    for i in 0..num_samples {
        let current_ts = start_ts + i;
        let t = i as f64 * 0.01;
        accel_stream.accel_x(current_ts, t.sin() * 2.0)?;
        accel_stream.accel_y(current_ts, t.cos() * 2.0)?;
        accel_stream.accel_z(current_ts, 9.81 + (t * 5.0).sin())?;

        if i % 10 == 0 {
            let voltage = 3.3 + (i % 100) as f64 * 0.001;
            let current = 1.2 + ((t.sin() * 2.0).abs() * 0.05);
            pwr_stream.voltage(current_ts, voltage)?;
            pwr_stream.current_consumption(current_ts, current)?;
        }
    }

    accel_stream.close()?;
    pwr_stream.close()?;
    println!("Successfully wrote telemetry to Parquet.\n");

    println!("Building analytical views using Polars...\n");
    let view_accel = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("AccelerometerTelemetry", &["accel_x", "accel_z"])
        .build()
        .await?;

    let df_accel = view_accel.collect()?;
    println!("--- View 1: Accelerometer (X, Z only) ---");
    println!("{}", df_accel.head(Some(10)));
    println!();

    let view_pwr = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("PowerSupplyTelemetry", &["voltage", "current_consumption"])
        .build()
        .await?;

    let df_pwr = view_pwr.collect()?;
    println!("--- View 2: Power Supply ---");
    println!("{}", df_pwr.head(Some(10)));
    println!();

    let view_fusion = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("AccelerometerTelemetry", &["accel_z"])
        .add_source("PowerSupplyTelemetry", &["voltage"])
        .with_relative_timestamp()
        .build()
        .await?;

    let df_fusion = view_fusion.collect()?;
    println!("--- View 3: Sensor Fusion (Accel Z + Voltage) ---");
    println!("{}", df_fusion.head(Some(10)));
    println!();

    Ok(())
}
