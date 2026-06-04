use anyhow::Result;
use arrow::array::{Array, Float64Array, Int64Array};
use fletch::{FletchSchema, FletchStreamBuilder, FletchWorkspace, Stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[derive(FletchSchema)]
struct TestTelemetry {
    sensor_a: f64,
    sensor_b: f64,
}

fn read_parquet_batch(
    dir: &Path,
    run_id: &str,
    stream_name: &str,
) -> Result<arrow::record_batch::RecordBatch> {
    let path = first_parquet_file(dir, run_id, stream_name)?;
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let batch = reader.next().unwrap()?;
    Ok(batch)
}

fn first_parquet_file(dir: &Path, run_id: &str, stream_name: &str) -> Result<PathBuf> {
    let data_dir = dir.join("runs").join(run_id).join(stream_name);
    let mut file_path = None;
    if data_dir.exists() {
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                file_path = Some(path);
                break;
            }
        }
    }
    file_path.ok_or_else(|| anyhow::anyhow!("no parquet file found in {:?}", data_dir))
}

#[tokio::test]
async fn test_out_of_order_timestamps_are_sorted() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "test_run_out_of_order";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<TestTelemetry>::try_new(&workspace, run_id).await?;
    stream.sensor_a(150, 1.5)?;
    stream.sensor_a(100, 1.0)?;
    stream.sensor_a(200, 2.0)?;
    stream.sensor_a(50, 0.5)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id, "TestTelemetry")?;
    let ts_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let val_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(ts_array.value(0), 50);
    assert_eq!(ts_array.value(1), 100);
    assert_eq!(ts_array.value(2), 150);
    assert_eq!(ts_array.value(3), 200);
    assert_eq!(val_array.value(0), 0.5);
    assert_eq!(val_array.value(1), 1.0);
    assert_eq!(val_array.value(2), 1.5);
    assert_eq!(val_array.value(3), 2.0);
    Ok(())
}

#[tokio::test]
async fn test_sparse_data_with_nulls() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "test_run_sparse";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<TestTelemetry>::try_new(&workspace, run_id).await?;
    stream.sensor_a(100, 10.0)?;
    stream.sensor_b(100, 20.0)?;
    stream.sensor_a(110, 11.0)?;
    stream.sensor_b(120, 22.0)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id, "TestTelemetry")?;
    let a_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let b_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(a_array.is_valid(1));
    assert!(b_array.is_null(1));
    assert!(a_array.is_null(2));
    assert!(b_array.is_valid(2));
    Ok(())
}

#[tokio::test]
async fn test_duplicate_timestamp_overwrites() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "test_run_duplicates";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<TestTelemetry>::try_new(&workspace, run_id).await?;
    stream.sensor_a(100, 1.0)?;
    stream.sensor_a(100, 9.9)?;
    stream.sensor_a(110, 2.0)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id, "TestTelemetry")?;
    let ts_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let val_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(ts_array.value(0), 100);
    assert_eq!(val_array.value(0), 9.9);
    Ok(())
}

#[tokio::test]
async fn test_empty_flush() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "test_run_empty";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let stream = Stream::<TestTelemetry>::try_new(&workspace, run_id).await?;
    let result = stream.close();
    assert!(result.is_ok(), "closing an empty stream should not fail");
    let data_dir = dir.path().join("runs").join(run_id).join("TestTelemetry");
    if data_dir.exists() {
        let mut has_parquet = false;
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                has_parquet = true;
                break;
            }
        }
        assert!(
            !has_parquet,
            "no parquet file should be written for empty streams"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dynamic_builder_writes_file_metadata() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "test_metadata";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let metadata = BTreeMap::from([
        (
            "modulo.test_id".to_string(),
            "digital-edge-test".to_string(),
        ),
        ("modulo.device_id".to_string(), "dx0-001".to_string()),
    ]);
    let mut stream = FletchStreamBuilder::new(&workspace, "DigitalTelemetry")
        .channel::<u8>("channel")?
        .channel::<bool>("rising")?
        .metadata_pairs(metadata)
        .build_dynamic(run_id)
        .await?;

    stream.write(42, "channel", 7_u8)?;
    stream.write(42, "rising", true)?;
    stream.close()?;

    let file_path = first_parquet_file(dir.path(), run_id, "DigitalTelemetry")?;
    let reader = SerializedFileReader::new(File::open(file_path)?)?;
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("metadata should be present");
    let pairs = metadata
        .iter()
        .map(|entry| {
            (
                entry.key.as_str(),
                entry.value.as_deref().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(pairs.get("fletch.run_id"), Some(&run_id));
    assert_eq!(pairs.get("fletch.stream_name"), Some(&"DigitalTelemetry"));
    assert_eq!(pairs.get("modulo.test_id"), Some(&"digital-edge-test"));
    assert_eq!(pairs.get("modulo.device_id"), Some(&"dx0-001"));
    Ok(())
}
