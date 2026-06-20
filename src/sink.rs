use anyhow::{Result, anyhow};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use crate::config::FletchConfig;

pub struct BackgroundSink {
    sender: mpsc::SyncSender<Option<RecordBatch>>,
    worker_handle: Option<JoinHandle<Result<()>>>,
}

impl BackgroundSink {
    pub fn spawn(config: FletchConfig, schema: Arc<Schema>) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Option<RecordBatch>>(100);

        let worker_handle = thread::spawn(move || -> Result<()> {
            let metadata = config
                .metadata
                .into_iter()
                .map(|(key, value)| KeyValue::new(key, value))
                .collect::<Vec<_>>();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_key_value_metadata(Some(metadata))
                .build();
            let mut writer: Option<ArrowWriter<File>> = None;

            while let Ok(Some(batch)) = receiver.recv() {
                if writer.is_none() {
                    let file = File::create(&config.file_path)?;
                    writer = Some(ArrowWriter::try_new(
                        file,
                        schema.clone(),
                        Some(props.clone()),
                    )?);
                }

                if let Some(writer) = writer.as_mut() {
                    writer.write(&batch)?;
                }
            }

            if let Some(writer) = writer {
                writer.close()?;
            }

            Ok(())
        });

        Ok(Self {
            sender,
            worker_handle: Some(worker_handle),
        })
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.sender
            .send(Some(batch))
            .map_err(|e| anyhow!("background worker died: {}", e))
    }

    pub fn close(&mut self) -> Result<()> {
        self.sender.send(None).ok();
        if let Some(handle) = self.worker_handle.take() {
            match handle.join() {
                Ok(res) => res?,
                Err(_) => return Err(anyhow!("storage worker thread panicked")),
            }
        }
        Ok(())
    }
}
