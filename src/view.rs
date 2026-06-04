use anyhow::{Result, anyhow};
use polars::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::workspace::FletchWorkspace;

struct ViewSource {
    stream_name: String,
    columns: Vec<String>,
}

pub struct FletchViewBuilder<'a> {
    workspace: &'a FletchWorkspace,
    run_id: Option<String>,
    sources: Vec<ViewSource>,
    add_relative_timestamp: bool,
}

impl<'a> FletchViewBuilder<'a> {
    pub fn new(workspace: &'a FletchWorkspace) -> Self {
        Self {
            workspace,
            run_id: None,
            sources: Vec::new(),
            add_relative_timestamp: false,
        }
    }

    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn add_source(mut self, stream_name: &str, columns: &[&str]) -> Self {
        self.sources.push(ViewSource {
            stream_name: stream_name.to_string(),
            columns: columns.iter().map(|column| column.to_string()).collect(),
        });
        self
    }

    pub fn with_relative_timestamp(mut self) -> Self {
        self.add_relative_timestamp = true;
        self
    }

    pub async fn build(self) -> Result<FletchView> {
        if self.sources.is_empty() {
            return Err(anyhow!("at least one source must be added to the view"));
        }

        let mut base_lf: Option<LazyFrame> = None;

        for source in self.sources {
            let file_paths = parquet_files_for_source(
                self.workspace.root(),
                self.run_id.as_deref(),
                &source.stream_name,
            )?;
            if file_paths.is_empty() {
                return Err(anyhow!(
                    "no parquet files found for stream {}",
                    source.stream_name
                ));
            }

            let mut lfs = Vec::new();
            for file_path in file_paths {
                let scan_args = ScanArgsParquet {
                    n_rows: None,
                    ..Default::default()
                };
                lfs.push(LazyFrame::scan_parquet(
                    PlRefPath::new(file_path.to_string_lossy().as_ref()),
                    scan_args,
                )?);
            }

            let mut lf = concat(lfs, Default::default())?;
            let mut selection = vec![col("timestamp_ns")];
            for column in source.columns {
                selection.push(col(&column));
            }
            lf = lf.select(selection);
            lf = lf.sort(["timestamp_ns"], Default::default());

            if let Some(existing_lf) = base_lf {
                let asof_options = AsOfOptions {
                    strategy: AsofStrategy::Backward,
                    ..Default::default()
                };
                let join_args = JoinArgs::new(JoinType::AsOf(Box::new(asof_options)));
                base_lf = Some(existing_lf.join(
                    lf,
                    [col("timestamp_ns")],
                    [col("timestamp_ns")],
                    join_args,
                ));
            } else {
                base_lf = Some(lf);
            }
        }

        let mut final_lf = base_lf.expect("sources were checked as non-empty");

        if self.add_relative_timestamp {
            final_lf = final_lf.with_columns([
                (col("timestamp_ns") - col("timestamp_ns").min()).alias("relative_time_ns")
            ]);
        }

        Ok(FletchView {
            lazy_frame: final_lf,
        })
    }
}

fn parquet_files_for_source(
    root: &Path,
    run_id: Option<&str>,
    stream_name: &str,
) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let runs_dir = root.join("runs");
    if !runs_dir.exists() {
        return Ok(files);
    }

    if let Some(run_id) = run_id {
        collect_stream_files(&runs_dir.join(run_id).join(stream_name), &mut files)?;
    } else {
        for run_entry in std::fs::read_dir(runs_dir)? {
            let run_entry = run_entry?;
            if run_entry.file_type()?.is_dir() {
                collect_stream_files(&run_entry.path().join(stream_name), &mut files)?;
            }
        }
    }

    files.sort();
    Ok(files)
}

fn collect_stream_files(stream_dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !stream_dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(stream_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            files.push(path);
        }
    }
    Ok(())
}

pub struct FletchView {
    lazy_frame: LazyFrame,
}

impl FletchView {
    pub fn collect(self) -> Result<DataFrame> {
        self.lazy_frame
            .collect()
            .map_err(|e| anyhow!("failed to compute view: {}", e))
    }

    pub fn into_lazy(self) -> LazyFrame {
        self.lazy_frame
    }

    pub fn to_csv<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file =
            File::create(path).map_err(|e| anyhow!("failed to create CSV file: {}", e))?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(&mut df)
            .map_err(|e| anyhow!("failed to write CSV: {}", e))?;
        Ok(())
    }

    pub fn to_parquet<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file =
            File::create(path).map_err(|e| anyhow!("failed to create Parquet file: {}", e))?;
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| anyhow!("failed to write Parquet: {}", e))?;
        Ok(())
    }
}
