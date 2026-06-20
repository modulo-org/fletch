use anyhow::Result;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::workspace::{FletchWorkspace, validate_path_component};

pub struct FletchConfig {
    pub file_path: PathBuf,
    pub metadata: BTreeMap<String, String>,
}

impl FletchConfig {
    pub fn init(
        workspace: &FletchWorkspace,
        stream_name: &str,
        run_id: &str,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<Self> {
        validate_path_component("run_id", run_id)?;
        validate_path_component("stream_name", stream_name)?;

        let file_name = format!("{}.parquet", Uuid::new_v4());
        let stream_dir = workspace.root().join("runs").join(run_id).join(stream_name);
        std::fs::create_dir_all(&stream_dir)?;

        let created_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();

        let mut metadata = user_metadata;
        metadata.insert("fletch.run_id".to_string(), run_id.to_string());
        metadata.insert("fletch.stream_name".to_string(), stream_name.to_string());
        metadata.insert("fletch.created_at_ns".to_string(), created_at_ns);

        Ok(Self {
            file_path: stream_dir.join(file_name),
            metadata,
        })
    }
}
