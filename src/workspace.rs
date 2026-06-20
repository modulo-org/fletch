use anyhow::{Result, anyhow};
use std::path::{Component, Path, PathBuf};

#[derive(Default, Clone)]
pub struct FletchWorkspaceBuilder {
    root: Option<PathBuf>,
}

impl FletchWorkspaceBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn root(mut self, root: impl AsRef<Path>) -> Self {
        self.root = Some(root.as_ref().to_path_buf());
        self
    }

    pub fn build(self) -> Result<FletchWorkspace> {
        let root = self
            .root
            .ok_or_else(|| anyhow!("workspace root is required. Use .root() to set it."))?;
        std::fs::create_dir_all(&root)?;
        Ok(FletchWorkspace { root })
    }
}

#[derive(Clone, Debug)]
pub struct FletchWorkspace {
    root: PathBuf,
}

impl FletchWorkspace {
    pub fn builder() -> FletchWorkspaceBuilder {
        FletchWorkspaceBuilder::new()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

pub(crate) fn validate_path_component(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{} must not be empty", name));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(anyhow!("{} must not contain path separators", name));
    }

    let mut components = Path::new(value).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(anyhow!("{} must be a single relative path component", name)),
    }
}
