use anyhow::{anyhow, Result};
use iceberg::NamespaceIdent;

#[derive(Default, Clone)]
pub struct FletchWorkspaceBuilder {
    uri: Option<String>,
    namespace_levels: Vec<String>,
    catalog: Option<String>
}

impl FletchWorkspaceBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn namespace(mut self, levels: &[&str]) -> Self {
        self.namespace_levels = levels.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn add_namespace_level(mut self, level: impl Into<String>) -> Self {
        self.namespace_levels.push(level.into());
        self
    }

    pub fn catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    pub fn build(self) -> Result<FletchWorkspace> {
        let uri = self.uri.ok_or_else(|| anyhow!("Workspace URI is required. Use .uri() to set it."))?;
        if self.namespace_levels.is_empty() {
            return Err(anyhow!("At least one namespace level is required to organize your telemetry."));
        }
        let refs: Vec<&str> = self.namespace_levels.iter().map(AsRef::as_ref).collect();
        let namespace = NamespaceIdent::from_strs(refs)
            .map_err(|e| anyhow!("Invalid namespace format provided to workspace: {}", e))?;
        let catalog = self.catalog.unwrap_or_else(|| "fletch_catalog".into());
        Ok(FletchWorkspace {
            uri,
            namespace,
            catalog
        })
    }
}

pub struct FletchWorkspace {
    uri: String,
    namespace: NamespaceIdent,
    catalog: String,
}

impl FletchWorkspace {
    pub fn builder() -> FletchWorkspaceBuilder {
        FletchWorkspaceBuilder::new()
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    pub fn catalog(&self) -> &str { &self.catalog }
}