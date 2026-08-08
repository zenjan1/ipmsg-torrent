//! Download Path Templates (Phase 70)
//!
//! Configurable templates for auto-organizing downloads into directory patterns.
//! Supports variables like {category}, {YYYY}, {MM}, {DD}, {name}, {ext}, {protocol}.
//!
//! # Example Templates
//! - `{category}/{YYYY}/{name}` → `video/2026/movie.mp4`
//! - `{YYYY}-{MM}-{DD}/{name}.{ext}` → `2026-08-09/document.pdf`
//! - `{protocol}/{category}/{name}` → `http/video/movie.mp4`

use chrono::Local;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;

use crate::save_path_manager::FileCategory;

/// Error type for path template operations
#[derive(Debug)]
pub enum PathTemplateError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    InvalidTemplate(String),
}

impl std::fmt::Display for PathTemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
            Self::InvalidTemplate(msg) => write!(f, "invalid template: {msg}"),
        }
    }
}

impl std::error::Error for PathTemplateError {}

impl From<std::io::Error> for PathTemplateError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for PathTemplateError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

/// Template variable types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateVariable {
    /// File category (video, music, document, image, archive, program, other)
    Category,
    /// 4-digit year
    Year,
    /// 2-digit month (01-12)
    Month,
    /// 2-digit day (01-31)
    Day,
    /// File name without extension
    Name,
    /// File extension (without dot)
    Extension,
    /// Download protocol (http, torrent, ed2k, magnet, p2p)
    Protocol,
    /// Original filename with extension
    Filename,
}

impl TemplateVariable {
    /// Parse variable name from string
    pub fn parse_var(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "category" => Some(Self::Category),
            "yyyy" | "year" => Some(Self::Year),
            "mm" | "month" => Some(Self::Month),
            "dd" | "day" => Some(Self::Day),
            "name" => Some(Self::Name),
            "ext" | "extension" => Some(Self::Extension),
            "protocol" => Some(Self::Protocol),
            "filename" => Some(Self::Filename),
            _ => None,
        }
    }

    /// Get all valid variable names for help text
    pub fn all_names() -> &'static [&'static str] {
        &[
            "category", "yyyy", "mm", "dd", "name", "ext", "protocol", "filename",
        ]
    }
}

/// A segment in a path template
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateSegment {
    /// Literal text
    Literal(String),
    /// Variable placeholder
    Variable(TemplateVariable),
}

/// Parsed path template
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate {
    /// Original template string
    pub template: String,
    /// Parsed segments
    pub segments: Vec<TemplateSegment>,
}

impl PathTemplate {
    /// Parse a template string into segments
    pub fn parse(template: &str) -> Result<Self, PathTemplateError> {
        if template.is_empty() {
            return Err(PathTemplateError::InvalidTemplate(
                "template cannot be empty".to_string(),
            ));
        }

        let mut segments = Vec::new();
        let mut chars = template.chars().peekable();
        let mut current_literal = String::new();

        while let Some(c) = chars.next() {
            if c == '{' {
                // Save any accumulated literal
                if !current_literal.is_empty() {
                    segments.push(TemplateSegment::Literal(current_literal.clone()));
                    current_literal.clear();
                }

                // Parse variable name
                let mut var_name = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(c) => var_name.push(c),
                        None => {
                            return Err(PathTemplateError::InvalidTemplate(
                                "unclosed variable brace".to_string(),
                            ));
                        }
                    }
                }

                // Parse variable
                match TemplateVariable::parse_var(&var_name) {
                    Some(var) => segments.push(TemplateSegment::Variable(var)),
                    None => {
                        return Err(PathTemplateError::InvalidTemplate(format!(
                            "unknown variable: {var_name}"
                        )));
                    }
                }
            } else {
                current_literal.push(c);
            }
        }

        // Save any remaining literal
        if !current_literal.is_empty() {
            segments.push(TemplateSegment::Literal(current_literal));
        }

        if segments.is_empty() {
            return Err(PathTemplateError::InvalidTemplate(
                "template must contain at least one segment".to_string(),
            ));
        }

        Ok(Self {
            template: template.to_string(),
            segments,
        })
    }

    /// Render the template with the given context
    pub fn render(&self, ctx: &TemplateContext) -> String {
        let mut result = String::new();

        for segment in &self.segments {
            match segment {
                TemplateSegment::Literal(text) => result.push_str(text),
                TemplateSegment::Variable(var) => {
                    let value = ctx.get_value(*var);
                    result.push_str(&value);
                }
            }
        }

        result
    }

    /// Validate that the template contains required variables
    pub fn validate(&self) -> Result<(), PathTemplateError> {
        // Must contain at least {name} or {filename} to identify the file
        let has_name = self.segments.iter().any(|s| {
            matches!(
                s,
                TemplateSegment::Variable(TemplateVariable::Name)
                    | TemplateSegment::Variable(TemplateVariable::Filename)
            )
        });

        if !has_name {
            return Err(PathTemplateError::InvalidTemplate(
                "template must contain {name} or {filename}".to_string(),
            ));
        }

        Ok(())
    }
}

/// Context for rendering a path template
#[derive(Debug, Clone)]
pub struct TemplateContext {
    /// File category
    pub category: FileCategory,
    /// Download protocol
    pub protocol: String,
    /// Original filename (with extension)
    pub filename: String,
    /// File name without extension
    pub name: String,
    /// File extension (without dot)
    pub ext: String,
    /// Timestamp for date variables (defaults to current time)
    pub timestamp: Option<chrono::DateTime<Local>>,
}

impl TemplateContext {
    /// Create a new context from filename and protocol
    pub fn new(filename: &str, protocol: &str) -> Self {
        let path = Path::new(filename);
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(filename)
            .to_string();
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let category = FileCategory::from_extension(&ext);

        Self {
            category,
            protocol: protocol.to_string(),
            filename: filename.to_string(),
            name,
            ext,
            timestamp: None,
        }
    }

    /// Set a custom timestamp for date variables
    pub fn with_timestamp(mut self, timestamp: chrono::DateTime<Local>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Get the value for a variable
    pub fn get_value(&self, var: TemplateVariable) -> String {
        let timestamp = self.timestamp.unwrap_or_else(Local::now);

        match var {
            TemplateVariable::Category => format!("{:?}", self.category).to_lowercase(),
            TemplateVariable::Year => timestamp.format("%Y").to_string(),
            TemplateVariable::Month => timestamp.format("%m").to_string(),
            TemplateVariable::Day => timestamp.format("%d").to_string(),
            TemplateVariable::Name => self.name.clone(),
            TemplateVariable::Extension => self.ext.clone(),
            TemplateVariable::Protocol => self.protocol.clone(),
            TemplateVariable::Filename => self.filename.clone(),
        }
    }
}

/// Path template configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathTemplateConfig {
    /// Whether path templates are enabled
    pub enabled: bool,
    /// The template string
    pub template: String,
    /// Parsed template (not serialized)
    #[serde(skip)]
    pub parsed: Option<PathTemplate>,
}

impl Default for PathTemplateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            template: "{category}/{name}".to_string(),
            parsed: None,
        }
    }
}

impl PathTemplateConfig {
    /// Create a new config with the given template
    pub fn new(template: &str) -> Result<Self, PathTemplateError> {
        let parsed = PathTemplate::parse(template)?;
        parsed.validate()?;

        Ok(Self {
            enabled: true,
            template: template.to_string(),
            parsed: Some(parsed),
        })
    }

    /// Get the parsed template, parsing if necessary
    pub fn get_parsed(&mut self) -> Result<&PathTemplate, PathTemplateError> {
        if self.parsed.is_none() {
            let parsed = PathTemplate::parse(&self.template)?;
            parsed.validate()?;
            self.parsed = Some(parsed);
        }
        Ok(self.parsed.as_ref().unwrap())
    }

    /// Render a path for the given context
    pub fn render(&mut self, ctx: &TemplateContext) -> Result<String, PathTemplateError> {
        if !self.enabled {
            return Ok(ctx.filename.clone());
        }

        let parsed = self.get_parsed()?;
        Ok(parsed.render(ctx))
    }
}

/// Path template manager for DownloadManager
#[derive(Debug)]
pub struct PathTemplateManager {
    config: RwLock<PathTemplateConfig>,
}

impl PathTemplateManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: RwLock::new(PathTemplateConfig::default()),
        }
    }

    /// Create a new manager with the given config
    pub fn with_config(config: PathTemplateConfig) -> Self {
        Self {
            config: RwLock::new(config),
        }
    }

    /// Replace the entire config (used during restoration from disk)
    pub async fn replace_config(&self, config: PathTemplateConfig) {
        *self.config.write().await = config;
    }

    /// Get the current config
    pub async fn get_config(&self) -> PathTemplateConfig {
        self.config.read().await.clone()
    }

    /// Set the template
    pub async fn set_template(&self, template: &str) -> Result<(), PathTemplateError> {
        let mut config = self.config.write().await;
        let parsed = PathTemplate::parse(template)?;
        parsed.validate()?;

        config.template = template.to_string();
        config.parsed = Some(parsed);
        config.enabled = true;

        Ok(())
    }

    /// Disable path templates
    pub async fn disable(&self) {
        let mut config = self.config.write().await;
        config.enabled = false;
    }

    /// Enable path templates
    pub async fn enable(&self) {
        let mut config = self.config.write().await;
        config.enabled = true;
    }

    /// Render a path for the given context
    pub async fn render(&self, ctx: &TemplateContext) -> Result<String, PathTemplateError> {
        let mut config = self.config.write().await;
        config.render(ctx)
    }

    /// Compute the full save path for a file
    pub async fn compute_save_path(
        &self,
        base_dir: &Path,
        filename: &str,
        protocol: &str,
    ) -> Result<PathBuf, PathTemplateError> {
        let ctx = TemplateContext::new(filename, protocol);
        let relative = self.render(&ctx).await?;
        Ok(base_dir.join(relative))
    }
}

impl Default for PathTemplateManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Persistence functions
/// Save path template config to disk
pub async fn save_path_template_config(
    config: &PathTemplateConfig,
    data_dir: &Path,
) -> Result<(), PathTemplateError> {
    let config_path = data_dir.join("path_template_config.json");
    let json = serde_json::to_string_pretty(config)?;
    let temp_path = config_path.with_extension("json.tmp");
    tokio::fs::write(&temp_path, json.as_bytes()).await?;
    tokio::fs::rename(&temp_path, &config_path).await?;
    Ok(())
}

/// Load path template config from disk
pub async fn load_path_template_config(
    data_dir: &Path,
) -> Result<Option<PathTemplateConfig>, PathTemplateError> {
    let config_path = data_dir.join("path_template_config.json");

    if !config_path.exists() {
        return Ok(None);
    }

    let json = tokio::fs::read_to_string(&config_path).await?;
    let config: PathTemplateConfig = serde_json::from_str(&json)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_template() {
        let template = PathTemplate::parse("{name}").unwrap();
        assert_eq!(template.segments.len(), 1);
        assert!(matches!(
            template.segments[0],
            TemplateSegment::Variable(TemplateVariable::Name)
        ));
    }

    #[test]
    fn test_parse_complex_template() {
        let template = PathTemplate::parse("{category}/{YYYY}/{name}.{ext}").unwrap();
        assert_eq!(template.segments.len(), 7);
    }

    #[test]
    fn test_parse_empty_template() {
        assert!(PathTemplate::parse("").is_err());
    }

    #[test]
    fn test_parse_unclosed_brace() {
        assert!(PathTemplate::parse("{name").is_err());
    }

    #[test]
    fn test_parse_unknown_variable() {
        assert!(PathTemplate::parse("{unknown}").is_err());
    }

    #[test]
    fn test_render_simple() {
        let template = PathTemplate::parse("{name}").unwrap();
        let ctx = TemplateContext::new("movie.mp4", "http");
        assert_eq!(template.render(&ctx), "movie");
    }

    #[test]
    fn test_render_with_extension() {
        let template = PathTemplate::parse("{name}.{ext}").unwrap();
        let ctx = TemplateContext::new("movie.mp4", "http");
        assert_eq!(template.render(&ctx), "movie.mp4");
    }

    #[test]
    fn test_render_with_category() {
        let template = PathTemplate::parse("{category}/{name}").unwrap();
        let ctx = TemplateContext::new("movie.mp4", "http");
        assert_eq!(template.render(&ctx), "video/movie");
    }

    #[test]
    fn test_render_with_protocol() {
        let template = PathTemplate::parse("{protocol}/{name}").unwrap();
        let ctx = TemplateContext::new("movie.mp4", "torrent");
        assert_eq!(template.render(&ctx), "torrent/movie");
    }

    #[test]
    fn test_render_with_date() {
        let template = PathTemplate::parse("{yyyy}-{mm}-{dd}/{name}").unwrap();
        let timestamp = chrono::DateTime::parse_from_rfc3339("2026-08-09T12:00:00+08:00")
            .unwrap()
            .with_timezone(&Local);
        let ctx = TemplateContext::new("movie.mp4", "http").with_timestamp(timestamp);
        assert_eq!(template.render(&ctx), "2026-08-09/movie");
    }

    #[test]
    fn test_render_filename() {
        let template = PathTemplate::parse("{filename}").unwrap();
        let ctx = TemplateContext::new("movie.mp4", "http");
        assert_eq!(template.render(&ctx), "movie.mp4");
    }

    #[test]
    fn test_validate_requires_name() {
        let template = PathTemplate::parse("{category}/{yyyy}").unwrap();
        assert!(template.validate().is_err());
    }

    #[test]
    fn test_validate_with_name() {
        let template = PathTemplate::parse("{category}/{name}").unwrap();
        assert!(template.validate().is_ok());
    }

    #[test]
    fn test_validate_with_filename() {
        let template = PathTemplate::parse("{filename}").unwrap();
        assert!(template.validate().is_ok());
    }

    #[test]
    fn test_template_context_new() {
        let ctx = TemplateContext::new("document.pdf", "http");
        assert_eq!(ctx.name, "document");
        assert_eq!(ctx.ext, "pdf");
        assert_eq!(ctx.filename, "document.pdf");
        assert_eq!(ctx.category, FileCategory::Document);
        assert_eq!(ctx.protocol, "http");
    }

    #[test]
    fn test_template_context_no_extension() {
        let ctx = TemplateContext::new("README", "http");
        assert_eq!(ctx.name, "README");
        assert_eq!(ctx.ext, "");
        assert_eq!(ctx.category, FileCategory::Other);
    }

    #[test]
    fn test_config_default() {
        let config = PathTemplateConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.template, "{category}/{name}");
    }

    #[test]
    fn test_config_new() {
        let config = PathTemplateConfig::new("{name}.{ext}").unwrap();
        assert!(config.enabled);
        assert_eq!(config.template, "{name}.{ext}");
        assert!(config.parsed.is_some());
    }

    #[test]
    fn test_config_new_invalid() {
        assert!(PathTemplateConfig::new("{unknown}").is_err());
        assert!(PathTemplateConfig::new("").is_err());
    }

    #[test]
    fn test_config_render() {
        let mut config = PathTemplateConfig::new("{category}/{name}").unwrap();
        let ctx = TemplateContext::new("song.mp3", "http");
        let result = config.render(&ctx).unwrap();
        assert_eq!(result, "music/song");
    }

    #[test]
    fn test_config_render_disabled() {
        let mut config = PathTemplateConfig {
            enabled: false,
            template: "{category}/{name}".to_string(),
            parsed: None,
        };
        let ctx = TemplateContext::new("song.mp3", "http");
        let result = config.render(&ctx).unwrap();
        assert_eq!(result, "song.mp3");
    }

    #[tokio::test]
    async fn test_manager_new() {
        let manager = PathTemplateManager::new();
        let config = manager.get_config().await;
        assert!(!config.enabled);
    }

    #[tokio::test]
    async fn test_manager_set_template() {
        let manager = PathTemplateManager::new();
        manager.set_template("{name}.{ext}").await.unwrap();
        let config = manager.get_config().await;
        assert!(config.enabled);
        assert_eq!(config.template, "{name}.{ext}");
    }

    #[tokio::test]
    async fn test_manager_render() {
        let manager = PathTemplateManager::new();
        manager.set_template("{category}/{name}").await.unwrap();
        let ctx = TemplateContext::new("video.mp4", "http");
        let result = manager.render(&ctx).await.unwrap();
        assert_eq!(result, "video/video");
    }

    #[tokio::test]
    async fn test_manager_disable() {
        let manager = PathTemplateManager::new();
        manager.set_template("{category}/{name}").await.unwrap();
        manager.disable().await;
        let config = manager.get_config().await;
        assert!(!config.enabled);
    }

    #[tokio::test]
    async fn test_manager_enable() {
        let manager = PathTemplateManager::new();
        manager.set_template("{category}/{name}").await.unwrap();
        manager.disable().await;
        manager.enable().await;
        let config = manager.get_config().await;
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_manager_compute_save_path() {
        let manager = PathTemplateManager::new();
        manager.set_template("{category}/{name}").await.unwrap();
        let base = Path::new("/downloads");
        let result = manager
            .compute_save_path(base, "movie.mp4", "http")
            .await
            .unwrap();
        assert_eq!(result, PathBuf::from("/downloads/video/movie"));
    }

    #[tokio::test]
    async fn test_persistence_save_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PathTemplateConfig::new("{yyyy}/{name}").unwrap();

        save_path_template_config(&config, temp_dir.path())
            .await
            .unwrap();

        let loaded = load_path_template_config(temp_dir.path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.template, config.template);
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[tokio::test]
    async fn test_persistence_load_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = load_path_template_config(temp_dir.path()).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_all_variable_names() {
        let names = TemplateVariable::all_names();
        assert!(names.contains(&"category"));
        assert!(names.contains(&"yyyy"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"ext"));
    }

    #[test]
    fn test_variable_parse_var() {
        assert_eq!(
            TemplateVariable::parse_var("category"),
            Some(TemplateVariable::Category)
        );
        assert_eq!(
            TemplateVariable::parse_var("YYYY"),
            Some(TemplateVariable::Year)
        );
        assert_eq!(
            TemplateVariable::parse_var("year"),
            Some(TemplateVariable::Year)
        );
        assert_eq!(TemplateVariable::parse_var("invalid"), None);
    }

    #[test]
    fn test_category_various_extensions() {
        let test_cases = vec![
            ("video.mkv", FileCategory::Video),
            ("song.flac", FileCategory::Music),
            ("doc.pdf", FileCategory::Document),
            ("image.png", FileCategory::Image),
            ("archive.zip", FileCategory::Archive),
            ("installer.exe", FileCategory::Program),
            ("unknown.xyz", FileCategory::Other),
        ];

        for (filename, expected) in test_cases {
            let ctx = TemplateContext::new(filename, "http");
            assert_eq!(ctx.category, expected, "Failed for {filename}");
        }
    }

    #[test]
    fn test_template_with_literals() {
        let template = PathTemplate::parse("downloads/{category}/files/{name}").unwrap();
        let ctx = TemplateContext::new("test.mp4", "http");
        assert_eq!(template.render(&ctx), "downloads/video/files/test");
    }

    #[test]
    fn test_template_multiple_same_variable() {
        let template = PathTemplate::parse("{yyyy}/{mm}/{dd}/{name}").unwrap();
        let timestamp = chrono::DateTime::parse_from_rfc3339("2026-12-25T12:00:00+08:00")
            .unwrap()
            .with_timezone(&Local);
        let ctx = TemplateContext::new("file.txt", "http").with_timestamp(timestamp);
        assert_eq!(template.render(&ctx), "2026/12/25/file");
    }
}
