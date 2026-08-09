//! Download Task Templates (Phase 100)
//!
//! Advanced reusable configurations for download tasks with URL pattern matching.
//! Templates can be automatically applied based on URL patterns, providing
//! intelligent configuration for different download sources.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Error type for template persistence operations
#[derive(Debug)]
pub enum TemplatePersistenceError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for TemplatePersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
        }
    }
}

impl std::error::Error for TemplatePersistenceError {}

impl From<std::io::Error> for TemplatePersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for TemplatePersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

/// A download template with comprehensive configuration options
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DownloadTemplate {
    /// Unique identifier for the template
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// URL patterns for auto-matching (supports wildcards)
    #[serde(default)]
    pub url_patterns: Vec<String>,
    /// Tags to apply to tasks using this template
    #[serde(default)]
    pub tags: Vec<String>,
    /// Group to assign tasks to
    #[serde(default)]
    pub group: Option<String>,
    /// Priority level (1=Low, 2=Normal, 3=High, 4=Urgent)
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Per-task speed limit in bytes/sec (None = unlimited)
    #[serde(default)]
    pub speed_limit_bps: Option<u64>,
    /// Bandwidth weight (1-10)
    #[serde(default = "default_bandwidth_weight")]
    pub bandwidth_weight: u8,
    /// Save path override (None = use default)
    #[serde(default)]
    pub save_path: Option<PathBuf>,
    /// Max retries for this template's tasks
    #[serde(default)]
    pub max_retries: Option<u32>,
    /// Expected checksum for verification (format: "algorithm:hash")
    #[serde(default)]
    pub checksum: Option<String>,
    /// Proxy URL to use for downloads (e.g., "socks5://127.0.0.1:1080")
    #[serde(default)]
    pub proxy_url: Option<String>,
    /// Maximum download time in seconds (None = unlimited)
    #[serde(default)]
    pub max_download_time_secs: Option<u64>,
    /// Whether this template is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Whether to auto-apply when URL matches pattern
    #[serde(default = "default_true")]
    pub auto_apply: bool,
    /// Template category for organization
    #[serde(default)]
    pub category: Option<String>,
    /// Creation timestamp (Unix epoch seconds)
    #[serde(default = "now_secs")]
    pub created_at: u64,
    /// Last used timestamp (Unix epoch seconds)
    #[serde(default)]
    pub last_used_at: Option<u64>,
    /// Number of times this template has been used
    #[serde(default)]
    pub use_count: u64,
}

fn default_priority() -> u8 {
    2
}

fn default_bandwidth_weight() -> u8 {
    1
}

fn default_true() -> bool {
    true
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl DownloadTemplate {
    /// Create a new template with required fields
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: None,
            url_patterns: Vec::new(),
            tags: Vec::new(),
            group: None,
            priority: default_priority(),
            speed_limit_bps: None,
            bandwidth_weight: default_bandwidth_weight(),
            save_path: None,
            max_retries: None,
            checksum: None,
            proxy_url: None,
            max_download_time_secs: None,
            enabled: true,
            auto_apply: true,
            category: None,
            created_at: now_secs(),
            last_used_at: None,
            use_count: 0,
        }
    }

    /// Check if a URL matches any of this template's patterns
    pub fn matches_url(&self, url: &str) -> bool {
        if self.url_patterns.is_empty() {
            return false;
        }
        self.url_patterns
            .iter()
            .any(|pattern| wildcard_match(pattern, url))
    }

    /// Record that this template was used
    pub fn record_use(&mut self) {
        self.use_count += 1;
        self.last_used_at = Some(now_secs());
    }

    /// Get a summary of this template
    pub fn summary(&self) -> TemplateSummary {
        TemplateSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            url_patterns: self.url_patterns.len(),
            tags: self.tags.clone(),
            group: self.group.clone(),
            priority: self.priority,
            enabled: self.enabled,
            auto_apply: self.auto_apply,
            category: self.category.clone(),
            use_count: self.use_count,
        }
    }
}

/// Summary of a template for listing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TemplateSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub url_patterns: usize,
    pub tags: Vec<String>,
    pub group: Option<String>,
    pub priority: u8,
    pub enabled: bool,
    pub auto_apply: bool,
    pub category: Option<String>,
    pub use_count: u64,
}

/// Statistics about download templates
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TemplateStats {
    pub total: usize,
    pub enabled: usize,
    pub categories: usize,
}

/// Manager for download templates
#[derive(Debug, Default)]
pub struct DownloadTemplateManager {
    templates: Vec<DownloadTemplate>,
}

impl DownloadTemplateManager {
    /// Create a new template manager
    pub fn new() -> Self {
        Self {
            templates: Vec::new(),
        }
    }

    /// Add or update a template
    pub fn add_template(&mut self, template: DownloadTemplate) {
        // Replace existing template with same ID
        self.templates.retain(|t| t.id != template.id);
        self.templates.push(template);
    }

    /// Remove a template by ID
    pub fn remove_template(&mut self, id: &str) -> Option<DownloadTemplate> {
        if let Some(pos) = self.templates.iter().position(|t| t.id == id) {
            Some(self.templates.remove(pos))
        } else {
            None
        }
    }

    /// Get a template by ID
    pub fn get_template(&self, id: &str) -> Option<&DownloadTemplate> {
        self.templates.iter().find(|t| t.id == id)
    }

    /// Get a mutable template by ID
    pub fn get_template_mut(&mut self, id: &str) -> Option<&mut DownloadTemplate> {
        self.templates.iter_mut().find(|t| t.id == id)
    }

    /// List all templates
    pub fn list_templates(&self) -> &[DownloadTemplate] {
        &self.templates
    }

    /// List enabled templates only
    pub fn list_enabled_templates(&self) -> Vec<&DownloadTemplate> {
        self.templates.iter().filter(|t| t.enabled).collect()
    }

    /// Find templates that match a URL
    pub fn find_matching_templates(&self, url: &str) -> Vec<&DownloadTemplate> {
        self.templates
            .iter()
            .filter(|t| t.enabled && t.auto_apply && t.matches_url(url))
            .collect()
    }

    /// Find the best matching template for a URL (highest priority, most specific pattern)
    pub fn find_best_template(&self, url: &str) -> Option<&DownloadTemplate> {
        let mut matching: Vec<&DownloadTemplate> = self
            .templates
            .iter()
            .filter(|t| t.enabled && t.auto_apply && t.matches_url(url))
            .collect();

        if matching.is_empty() {
            return None;
        }

        // Sort by: more patterns (more specific) first, then by creation time (older first)
        matching.sort_by(|a, b| {
            b.url_patterns
                .len()
                .cmp(&a.url_patterns.len())
                .then(a.created_at.cmp(&b.created_at))
        });

        matching.first().copied()
    }

    /// List templates by category
    pub fn list_by_category(&self, category: &str) -> Vec<&DownloadTemplate> {
        self.templates
            .iter()
            .filter(|t| t.category.as_deref() == Some(category))
            .collect()
    }

    /// List all unique categories
    pub fn list_categories(&self) -> Vec<String> {
        let mut categories: Vec<String> = self
            .templates
            .iter()
            .filter_map(|t| t.category.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        categories.sort();
        categories
    }

    /// Enable or disable a template
    pub fn set_enabled(&mut self, id: &str, enabled: bool) -> bool {
        if let Some(template) = self.get_template_mut(id) {
            template.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Enable or disable auto-apply for a template
    pub fn set_auto_apply(&mut self, id: &str, auto_apply: bool) -> bool {
        if let Some(template) = self.get_template_mut(id) {
            template.auto_apply = auto_apply;
            true
        } else {
            false
        }
    }

    /// Record usage of a template
    pub fn record_use(&mut self, id: &str) -> bool {
        if let Some(template) = self.get_template_mut(id) {
            template.record_use();
            true
        } else {
            false
        }
    }

    /// Get template count
    pub fn count(&self) -> usize {
        self.templates.len()
    }

    /// Get enabled template count
    pub fn enabled_count(&self) -> usize {
        self.templates.iter().filter(|t| t.enabled).count()
    }

    /// Clear all templates
    pub fn clear(&mut self) {
        self.templates.clear();
    }

    /// Get all summaries
    pub fn get_summaries(&self) -> Vec<TemplateSummary> {
        self.templates.iter().map(|t| t.summary()).collect()
    }
}

/// Simple wildcard matching (supports * and ?)
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    wildcard_match_recursive(&pattern_chars, &text_chars)
}

fn wildcard_match_recursive(pattern: &[char], text: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        '*' => {
            // Try matching rest of pattern with each suffix of text
            if wildcard_match_recursive(&pattern[1..], text) {
                return true;
            }
            if !text.is_empty() {
                return wildcard_match_recursive(pattern, &text[1..]);
            }
            false
        }
        '?' => {
            if text.is_empty() {
                return false;
            }
            wildcard_match_recursive(&pattern[1..], &text[1..])
        }
        c => {
            if text.is_empty() {
                return false;
            }
            if c.eq_ignore_ascii_case(&text[0]) {
                wildcard_match_recursive(&pattern[1..], &text[1..])
            } else {
                false
            }
        }
    }
}

/// Save templates to disk (atomic write)
pub fn save_templates(
    templates: &[DownloadTemplate],
    data_dir: &Path,
) -> Result<(), TemplatePersistenceError> {
    let path = data_dir.join("download_templates.json");
    let json = serde_json::to_string_pretty(templates)?;
    let temp_path = data_dir.join("download_templates.json.tmp");
    std::fs::write(&temp_path, &json)?;
    std::fs::rename(&temp_path, &path)?;
    Ok(())
}

/// Load templates from disk
pub fn load_templates(data_dir: &Path) -> Result<Vec<DownloadTemplate>, TemplatePersistenceError> {
    let path = data_dir.join("download_templates.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let json = std::fs::read_to_string(&path)?;
    let templates: Vec<DownloadTemplate> = serde_json::from_str(&json)?;
    Ok(templates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_template_creation() {
        let template = DownloadTemplate::new("test-1", "Test Template");
        assert_eq!(template.id, "test-1");
        assert_eq!(template.name, "Test Template");
        assert!(template.enabled);
        assert!(template.auto_apply);
        assert_eq!(template.priority, 2);
        assert_eq!(template.bandwidth_weight, 1);
        assert_eq!(template.use_count, 0);
    }

    #[test]
    fn test_wildcard_match_exact() {
        assert!(wildcard_match(
            "https://example.com/file.zip",
            "https://example.com/file.zip"
        ));
        assert!(!wildcard_match(
            "https://example.com/file.zip",
            "https://example.com/other.zip"
        ));
    }

    #[test]
    fn test_wildcard_match_star() {
        assert!(wildcard_match(
            "https://example.com/*",
            "https://example.com/file.zip"
        ));
        assert!(wildcard_match(
            "https://example.com/*",
            "https://example.com/anything.zip"
        ));
        assert!(!wildcard_match(
            "https://example.com/*.zip",
            "https://example.com/file.tar"
        ));
        assert!(!wildcard_match(
            "https://example.com/*",
            "https://other.com/file.zip"
        ));
    }

    #[test]
    fn test_wildcard_match_question() {
        assert!(wildcard_match(
            "https://example.com/file?.zip",
            "https://example.com/file1.zip"
        ));
        assert!(wildcard_match(
            "https://example.com/file?.zip",
            "https://example.com/fileA.zip"
        ));
        assert!(!wildcard_match(
            "https://example.com/file?.zip",
            "https://example.com/file12.zip"
        ));
    }

    #[test]
    fn test_wildcard_case_insensitive() {
        assert!(wildcard_match(
            "HTTPS://EXAMPLE.COM/*",
            "https://example.com/file.zip"
        ));
        assert!(wildcard_match(
            "https://example.com/*",
            "HTTPS://EXAMPLE.COM/FILE.ZIP"
        ));
    }

    #[test]
    fn test_template_matches_url() {
        let mut template = DownloadTemplate::new("test", "Test");
        template.url_patterns = vec![
            "*github.com/*".to_string(),
            "*githubusercontent.com/*".to_string(),
        ];

        assert!(template.matches_url("https://github.com/user/repo/releases/download/file.zip"));
        assert!(template.matches_url("https://objects.githubusercontent.com/file.tar.gz"));
        assert!(!template.matches_url("https://example.com/file.zip"));
    }

    #[test]
    fn test_template_no_patterns_no_match() {
        let template = DownloadTemplate::new("test", "Test");
        assert!(!template.matches_url("https://example.com/file.zip"));
    }

    #[test]
    fn test_template_record_use() {
        let mut template = DownloadTemplate::new("test", "Test");
        assert_eq!(template.use_count, 0);
        assert!(template.last_used_at.is_none());

        template.record_use();
        assert_eq!(template.use_count, 1);
        assert!(template.last_used_at.is_some());

        template.record_use();
        assert_eq!(template.use_count, 2);
    }

    #[test]
    fn test_manager_add_remove() {
        let mut manager = DownloadTemplateManager::new();
        let template = DownloadTemplate::new("test-1", "Test 1");

        manager.add_template(template.clone());
        assert_eq!(manager.count(), 1);
        assert!(manager.get_template("test-1").is_some());

        let removed = manager.remove_template("test-1");
        assert!(removed.is_some());
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_manager_replace_existing() {
        let mut manager = DownloadTemplateManager::new();

        let mut template1 = DownloadTemplate::new("test-1", "Test 1");
        template1.priority = 1;
        manager.add_template(template1);

        let mut template2 = DownloadTemplate::new("test-1", "Test 1 Updated");
        template2.priority = 3;
        manager.add_template(template2);

        assert_eq!(manager.count(), 1);
        let t = manager.get_template("test-1").unwrap();
        assert_eq!(t.name, "Test 1 Updated");
        assert_eq!(t.priority, 3);
    }

    #[test]
    fn test_manager_find_matching() {
        let mut manager = DownloadTemplateManager::new();

        let mut t1 = DownloadTemplate::new("github", "GitHub");
        t1.url_patterns = vec!["*github.com/*".to_string()];
        manager.add_template(t1);

        let mut t2 = DownloadTemplate::new("archive", "Archive");
        t2.url_patterns = vec!["*archive.org/*".to_string()];
        manager.add_template(t2);

        let mut t3 = DownloadTemplate::new("disabled", "Disabled");
        t3.url_patterns = vec!["*github.com/*".to_string()];
        t3.enabled = false;
        manager.add_template(t3);

        let matching = manager.find_matching_templates("https://github.com/user/repo/file.zip");
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].id, "github");
    }

    #[test]
    fn test_manager_find_best_template() {
        let mut manager = DownloadTemplateManager::new();

        // General pattern
        let mut t1 = DownloadTemplate::new("general", "General");
        t1.url_patterns = vec!["*.com/*".to_string()];
        manager.add_template(t1);

        // More specific pattern
        let mut t2 = DownloadTemplate::new("specific", "Specific");
        t2.url_patterns = vec!["*.github.com/*".to_string(), "*releases*".to_string()];
        manager.add_template(t2);

        let best = manager.find_best_template("https://github.com/user/repo/releases/file.zip");
        assert!(best.is_some());
        assert_eq!(best.unwrap().id, "specific"); // More patterns = more specific
    }

    #[test]
    fn test_manager_list_by_category() {
        let mut manager = DownloadTemplateManager::new();

        let mut t1 = DownloadTemplate::new("t1", "T1");
        t1.category = Some("Work".to_string());
        manager.add_template(t1);

        let mut t2 = DownloadTemplate::new("t2", "T2");
        t2.category = Some("Personal".to_string());
        manager.add_template(t2);

        let mut t3 = DownloadTemplate::new("t3", "T3");
        t3.category = Some("Work".to_string());
        manager.add_template(t3);

        let work = manager.list_by_category("Work");
        assert_eq!(work.len(), 2);

        let categories = manager.list_categories();
        assert_eq!(categories.len(), 2);
        assert!(categories.contains(&"Work".to_string()));
        assert!(categories.contains(&"Personal".to_string()));
    }

    #[test]
    fn test_manager_enable_disable() {
        let mut manager = DownloadTemplateManager::new();
        let template = DownloadTemplate::new("test", "Test");
        manager.add_template(template);

        assert!(manager.set_enabled("test", false));
        assert!(!manager.get_template("test").unwrap().enabled);

        assert!(manager.set_enabled("test", true));
        assert!(manager.get_template("test").unwrap().enabled);

        assert!(!manager.set_enabled("nonexistent", true));
    }

    #[test]
    fn test_manager_auto_apply() {
        let mut manager = DownloadTemplateManager::new();
        let template = DownloadTemplate::new("test", "Test");
        manager.add_template(template);

        assert!(manager.set_auto_apply("test", false));
        assert!(!manager.get_template("test").unwrap().auto_apply);
    }

    #[test]
    fn test_manager_record_use() {
        let mut manager = DownloadTemplateManager::new();
        let template = DownloadTemplate::new("test", "Test");
        manager.add_template(template);

        assert!(manager.record_use("test"));
        assert_eq!(manager.get_template("test").unwrap().use_count, 1);

        assert!(!manager.record_use("nonexistent"));
    }

    #[test]
    fn test_manager_summaries() {
        let mut manager = DownloadTemplateManager::new();

        let mut t1 = DownloadTemplate::new("t1", "Template 1");
        t1.tags = vec!["tag1".to_string()];
        t1.group = Some("group1".to_string());
        manager.add_template(t1);

        let summaries = manager.get_summaries();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "t1");
        assert_eq!(summaries[0].tags, vec!["tag1"]);
        assert_eq!(summaries[0].group, Some("group1".to_string()));
    }

    #[test]
    fn test_persistence_save_load() {
        let temp_dir = TempDir::new().unwrap();

        let mut templates = Vec::new();
        let mut t1 = DownloadTemplate::new("t1", "Template 1");
        t1.url_patterns = vec!["*.example.com/*".to_string()];
        t1.tags = vec!["test".to_string()];
        templates.push(t1);

        let mut t2 = DownloadTemplate::new("t2", "Template 2");
        t2.priority = 3;
        t2.speed_limit_bps = Some(1_000_000);
        templates.push(t2);

        save_templates(&templates, temp_dir.path()).unwrap();

        let loaded = load_templates(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "t1");
        assert_eq!(loaded[0].url_patterns, vec!["*.example.com/*"]);
        assert_eq!(loaded[1].id, "t2");
        assert_eq!(loaded[1].priority, 3);
        assert_eq!(loaded[1].speed_limit_bps, Some(1_000_000));
    }

    #[test]
    fn test_persistence_empty() {
        let temp_dir = TempDir::new().unwrap();
        let loaded = load_templates(temp_dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_persistence_overwrite() {
        let temp_dir = TempDir::new().unwrap();

        let templates1 = vec![DownloadTemplate::new("t1", "Template 1")];
        save_templates(&templates1, temp_dir.path()).unwrap();

        let templates2 = vec![
            DownloadTemplate::new("t2", "Template 2"),
            DownloadTemplate::new("t3", "Template 3"),
        ];
        save_templates(&templates2, temp_dir.path()).unwrap();

        let loaded = load_templates(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "t2");
    }

    #[test]
    fn test_template_summary() {
        let mut template = DownloadTemplate::new("test", "Test Template");
        template.description = Some("A test template".to_string());
        template.url_patterns = vec!["*.com/*".to_string(), "*.org/*".to_string()];
        template.tags = vec!["tag1".to_string(), "tag2".to_string()];
        template.group = Some("mygroup".to_string());
        template.category = Some("work".to_string());
        template.use_count = 5;

        let summary = template.summary();
        assert_eq!(summary.id, "test");
        assert_eq!(summary.name, "Test Template");
        assert_eq!(summary.description, Some("A test template".to_string()));
        assert_eq!(summary.url_patterns, 2);
        assert_eq!(summary.tags.len(), 2);
        assert_eq!(summary.group, Some("mygroup".to_string()));
        assert_eq!(summary.category, Some("work".to_string()));
        assert_eq!(summary.use_count, 5);
    }

    #[test]
    fn test_manager_list_enabled() {
        let mut manager = DownloadTemplateManager::new();

        let mut t1 = DownloadTemplate::new("t1", "Enabled");
        t1.enabled = true;
        manager.add_template(t1);

        let mut t2 = DownloadTemplate::new("t2", "Disabled");
        t2.enabled = false;
        manager.add_template(t2);

        let mut t3 = DownloadTemplate::new("t3", "Also Enabled");
        t3.enabled = true;
        manager.add_template(t3);

        let enabled = manager.list_enabled_templates();
        assert_eq!(enabled.len(), 2);
        assert_eq!(manager.enabled_count(), 2);
    }

    #[test]
    fn test_manager_clear() {
        let mut manager = DownloadTemplateManager::new();
        manager.add_template(DownloadTemplate::new("t1", "T1"));
        manager.add_template(DownloadTemplate::new("t2", "T2"));
        assert_eq!(manager.count(), 2);

        manager.clear();
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_wildcard_complex_patterns() {
        // Multiple wildcards
        assert!(wildcard_match(
            "*example*/*file*",
            "https://example.com/path/file.zip"
        ));
        assert!(wildcard_match("*://*/*", "https://example.com/path"));

        // Empty pattern
        assert!(!wildcard_match("", "something"));
        assert!(wildcard_match("", ""));

        // Just wildcard
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*", ""));
    }

    #[test]
    fn test_template_serialization_roundtrip() {
        let mut template = DownloadTemplate::new("test", "Test");
        template.url_patterns = vec!["*.example.com/*".to_string()];
        template.tags = vec!["tag1".to_string()];
        template.group = Some("group".to_string());
        template.priority = 3;
        template.speed_limit_bps = Some(5_000_000);
        template.bandwidth_weight = 5;
        template.save_path = Some(PathBuf::from("/downloads"));
        template.max_retries = Some(3);
        template.checksum = Some("sha256:abc123".to_string());
        template.proxy_url = Some("socks5://127.0.0.1:1080".to_string());
        template.max_download_time_secs = Some(3600);
        template.category = Some("work".to_string());

        let json = serde_json::to_string(&template).unwrap();
        let deserialized: DownloadTemplate = serde_json::from_str(&json).unwrap();

        assert_eq!(template, deserialized);
    }
}
