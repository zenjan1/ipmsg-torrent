//! Download Label Presets (Phase 62)
//!
//! Predefined configurations that can be applied to new download tasks.
//! A preset bundles common settings (tags, group, priority, speed limit, etc.)
//! so users can quickly apply consistent configurations.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Error type for preset persistence operations
#[derive(Debug)]
pub enum PresetPersistenceError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for PresetPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
        }
    }
}

impl std::error::Error for PresetPersistenceError {}

impl From<std::io::Error> for PresetPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for PresetPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

/// A download preset bundles common task settings into a reusable template
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DownloadPreset {
    /// Unique identifier for the preset
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Tags to apply to tasks using this preset
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
    /// Max retries for this preset's tasks
    #[serde(default)]
    pub max_retries: Option<u32>,
    /// Whether this preset is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// Category for organization (e.g., "media", "documents", "work")
    #[serde(default)]
    pub category: Option<String>,
    /// Number of times this preset has been applied
    #[serde(default)]
    pub use_count: u64,
    /// Last time this preset was applied (Unix timestamp)
    #[serde(default)]
    pub last_used_at: Option<u64>,
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

impl DownloadPreset {
    /// Create a new preset with the given id and name
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            tags: Vec::new(),
            group: None,
            priority: 2,
            speed_limit_bps: None,
            bandwidth_weight: 1,
            save_path: None,
            max_retries: None,
            enabled: true,
            description: None,
            category: None,
            use_count: 0,
            last_used_at: None,
        }
    }

    /// Record that this preset was used
    pub fn record_usage(&mut self) {
        self.use_count += 1;
        self.last_used_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
    }

    /// Format the preset for display
    pub fn display(&self) -> String {
        let mut parts = vec![format!("📋 {} ({})", self.name, self.id)];
        if let Some(ref desc) = self.description {
            parts.push(format!("  Description: {desc}"));
        }
        if !self.tags.is_empty() {
            parts.push(format!("  Tags: {}", self.tags.join(", ")));
        }
        if let Some(ref group) = self.group {
            parts.push(format!("  Group: {group}"));
        }
        parts.push(format!("  Priority: {}", priority_label(self.priority)));
        if let Some(limit) = self.speed_limit_bps {
            parts.push(format!("  Speed Limit: {}", format_speed(limit)));
        }
        parts.push(format!("  Bandwidth Weight: {}", self.bandwidth_weight));
        if let Some(ref path) = self.save_path {
            parts.push(format!("  Save Path: {}", path.display()));
        }
        if let Some(retries) = self.max_retries {
            parts.push(format!("  Max Retries: {retries}"));
        }
        if !self.enabled {
            parts.push("  ⚠️  Disabled".to_string());
        }
        parts.join("\n")
    }
}

fn priority_label(p: u8) -> &'static str {
    match p {
        1 => "Low",
        2 => "Normal",
        3 => "High",
        4 => "Urgent",
        _ => "Unknown",
    }
}

fn format_speed(bps: u64) -> String {
    if bps >= 1_048_576 {
        format!("{:.1} MB/s", bps as f64 / 1_048_576.0)
    } else if bps >= 1024 {
        format!("{:.1} KB/s", bps as f64 / 1024.0)
    } else {
        format!("{bps} B/s")
    }
}

/// Parse priority from user input
pub fn parse_priority(s: &str) -> Option<u8> {
    match s.trim().to_lowercase().as_str() {
        "low" | "1" => Some(1),
        "normal" | "default" | "2" => Some(2),
        "high" | "3" => Some(3),
        "urgent" | "4" => Some(4),
        _ => s
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|&p| (1..=4).contains(&p)),
    }
}

/// Summary of preset usage statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetUsageSummary {
    /// Total number of presets
    pub total_presets: usize,
    /// Number of enabled presets
    pub enabled_presets: usize,
    /// Number of disabled presets
    pub disabled_presets: usize,
    /// Total usage count across all presets
    pub total_usage_count: u64,
    /// Most used preset ID and count
    pub most_used_preset: Option<(String, u64)>,
    /// Least used preset ID and count (among used presets)
    pub least_used_preset: Option<(String, u64)>,
    /// Presets never used
    pub unused_presets_count: usize,
    /// Categories and their preset counts
    pub categories: std::collections::HashMap<String, usize>,
}

/// Manager for download presets with enhanced functionality
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetManager {
    /// All presets
    pub presets: Vec<DownloadPreset>,
}

impl PresetManager {
    /// Create a new preset manager
    pub fn new() -> Self {
        Self { presets: Vec::new() }
    }

    /// Create from existing presets
    pub fn from_presets(presets: Vec<DownloadPreset>) -> Self {
        Self { presets }
    }

    /// Add a preset
    pub fn add(&mut self, preset: DownloadPreset) {
        self.presets.retain(|p| p.id != preset.id);
        self.presets.push(preset);
    }

    /// Remove a preset by ID
    pub fn remove(&mut self, id: &str) -> bool {
        let len_before = self.presets.len();
        self.presets.retain(|p| p.id != id);
        self.presets.len() < len_before
    }

    /// Get a preset by ID
    pub fn get(&self, id: &str) -> Option<&DownloadPreset> {
        self.presets.iter().find(|p| p.id == id)
    }

    /// Get a mutable preset by ID
    pub fn get_mut(&mut self, id: &str) -> Option<&mut DownloadPreset> {
        self.presets.iter_mut().find(|p| p.id == id)
    }

    /// List all presets
    pub fn list(&self) -> &[DownloadPreset] {
        &self.presets
    }

    /// List presets by category
    pub fn list_by_category(&self, category: &str) -> Vec<&DownloadPreset> {
        self.presets
            .iter()
            .filter(|p| p.category.as_deref() == Some(category))
            .collect()
    }

    /// Get all unique categories
    pub fn categories(&self) -> Vec<String> {
        let mut cats: Vec<String> = self
            .presets
            .iter()
            .filter_map(|p| p.category.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        cats.sort();
        cats
    }

    /// Enable a preset
    pub fn enable(&mut self, id: &str) -> bool {
        if let Some(preset) = self.get_mut(id) {
            preset.enabled = true;
            true
        } else {
            false
        }
    }

    /// Disable a preset
    pub fn disable(&mut self, id: &str) -> bool {
        if let Some(preset) = self.get_mut(id) {
            preset.enabled = false;
            true
        } else {
            false
        }
    }

    /// Update a preset's fields
    pub fn update(&mut self, id: &str, updates: PresetUpdate) -> bool {
        if let Some(preset) = self.get_mut(id) {
            if let Some(name) = updates.name {
                preset.name = name;
            }
            if let Some(tags) = updates.tags {
                preset.tags = tags;
            }
            if let Some(group) = updates.group {
                preset.group = Some(group);
            }
            if let Some(priority) = updates.priority {
                preset.priority = priority;
            }
            if let Some(speed_limit) = updates.speed_limit_bps {
                preset.speed_limit_bps = Some(speed_limit);
            }
            if let Some(weight) = updates.bandwidth_weight {
                preset.bandwidth_weight = weight;
            }
            if let Some(path) = updates.save_path {
                preset.save_path = Some(PathBuf::from(path));
            }
            if let Some(retries) = updates.max_retries {
                preset.max_retries = Some(retries);
            }
            if let Some(desc) = updates.description {
                preset.description = Some(desc);
            }
            if let Some(cat) = updates.category {
                preset.category = Some(cat);
            }
            if let Some(enabled) = updates.enabled {
                preset.enabled = enabled;
            }
            true
        } else {
            false
        }
    }

    /// Record usage of a preset
    pub fn record_usage(&mut self, id: &str) -> bool {
        if let Some(preset) = self.get_mut(id) {
            preset.record_usage();
            true
        } else {
            false
        }
    }

    /// Get usage summary
    pub fn usage_summary(&self) -> PresetUsageSummary {
        let mut summary = PresetUsageSummary::default();
        summary.total_presets = self.presets.len();

        let mut most_used: Option<(&str, u64)> = None;
        let mut least_used: Option<(&str, u64)> = None;

        for preset in &self.presets {
            if preset.enabled {
                summary.enabled_presets += 1;
            } else {
                summary.disabled_presets += 1;
            }

            summary.total_usage_count += preset.use_count;

            if preset.use_count == 0 {
                summary.unused_presets_count += 1;
            }

            match most_used {
                None => most_used = Some((&preset.id, preset.use_count)),
                Some((_, count)) if preset.use_count > count => {
                    most_used = Some((&preset.id, preset.use_count));
                }
                _ => {}
            }

            match least_used {
                None if preset.use_count > 0 => {
                    least_used = Some((&preset.id, preset.use_count))
                }
                Some((_, count)) if preset.use_count > 0 && preset.use_count < count => {
                    least_used = Some((&preset.id, preset.use_count));
                }
                _ => {}
            }

            if let Some(ref cat) = preset.category {
                *summary.categories.entry(cat.clone()).or_insert(0) += 1;
            }
        }

        summary.most_used_preset = most_used.map(|(id, count)| (id.to_string(), count));
        summary.least_used_preset = least_used.map(|(id, count)| (id.to_string(), count));

        summary
    }

    /// Get presets sorted by usage count (most used first)
    pub fn sorted_by_usage(&self) -> Vec<&DownloadPreset> {
        let mut sorted: Vec<&DownloadPreset> = self.presets.iter().collect();
        sorted.sort_by(|a, b| b.use_count.cmp(&a.use_count));
        sorted
    }

    /// Get presets sorted by last used time (most recent first)
    pub fn sorted_by_last_used(&self) -> Vec<&DownloadPreset> {
        let mut sorted: Vec<&DownloadPreset> = self.presets.iter().collect();
        sorted.sort_by(|a, b| b.last_used_at.cmp(&a.last_used_at));
        sorted
    }
}

/// Update structure for modifying preset fields
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetUpdate {
    pub name: Option<String>,
    pub tags: Option<Vec<String>>,
    pub group: Option<String>,
    pub priority: Option<u8>,
    pub speed_limit_bps: Option<u64>,
    pub bandwidth_weight: Option<u8>,
    pub save_path: Option<String>,
    pub max_retries: Option<u32>,
    pub description: Option<String>,
    pub category: Option<String>,
    pub enabled: Option<bool>,
}

/// Save presets to disk (atomic write)
pub fn save_presets(
    presets: &[DownloadPreset],
    data_dir: &Path,
) -> Result<(), PresetPersistenceError> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join("download_presets.json");
    let json = serde_json::to_string_pretty(presets)?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load presets from disk
pub fn load_presets(data_dir: &Path) -> Result<Vec<DownloadPreset>, PresetPersistenceError> {
    let path = data_dir.join("download_presets.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(&path)?;
    if data.trim().is_empty() {
        return Ok(Vec::new());
    }
    let presets: Vec<DownloadPreset> = serde_json::from_str(&data)?;
    Ok(presets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_preset_new_defaults() {
        let p = DownloadPreset::new("test".to_string(), "Test Preset".to_string());
        assert_eq!(p.id, "test");
        assert_eq!(p.name, "Test Preset");
        assert!(p.tags.is_empty());
        assert!(p.group.is_none());
        assert_eq!(p.priority, 2);
        assert!(p.speed_limit_bps.is_none());
        assert_eq!(p.bandwidth_weight, 1);
        assert!(p.save_path.is_none());
        assert!(p.max_retries.is_none());
        assert!(p.enabled);
        assert!(p.description.is_none());
    }

    #[test]
    fn test_preset_serialization_roundtrip() {
        let mut p = DownloadPreset::new("large".to_string(), "Large Files".to_string());
        p.tags = vec!["large".to_string(), "important".to_string()];
        p.group = Some("media".to_string());
        p.priority = 3;
        p.speed_limit_bps = Some(1_048_576);
        p.bandwidth_weight = 5;
        p.save_path = Some(PathBuf::from("/data/large"));
        p.max_retries = Some(10);
        p.description = Some("For large file downloads".to_string());

        let json = serde_json::to_string(&p).unwrap();
        let restored: DownloadPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(p, restored);
    }

    #[test]
    fn test_preset_deserialize_backward_compat() {
        // Old format without new fields should use defaults
        let json = r#"{"id":"old","name":"Old Preset"}"#;
        let p: DownloadPreset = serde_json::from_str(json).unwrap();
        assert_eq!(p.id, "old");
        assert_eq!(p.name, "Old Preset");
        assert!(p.tags.is_empty());
        assert!(p.group.is_none());
        assert_eq!(p.priority, 2);
        assert!(p.speed_limit_bps.is_none());
        assert_eq!(p.bandwidth_weight, 1);
        assert!(p.save_path.is_none());
        assert!(p.max_retries.is_none());
        assert!(p.enabled);
    }

    #[test]
    fn test_save_and_load_presets() {
        let dir = tempdir().unwrap();
        let presets = vec![
            DownloadPreset::new("fast".to_string(), "Fast Downloads".to_string()),
            DownloadPreset::new("slow".to_string(), "Slow Downloads".to_string()),
        ];

        save_presets(&presets, dir.path()).unwrap();
        let loaded = load_presets(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "fast");
        assert_eq!(loaded[1].id, "slow");
    }

    #[test]
    fn test_load_presets_missing_file() {
        let dir = tempdir().unwrap();
        let loaded = load_presets(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_presets_empty_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("download_presets.json"), "").unwrap();
        let loaded = load_presets(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_parse_priority() {
        assert_eq!(parse_priority("low"), Some(1));
        assert_eq!(parse_priority("normal"), Some(2));
        assert_eq!(parse_priority("high"), Some(3));
        assert_eq!(parse_priority("urgent"), Some(4));
        assert_eq!(parse_priority("1"), Some(1));
        assert_eq!(parse_priority("2"), Some(2));
        assert_eq!(parse_priority("3"), Some(3));
        assert_eq!(parse_priority("4"), Some(4));
        assert_eq!(parse_priority("5"), None);
        assert_eq!(parse_priority("invalid"), None);
    }

    #[test]
    fn test_preset_display() {
        let mut p = DownloadPreset::new("media".to_string(), "Media Downloads".to_string());
        p.tags = vec!["video".to_string(), "audio".to_string()];
        p.group = Some("entertainment".to_string());
        p.priority = 3;
        p.speed_limit_bps = Some(2_097_152);
        p.description = Some("For media files".to_string());

        let display = p.display();
        assert!(display.contains("Media Downloads"));
        assert!(display.contains("video, audio"));
        assert!(display.contains("entertainment"));
        assert!(display.contains("High"));
        assert!(display.contains("2.0 MB/s"));
        assert!(display.contains("For media files"));
    }

    #[test]
    fn test_preset_display_disabled() {
        let mut p = DownloadPreset::new("old".to_string(), "Old Preset".to_string());
        p.enabled = false;

        let display = p.display();
        assert!(display.contains("Disabled"));
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500), "500 B/s");
        assert_eq!(format_speed(1024), "1.0 KB/s");
        assert_eq!(format_speed(1536), "1.5 KB/s");
        assert_eq!(format_speed(1_048_576), "1.0 MB/s");
        assert_eq!(format_speed(10_485_760), "10.0 MB/s");
    }

    #[test]
    fn test_save_presets_atomic_write() {
        let dir = tempdir().unwrap();
        let presets = vec![DownloadPreset::new("test".to_string(), "Test".to_string())];

        // First save
        save_presets(&presets, dir.path()).unwrap();

        // Second save should overwrite cleanly
        let presets2 = vec![
            DownloadPreset::new("a".to_string(), "A".to_string()),
            DownloadPreset::new("b".to_string(), "B".to_string()),
        ];
        save_presets(&presets2, dir.path()).unwrap();

        let loaded = load_presets(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "a");
    }

    #[test]
    fn test_preset_with_all_fields() {
        let mut p = DownloadPreset::new("full".to_string(), "Full Preset".to_string());
        p.tags = vec!["tag1".to_string(), "tag2".to_string()];
        p.group = Some("group1".to_string());
        p.priority = 4;
        p.speed_limit_bps = Some(5_242_880);
        p.bandwidth_weight = 8;
        p.save_path = Some(PathBuf::from("/downloads/full"));
        p.max_retries = Some(15);
        p.enabled = true;
        p.description = Some("A preset with all fields set".to_string());

        let json = serde_json::to_string_pretty(&p).unwrap();
        let restored: DownloadPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(p, restored);

        let display = p.display();
        assert!(display.contains("Urgent"));
        assert!(display.contains("5.0 MB/s"));
        assert!(display.contains("Bandwidth Weight: 8"));
        assert!(display.contains("Max Retries: 15"));
    }

    #[test]
    fn test_preset_record_usage() {
        let mut p = DownloadPreset::new("test".to_string(), "Test".to_string());
        assert_eq!(p.use_count, 0);
        assert!(p.last_used_at.is_none());

        p.record_usage();
        assert_eq!(p.use_count, 1);
        assert!(p.last_used_at.is_some());

        p.record_usage();
        assert_eq!(p.use_count, 2);
    }

    #[test]
    fn test_preset_category_field() {
        let mut p = DownloadPreset::new("test".to_string(), "Test".to_string());
        assert!(p.category.is_none());

        p.category = Some("media".to_string());
        let json = serde_json::to_string(&p).unwrap();
        let restored: DownloadPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.category.as_deref(), Some("media"));
    }

    #[test]
    fn test_preset_backward_compat_new_fields() {
        let json = r#"{"id":"old","name":"Old Preset","tags":[],"priority":2,"bandwidth_weight":1,"enabled":true}"#;
        let p: DownloadPreset = serde_json::from_str(json).unwrap();
        assert!(p.category.is_none());
        assert_eq!(p.use_count, 0);
        assert!(p.last_used_at.is_none());
    }

    // --- PresetManager tests ---

    #[test]
    fn test_preset_manager_new() {
        let mgr = PresetManager::new();
        assert!(mgr.presets.is_empty());
    }

    #[test]
    fn test_preset_manager_add_and_get() {
        let mut mgr = PresetManager::new();
        let p = DownloadPreset::new("fast".to_string(), "Fast".to_string());
        mgr.add(p.clone());

        assert_eq!(mgr.get("fast").unwrap().id, "fast");
        assert_eq!(mgr.list().len(), 1);
    }

    #[test]
    fn test_preset_manager_add_replaces_existing() {
        let mut mgr = PresetManager::new();
        let mut p1 = DownloadPreset::new("fast".to_string(), "Fast v1".to_string());
        p1.priority = 1;
        mgr.add(p1);

        let mut p2 = DownloadPreset::new("fast".to_string(), "Fast v2".to_string());
        p2.priority = 3;
        mgr.add(p2);

        assert_eq!(mgr.list().len(), 1);
        assert_eq!(mgr.get("fast").unwrap().name, "Fast v2");
        assert_eq!(mgr.get("fast").unwrap().priority, 3);
    }

    #[test]
    fn test_preset_manager_remove() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("a".to_string(), "A".to_string()));
        mgr.add(DownloadPreset::new("b".to_string(), "B".to_string()));

        assert!(mgr.remove("a"));
        assert!(!mgr.remove("nonexistent"));
        assert_eq!(mgr.list().len(), 1);
    }

    #[test]
    fn test_preset_manager_enable_disable() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("test".to_string(), "Test".to_string()));

        assert!(mgr.get("test").unwrap().enabled);
        mgr.disable("test");
        assert!(!mgr.get("test").unwrap().enabled);
        mgr.enable("test");
        assert!(mgr.get("test").unwrap().enabled);

        assert!(!mgr.enable("nonexistent"));
        assert!(!mgr.disable("nonexistent"));
    }

    #[test]
    fn test_preset_manager_update() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("test".to_string(), "Test".to_string()));

        let updates = PresetUpdate {
            name: Some("Updated".to_string()),
            priority: Some(4),
            category: Some("work".to_string()),
            speed_limit_bps: Some(1_000_000),
            ..Default::default()
        };
        assert!(mgr.update("test", updates));

        let p = mgr.get("test").unwrap();
        assert_eq!(p.name, "Updated");
        assert_eq!(p.priority, 4);
        assert_eq!(p.category.as_deref(), Some("work"));
        assert_eq!(p.speed_limit_bps, Some(1_000_000));

        assert!(!mgr.update("nonexistent", PresetUpdate::default()));
    }

    #[test]
    fn test_preset_manager_categories() {
        let mut mgr = PresetManager::new();

        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.category = Some("media".to_string());
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.category = Some("work".to_string());
        let mut p3 = DownloadPreset::new("c".to_string(), "C".to_string());
        p3.category = Some("media".to_string());
        let p4 = DownloadPreset::new("d".to_string(), "D".to_string());

        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);
        mgr.add(p4);

        let cats = mgr.categories();
        assert_eq!(cats.len(), 2);
        assert!(cats.contains(&"media".to_string()));
        assert!(cats.contains(&"work".to_string()));

        let media = mgr.list_by_category("media");
        assert_eq!(media.len(), 2);

        let work = mgr.list_by_category("work");
        assert_eq!(work.len(), 1);

        let none = mgr.list_by_category("nonexistent");
        assert!(none.is_empty());
    }

    #[test]
    fn test_preset_manager_record_usage() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("test".to_string(), "Test".to_string()));

        assert!(mgr.record_usage("test"));
        assert_eq!(mgr.get("test").unwrap().use_count, 1);

        assert!(mgr.record_usage("test"));
        assert_eq!(mgr.get("test").unwrap().use_count, 2);

        assert!(!mgr.record_usage("nonexistent"));
    }

    #[test]
    fn test_preset_manager_usage_summary() {
        let mut mgr = PresetManager::new();

        let mut p1 = DownloadPreset::new("popular".to_string(), "Popular".to_string());
        p1.category = Some("media".to_string());
        p1.use_count = 10;
        let mut p2 = DownloadPreset::new("rare".to_string(), "Rare".to_string());
        p2.category = Some("work".to_string());
        p2.use_count = 1;
        let p3 = DownloadPreset::new("unused".to_string(), "Unused".to_string());
        let mut p4 = DownloadPreset::new("disabled".to_string(), "Disabled".to_string());
        p4.enabled = false;

        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);
        mgr.add(p4);

        let summary = mgr.usage_summary();
        assert_eq!(summary.total_presets, 4);
        assert_eq!(summary.enabled_presets, 3);
        assert_eq!(summary.disabled_presets, 1);
        assert_eq!(summary.total_usage_count, 11);
        assert_eq!(summary.most_used_preset, Some(("popular".to_string(), 10)));
        assert_eq!(summary.least_used_preset, Some(("rare".to_string(), 1)));
        assert_eq!(summary.unused_presets_count, 1);
        assert_eq!(*summary.categories.get("media").unwrap(), 1);
        assert_eq!(*summary.categories.get("work").unwrap(), 1);
    }

    #[test]
    fn test_preset_manager_usage_summary_empty() {
        let mgr = PresetManager::new();
        let summary = mgr.usage_summary();
        assert_eq!(summary.total_presets, 0);
        assert!(summary.most_used_preset.is_none());
        assert!(summary.least_used_preset.is_none());
    }

    #[test]
    fn test_preset_manager_sorted_by_usage() {
        let mut mgr = PresetManager::new();

        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.use_count = 5;
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.use_count = 10;
        let p3 = DownloadPreset::new("c".to_string(), "C".to_string());

        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);

        let sorted = mgr.sorted_by_usage();
        assert_eq!(sorted[0].id, "b");
        assert_eq!(sorted[1].id, "a");
        assert_eq!(sorted[2].id, "c");
    }

    #[test]
    fn test_preset_manager_sorted_by_last_used() {
        let mut mgr = PresetManager::new();

        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.last_used_at = Some(100);
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.last_used_at = Some(200);
        let p3 = DownloadPreset::new("c".to_string(), "C".to_string());

        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);

        let sorted = mgr.sorted_by_last_used();
        assert_eq!(sorted[0].id, "b");
        assert_eq!(sorted[1].id, "a");
        assert_eq!(sorted[2].id, "c"); // None sorts last
    }

    #[test]
    fn test_preset_manager_from_presets() {
        let presets = vec![
            DownloadPreset::new("a".to_string(), "A".to_string()),
            DownloadPreset::new("b".to_string(), "B".to_string()),
        ];
        let mgr = PresetManager::from_presets(presets);
        assert_eq!(mgr.list().len(), 2);
    }

    #[test]
    fn test_preset_manager_get_mut() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("test".to_string(), "Test".to_string()));

        if let Some(p) = mgr.get_mut("test") {
            p.name = "Modified".to_string();
        }

        assert_eq!(mgr.get("test").unwrap().name, "Modified");
    }

    #[test]
    fn test_preset_new_defaults_include_new_fields() {
        let p = DownloadPreset::new("t".to_string(), "T".to_string());
        assert!(p.category.is_none());
        assert_eq!(p.use_count, 0);
        assert!(p.last_used_at.is_none());
    }

    #[test]
    fn test_preset_record_usage() {
        let mut p = DownloadPreset::new("t".to_string(), "T".to_string());
        assert_eq!(p.use_count, 0);
        assert!(p.last_used_at.is_none());
        p.record_usage();
        assert_eq!(p.use_count, 1);
        assert!(p.last_used_at.is_some());
        let ts = p.last_used_at.unwrap();
        p.record_usage();
        assert_eq!(p.use_count, 2);
        assert!(p.last_used_at.unwrap() >= ts);
    }

    #[test]
    fn test_preset_manager_update() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("a".to_string(), "A".to_string()));
        let upd = PresetUpdate {
            name: Some("Updated".to_string()),
            category: Some("media".to_string()),
            priority: Some(3),
            ..Default::default()
        };
        assert!(mgr.update("a", upd));
        let p = mgr.get("a").unwrap();
        assert_eq!(p.name, "Updated");
        assert_eq!(p.category.as_deref(), Some("media"));
        assert_eq!(p.priority, 3);
    }

    #[test]
    fn test_preset_manager_update_not_found() {
        let mut mgr = PresetManager::new();
        let upd = PresetUpdate {
            name: Some("X".to_string()),
            ..Default::default()
        };
        assert!(!mgr.update("nonexistent", upd));
    }

    #[test]
    fn test_preset_manager_enable_disable() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("a".to_string(), "A".to_string()));
        assert!(mgr.get("a").unwrap().enabled);
        assert!(mgr.disable("a"));
        assert!(!mgr.get("a").unwrap().enabled);
        assert!(mgr.enable("a"));
        assert!(mgr.get("a").unwrap().enabled);
        assert!(!mgr.enable("nonexistent"));
        assert!(!mgr.disable("nonexistent"));
    }

    #[test]
    fn test_preset_manager_categories() {
        let mut mgr = PresetManager::new();
        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.category = Some("media".to_string());
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.category = Some("docs".to_string());
        let mut p3 = DownloadPreset::new("c".to_string(), "C".to_string());
        p3.category = Some("media".to_string());
        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);
        let mut cats = mgr.categories();
        cats.sort();
        assert_eq!(cats, vec!["docs", "media"]);
    }

    #[test]
    fn test_preset_manager_list_by_category() {
        let mut mgr = PresetManager::new();
        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.category = Some("media".to_string());
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.category = Some("docs".to_string());
        let p3 = DownloadPreset::new("c".to_string(), "C".to_string()); // no category
        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);
        let media = mgr.list_by_category("media");
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].id, "a");
        let none = mgr.list_by_category("nonexistent");
        assert!(none.is_empty());
    }

    #[test]
    fn test_preset_usage_summary() {
        let mut mgr = PresetManager::new();
        let mut p1 = DownloadPreset::new("a".to_string(), "A".to_string());
        p1.use_count = 5;
        p1.category = Some("media".to_string());
        let mut p2 = DownloadPreset::new("b".to_string(), "B".to_string());
        p2.use_count = 10;
        p2.enabled = false;
        p2.category = Some("media".to_string());
        let p3 = DownloadPreset::new("c".to_string(), "C".to_string()); // use_count=0
        mgr.add(p1);
        mgr.add(p2);
        mgr.add(p3);
        let s = mgr.usage_summary();
        assert_eq!(s.total_presets, 3);
        assert_eq!(s.enabled_presets, 2);
        assert_eq!(s.disabled_presets, 1);
        assert_eq!(s.total_usage_count, 15);
        assert_eq!(s.most_used_preset, Some(("b".to_string(), 10)));
        assert_eq!(s.least_used_preset, Some(("a".to_string(), 5)));
        assert_eq!(s.unused_presets_count, 1);
        assert_eq!(s.categories.get("media"), Some(&2));
    }

    #[test]
    fn test_preset_usage_summary_empty() {
        let mgr = PresetManager::new();
        let s = mgr.usage_summary();
        assert_eq!(s.total_presets, 0);
        assert!(s.most_used_preset.is_none());
        assert!(s.least_used_preset.is_none());
    }

    #[test]
    fn test_preset_manager_record_usage() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("a".to_string(), "A".to_string()));
        assert!(mgr.record_usage("a"));
        assert_eq!(mgr.get("a").unwrap().use_count, 1);
        assert!(!mgr.record_usage("nonexistent"));
    }

    #[test]
    fn test_preset_deserialize_with_new_fields() {
        let json = r#"{"id":"x","name":"X","category":"work","use_count":42,"last_used_at":1700000000}"#;
        let p: DownloadPreset = serde_json::from_str(json).unwrap();
        assert_eq!(p.category.as_deref(), Some("work"));
        assert_eq!(p.use_count, 42);
        assert_eq!(p.last_used_at, Some(1700000000));
    }

    #[test]
    fn test_preset_serialization_with_new_fields() {
        let mut p = DownloadPreset::new("x".to_string(), "X".to_string());
        p.category = Some("media".to_string());
        p.use_count = 10;
        p.last_used_at = Some(1234567890);
        let json = serde_json::to_string(&p).unwrap();
        let restored: DownloadPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(p, restored);
    }

    #[test]
    fn test_preset_update_all_fields() {
        let mut mgr = PresetManager::new();
        mgr.add(DownloadPreset::new("a".to_string(), "A".to_string()));
        let upd = PresetUpdate {
            name: Some("NewName".to_string()),
            tags: Some(vec!["t1".to_string()]),
            group: Some("g".to_string()),
            priority: Some(4),
            speed_limit_bps: Some(1000),
            bandwidth_weight: Some(5),
            save_path: Some("/tmp".to_string()),
            max_retries: Some(3),
            description: Some("desc".to_string()),
            category: Some("cat".to_string()),
            enabled: Some(false),
        };
        assert!(mgr.update("a", upd));
        let p = mgr.get("a").unwrap();
        assert_eq!(p.name, "NewName");
        assert_eq!(p.tags, vec!["t1"]);
        assert_eq!(p.group, Some("g".to_string()));
        assert_eq!(p.priority, 4);
        assert_eq!(p.speed_limit_bps, Some(1000));
        assert_eq!(p.bandwidth_weight, 5);
        assert_eq!(p.save_path, Some(PathBuf::from("/tmp")));
        assert_eq!(p.max_retries, Some(3));
        assert_eq!(p.description, Some("desc".to_string()));
        assert_eq!(p.category.as_deref(), Some("cat"));
        assert!(!p.enabled);
    }
}
