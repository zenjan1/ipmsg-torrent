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
        }
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
}
