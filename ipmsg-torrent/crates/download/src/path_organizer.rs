//! Download path organizer for automatic file organization based on extension
//!
//! Automatically moves completed downloads into categorized subdirectories
//! based on file extension (e.g., videos/, music/, images/, documents/).
//!
//! Features:
//! - Built-in category mappings for common file types
//! - Custom extension-to-category rules
//! - Configurable base directory for organized files
//! - Enable/disable per-task or globally
//! - Persistence to `path_organizer_config.json`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;
use tracing::debug;

/// Errors from path organizer operations.
#[derive(Error, Debug)]
pub enum PathOrganizerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Invalid category name: {0}")]
    InvalidCategory(String),
}

/// A category for organizing downloaded files.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileCategory {
    /// Category name (e.g., "videos", "music", "images")
    pub name: String,
    /// File extensions that belong to this category (lowercase, without dot)
    pub extensions: Vec<String>,
    /// Subdirectory name for this category
    pub directory: String,
}

impl FileCategory {
    /// Create a new file category.
    pub fn new(name: String, extensions: Vec<String>, directory: String) -> Self {
        Self {
            name,
            extensions,
            directory,
        }
    }

    /// Check if a file extension belongs to this category.
    pub fn matches_extension(&self, ext: &str) -> bool {
        let ext_lower = ext.to_lowercase();
        self.extensions
            .iter()
            .any(|e| e.to_lowercase() == ext_lower)
    }
}

/// Configuration for the path organizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathOrganizerConfig {
    /// Whether the organizer is enabled globally
    pub enabled: bool,
    /// Base directory for organized files (None = use download's save_path)
    pub base_directory: Option<PathBuf>,
    /// Category mappings (extension -> category name)
    pub categories: Vec<FileCategory>,
    /// Whether to create the category directory if it doesn't exist
    pub create_directories: bool,
    /// Whether to skip organizing if the file is already in a category directory
    pub skip_if_organized: bool,
}

impl Default for PathOrganizerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_directory: None,
            categories: default_categories(),
            create_directories: true,
            skip_if_organized: true,
        }
    }
}

/// Get the default file categories.
fn default_categories() -> Vec<FileCategory> {
    vec![
        FileCategory::new(
            "videos".to_string(),
            vec![
                "mp4".into(),
                "mkv".into(),
                "avi".into(),
                "mov".into(),
                "wmv".into(),
                "flv".into(),
                "webm".into(),
                "m4v".into(),
                "mpg".into(),
                "mpeg".into(),
                "3gp".into(),
                "ts".into(),
            ],
            "videos".to_string(),
        ),
        FileCategory::new(
            "music".to_string(),
            vec![
                "mp3".into(),
                "flac".into(),
                "wav".into(),
                "aac".into(),
                "ogg".into(),
                "wma".into(),
                "m4a".into(),
                "opus".into(),
                "aiff".into(),
            ],
            "music".to_string(),
        ),
        FileCategory::new(
            "images".to_string(),
            vec![
                "jpg".into(),
                "jpeg".into(),
                "png".into(),
                "gif".into(),
                "bmp".into(),
                "webp".into(),
                "svg".into(),
                "tiff".into(),
                "ico".into(),
                "raw".into(),
            ],
            "images".to_string(),
        ),
        FileCategory::new(
            "documents".to_string(),
            vec![
                "pdf".into(),
                "doc".into(),
                "docx".into(),
                "xls".into(),
                "xlsx".into(),
                "ppt".into(),
                "pptx".into(),
                "txt".into(),
                "rtf".into(),
                "odt".into(),
                "ods".into(),
                "odp".into(),
                "epub".into(),
                "mobi".into(),
            ],
            "documents".to_string(),
        ),
        FileCategory::new(
            "archives".to_string(),
            vec![
                "zip".into(),
                "rar".into(),
                "7z".into(),
                "tar".into(),
                "gz".into(),
                "bz2".into(),
                "xz".into(),
                "tgz".into(),
                "lz".into(),
                "zst".into(),
            ],
            "archives".to_string(),
        ),
        FileCategory::new(
            "programs".to_string(),
            vec![
                "exe".into(),
                "msi".into(),
                "dmg".into(),
                "deb".into(),
                "rpm".into(),
                "apk".into(),
                "appimage".into(),
                "flatpak".into(),
            ],
            "programs".to_string(),
        ),
    ]
}

/// Result of organizing a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizeResult {
    /// Original file path
    pub original_path: PathBuf,
    /// New file path after organizing
    pub new_path: PathBuf,
    /// Category name the file was organized into
    pub category: String,
    /// Whether the file was actually moved
    pub moved: bool,
}

/// Summary of path organizer statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PathOrganizerSummary {
    /// Total number of files organized
    pub total_organized: u64,
    /// Number of files organized per category
    pub by_category: HashMap<String, u64>,
    /// Total bytes moved
    pub total_bytes_moved: u64,
}

/// Manager for path organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathOrganizerManager {
    /// Configuration
    pub config: PathOrganizerConfig,
    /// Statistics
    #[serde(default)]
    pub summary: PathOrganizerSummary,
}

impl Default for PathOrganizerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PathOrganizerManager {
    /// Create a new path organizer manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: PathOrganizerConfig::default(),
            summary: PathOrganizerSummary::default(),
        }
    }

    /// Set whether the organizer is enabled.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Check if the organizer is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Set the base directory for organized files.
    pub fn set_base_directory(&mut self, dir: Option<PathBuf>) {
        self.config.base_directory = dir;
    }

    /// Get the base directory.
    pub fn get_base_directory(&self) -> Option<&PathBuf> {
        self.config.base_directory.as_ref()
    }

    /// Add a custom category.
    pub fn add_category(&mut self, category: FileCategory) {
        // Remove existing category with same name if present
        self.config.categories.retain(|c| c.name != category.name);
        self.config.categories.push(category);
    }

    /// Remove a category by name.
    pub fn remove_category(&mut self, name: &str) -> bool {
        let len_before = self.config.categories.len();
        self.config.categories.retain(|c| c.name != name);
        self.config.categories.len() < len_before
    }

    /// Get a category by name.
    pub fn get_category(&self, name: &str) -> Option<&FileCategory> {
        self.config.categories.iter().find(|c| c.name == name)
    }

    /// List all categories.
    pub fn list_categories(&self) -> &[FileCategory] {
        &self.config.categories
    }

    /// Find the category for a given file extension.
    pub fn find_category_for_extension(&self, ext: &str) -> Option<&FileCategory> {
        self.config
            .categories
            .iter()
            .find(|cat| cat.matches_extension(ext))
    }

    /// Determine the target directory for a file based on its extension.
    pub fn get_target_directory(&self, file_path: &Path, save_path: &Path) -> Option<PathBuf> {
        let ext = file_path.extension()?.to_str()?;
        let category = self.find_category_for_extension(ext)?;

        let base = self.config.base_directory.as_deref().unwrap_or(save_path);

        Some(base.join(&category.directory))
    }

    /// Organize a single file by moving it to the appropriate category directory.
    pub async fn organize_file(
        &mut self,
        file_path: &Path,
        save_path: &Path,
    ) -> Result<Option<OrganizeResult>, PathOrganizerError> {
        // Check if organizer is enabled
        if !self.config.enabled {
            return Ok(None);
        }

        // Check if file exists
        if !file_path.exists() {
            return Err(PathOrganizerError::FileNotFound(
                file_path.display().to_string(),
            ));
        }

        // Get file extension
        let ext = match file_path.extension().and_then(|e| e.to_str()) {
            Some(e) => e,
            None => return Ok(None), // No extension, skip
        };

        // Find matching category
        let category = match self.find_category_for_extension(ext) {
            Some(c) => c.clone(),
            None => return Ok(None), // No matching category, skip
        };

        // Determine target directory
        let base = self.config.base_directory.as_deref().unwrap_or(save_path);

        let target_dir = base.join(&category.directory);

        // Check if file is already in the target directory
        if self.config.skip_if_organized && file_path.parent() == Some(target_dir.as_path()) {
            return Ok(None); // Already organized
        }

        // Create target directory if needed
        if self.config.create_directories && !target_dir.exists() {
            fs::create_dir_all(&target_dir).await?;
        }

        // Move file
        let file_name = file_path.file_name().unwrap();
        let target_path = target_dir.join(file_name);

        // Handle name collision
        let final_path = if target_path.exists() {
            let stem = file_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("file");
            let ext_str = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");

            let mut counter = 1;
            loop {
                let new_name = if ext_str.is_empty() {
                    format!("{} ({})", stem, counter)
                } else {
                    format!("{} ({}).{}", stem, counter, ext_str)
                };
                let candidate = target_dir.join(new_name);
                if !candidate.exists() {
                    break candidate;
                }
                counter += 1;
            }
        } else {
            target_path
        };

        fs::rename(file_path, &final_path).await?;

        // Update statistics
        let file_size = fs::metadata(&final_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        self.summary.total_organized += 1;
        *self
            .summary
            .by_category
            .entry(category.name.clone())
            .or_insert(0) += 1;
        self.summary.total_bytes_moved += file_size;

        debug!(
            "Organized file {:?} to {:?} (category: {})",
            file_path, final_path, category.name
        );

        Ok(Some(OrganizeResult {
            original_path: file_path.to_path_buf(),
            new_path: final_path,
            category: category.name,
            moved: true,
        }))
    }

    /// Get the organizer summary.
    pub fn get_summary(&self) -> &PathOrganizerSummary {
        &self.summary
    }

    /// Reset the organizer statistics.
    pub fn reset_summary(&mut self) {
        self.summary = PathOrganizerSummary::default();
    }

    /// Reset to default categories.
    pub fn reset_categories(&mut self) {
        self.config.categories = default_categories();
    }
}

/// Save path organizer configuration to disk (atomic write).
pub async fn save_path_organizer_config(
    manager: &PathOrganizerManager,
    path: &Path,
) -> Result<(), PathOrganizerError> {
    let json = serde_json::to_string_pretty(manager)?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, &json).await?;
    fs::rename(&temp_path, path).await?;
    debug!("Saved path organizer config to {:?}", path);
    Ok(())
}

/// Load path organizer configuration from disk.
pub async fn load_path_organizer_config(
    path: &Path,
) -> Result<PathOrganizerManager, PathOrganizerError> {
    if !path.exists() {
        debug!("Path organizer config not found, using defaults");
        return Ok(PathOrganizerManager::new());
    }

    let json = fs::read_to_string(path).await?;
    let manager: PathOrganizerManager = serde_json::from_str(&json)?;
    debug!("Loaded path organizer config from {:?}", path);
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_categories() {
        let categories = default_categories();
        assert!(!categories.is_empty());

        // Check that videos category exists
        let videos = categories.iter().find(|c| c.name == "videos");
        assert!(videos.is_some());
        let videos = videos.unwrap();
        assert!(videos.extensions.contains(&"mp4".to_string()));
        assert!(videos.extensions.contains(&"mkv".to_string()));
    }

    #[test]
    fn test_category_matches_extension() {
        let category = FileCategory::new(
            "videos".to_string(),
            vec!["mp4".into(), "mkv".into()],
            "videos".to_string(),
        );

        assert!(category.matches_extension("mp4"));
        assert!(category.matches_extension("MP4"));
        assert!(category.matches_extension("mkv"));
        assert!(!category.matches_extension("avi"));
    }

    #[test]
    fn test_organizer_manager_new() {
        let manager = PathOrganizerManager::new();
        assert!(!manager.is_enabled());
        assert!(!manager.config.categories.is_empty());
    }

    #[test]
    fn test_organizer_enable_disable() {
        let mut manager = PathOrganizerManager::new();
        assert!(!manager.is_enabled());

        manager.set_enabled(true);
        assert!(manager.is_enabled());

        manager.set_enabled(false);
        assert!(!manager.is_enabled());
    }

    #[test]
    fn test_add_remove_category() {
        let mut manager = PathOrganizerManager::new();

        let custom = FileCategory::new(
            "custom".to_string(),
            vec!["xyz".into()],
            "custom_files".to_string(),
        );

        manager.add_category(custom.clone());
        assert!(manager.get_category("custom").is_some());

        let found = manager.find_category_for_extension("xyz");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "custom");

        assert!(manager.remove_category("custom"));
        assert!(manager.get_category("custom").is_none());
    }

    #[test]
    fn test_find_category_for_extension() {
        let manager = PathOrganizerManager::new();

        let mp4_cat = manager.find_category_for_extension("mp4");
        assert!(mp4_cat.is_some());
        assert_eq!(mp4_cat.unwrap().name, "videos");

        let mp3_cat = manager.find_category_for_extension("mp3");
        assert!(mp3_cat.is_some());
        assert_eq!(mp3_cat.unwrap().name, "music");

        let jpg_cat = manager.find_category_for_extension("jpg");
        assert!(jpg_cat.is_some());
        assert_eq!(jpg_cat.unwrap().name, "images");

        let pdf_cat = manager.find_category_for_extension("pdf");
        assert!(pdf_cat.is_some());
        assert_eq!(pdf_cat.unwrap().name, "documents");

        let unknown = manager.find_category_for_extension("unknown_ext");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_get_target_directory() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);

        let save_path = PathBuf::from("/downloads");
        let file_path = PathBuf::from("/downloads/video.mp4");

        let target = manager.get_target_directory(&file_path, &save_path);
        assert!(target.is_some());
        assert_eq!(target.unwrap(), PathBuf::from("/downloads/videos"));
    }

    #[test]
    fn test_get_target_directory_with_base() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);
        manager.set_base_directory(Some(PathBuf::from("/organized")));

        let save_path = PathBuf::from("/downloads");
        let file_path = PathBuf::from("/downloads/video.mp4");

        let target = manager.get_target_directory(&file_path, &save_path);
        assert!(target.is_some());
        assert_eq!(target.unwrap(), PathBuf::from("/organized/videos"));
    }

    #[tokio::test]
    async fn test_organize_file_disabled() {
        let mut manager = PathOrganizerManager::new();
        // Not enabled by default

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("test.mp4");
        fs::write(&file_path, b"test content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();
        assert!(result.is_none()); // Should skip when disabled
    }

    #[tokio::test]
    async fn test_organize_file_no_extension() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("README");
        fs::write(&file_path, b"test content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();
        assert!(result.is_none()); // Should skip files without extension
    }

    #[tokio::test]
    async fn test_organize_file_unknown_extension() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("file.unknownext");
        fs::write(&file_path, b"test content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();
        assert!(result.is_none()); // Should skip unknown extensions
    }

    #[tokio::test]
    async fn test_organize_file_success() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("video.mp4");
        fs::write(&file_path, b"test video content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();

        assert!(result.is_some());
        let result = result.unwrap();
        assert!(result.moved);
        assert_eq!(result.category, "videos");
        assert_eq!(
            result.new_path,
            temp.path().join("videos").join("video.mp4")
        );

        // Check file was actually moved
        assert!(!file_path.exists());
        assert!(result.new_path.exists());

        // Check statistics
        assert_eq!(manager.summary.total_organized, 1);
        assert_eq!(*manager.summary.by_category.get("videos").unwrap(), 1);
    }

    #[tokio::test]
    async fn test_organize_file_already_organized() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);
        manager.config.skip_if_organized = true;

        let temp = TempDir::new().unwrap();
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir).await.unwrap();

        let file_path = videos_dir.join("video.mp4");
        fs::write(&file_path, b"test content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();

        assert!(result.is_none()); // Should skip, already in videos/
    }

    #[tokio::test]
    async fn test_organize_file_name_collision() {
        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir).await.unwrap();

        // Create existing file
        let existing = videos_dir.join("video.mp4");
        fs::write(&existing, b"existing content").await.unwrap();

        // Try to organize new file with same name
        let file_path = temp.path().join("video.mp4");
        fs::write(&file_path, b"new content").await.unwrap();

        let result = manager
            .organize_file(&file_path, temp.path())
            .await
            .unwrap();

        assert!(result.is_some());
        let result = result.unwrap();
        // Should have been renamed to avoid collision
        assert!(result.new_path.to_str().unwrap().contains("(1)"));
        assert!(result.new_path.exists());
        assert!(existing.exists()); // Original should still exist
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("path_organizer_config.json");

        let mut manager = PathOrganizerManager::new();
        manager.set_enabled(true);
        manager.set_base_directory(Some(PathBuf::from("/organized")));

        save_path_organizer_config(&manager, &config_path)
            .await
            .unwrap();

        let loaded = load_path_organizer_config(&config_path).await.unwrap();
        assert!(loaded.is_enabled());
        assert_eq!(
            loaded.get_base_directory(),
            Some(&PathBuf::from("/organized"))
        );
    }

    #[tokio::test]
    async fn test_load_config_not_found() {
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("nonexistent.json");

        let loaded = load_path_organizer_config(&config_path).await.unwrap();
        assert!(!loaded.is_enabled()); // Should return defaults
    }

    #[test]
    fn test_reset_summary() {
        let mut manager = PathOrganizerManager::new();
        manager.summary.total_organized = 100;
        manager.summary.total_bytes_moved = 1000000;
        manager.summary.by_category.insert("videos".to_string(), 50);

        manager.reset_summary();

        assert_eq!(manager.summary.total_organized, 0);
        assert_eq!(manager.summary.total_bytes_moved, 0);
        assert!(manager.summary.by_category.is_empty());
    }

    #[test]
    fn test_reset_categories() {
        let mut manager = PathOrganizerManager::new();
        manager.config.categories.clear();
        assert!(manager.config.categories.is_empty());

        manager.reset_categories();
        assert!(!manager.config.categories.is_empty());
    }

    // ===== Phase 218: Comprehensive Test Coverage =====

    // --- FileCategory serde + traits ---
    #[test]
    fn file_category_serde_roundtrip() {
        let cat = FileCategory::new(
            "test".into(),
            vec!["a".into(), "b".into()],
            "test_dir".into(),
        );
        let json = serde_json::to_string(&cat).unwrap();
        let back: FileCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "test");
        assert_eq!(back.extensions, vec!["a", "b"]);
        assert_eq!(back.directory, "test_dir");
    }

    #[test]
    fn file_category_extra_fields_ignored() {
        let json = r#"{"name":"x","extensions":["z"],"directory":"d","extra":123}"#;
        let cat: FileCategory = serde_json::from_str(json).unwrap();
        assert_eq!(cat.name, "x");
        assert_eq!(cat.extensions, vec!["z"]);
    }

    #[test]
    fn file_category_clone_debug() {
        let cat = FileCategory::new("v".into(), vec!["mp4".into()], "v".into());
        let cloned = cat.clone();
        assert_eq!(cloned.name, cat.name);
        let _ = format!("{:?}", cat);
    }

    #[test]
    fn file_category_eq_hash() {
        let a = FileCategory::new("v".into(), vec!["mp4".into()], "v".into());
        let b = FileCategory::new("v".into(), vec!["mp4".into()], "v".into());
        assert_eq!(a, b);
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    // --- matches_extension edge cases ---
    #[test]
    fn matches_extension_empty() {
        let cat = FileCategory::new("v".into(), vec!["mp4".into()], "v".into());
        assert!(!cat.matches_extension(""));
    }

    #[test]
    fn matches_extension_unicode() {
        let cat = FileCategory::new("v".into(), vec!["视频".into()], "v".into());
        assert!(cat.matches_extension("视频"));
        assert!(!cat.matches_extension("VIDEO"));
    }

    #[test]
    fn matches_extension_mixed_case() {
        let cat = FileCategory::new("v".into(), vec!["Mp4".into()], "v".into());
        assert!(cat.matches_extension("mp4"));
        assert!(cat.matches_extension("MP4"));
        assert!(cat.matches_extension("Mp4"));
    }

    // --- PathOrganizerConfig serde ---
    #[test]
    fn config_serde_roundtrip() {
        let cfg = PathOrganizerConfig {
            enabled: true,
            base_directory: Some(PathBuf::from("/tmp/test")),
            categories: vec![FileCategory::new("x".into(), vec!["y".into()], "z".into())],
            create_directories: false,
            skip_if_organized: false,
        };
        let json = serde_json::to_string_pretty(&cfg).unwrap();
        let back: PathOrganizerConfig = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert_eq!(back.base_directory, Some(PathBuf::from("/tmp/test")));
        assert!(!back.create_directories);
        assert!(!back.skip_if_organized);
        assert_eq!(back.categories.len(), 1);
    }

    #[test]
    fn config_default_values() {
        let cfg = PathOrganizerConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.base_directory.is_none());
        assert!(cfg.create_directories);
        assert!(cfg.skip_if_organized);
        assert_eq!(cfg.categories.len(), 6);
    }

    #[test]
    fn config_extra_fields_ignored() {
        let json = r#"{"enabled":true,"base_directory":null,"categories":[],"create_directories":true,"skip_if_organized":true,"unknown_field":"val"}"#;
        let cfg: PathOrganizerConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.enabled);
    }

    // --- OrganizeResult serde ---
    #[test]
    fn organize_result_serde_roundtrip() {
        let r = OrganizeResult {
            original_path: PathBuf::from("/a/b.mp4"),
            new_path: PathBuf::from("/a/videos/b.mp4"),
            category: "videos".into(),
            moved: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: OrganizeResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.original_path, r.original_path);
        assert_eq!(back.new_path, r.new_path);
        assert_eq!(back.category, "videos");
        assert!(back.moved);
    }

    // --- PathOrganizerSummary serde ---
    #[test]
    fn summary_serde_roundtrip() {
        let mut s = PathOrganizerSummary::default();
        s.total_organized = 42;
        s.total_bytes_moved = 1_000_000;
        s.by_category.insert("videos".into(), 20);
        s.by_category.insert("music".into(), 22);
        let json = serde_json::to_string(&s).unwrap();
        let back: PathOrganizerSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_organized, 42);
        assert_eq!(back.total_bytes_moved, 1_000_000);
        assert_eq!(back.by_category.len(), 2);
    }

    #[test]
    fn summary_default_is_zero() {
        let s = PathOrganizerSummary::default();
        assert_eq!(s.total_organized, 0);
        assert_eq!(s.total_bytes_moved, 0);
        assert!(s.by_category.is_empty());
    }

    // --- PathOrganizerManager Clone/Debug ---
    #[test]
    fn manager_clone_debug() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        let cloned = m.clone();
        assert!(cloned.is_enabled());
        let _ = format!("{:?}", m);
    }

    #[test]
    fn manager_default_equals_new() {
        let a = PathOrganizerManager::new();
        let b = PathOrganizerManager::default();
        assert_eq!(a.is_enabled(), b.is_enabled());
        assert_eq!(a.config.categories.len(), b.config.categories.len());
    }

    // --- Category management edge cases ---
    #[test]
    fn add_category_replaces_existing() {
        let mut m = PathOrganizerManager::new();
        let initial_count = m.config.categories.len();
        let new_videos =
            FileCategory::new("videos".into(), vec!["new_ext".into()], "new_videos".into());
        m.add_category(new_videos);
        assert_eq!(m.config.categories.len(), initial_count);
        let cat = m.get_category("videos").unwrap();
        assert_eq!(cat.directory, "new_videos");
        assert!(cat.extensions.contains(&"new_ext".to_string()));
    }

    #[test]
    fn remove_category_nonexistent() {
        let mut m = PathOrganizerManager::new();
        assert!(!m.remove_category("nonexistent_category"));
    }

    #[test]
    fn list_categories_returns_all() {
        let m = PathOrganizerManager::new();
        let cats = m.list_categories();
        assert_eq!(cats.len(), 6);
        let names: Vec<&str> = cats.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"videos"));
        assert!(names.contains(&"music"));
        assert!(names.contains(&"images"));
        assert!(names.contains(&"documents"));
        assert!(names.contains(&"archives"));
        assert!(names.contains(&"programs"));
    }

    #[test]
    fn find_category_all_default_extensions() {
        let m = PathOrganizerManager::new();
        assert_eq!(m.find_category_for_extension("mkv").unwrap().name, "videos");
        assert_eq!(m.find_category_for_extension("avi").unwrap().name, "videos");
        assert_eq!(m.find_category_for_extension("flv").unwrap().name, "videos");
        assert_eq!(m.find_category_for_extension("flac").unwrap().name, "music");
        assert_eq!(m.find_category_for_extension("ogg").unwrap().name, "music");
        assert_eq!(m.find_category_for_extension("png").unwrap().name, "images");
        assert_eq!(m.find_category_for_extension("gif").unwrap().name, "images");
        assert_eq!(
            m.find_category_for_extension("webp").unwrap().name,
            "images"
        );
        assert_eq!(
            m.find_category_for_extension("docx").unwrap().name,
            "documents"
        );
        assert_eq!(
            m.find_category_for_extension("epub").unwrap().name,
            "documents"
        );
        assert_eq!(
            m.find_category_for_extension("zip").unwrap().name,
            "archives"
        );
        assert_eq!(
            m.find_category_for_extension("7z").unwrap().name,
            "archives"
        );
        assert_eq!(
            m.find_category_for_extension("tar").unwrap().name,
            "archives"
        );
        assert_eq!(
            m.find_category_for_extension("exe").unwrap().name,
            "programs"
        );
        assert_eq!(
            m.find_category_for_extension("deb").unwrap().name,
            "programs"
        );
    }

    // --- get_target_directory edge cases ---
    #[test]
    fn get_target_directory_no_extension() {
        let m = PathOrganizerManager::new();
        let save = PathBuf::from("/dl");
        let file = PathBuf::from("/dl/README");
        assert!(m.get_target_directory(&file, &save).is_none());
    }

    #[test]
    fn get_target_directory_unknown_extension() {
        let m = PathOrganizerManager::new();
        let save = PathBuf::from("/dl");
        let file = PathBuf::from("/dl/file.xyz123");
        assert!(m.get_target_directory(&file, &save).is_none());
    }

    #[test]
    fn get_target_directory_case_insensitive() {
        let m = PathOrganizerManager::new();
        let save = PathBuf::from("/dl");
        let file = PathBuf::from("/dl/video.MP4");
        let target = m.get_target_directory(&file, &save).unwrap();
        assert_eq!(target, PathBuf::from("/dl/videos"));
    }

    // --- organize_file edge cases ---
    #[tokio::test]
    async fn organize_file_not_found() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("nonexistent.mp4");
        let result = m.organize_file(&file, temp.path()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not found") || err.to_string().contains("File not found")
        );
    }

    #[tokio::test]
    async fn organize_file_multiple_files_stats() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        let temp = TempDir::new().unwrap();

        let f1 = temp.path().join("video1.mp4");
        let f2 = temp.path().join("video2.mkv");
        let f3 = temp.path().join("song1.mp3");
        fs::write(&f1, b"vid1").await.unwrap();
        fs::write(&f2, b"vid2").await.unwrap();
        fs::write(&f3, b"song1").await.unwrap();

        m.organize_file(&f1, temp.path()).await.unwrap();
        m.organize_file(&f2, temp.path()).await.unwrap();
        m.organize_file(&f3, temp.path()).await.unwrap();

        assert_eq!(m.summary.total_organized, 3);
        assert_eq!(*m.summary.by_category.get("videos").unwrap(), 2);
        assert_eq!(*m.summary.by_category.get("music").unwrap(), 1);
        assert!(m.summary.total_bytes_moved > 0);
    }

    #[tokio::test]
    async fn organize_file_skip_if_organized_disabled() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        m.config.skip_if_organized = false;

        let temp = TempDir::new().unwrap();
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir).await.unwrap();

        let file_path = videos_dir.join("video.mp4");
        fs::write(&file_path, b"test").await.unwrap();

        let result = m.organize_file(&file_path, temp.path()).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn organize_file_create_directories_false() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        m.config.create_directories = false;

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("video.mp4");
        fs::write(&file_path, b"test").await.unwrap();

        let result = m.organize_file(&file_path, temp.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn organize_file_name_collision_multiple() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir).await.unwrap();

        fs::write(videos_dir.join("video.mp4"), b"orig")
            .await
            .unwrap();
        fs::write(videos_dir.join("video (1).mp4"), b"1")
            .await
            .unwrap();
        fs::write(videos_dir.join("video (2).mp4"), b"2")
            .await
            .unwrap();

        let file_path = temp.path().join("video.mp4");
        fs::write(&file_path, b"new").await.unwrap();

        let result = m
            .organize_file(&file_path, temp.path())
            .await
            .unwrap()
            .unwrap();
        assert!(result.new_path.to_str().unwrap().contains("(3)"));
        assert!(result.new_path.exists());
    }

    #[tokio::test]
    async fn organize_file_unicode_filename() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("日本語ビデオ.mp4");
        fs::write(&file_path, b"unicode").await.unwrap();

        let result = m
            .organize_file(&file_path, temp.path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.category, "videos");
        assert!(result.new_path.exists());
        assert!(result.new_path.to_str().unwrap().contains("日本語ビデオ"));
    }

    #[tokio::test]
    async fn organize_file_emoji_filename() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("🎬movie🎥.mkv");
        fs::write(&file_path, b"emoji").await.unwrap();

        let result = m
            .organize_file(&file_path, temp.path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.category, "videos");
        assert!(result.new_path.exists());
    }

    // --- Persistence edge cases ---
    #[tokio::test]
    async fn save_creates_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        assert!(!path.exists());

        let m = PathOrganizerManager::new();
        save_path_organizer_config(&m, &path).await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn save_overwrites_existing() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");

        let mut m = PathOrganizerManager::new();
        m.set_enabled(false);
        save_path_organizer_config(&m, &path).await.unwrap();

        m.set_enabled(true);
        save_path_organizer_config(&m, &path).await.unwrap();

        let loaded = load_path_organizer_config(&path).await.unwrap();
        assert!(loaded.is_enabled());
    }

    #[tokio::test]
    async fn save_no_tmp_leftover() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        let tmp_path = path.with_extension("json.tmp");

        let m = PathOrganizerManager::new();
        save_path_organizer_config(&m, &path).await.unwrap();
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn load_corrupt_json() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        fs::write(&path, "this is not json").await.unwrap();

        let result = load_path_organizer_config(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_empty_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        fs::write(&path, "").await.unwrap();

        let result = load_path_organizer_config(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_empty_json_object() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        fs::write(&path, "{}").await.unwrap();

        // {} is missing required "config" field, so it should fail
        let result = load_path_organizer_config(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_minimal_valid_json() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");
        let minimal = r#"{"config":{"enabled":false,"base_directory":null,"categories":[],"create_directories":true,"skip_if_organized":true},"summary":{"total_organized":0,"by_category":{},"total_bytes_moved":0}}"#;
        fs::write(&path, minimal).await.unwrap();

        let loaded = load_path_organizer_config(&path).await.unwrap();
        assert!(!loaded.is_enabled());
        assert_eq!(loaded.summary.total_organized, 0);
    }

    #[tokio::test]
    async fn save_load_full_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("config.json");

        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        m.set_base_directory(Some(temp.path().to_path_buf()));
        m.summary.total_organized = 99;
        m.summary.by_category.insert("videos".into(), 50);

        save_path_organizer_config(&m, &path).await.unwrap();
        let loaded = load_path_organizer_config(&path).await.unwrap();

        assert!(loaded.is_enabled());
        assert_eq!(loaded.summary.total_organized, 99);
        assert_eq!(*loaded.summary.by_category.get("videos").unwrap(), 50);
    }

    // --- Error Display ---
    #[test]
    fn error_display_io() {
        let err = PathOrganizerError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let s = format!("{}", err);
        assert!(s.contains("I/O error") || s.contains("gone"));
    }

    #[test]
    fn error_display_json() {
        let err = PathOrganizerError::Json(
            serde_json::from_str::<PathOrganizerConfig>("bad").unwrap_err(),
        );
        let s = format!("{}", err);
        assert!(s.contains("JSON error") || s.contains("json"));
    }

    #[test]
    fn error_display_file_not_found() {
        let err = PathOrganizerError::FileNotFound("/path/to/file".into());
        let s = format!("{}", err);
        assert!(s.contains("not found") || s.contains("File not found"));
    }

    #[test]
    fn error_display_invalid_category() {
        let err = PathOrganizerError::InvalidCategory("bad_cat".into());
        let s = format!("{}", err);
        assert!(s.contains("bad_cat"));
    }

    #[test]
    fn error_debug() {
        let err = PathOrganizerError::FileNotFound("test".into());
        let _ = format!("{:?}", err);
    }

    // --- Complete workflow ---
    #[tokio::test]
    async fn complete_lifecycle() {
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("config.json");

        let mut m = PathOrganizerManager::new();
        assert!(!m.is_enabled());

        m.set_enabled(true);
        m.set_base_directory(Some(temp.path().to_path_buf()));

        m.add_category(FileCategory::new(
            "custom".into(),
            vec!["custom_ext".into()],
            "custom_dir".into(),
        ));
        assert!(m.find_category_for_extension("custom_ext").is_some());

        let f1 = temp.path().join("test.mp4");
        let f2 = temp.path().join("test.custom_ext");
        fs::write(&f1, b"video").await.unwrap();
        fs::write(&f2, b"custom").await.unwrap();

        let r1 = m.organize_file(&f1, temp.path()).await.unwrap().unwrap();
        let r2 = m.organize_file(&f2, temp.path()).await.unwrap().unwrap();

        assert_eq!(r1.category, "videos");
        assert_eq!(r2.category, "custom");
        assert_eq!(m.summary.total_organized, 2);

        save_path_organizer_config(&m, &config_path).await.unwrap();
        let loaded = load_path_organizer_config(&config_path).await.unwrap();
        assert!(loaded.is_enabled());
        assert_eq!(loaded.summary.total_organized, 2);
        assert!(loaded.find_category_for_extension("custom_ext").is_some());

        let mut loaded = loaded;
        loaded.reset_summary();
        assert_eq!(loaded.summary.total_organized, 0);
        loaded.reset_categories();
        assert!(loaded.find_category_for_extension("custom_ext").is_none());
        assert!(loaded.find_category_for_extension("mp4").is_some());
    }

    // --- Default categories content verification ---
    #[test]
    fn default_categories_all_six() {
        let cats = default_categories();
        assert_eq!(cats.len(), 6);
        let names: Vec<&str> = cats.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"videos"));
        assert!(names.contains(&"music"));
        assert!(names.contains(&"images"));
        assert!(names.contains(&"documents"));
        assert!(names.contains(&"archives"));
        assert!(names.contains(&"programs"));
    }

    #[test]
    fn default_categories_directories_match_names() {
        for cat in default_categories() {
            assert_eq!(cat.name, cat.directory);
        }
    }

    #[test]
    fn default_categories_no_empty_extensions() {
        for cat in default_categories() {
            assert!(
                !cat.extensions.is_empty(),
                "category {} has no extensions",
                cat.name
            );
        }
    }

    #[test]
    fn default_categories_extensions_lowercase() {
        for cat in default_categories() {
            for ext in &cat.extensions {
                assert_eq!(
                    ext,
                    &ext.to_lowercase(),
                    "extension {} in {} not lowercase",
                    ext,
                    cat.name
                );
            }
        }
    }

    // --- get_summary ---
    #[test]
    fn get_summary_returns_reference() {
        let mut m = PathOrganizerManager::new();
        m.summary.total_organized = 10;
        let s = m.get_summary();
        assert_eq!(s.total_organized, 10);
    }

    // --- set_base_directory ---
    #[test]
    fn set_base_directory_none() {
        let mut m = PathOrganizerManager::new();
        m.set_base_directory(Some(PathBuf::from("/tmp")));
        assert!(m.get_base_directory().is_some());
        m.set_base_directory(None);
        assert!(m.get_base_directory().is_none());
    }

    #[tokio::test]
    async fn organize_file_collision_preserves_original_content() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir).await.unwrap();

        let existing = videos_dir.join("video.mp4");
        fs::write(&existing, b"original_content").await.unwrap();

        let new_file = temp.path().join("video.mp4");
        fs::write(&new_file, b"new_content").await.unwrap();

        let result = m
            .organize_file(&new_file, temp.path())
            .await
            .unwrap()
            .unwrap();

        let orig_content = fs::read_to_string(&existing).await.unwrap();
        assert_eq!(orig_content, "original_content");

        let new_content = fs::read_to_string(&result.new_path).await.unwrap();
        assert_eq!(new_content, "new_content");
    }

    // --- PathOrganizerManager serde ---
    #[test]
    fn manager_serde_roundtrip() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);
        m.set_base_directory(Some(PathBuf::from("/test")));
        m.summary.total_organized = 5;

        let json = serde_json::to_string_pretty(&m).unwrap();
        let back: PathOrganizerManager = serde_json::from_str(&json).unwrap();
        assert!(back.is_enabled());
        assert_eq!(back.summary.total_organized, 5);
    }

    #[test]
    fn manager_serde_extra_fields_ignored() {
        let m = PathOrganizerManager::new();
        let mut json: serde_json::Value = serde_json::to_value(&m).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra".into(), serde_json::json!(42));
        let back: PathOrganizerManager = serde_json::from_value(json).unwrap();
        assert_eq!(back.is_enabled(), m.is_enabled());
    }

    #[tokio::test]
    async fn organize_file_chinese_path() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("中文文件.mp4");
        fs::write(&file_path, b"chinese").await.unwrap();

        let result = m
            .organize_file(&file_path, temp.path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.category, "videos");
        assert!(result.new_path.exists());
    }

    #[tokio::test]
    async fn organize_file_dot_only_name() {
        let mut m = PathOrganizerManager::new();
        m.set_enabled(true);

        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join(".hidden");
        fs::write(&file_path, b"hidden").await.unwrap();

        let result = m.organize_file(&file_path, temp.path()).await.unwrap();
        assert!(result.is_none());
    }
}
