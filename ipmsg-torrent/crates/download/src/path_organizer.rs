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
}
