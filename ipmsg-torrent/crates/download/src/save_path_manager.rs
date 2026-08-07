//! Save path management for download tasks
//!
//! Features:
//! - Default download directory configuration
//! - Per-task custom save paths
//! - Auto-organize by file type (videos, music, documents, etc.)
//! - Path validation and creation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;

/// File category for auto-organization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileCategory {
    Video,
    Music,
    Document,
    Image,
    Archive,
    Program,
    Other,
}

impl FileCategory {
    /// Detect category from file extension
    pub fn from_extension(ext: &str) -> Self {
        let ext = ext.to_lowercase();
        match ext.as_str() {
            // Video formats
            "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "mpg" | "mpeg"
            | "3gp" => FileCategory::Video,

            // Audio formats
            "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" | "ape" => FileCategory::Music,

            // Document formats
            "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx" | "txt" | "md" | "rtf"
            | "epub" | "mobi" => FileCategory::Document,

            // Image formats
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "ico" | "psd" | "tiff"
            | "raw" => FileCategory::Image,

            // Archive formats
            "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" | "tgz" | "iso" => {
                FileCategory::Archive
            }

            // Program formats
            "exe" | "msi" | "dmg" | "deb" | "rpm" | "apk" | "appimage" => FileCategory::Program,

            // Everything else
            _ => FileCategory::Other,
        }
    }

    /// Get category directory name
    pub fn dir_name(&self) -> &'static str {
        match self {
            FileCategory::Video => "Videos",
            FileCategory::Music => "Music",
            FileCategory::Document => "Documents",
            FileCategory::Image => "Images",
            FileCategory::Archive => "Archives",
            FileCategory::Program => "Programs",
            FileCategory::Other => "Other",
        }
    }
}

/// Save path configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavePathConfig {
    /// Base download directory
    pub base_dir: PathBuf,
    /// Enable auto-organization by file type
    pub auto_organize: bool,
    /// Custom subdirectory for each category (optional)
    pub category_dirs: HashMap<FileCategory, String>,
}

impl Default for SavePathConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("downloads"),
            auto_organize: false,
            category_dirs: HashMap::new(),
        }
    }
}

/// Manages save paths for download tasks
pub struct SavePathManager {
    config: RwLock<SavePathConfig>,
}

impl SavePathManager {
    /// Create a new save path manager with default config
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            config: RwLock::new(SavePathConfig {
                base_dir,
                ..Default::default()
            }),
        }
    }

    /// Create a new save path manager with custom config
    pub fn with_config(config: SavePathConfig) -> Self {
        Self {
            config: RwLock::new(config),
        }
    }

    /// Get current configuration
    pub async fn get_config(&self) -> SavePathConfig {
        self.config.read().await.clone()
    }

    /// Update configuration
    pub async fn set_config(&self, config: SavePathConfig) {
        *self.config.write().await = config;
    }

    /// Set base download directory
    pub async fn set_base_dir(&self, dir: PathBuf) {
        self.config.write().await.base_dir = dir;
    }

    /// Enable or disable auto-organization
    pub async fn set_auto_organize(&self, enabled: bool) {
        self.config.write().await.auto_organize = enabled;
    }

    /// Set custom directory name for a category
    pub async fn set_category_dir(&self, category: FileCategory, dir_name: String) {
        self.config
            .write()
            .await
            .category_dirs
            .insert(category, dir_name);
    }

    /// Calculate save path for a file
    ///
    /// Returns the full path where the file should be saved.
    /// If auto_organize is enabled, files are placed in category subdirectories.
    pub async fn get_save_path(&self, file_name: &str) -> PathBuf {
        let config = self.config.read().await;
        let mut path = config.base_dir.clone();

        if config.auto_organize {
            let category = Self::detect_category(file_name);
            let dir_name = config
                .category_dirs
                .get(&category)
                .map(|s| s.as_str())
                .unwrap_or_else(|| category.dir_name());
            path.push(dir_name);
        }

        path
    }

    /// Calculate save path with explicit category override
    pub async fn get_save_path_with_category(
        &self,
        _file_name: &str,
        category: FileCategory,
    ) -> PathBuf {
        let config = self.config.read().await;
        let mut path = config.base_dir.clone();

        if config.auto_organize {
            let dir_name = config
                .category_dirs
                .get(&category)
                .map(|s| s.as_str())
                .unwrap_or_else(|| category.dir_name());
            path.push(dir_name);
        }

        path
    }

    /// Detect file category from filename
    pub fn detect_category(file_name: &str) -> FileCategory {
        Path::new(file_name)
            .extension()
            .and_then(|ext| ext.to_str())
            .map(FileCategory::from_extension)
            .unwrap_or(FileCategory::Other)
    }

    /// Validate and create directory if needed
    pub async fn ensure_directory(path: &Path) -> Result<(), SavePathError> {
        if !path.exists() {
            tokio::fs::create_dir_all(path)
                .await
                .map_err(|e| SavePathError::CreateDirFailed {
                    path: path.to_path_buf(),
                    source: e,
                })?;
        }

        if !path.is_dir() {
            return Err(SavePathError::NotADirectory {
                path: path.to_path_buf(),
            });
        }

        Ok(())
    }

    /// Check if path is writable
    pub async fn check_writable(path: &Path) -> Result<bool, SavePathError> {
        // Try to create a temporary file
        let test_file = path.join(".ipmsg_write_test");
        match tokio::fs::write(&test_file, b"test").await {
            Ok(_) => {
                let _ = tokio::fs::remove_file(&test_file).await;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
}

/// Errors that can occur during save path operations
#[derive(Debug, thiserror::Error)]
pub enum SavePathError {
    #[error("failed to create directory {path}: {source}")]
    CreateDirFailed {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("path is not a directory: {path:?}")]
    NotADirectory { path: PathBuf },

    #[error("path does not exist: {path:?}")]
    NotExists { path: PathBuf },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_category_from_extension() {
        assert_eq!(FileCategory::from_extension("mp4"), FileCategory::Video);
        assert_eq!(FileCategory::from_extension("MP4"), FileCategory::Video);
        assert_eq!(FileCategory::from_extension("mkv"), FileCategory::Video);
        assert_eq!(FileCategory::from_extension("mp3"), FileCategory::Music);
        assert_eq!(FileCategory::from_extension("flac"), FileCategory::Music);
        assert_eq!(FileCategory::from_extension("pdf"), FileCategory::Document);
        assert_eq!(FileCategory::from_extension("docx"), FileCategory::Document);
        assert_eq!(FileCategory::from_extension("jpg"), FileCategory::Image);
        assert_eq!(FileCategory::from_extension("png"), FileCategory::Image);
        assert_eq!(FileCategory::from_extension("zip"), FileCategory::Archive);
        assert_eq!(FileCategory::from_extension("rar"), FileCategory::Archive);
        assert_eq!(FileCategory::from_extension("exe"), FileCategory::Program);
        assert_eq!(FileCategory::from_extension("unknown"), FileCategory::Other);
        assert_eq!(FileCategory::from_extension(""), FileCategory::Other);
    }

    #[test]
    fn test_file_category_dir_name() {
        assert_eq!(FileCategory::Video.dir_name(), "Videos");
        assert_eq!(FileCategory::Music.dir_name(), "Music");
        assert_eq!(FileCategory::Document.dir_name(), "Documents");
        assert_eq!(FileCategory::Image.dir_name(), "Images");
        assert_eq!(FileCategory::Archive.dir_name(), "Archives");
        assert_eq!(FileCategory::Program.dir_name(), "Programs");
        assert_eq!(FileCategory::Other.dir_name(), "Other");
    }

    #[test]
    fn test_detect_category() {
        assert_eq!(
            SavePathManager::detect_category("movie.mp4"),
            FileCategory::Video
        );
        assert_eq!(
            SavePathManager::detect_category("song.flac"),
            FileCategory::Music
        );
        assert_eq!(
            SavePathManager::detect_category("document.pdf"),
            FileCategory::Document
        );
        assert_eq!(
            SavePathManager::detect_category("photo.jpg"),
            FileCategory::Image
        );
        assert_eq!(
            SavePathManager::detect_category("archive.zip"),
            FileCategory::Archive
        );
        assert_eq!(
            SavePathManager::detect_category("installer.exe"),
            FileCategory::Program
        );
        assert_eq!(
            SavePathManager::detect_category("unknown.xyz"),
            FileCategory::Other
        );
        assert_eq!(
            SavePathManager::detect_category("no_extension"),
            FileCategory::Other
        );
    }

    #[tokio::test]
    async fn test_save_path_manager_default() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        let config = manager.get_config().await;

        assert_eq!(config.base_dir, PathBuf::from("/downloads"));
        assert!(!config.auto_organize);
        assert!(config.category_dirs.is_empty());
    }

    #[tokio::test]
    async fn test_save_path_manager_set_config() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));

        manager.set_base_dir(PathBuf::from("/new_downloads")).await;
        manager.set_auto_organize(true).await;
        manager
            .set_category_dir(FileCategory::Video, "MyVideos".to_string())
            .await;

        let config = manager.get_config().await;
        assert_eq!(config.base_dir, PathBuf::from("/new_downloads"));
        assert!(config.auto_organize);
        assert_eq!(
            config.category_dirs.get(&FileCategory::Video),
            Some(&"MyVideos".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_save_path_no_organize() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(false).await;

        let path = manager.get_save_path("movie.mp4").await;
        assert_eq!(path, PathBuf::from("/downloads"));
    }

    #[tokio::test]
    async fn test_get_save_path_with_organize() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        let path = manager.get_save_path("movie.mp4").await;
        assert_eq!(path, PathBuf::from("/downloads/Videos"));

        let path = manager.get_save_path("song.mp3").await;
        assert_eq!(path, PathBuf::from("/downloads/Music"));

        let path = manager.get_save_path("document.pdf").await;
        assert_eq!(path, PathBuf::from("/downloads/Documents"));
    }

    #[tokio::test]
    async fn test_get_save_path_with_custom_category_dir() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;
        manager
            .set_category_dir(FileCategory::Video, "MyVideos".to_string())
            .await;

        let path = manager.get_save_path("movie.mp4").await;
        assert_eq!(path, PathBuf::from("/downloads/MyVideos"));

        // Other categories use default names
        let path = manager.get_save_path("song.mp3").await;
        assert_eq!(path, PathBuf::from("/downloads/Music"));
    }

    #[tokio::test]
    async fn test_get_save_path_with_category_override() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        // Override category for specific file
        let path = manager
            .get_save_path_with_category("movie.mp4", FileCategory::Archive)
            .await;
        assert_eq!(path, PathBuf::from("/downloads/Archives"));
    }

    #[tokio::test]
    async fn test_ensure_directory() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_ensure_dir");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        // Should create directory
        let result = SavePathManager::ensure_directory(&temp_dir).await;
        assert!(result.is_ok());
        assert!(temp_dir.exists());

        // Should succeed on existing directory
        let result = SavePathManager::ensure_directory(&temp_dir).await;
        assert!(result.is_ok());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_check_writable() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_writable");
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let writable = SavePathManager::check_writable(&temp_dir).await.unwrap();
        assert!(writable);

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[test]
    fn test_save_path_config_serialize() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/downloads"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "MyVideos".to_string());
                map.insert(FileCategory::Music, "MyMusic".to_string());
                map
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SavePathConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.base_dir, config.base_dir);
        assert_eq!(deserialized.auto_organize, config.auto_organize);
        assert_eq!(deserialized.category_dirs.len(), 2);
    }
}
