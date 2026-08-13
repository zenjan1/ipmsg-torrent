//! Save path management for download tasks
//!
//! Features:
//! - Default download directory configuration
//! - Per-task custom save paths
//! - Auto-organize by file type (videos, music, documents, etc.)
//! - Path validation and creation
//! - Configuration persistence to disk

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

// ─── Persistence Functions ───

/// Save save-path configuration to disk.
///
/// Writes atomically using a temporary file to prevent corruption.
pub async fn save_save_path_config(
    data_dir: &Path,
    config: &SavePathConfig,
) -> Result<(), SavePathPersistenceError> {
    let config_path = data_dir.join("save_path_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| SavePathPersistenceError::Serialize(e.to_string()))?;

    // Atomic write: write to temp file, then rename
    let tmp_path = data_dir.join("save_path_config.json.tmp");
    tokio::fs::write(&tmp_path, &json)
        .await
        .map_err(|e| SavePathPersistenceError::Io(e.to_string()))?;
    tokio::fs::rename(&tmp_path, &config_path)
        .await
        .map_err(|e| SavePathPersistenceError::Io(e.to_string()))?;

    Ok(())
}

/// Load save-path configuration from disk.
///
/// Returns `Ok(None)` if the config file doesn't exist (first run).
/// Returns `Err` if the file exists but can't be parsed.
pub async fn load_save_path_config(
    data_dir: &Path,
) -> Result<Option<SavePathConfig>, SavePathPersistenceError> {
    let config_path = data_dir.join("save_path_config.json");

    if !config_path.exists() {
        return Ok(None);
    }

    let json = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| SavePathPersistenceError::Io(e.to_string()))?;

    let config: SavePathConfig = serde_json::from_str(&json)
        .map_err(|e| SavePathPersistenceError::Deserialize(e.to_string()))?;

    Ok(Some(config))
}

/// Errors from save-path configuration persistence.
#[derive(Debug, thiserror::Error)]
pub enum SavePathPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialize(String),
    #[error("deserialization error: {0}")]
    Deserialize(String),
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

    #[tokio::test]
    async fn test_save_and_load_config() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_save_path_persist");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let config = SavePathConfig {
            base_dir: PathBuf::from("/my/downloads"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "MyVideos".to_string());
                map.insert(FileCategory::Music, "MyMusic".to_string());
                map
            },
        };

        // Save config
        save_save_path_config(&temp_dir, &config).await.unwrap();

        // Verify file exists
        let config_path = temp_dir.join("save_path_config.json");
        assert!(config_path.exists());

        // Load config
        let loaded = load_save_path_config(&temp_dir).await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();

        assert_eq!(loaded.base_dir, config.base_dir);
        assert_eq!(loaded.auto_organize, config.auto_organize);
        assert_eq!(loaded.category_dirs.len(), 2);
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Video),
            Some(&"MyVideos".to_string())
        );

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_save_path_missing");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Load from non-existent file should return Ok(None)
        let result = load_save_path_config(&temp_dir).await.unwrap();
        assert!(result.is_none());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_save_and_load_empty_category_dirs() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_save_path_empty_cats");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let config = SavePathConfig {
            base_dir: PathBuf::from("/downloads"),
            auto_organize: false,
            category_dirs: HashMap::new(),
        };

        save_save_path_config(&temp_dir, &config).await.unwrap();
        let loaded = load_save_path_config(&temp_dir).await.unwrap().unwrap();

        assert_eq!(loaded.base_dir, config.base_dir);
        assert!(!loaded.auto_organize);
        assert!(loaded.category_dirs.is_empty());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_load_config_corrupted_file() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_save_path_corrupt");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Write invalid JSON
        let config_path = temp_dir.join("save_path_config.json");
        tokio::fs::write(&config_path, "not valid json {{{")
            .await
            .unwrap();

        // Should return error
        let result = load_save_path_config(&temp_dir).await;
        assert!(result.is_err());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_save_overwrite_config() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_save_path_overwrite");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Save initial config
        let config1 = SavePathConfig {
            base_dir: PathBuf::from("/downloads1"),
            auto_organize: false,
            category_dirs: HashMap::new(),
        };
        save_save_path_config(&temp_dir, &config1).await.unwrap();

        // Overwrite with new config
        let config2 = SavePathConfig {
            base_dir: PathBuf::from("/downloads2"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Document, "Docs".to_string());
                map
            },
        };
        save_save_path_config(&temp_dir, &config2).await.unwrap();

        // Load should get the second config
        let loaded = load_save_path_config(&temp_dir).await.unwrap().unwrap();
        assert_eq!(loaded.base_dir, PathBuf::from("/downloads2"));
        assert!(loaded.auto_organize);
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Document),
            Some(&"Docs".to_string())
        );

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    // ========== Phase 191: Comprehensive Test Coverage ==========

    // --- Serialization Tests ---

    #[test]
    fn test_file_category_serde_roundtrip() {
        let categories = vec![
            FileCategory::Video,
            FileCategory::Music,
            FileCategory::Document,
            FileCategory::Image,
            FileCategory::Archive,
            FileCategory::Program,
            FileCategory::Other,
        ];

        for cat in categories {
            let json = serde_json::to_string(&cat).unwrap();
            let deserialized: FileCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, cat);
        }
    }

    #[test]
    fn test_file_category_snake_case_rename() {
        // Verify serde rename_all = "lowercase" works correctly
        let video_json = serde_json::to_string(&FileCategory::Video).unwrap();
        assert_eq!(video_json, "\"video\"");

        let music_json = serde_json::to_string(&FileCategory::Music).unwrap();
        assert_eq!(music_json, "\"music\"");

        let document_json = serde_json::to_string(&FileCategory::Document).unwrap();
        assert_eq!(document_json, "\"document\"");

        let image_json = serde_json::to_string(&FileCategory::Image).unwrap();
        assert_eq!(image_json, "\"image\"");

        let archive_json = serde_json::to_string(&FileCategory::Archive).unwrap();
        assert_eq!(archive_json, "\"archive\"");

        let program_json = serde_json::to_string(&FileCategory::Program).unwrap();
        assert_eq!(program_json, "\"program\"");

        let other_json = serde_json::to_string(&FileCategory::Other).unwrap();
        assert_eq!(other_json, "\"other\"");
    }

    #[test]
    fn test_file_category_deserialize_from_lowercase() {
        // Verify we can deserialize from lowercase strings
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"video\"").unwrap(),
            FileCategory::Video
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"music\"").unwrap(),
            FileCategory::Music
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"document\"").unwrap(),
            FileCategory::Document
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"image\"").unwrap(),
            FileCategory::Image
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"archive\"").unwrap(),
            FileCategory::Archive
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"program\"").unwrap(),
            FileCategory::Program
        );
        assert_eq!(
            serde_json::from_str::<FileCategory>("\"other\"").unwrap(),
            FileCategory::Other
        );
    }

    #[test]
    fn test_save_path_config_serialization_roundtrip() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/home/user/downloads"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "Videos".to_string());
                map.insert(FileCategory::Music, "Music".to_string());
                map.insert(FileCategory::Document, "Docs".to_string());
                map.insert(FileCategory::Image, "Photos".to_string());
                map.insert(FileCategory::Archive, "Archives".to_string());
                map.insert(FileCategory::Program, "Programs".to_string());
                map
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SavePathConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.base_dir, config.base_dir);
        assert_eq!(deserialized.auto_organize, config.auto_organize);
        assert_eq!(deserialized.category_dirs.len(), 6);
        assert_eq!(
            deserialized.category_dirs.get(&FileCategory::Video),
            Some(&"Videos".to_string())
        );
        // Verify a category not in the map returns None
        assert_eq!(deserialized.category_dirs.get(&FileCategory::Other), None);
    }

    #[test]
    fn test_save_path_config_json_structure() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/downloads"),
            auto_organize: false,
            category_dirs: HashMap::new(),
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains("\"base_dir\""));
        assert!(json.contains("\"auto_organize\""));
        assert!(json.contains("\"category_dirs\""));
        assert!(json.contains("/downloads"));
    }

    #[test]
    fn test_save_path_config_with_custom_dirs_json() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/my/downloads"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "MyVideos".to_string());
                map.insert(FileCategory::Music, "MyMusic".to_string());
                map
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("MyVideos"));
        assert!(json.contains("MyMusic"));
        assert!(json.contains("\"auto_organize\":true"));
    }

    // --- Default Value Tests ---

    #[test]
    fn test_save_path_config_default_values() {
        let config = SavePathConfig::default();
        assert_eq!(config.base_dir, PathBuf::from("downloads"));
        assert!(!config.auto_organize);
        assert!(config.category_dirs.is_empty());
    }

    #[test]
    fn test_save_path_config_default_base_dir_is_relative() {
        let config = SavePathConfig::default();
        assert!(config.base_dir.is_relative());
    }

    // --- Constructor Tests ---

    #[tokio::test]
    async fn test_save_path_manager_with_config() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/custom/path"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "CustomVideos".to_string());
                map
            },
        };

        let manager = SavePathManager::with_config(config);
        let loaded = manager.get_config().await;

        assert_eq!(loaded.base_dir, PathBuf::from("/custom/path"));
        assert!(loaded.auto_organize);
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Video),
            Some(&"CustomVideos".to_string())
        );
    }

    #[tokio::test]
    async fn test_save_path_manager_new_uses_provided_base_dir() {
        let manager = SavePathManager::new(PathBuf::from("/test/dir"));
        let config = manager.get_config().await;
        assert_eq!(config.base_dir, PathBuf::from("/test/dir"));
        assert!(!config.auto_organize);
    }

    // --- FileCategory Extension Tests (Comprehensive) ---

    #[test]
    fn test_file_category_video_extensions() {
        let video_exts = vec![
            "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "mpg", "mpeg", "3gp",
        ];
        for ext in video_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Video,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_music_extensions() {
        let music_exts = vec!["mp3", "wav", "flac", "aac", "ogg", "wma", "m4a", "ape"];
        for ext in music_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Music,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_document_extensions() {
        let doc_exts = vec![
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "md", "rtf", "epub", "mobi",
        ];
        for ext in doc_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Document,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_image_extensions() {
        let image_exts = vec![
            "jpg", "jpeg", "png", "gif", "bmp", "webp", "svg", "ico", "psd", "tiff", "raw",
        ];
        for ext in image_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Image,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_archive_extensions() {
        let archive_exts = vec!["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "tgz", "iso"];
        for ext in archive_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Archive,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_program_extensions() {
        let program_exts = vec!["exe", "msi", "dmg", "deb", "rpm", "apk", "appimage"];
        for ext in program_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Program,
                "Failed for extension: {}",
                ext
            );
        }
    }

    #[test]
    fn test_file_category_case_insensitive() {
        // Test uppercase
        assert_eq!(FileCategory::from_extension("MP4"), FileCategory::Video);
        assert_eq!(FileCategory::from_extension("MP3"), FileCategory::Music);
        assert_eq!(FileCategory::from_extension("PDF"), FileCategory::Document);
        assert_eq!(FileCategory::from_extension("JPG"), FileCategory::Image);
        assert_eq!(FileCategory::from_extension("ZIP"), FileCategory::Archive);
        assert_eq!(FileCategory::from_extension("EXE"), FileCategory::Program);

        // Test mixed case
        assert_eq!(FileCategory::from_extension("Mp4"), FileCategory::Video);
        assert_eq!(FileCategory::from_extension("Flac"), FileCategory::Music);
        assert_eq!(FileCategory::from_extension("Docx"), FileCategory::Document);
    }

    #[test]
    fn test_file_category_unknown_extensions() {
        let unknown_exts = vec!["xyz", "abc", "123", "custom", ""];
        for ext in unknown_exts {
            assert_eq!(
                FileCategory::from_extension(ext),
                FileCategory::Other,
                "Failed for extension: {}",
                ext
            );
        }
    }

    // --- Path Calculation Edge Cases ---

    #[tokio::test]
    async fn test_get_save_path_filename_without_extension() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        let path = manager.get_save_path("README").await;
        // No extension means Other category
        assert_eq!(path, PathBuf::from("/downloads/Other"));
    }

    #[tokio::test]
    async fn test_get_save_path_filename_with_multiple_dots() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        let path = manager.get_save_path("my.video.file.mp4").await;
        // Should use the last extension
        assert_eq!(path, PathBuf::from("/downloads/Videos"));
    }

    #[tokio::test]
    async fn test_get_save_path_empty_filename() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        let path = manager.get_save_path("").await;
        // Empty filename has no extension, goes to Other
        assert_eq!(path, PathBuf::from("/downloads/Other"));
    }

    #[tokio::test]
    async fn test_get_save_path_with_category_no_organize() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(false).await;

        // Even with category override, should return base dir when auto_organize is false
        let path = manager
            .get_save_path_with_category("movie.mp4", FileCategory::Video)
            .await;
        assert_eq!(path, PathBuf::from("/downloads"));
    }

    #[tokio::test]
    async fn test_get_save_path_with_category_all_categories() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Video)
                .await,
            PathBuf::from("/downloads/Videos")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Music)
                .await,
            PathBuf::from("/downloads/Music")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Document)
                .await,
            PathBuf::from("/downloads/Documents")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Image)
                .await,
            PathBuf::from("/downloads/Images")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Archive)
                .await,
            PathBuf::from("/downloads/Archives")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Program)
                .await,
            PathBuf::from("/downloads/Programs")
        );
        assert_eq!(
            manager
                .get_save_path_with_category("test", FileCategory::Other)
                .await,
            PathBuf::from("/downloads/Other")
        );
    }

    #[tokio::test]
    async fn test_get_save_path_custom_dir_overrides_default() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        // Set custom dirs for all categories
        manager
            .set_category_dir(FileCategory::Video, "CustomVideos".to_string())
            .await;
        manager
            .set_category_dir(FileCategory::Music, "CustomMusic".to_string())
            .await;
        manager
            .set_category_dir(FileCategory::Document, "CustomDocs".to_string())
            .await;

        assert_eq!(
            manager.get_save_path("movie.mp4").await,
            PathBuf::from("/downloads/CustomVideos")
        );
        assert_eq!(
            manager.get_save_path("song.mp3").await,
            PathBuf::from("/downloads/CustomMusic")
        );
        assert_eq!(
            manager.get_save_path("doc.pdf").await,
            PathBuf::from("/downloads/CustomDocs")
        );

        // Categories without custom dirs still use defaults
        assert_eq!(
            manager.get_save_path("photo.jpg").await,
            PathBuf::from("/downloads/Images")
        );
    }

    // --- FileCategory Trait Tests ---

    #[test]
    fn test_file_category_clone() {
        let cat = FileCategory::Video;
        let cloned = cat.clone();
        assert_eq!(cat, cloned);
    }

    #[test]
    fn test_file_category_copy() {
        let cat = FileCategory::Video;
        let copied = cat; // Copy
        assert_eq!(cat, copied);
    }

    #[test]
    fn test_file_category_debug() {
        let cat = FileCategory::Video;
        let debug_str = format!("{:?}", cat);
        assert_eq!(debug_str, "Video");
    }

    #[test]
    fn test_file_category_partial_eq() {
        assert_eq!(FileCategory::Video, FileCategory::Video);
        assert_ne!(FileCategory::Video, FileCategory::Music);
        assert_ne!(FileCategory::Document, FileCategory::Image);
    }

    #[test]
    fn test_file_category_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(FileCategory::Video);
        set.insert(FileCategory::Music);
        set.insert(FileCategory::Video); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&FileCategory::Video));
        assert!(set.contains(&FileCategory::Music));
    }

    // --- SavePathConfig Trait Tests ---

    #[test]
    fn test_save_path_config_clone() {
        let config = SavePathConfig {
            base_dir: PathBuf::from("/test"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "Videos".to_string());
                map
            },
        };

        let cloned = config.clone();
        assert_eq!(cloned.base_dir, config.base_dir);
        assert_eq!(cloned.auto_organize, config.auto_organize);
        assert_eq!(cloned.category_dirs.len(), config.category_dirs.len());
    }

    #[test]
    fn test_save_path_config_debug() {
        let config = SavePathConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SavePathConfig"));
        assert!(debug_str.contains("base_dir"));
        assert!(debug_str.contains("auto_organize"));
    }

    // --- Error Display Tests ---

    #[test]
    fn test_save_path_error_display_create_dir_failed() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let err = SavePathError::CreateDirFailed {
            path: PathBuf::from("/test/path"),
            source: io_err,
        };
        let display = err.to_string();
        assert!(display.contains("failed to create directory"));
        assert!(display.contains("/test/path"));
    }

    #[test]
    fn test_save_path_error_display_not_a_directory() {
        let err = SavePathError::NotADirectory {
            path: PathBuf::from("/test/file.txt"),
        };
        let display = err.to_string();
        assert!(display.contains("not a directory"));
    }

    #[test]
    fn test_save_path_error_display_not_exists() {
        let err = SavePathError::NotExists {
            path: PathBuf::from("/missing/path"),
        };
        let display = err.to_string();
        assert!(display.contains("does not exist"));
    }

    #[test]
    fn test_save_path_persistence_error_display_io() {
        let err = SavePathPersistenceError::Io("disk full".to_string());
        assert_eq!(err.to_string(), "IO error: disk full");
    }

    #[test]
    fn test_save_path_persistence_error_display_serialize() {
        let err = SavePathPersistenceError::Serialize("invalid data".to_string());
        assert_eq!(err.to_string(), "serialization error: invalid data");
    }

    #[test]
    fn test_save_path_persistence_error_display_deserialize() {
        let err = SavePathPersistenceError::Deserialize("parse failed".to_string());
        assert_eq!(err.to_string(), "deserialization error: parse failed");
    }

    // --- Persistence Edge Cases ---

    #[tokio::test]
    async fn test_save_config_creates_atomic_tmp_file() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_atomic_write");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let config = SavePathConfig::default();
        save_save_path_config(&temp_dir, &config).await.unwrap();

        // After successful save, tmp file should not exist (renamed)
        let tmp_path = temp_dir.join("save_path_config.json.tmp");
        assert!(!tmp_path.exists());

        // Main config file should exist
        let config_path = temp_dir.join("save_path_config.json");
        assert!(config_path.exists());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_save_and_load_all_categories() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_all_categories");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let config = SavePathConfig {
            base_dir: PathBuf::from("/all/categories"),
            auto_organize: true,
            category_dirs: {
                let mut map = HashMap::new();
                map.insert(FileCategory::Video, "V".to_string());
                map.insert(FileCategory::Music, "M".to_string());
                map.insert(FileCategory::Document, "D".to_string());
                map.insert(FileCategory::Image, "I".to_string());
                map.insert(FileCategory::Archive, "A".to_string());
                map.insert(FileCategory::Program, "P".to_string());
                map.insert(FileCategory::Other, "O".to_string());
                map
            },
        };

        save_save_path_config(&temp_dir, &config).await.unwrap();
        let loaded = load_save_path_config(&temp_dir).await.unwrap().unwrap();

        assert_eq!(loaded.category_dirs.len(), 7);
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Video),
            Some(&"V".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Music),
            Some(&"M".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Document),
            Some(&"D".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Image),
            Some(&"I".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Archive),
            Some(&"A".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Program),
            Some(&"P".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Other),
            Some(&"O".to_string())
        );

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_load_config_with_extra_fields_ignores_them() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_extra_fields");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Write JSON with extra fields
        let config_path = temp_dir.join("save_path_config.json");
        let json_with_extra = r#"{
            "base_dir": "/downloads",
            "auto_organize": false,
            "category_dirs": {},
            "extra_field": "should be ignored",
            "another_extra": 123
        }"#;
        tokio::fs::write(&config_path, json_with_extra)
            .await
            .unwrap();

        // Should load successfully, ignoring extra fields
        let result = load_save_path_config(&temp_dir).await;
        assert!(result.is_ok());
        let loaded = result.unwrap().unwrap();
        assert_eq!(loaded.base_dir, PathBuf::from("/downloads"));

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    // --- Directory Operations Edge Cases ---

    #[tokio::test]
    async fn test_ensure_directory_on_file_returns_error() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_ensure_on_file");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Create a file (not directory)
        let file_path = temp_dir.join("not_a_dir.txt");
        tokio::fs::write(&file_path, "test").await.unwrap();

        // ensure_directory should fail
        let result = SavePathManager::ensure_directory(&file_path).await;
        assert!(result.is_err());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_ensure_directory_creates_nested_dirs() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_nested_dirs");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        let nested_path = temp_dir.join("level1").join("level2").join("level3");

        let result = SavePathManager::ensure_directory(&nested_path).await;
        assert!(result.is_ok());
        assert!(nested_path.exists());
        assert!(nested_path.is_dir());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_check_writable_on_nonexistent_dir() {
        let non_existent = std::env::temp_dir().join("ipmsg_test_nonexistent_dir_12345");
        let _ = tokio::fs::remove_dir_all(&non_existent).await;

        // Should return false or error for non-existent directory
        let result = SavePathManager::check_writable(&non_existent).await;
        // Either Ok(false) or Err is acceptable
        match result {
            Ok(writable) => assert!(!writable),
            Err(_) => {} // Also acceptable
        }
    }

    // --- Complex Scenario Tests ---

    #[tokio::test]
    async fn test_complete_workflow() {
        let temp_dir = std::env::temp_dir().join("ipmsg_test_complete_workflow");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // 1. Create manager with default config
        let manager = SavePathManager::new(PathBuf::from("/initial"));
        let config = manager.get_config().await;
        assert_eq!(config.base_dir, PathBuf::from("/initial"));
        assert!(!config.auto_organize);

        // 2. Update configuration
        manager.set_base_dir(PathBuf::from("/updated")).await;
        manager.set_auto_organize(true).await;
        manager
            .set_category_dir(FileCategory::Video, "Movies".to_string())
            .await;
        manager
            .set_category_dir(FileCategory::Music, "Songs".to_string())
            .await;

        // 3. Verify path calculation
        let video_path = manager.get_save_path("movie.mp4").await;
        assert_eq!(video_path, PathBuf::from("/updated/Movies"));

        let music_path = manager.get_save_path("song.mp3").await;
        assert_eq!(music_path, PathBuf::from("/updated/Songs"));

        let doc_path = manager.get_save_path("doc.pdf").await;
        assert_eq!(doc_path, PathBuf::from("/updated/Documents")); // Default

        // 4. Save config to disk
        let config = manager.get_config().await;
        save_save_path_config(&temp_dir, &config).await.unwrap();

        // 5. Load config from disk
        let loaded = load_save_path_config(&temp_dir).await.unwrap().unwrap();
        assert_eq!(loaded.base_dir, PathBuf::from("/updated"));
        assert!(loaded.auto_organize);
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Video),
            Some(&"Movies".to_string())
        );
        assert_eq!(
            loaded.category_dirs.get(&FileCategory::Music),
            Some(&"Songs".to_string())
        );

        // 6. Create new manager with loaded config
        let manager2 = SavePathManager::with_config(loaded);
        let video_path2 = manager2.get_save_path("movie.mp4").await;
        assert_eq!(video_path2, PathBuf::from("/updated/Movies"));

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_category_dir_override_and_restore() {
        let manager = SavePathManager::new(PathBuf::from("/downloads"));
        manager.set_auto_organize(true).await;

        // Initially uses default
        let path1 = manager.get_save_path("movie.mp4").await;
        assert_eq!(path1, PathBuf::from("/downloads/Videos"));

        // Override with custom
        manager
            .set_category_dir(FileCategory::Video, "CustomVids".to_string())
            .await;
        let path2 = manager.get_save_path("movie.mp4").await;
        assert_eq!(path2, PathBuf::from("/downloads/CustomVids"));

        // Override again with different value
        manager
            .set_category_dir(FileCategory::Video, "AnotherDir".to_string())
            .await;
        let path3 = manager.get_save_path("movie.mp4").await;
        assert_eq!(path3, PathBuf::from("/downloads/AnotherDir"));
    }

    #[tokio::test]
    async fn test_multiple_managers_independent() {
        let manager1 = SavePathManager::new(PathBuf::from("/dir1"));
        let manager2 = SavePathManager::new(PathBuf::from("/dir2"));

        manager1.set_auto_organize(true).await;
        manager2.set_auto_organize(false).await;

        let config1 = manager1.get_config().await;
        let config2 = manager2.get_config().await;

        assert_eq!(config1.base_dir, PathBuf::from("/dir1"));
        assert!(config1.auto_organize);
        assert_eq!(config2.base_dir, PathBuf::from("/dir2"));
        assert!(!config2.auto_organize);
    }
}
