//! Download File Type Statistics - Track download statistics by file extension and category
//!
//! This module provides:
//! - Track downloads by file extension (mp4, mp3, pdf, zip, etc.)
//! - Categorize files into types (video, audio, document, archive, image, other)
//! - Statistics per extension and per category
//! - Top downloads by size and count
//! - Persistent storage of statistics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// File category based on extension
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileCategory {
    Video,
    Audio,
    Document,
    Archive,
    Image,
    Executable,
    Other,
}

impl std::fmt::Display for FileCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileCategory::Video => write!(f, "video"),
            FileCategory::Audio => write!(f, "audio"),
            FileCategory::Document => write!(f, "document"),
            FileCategory::Archive => write!(f, "archive"),
            FileCategory::Image => write!(f, "image"),
            FileCategory::Executable => write!(f, "executable"),
            FileCategory::Other => write!(f, "other"),
        }
    }
}

/// Statistics for a single file extension
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtensionStats {
    /// File extension (without dot, lowercase)
    pub extension: String,
    /// Number of downloads with this extension
    pub count: u64,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Total download duration in seconds
    pub total_duration_secs: u64,
    /// Average speed in bytes per second
    pub avg_speed_bps: u64,
    /// Last download timestamp (Unix epoch)
    pub last_download_at: Option<u64>,
}

/// Statistics for a file category
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryStats {
    /// File category
    pub category: FileCategory,
    /// Number of downloads in this category
    pub count: u64,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Number of unique extensions in this category
    pub unique_extensions: usize,
}

impl Default for CategoryStats {
    fn default() -> Self {
        Self {
            category: FileCategory::Other,
            count: 0,
            total_bytes: 0,
            unique_extensions: 0,
        }
    }
}

/// Configuration for file statistics tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatsConfig {
    /// Enable file statistics tracking
    pub enabled: bool,
    /// Maximum number of extensions to track
    pub max_extensions: usize,
    /// Track individual file extensions
    pub track_extensions: bool,
    /// Track file categories
    pub track_categories: bool,
}

impl Default for FileStatsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_extensions: 100,
            track_extensions: true,
            track_categories: true,
        }
    }
}

/// Summary of file type statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatsSummary {
    /// Total number of tracked downloads
    pub total_downloads: u64,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Number of unique extensions tracked
    pub unique_extensions: usize,
    /// Statistics per category
    pub by_category: HashMap<FileCategory, CategoryStats>,
    /// Top extensions by download count
    pub top_by_count: Vec<ExtensionStats>,
    /// Top extensions by total bytes
    pub top_by_size: Vec<ExtensionStats>,
    /// Configuration
    pub config: FileStatsConfig,
}

/// Data structure for persisting file statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileStatsData {
    /// Statistics per extension
    pub extensions: HashMap<String, ExtensionStats>,
    /// Total downloads tracked
    pub total_downloads: u64,
    /// Total bytes tracked
    pub total_bytes: u64,
}

/// File type statistics tracker
pub struct FileTypeStatsTracker {
    config: Arc<RwLock<FileStatsConfig>>,
    data: Arc<RwLock<FileStatsData>>,
    stats_file: String,
    config_file: String,
    dirty: Arc<AtomicU64>,
}

impl FileTypeStatsTracker {
    /// Create a new file type statistics tracker
    pub fn new(config: FileStatsConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            data: Arc::new(RwLock::new(FileStatsData::default())),
            stats_file: "download_file_stats.json".to_string(),
            config_file: "download_file_stats_config.json".to_string(),
            dirty: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Load configuration from disk
    pub async fn load_config(&self) -> Result<(), std::io::Error> {
        if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await
            && let Ok(config) = serde_json::from_str::<FileStatsConfig>(&content)
        {
            *self.config.write().await = config;
        }
        Ok(())
    }

    /// Save configuration to disk
    pub async fn save_config(&self) -> Result<(), std::io::Error> {
        let config = self.config.read().await.clone();
        let content = serde_json::to_string_pretty(&config).map_err(std::io::Error::other)?;
        tokio::fs::write(&self.config_file, content).await
    }

    /// Load statistics data from disk
    pub async fn load_data(&self) -> Result<(), std::io::Error> {
        if let Ok(content) = tokio::fs::read_to_string(&self.stats_file).await
            && let Ok(data) = serde_json::from_str::<FileStatsData>(&content)
        {
            *self.data.write().await = data;
        }
        Ok(())
    }

    /// Save statistics data to disk
    pub async fn save_data(&self) -> Result<(), std::io::Error> {
        let data = self.data.read().await.clone();
        let content = serde_json::to_string_pretty(&data).map_err(std::io::Error::other)?;
        tokio::fs::write(&self.stats_file, content).await
    }

    /// Get configuration
    pub async fn get_config(&self) -> FileStatsConfig {
        self.config.read().await.clone()
    }

    /// Set configuration
    pub async fn set_config(&self, config: FileStatsConfig) -> Result<(), std::io::Error> {
        *self.config.write().await = config;
        self.save_config().await
    }

    /// Extract file extension from URL or filename
    pub fn extract_extension(url_or_filename: &str) -> Option<String> {
        // Try to parse as URL first
        let path = if url_or_filename.contains("://") {
            url::Url::parse(url_or_filename)
                .ok()
                .map(|u| u.path().to_string())
                .unwrap_or_default()
        } else {
            url_or_filename.to_string()
        };

        // Extract extension from path
        Path::new(&path)
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase())
    }

    /// Categorize file by extension
    pub fn categorize_extension(extension: &str) -> FileCategory {
        let ext = extension.to_lowercase();
        match ext.as_str() {
            // Video formats
            "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "mpg" | "mpeg"
            | "3gp" | "ts" => FileCategory::Video,

            // Audio formats
            "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" | "opus" | "aiff" | "mid"
            | "midi" => FileCategory::Audio,

            // Document formats
            "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx" | "txt" | "rtf" | "odt"
            | "ods" | "odp" | "epub" | "mobi" | "csv" | "json" | "xml" | "html" | "htm" | "md" => {
                FileCategory::Document
            }

            // Archive formats
            "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" | "iso" | "dmg" | "deb" | "rpm"
            | "apk" | "jar" => FileCategory::Archive,

            // Image formats
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "svg" | "webp" | "ico" | "tiff" | "psd"
            | "raw" => FileCategory::Image,

            // Executable formats
            "exe" | "msi" | "app" | "bin" | "sh" | "bat" | "cmd" | "ps1" => {
                FileCategory::Executable
            }

            // Everything else
            _ => FileCategory::Other,
        }
    }

    /// Record a completed download
    pub async fn record_download(
        &self,
        url_or_filename: &str,
        bytes: u64,
        duration_secs: u64,
    ) -> Result<(), std::io::Error> {
        let config = self.config.read().await.clone();
        if !config.enabled {
            return Ok(());
        }

        let extension = match Self::extract_extension(url_or_filename) {
            Some(ext) => ext,
            None => "unknown".to_string(),
        };

        let _category = Self::categorize_extension(&extension);

        let mut data = self.data.write().await;

        // Update extension stats
        if config.track_extensions {
            let stats =
                data.extensions
                    .entry(extension.clone())
                    .or_insert_with(|| ExtensionStats {
                        extension: extension.clone(),
                        ..Default::default()
                    });

            stats.count += 1;
            stats.total_bytes += bytes;
            stats.total_duration_secs += duration_secs;
            if stats.total_duration_secs > 0 {
                stats.avg_speed_bps = stats.total_bytes / stats.total_duration_secs;
            }
            stats.last_download_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );

            // Enforce max extensions limit
            if data.extensions.len() > config.max_extensions {
                // Remove least recently used
                let mut lru: Vec<_> = data
                    .extensions
                    .iter()
                    .map(|(k, v)| (k.clone(), v.last_download_at.unwrap_or(0)))
                    .collect();
                lru.sort_by_key(|(_, t)| *t);
                if let Some((oldest_key, _)) = lru.first() {
                    data.extensions.remove(oldest_key);
                }
            }
        }

        // Update totals
        data.total_downloads += 1;
        data.total_bytes += bytes;

        // Mark as dirty for periodic save
        self.dirty.fetch_add(1, Ordering::Relaxed);

        // Auto-save every 10 downloads
        if self.dirty.load(Ordering::Relaxed) % 10 == 0 {
            drop(data);
            self.save_data().await?;
        }

        Ok(())
    }

    /// Get statistics summary
    pub async fn get_summary(&self) -> FileStatsSummary {
        let config = self.config.read().await.clone();
        let data = self.data.read().await.clone();

        // Calculate category statistics
        let mut by_category: HashMap<FileCategory, CategoryStats> = HashMap::new();
        if config.track_categories {
            for stats in data.extensions.values() {
                let category = Self::categorize_extension(&stats.extension);
                let cat_stats = by_category
                    .entry(category)
                    .or_insert_with(|| CategoryStats {
                        category,
                        ..Default::default()
                    });
                cat_stats.count += stats.count;
                cat_stats.total_bytes += stats.total_bytes;
                cat_stats.unique_extensions += 1;
            }
        }

        // Top extensions by count
        let mut top_by_count: Vec<_> = data.extensions.values().cloned().collect();
        top_by_count.sort_by(|a, b| b.count.cmp(&a.count));
        top_by_count.truncate(10);

        // Top extensions by size
        let mut top_by_size: Vec<_> = data.extensions.values().cloned().collect();
        top_by_size.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
        top_by_size.truncate(10);

        FileStatsSummary {
            total_downloads: data.total_downloads,
            total_bytes: data.total_bytes,
            unique_extensions: data.extensions.len(),
            by_category,
            top_by_count,
            top_by_size,
            config,
        }
    }

    /// Get statistics for a specific extension
    pub async fn get_extension_stats(&self, extension: &str) -> Option<ExtensionStats> {
        let data = self.data.read().await;
        data.extensions.get(&extension.to_lowercase()).cloned()
    }

    /// Clear all statistics
    pub async fn clear(&self) -> Result<(), std::io::Error> {
        let mut data = self.data.write().await;
        data.extensions.clear();
        data.total_downloads = 0;
        data.total_bytes = 0;
        self.dirty.store(0, Ordering::Relaxed);
        drop(data);
        self.save_data().await
    }

    /// Format summary as human-readable string
    pub async fn format_summary(&self) -> String {
        let summary = self.get_summary().await;
        let mut output = String::new();

        output.push_str("📊 Download File Type Statistics\n");
        output.push_str(&format!(
            "Total Downloads: {} | Total Size: {}\n\n",
            summary.total_downloads,
            Self::format_bytes(summary.total_bytes)
        ));

        // Category breakdown
        if !summary.by_category.is_empty() {
            output.push_str("📁 By Category:\n");
            let mut categories: Vec<_> = summary.by_category.values().collect();
            categories.sort_by(|a, b| b.count.cmp(&a.count));
            for cat in categories {
                output.push_str(&format!(
                    "  {:12} {:5} downloads ({})\n",
                    format!("{}", cat.category),
                    cat.count,
                    Self::format_bytes(cat.total_bytes)
                ));
            }
            output.push('\n');
        }

        // Top by count
        if !summary.top_by_count.is_empty() {
            output.push_str("🔝 Top Extensions (by count):\n");
            for (i, stats) in summary.top_by_count.iter().enumerate() {
                output.push_str(&format!(
                    "  {:2}. {:8} {:5} downloads ({})\n",
                    i + 1,
                    stats.extension,
                    stats.count,
                    Self::format_bytes(stats.total_bytes)
                ));
            }
            output.push('\n');
        }

        // Top by size
        if !summary.top_by_size.is_empty() {
            output.push_str("💾 Top Extensions (by size):\n");
            for (i, stats) in summary.top_by_size.iter().enumerate() {
                output.push_str(&format!(
                    "  {:2}. {:8} {} ({:5} downloads)\n",
                    i + 1,
                    stats.extension,
                    Self::format_bytes(stats.total_bytes),
                    stats.count
                ));
            }
        }

        output
    }

    /// Format bytes to human-readable string
    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.2} TB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

impl Default for FileTypeStatsTracker {
    fn default() -> Self {
        Self::new(FileStatsConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_extension_from_url() {
        assert_eq!(
            FileTypeStatsTracker::extract_extension("https://example.com/file.mp4"),
            Some("mp4".to_string())
        );
        assert_eq!(
            FileTypeStatsTracker::extract_extension("http://example.com/path/to/document.pdf"),
            Some("pdf".to_string())
        );
        assert_eq!(
            FileTypeStatsTracker::extract_extension("https://example.com/archive.tar.gz"),
            Some("gz".to_string())
        );
    }

    #[test]
    fn test_extract_extension_from_filename() {
        assert_eq!(
            FileTypeStatsTracker::extract_extension("video.mp4"),
            Some("mp4".to_string())
        );
        assert_eq!(
            FileTypeStatsTracker::extract_extension("document.PDF"),
            Some("pdf".to_string())
        );
        assert_eq!(
            FileTypeStatsTracker::extract_extension("no_extension"),
            None
        );
    }

    #[test]
    fn test_categorize_video() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("mp4"),
            FileCategory::Video
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("mkv"),
            FileCategory::Video
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("avi"),
            FileCategory::Video
        );
    }

    #[test]
    fn test_categorize_audio() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("mp3"),
            FileCategory::Audio
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("flac"),
            FileCategory::Audio
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("wav"),
            FileCategory::Audio
        );
    }

    #[test]
    fn test_categorize_document() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("pdf"),
            FileCategory::Document
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("docx"),
            FileCategory::Document
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("txt"),
            FileCategory::Document
        );
    }

    #[test]
    fn test_categorize_archive() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("zip"),
            FileCategory::Archive
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("rar"),
            FileCategory::Archive
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("7z"),
            FileCategory::Archive
        );
    }

    #[test]
    fn test_categorize_image() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("jpg"),
            FileCategory::Image
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("png"),
            FileCategory::Image
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("gif"),
            FileCategory::Image
        );
    }

    #[test]
    fn test_categorize_executable() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("exe"),
            FileCategory::Executable
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("msi"),
            FileCategory::Executable
        );
    }

    #[test]
    fn test_categorize_other() {
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("xyz"),
            FileCategory::Other
        );
        assert_eq!(
            FileTypeStatsTracker::categorize_extension("custom"),
            FileCategory::Other
        );
    }

    #[tokio::test]
    async fn test_record_download() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("https://example.com/video.mp4", 1024 * 1024, 10)
            .await
            .unwrap();

        let summary = tracker.get_summary().await;
        assert_eq!(summary.total_downloads, 1);
        assert_eq!(summary.total_bytes, 1024 * 1024);
        assert!(summary.unique_extensions > 0);
    }

    #[tokio::test]
    async fn test_multiple_downloads() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("video.mp4", 1000, 10)
            .await
            .unwrap();
        tracker
            .record_download("video2.mp4", 2000, 20)
            .await
            .unwrap();
        tracker.record_download("audio.mp3", 500, 5).await.unwrap();

        let summary = tracker.get_summary().await;
        assert_eq!(summary.total_downloads, 3);
        assert_eq!(summary.total_bytes, 3500);

        let mp4_stats = tracker.get_extension_stats("mp4").await.unwrap();
        assert_eq!(mp4_stats.count, 2);
        assert_eq!(mp4_stats.total_bytes, 3000);
    }

    #[tokio::test]
    async fn test_category_stats() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("video.mp4", 1000, 10)
            .await
            .unwrap();
        tracker.record_download("audio.mp3", 500, 5).await.unwrap();
        tracker
            .record_download("document.pdf", 2000, 15)
            .await
            .unwrap();

        let summary = tracker.get_summary().await;
        assert!(!summary.by_category.is_empty());

        let video_stats = summary.by_category.get(&FileCategory::Video).unwrap();
        assert_eq!(video_stats.count, 1);
        assert_eq!(video_stats.total_bytes, 1000);

        let audio_stats = summary.by_category.get(&FileCategory::Audio).unwrap();
        assert_eq!(audio_stats.count, 1);
        assert_eq!(audio_stats.total_bytes, 500);
    }

    #[tokio::test]
    async fn test_disabled_tracking() {
        let config = FileStatsConfig {
            enabled: false,
            ..Default::default()
        };
        let tracker = FileTypeStatsTracker::new(config);

        tracker
            .record_download("video.mp4", 1000, 10)
            .await
            .unwrap();

        let summary = tracker.get_summary().await;
        assert_eq!(summary.total_downloads, 0);
    }

    #[tokio::test]
    async fn test_clear_stats() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("video.mp4", 1000, 10)
            .await
            .unwrap();
        tracker.record_download("audio.mp3", 500, 5).await.unwrap();

        tracker.clear().await.unwrap();

        let summary = tracker.get_summary().await;
        assert_eq!(summary.total_downloads, 0);
        assert_eq!(summary.total_bytes, 0);
        assert_eq!(summary.unique_extensions, 0);
    }

    #[tokio::test]
    async fn test_format_bytes() {
        assert_eq!(FileTypeStatsTracker::format_bytes(500), "500 B");
        assert_eq!(FileTypeStatsTracker::format_bytes(1024), "1.00 KB");
        assert_eq!(FileTypeStatsTracker::format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(
            FileTypeStatsTracker::format_bytes(1024 * 1024 * 1024),
            "1.00 GB"
        );
        assert_eq!(
            FileTypeStatsTracker::format_bytes(1024 * 1024 * 1024 * 1024),
            "1.00 TB"
        );
    }

    #[tokio::test]
    async fn test_format_summary() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("video.mp4", 1024 * 1024, 10)
            .await
            .unwrap();
        tracker
            .record_download("audio.mp3", 512 * 1024, 5)
            .await
            .unwrap();

        let summary = tracker.format_summary().await;
        assert!(summary.contains("Download File Type Statistics"));
        assert!(summary.contains("Total Downloads: 2"));
        assert!(summary.contains("By Category"));
    }

    #[tokio::test]
    async fn test_unknown_extension() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        tracker
            .record_download("file.unknownext", 1000, 10)
            .await
            .unwrap();

        let stats = tracker.get_extension_stats("unknownext").await.unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.extension, "unknownext");
    }

    #[tokio::test]
    async fn test_config_persistence() {
        let tracker = FileTypeStatsTracker::new(FileStatsConfig::default());

        let new_config = FileStatsConfig {
            enabled: false,
            max_extensions: 50,
            track_extensions: true,
            track_categories: false,
        };

        tracker.set_config(new_config.clone()).await.unwrap();

        let loaded_config = tracker.get_config().await;
        assert_eq!(loaded_config.enabled, false);
        assert_eq!(loaded_config.max_extensions, 50);
        assert_eq!(loaded_config.track_categories, false);

        // Cleanup
        let _ = tokio::fs::remove_file(&tracker.config_file).await;
    }

    #[test]
    fn test_file_category_display() {
        assert_eq!(format!("{}", FileCategory::Video), "video");
        assert_eq!(format!("{}", FileCategory::Audio), "audio");
        assert_eq!(format!("{}", FileCategory::Document), "document");
        assert_eq!(format!("{}", FileCategory::Archive), "archive");
        assert_eq!(format!("{}", FileCategory::Image), "image");
        assert_eq!(format!("{}", FileCategory::Executable), "executable");
        assert_eq!(format!("{}", FileCategory::Other), "other");
    }
}
