//! Watch folder (monitor directory) support for automatic URL import.
//!
//! Users configure directories to monitor. When text files containing URLs
//! (HTTP, HTTPS, FTP, ed2k, magnet) are placed in a watched directory, the
//! system automatically extracts those URLs and creates download tasks.
//!
//! Features:
//! - Multiple watch folders with optional sub-directory recursion
//! - Supported file extensions filter (default: `.txt`, `.url`, `.m3u`, `.dl`)
//! - URL extraction using the existing `link_extractor` module
//! - Automatic file cleanup after processing (optional, configurable)
//! - Deduplication via URL dedup system
//! - Persistence to `watch_folders.json`

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;
use tracing::{debug, warn};

/// Errors from watch folder operations.
#[derive(Error, Debug)]
pub enum WatchFolderError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Watch folder not found: {0}")]
    NotFound(String),
    #[error("Watch folder already exists: {0}")]
    AlreadyExists(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

/// Configuration for a single watch folder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderEntry {
    /// Unique identifier for this watch folder.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Directory path to monitor.
    pub path: PathBuf,
    /// Whether to recursively scan sub-directories.
    pub recursive: bool,
    /// File extensions to process (lowercase, without dot). Empty means all files.
    pub extensions: Vec<String>,
    /// Whether to delete/move files after successful processing.
    pub cleanup_after: bool,
    /// Optional sub-directory to move processed files into (relative to watch path).
    /// If `cleanup_after` is true and this is set, files are moved here instead of deleted.
    pub processed_subdir: Option<String>,
    /// Whether this watch folder is enabled.
    pub enabled: bool,
    /// Optional tags to apply to auto-imported downloads.
    pub tags: Vec<String>,
    /// Optional group to assign to auto-imported downloads.
    pub group: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last scan timestamp.
    pub last_scanned: Option<DateTime<Utc>>,
}

/// Summary of a watch folder scan.
#[derive(Debug, Clone, Default)]
pub struct WatchFolderScanResult {
    /// Number of files scanned.
    pub files_scanned: usize,
    /// Number of URLs extracted.
    pub urls_extracted: usize,
    /// URLs that were already known (deduplicated).
    pub urls_deduped: usize,
    /// Errors encountered during scan.
    pub errors: Vec<String>,
}

/// Persisted state for all watch folders.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WatchFolderState {
    pub folders: Vec<WatchFolderEntry>,
    #[serde(default)]
    pub auto_scan_config: Option<WatchFolderAutoScanConfig>,
}

/// Configuration for automatic watch folder scanning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderAutoScanConfig {
    /// Whether auto-scanning is enabled.
    pub enabled: bool,
    /// Scan interval in seconds (default: 300 = 5 minutes).
    pub interval_secs: u64,
    /// Last auto-scan timestamp.
    #[serde(default)]
    pub last_auto_scan: Option<DateTime<Utc>>,
}

impl Default for WatchFolderAutoScanConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: 300,
            last_auto_scan: None,
        }
    }
}

/// Default file extensions to watch.
pub fn default_extensions() -> Vec<String> {
    vec![
        "txt".into(),
        "url".into(),
        "m3u".into(),
        "dl".into(),
        "nfo".into(),
    ]
}

/// Generate a unique ID for a watch folder.
fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("wf-{:x}", ts)
}

/// Persistence functions.

pub fn save_watch_folders(path: &Path, state: &WatchFolderState) -> Result<(), WatchFolderError> {
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(state)?;
    std::fs::write(&tmp, json.as_bytes())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn load_watch_folders(path: &Path) -> Result<WatchFolderState, WatchFolderError> {
    if !path.exists() {
        return Ok(WatchFolderState::default());
    }
    let data = std::fs::read_to_string(path)?;
    let state: WatchFolderState = serde_json::from_str(&data)?;
    Ok(state)
}

/// Extract URLs from a text file content.
///
/// Uses pattern matching for common URL schemes:
/// http://, https://, ftp://, ed2k://, magnet:?
pub fn extract_urls_from_text(text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut seen = HashSet::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Try to extract URLs from each line
        for word in line.split_whitespace() {
            if let Some(url) = extract_url_from_token(word) {
                if seen.insert(url.clone()) {
                    urls.push(url);
                }
            }
        }
    }
    urls
}

/// Try to extract a URL from a token (word).
/// Handles cases where URL might be surrounded by quotes or other punctuation.
fn extract_url_from_token(token: &str) -> Option<String> {
    // Strip surrounding quotes/brackets
    let trimmed = token.trim_matches(|c: char| {
        c == '"'
            || c == '\''
            || c == '<'
            || c == '>'
            || c == '('
            || c == ')'
            || c == '['
            || c == ']'
            || c == ','
    });

    // Check for known URL schemes
    let schemes = ["https://", "http://", "ftp://", "ed2k://", "magnet:?"];
    for scheme in &schemes {
        if let Some(pos) = trimmed.find(scheme) {
            let url = &trimmed[pos..];
            // For magnet links, take until whitespace or end
            if scheme.starts_with("magnet:") {
                let end = url
                    .find(|c: char| c.is_whitespace() || c == '"')
                    .unwrap_or(url.len());
                let url = &url[..end];
                if url.len() > 10 {
                    return Some(url.to_string());
                }
            } else {
                // For other URLs, take until whitespace or end
                let end = url
                    .find(|c: char| {
                        c.is_whitespace() || c == '"' || c == '\'' || c == '<' || c == '>'
                    })
                    .unwrap_or(url.len());
                let url = &url[..end];
                // Remove trailing punctuation that's unlikely part of URL
                let url = url.trim_end_matches(|c: char| {
                    c == '.' || c == ',' || c == ';' || c == ')' || c == ']'
                });
                if url.len() > scheme.len() + 1 {
                    return Some(url.to_string());
                }
            }
        }
    }
    None
}

/// Check if a file has a matching extension.
fn matches_extension(path: &Path, extensions: &[String]) -> bool {
    if extensions.is_empty() {
        return true; // Empty means match all
    }
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_lowercase();
        extensions.iter().any(|e| e.to_lowercase() == ext_lower)
    } else {
        false
    }
}

/// Scan a directory and extract URLs from matching files.
pub async fn scan_directory(
    dir: &Path,
    recursive: bool,
    extensions: &[String],
) -> Result<(Vec<String>, Vec<PathBuf>, Vec<String>), WatchFolderError> {
    let mut urls = Vec::new();
    let mut files = Vec::new();
    let mut errors = Vec::new();
    let mut seen = HashSet::new();

    if !dir.exists() || !dir.is_dir() {
        return Ok((urls, files, errors));
    }

    let mut entries = match fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) => {
            errors.push(format!("Failed to read directory {}: {}", dir.display(), e));
            return Ok((urls, files, errors));
        }
    };

    let mut sorted_entries: Vec<tokio::fs::DirEntry> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        sorted_entries.push(entry);
    }
    sorted_entries.sort_by_key(|a| a.file_name());

    for entry in sorted_entries {
        let path = entry.path();
        let ft = match entry.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };

        if ft.is_dir() && recursive {
            let sub_result = Box::pin(scan_directory(&path, recursive, extensions));
            if let Ok((sub_urls, sub_files, sub_errors)) = sub_result.await {
                for u in sub_urls {
                    if seen.insert(u.clone()) {
                        urls.push(u);
                    }
                }
                files.extend(sub_files);
                errors.extend(sub_errors);
            }
            continue;
        }

        if !ft.is_file() {
            continue;
        }

        if !matches_extension(&path, extensions) {
            continue;
        }

        let content: String = match fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                debug!("Could not read {} as text: {}", path.display(), e);
                errors.push(format!("Failed to read {}: {}", path.display(), e));
                continue;
            }
        };
        let extracted = extract_urls_from_text(&content);
        if !extracted.is_empty() {
            for u in extracted {
                if seen.insert(u.clone()) {
                    urls.push(u);
                }
            }
            files.push(path);
        }
    }

    Ok((urls, files, errors))
}

/// Watch folder manager that handles configuration and scanning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderManager {
    /// All configured watch folders.
    pub folders: Vec<WatchFolderEntry>,
    /// Set of already-processed file paths (to avoid re-processing).
    #[serde(default)]
    pub processed_files: HashSet<String>,
    /// Auto-scan configuration.
    #[serde(default)]
    pub auto_scan_config: WatchFolderAutoScanConfig,
}

impl Default for WatchFolderManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchFolderManager {
    pub fn new() -> Self {
        Self {
            folders: Vec::new(),
            processed_files: HashSet::new(),
            auto_scan_config: WatchFolderAutoScanConfig::default(),
        }
    }

    /// Add a new watch folder.
    pub fn add_folder(
        &mut self,
        name: String,
        path: PathBuf,
        recursive: bool,
        extensions: Vec<String>,
        cleanup_after: bool,
        tags: Vec<String>,
        group: Option<String>,
    ) -> Result<String, WatchFolderError> {
        if !path.exists() {
            return Err(WatchFolderError::InvalidPath(format!(
                "Directory does not exist: {}",
                path.display()
            )));
        }
        if !path.is_dir() {
            return Err(WatchFolderError::InvalidPath(format!(
                "Path is not a directory: {}",
                path.display()
            )));
        }

        let canonical = path
            .canonicalize()
            .map_err(|e| WatchFolderError::InvalidPath(format!("{}: {}", path.display(), e)))?;

        // Check for duplicates
        if self.folders.iter().any(|f| f.path == canonical) {
            return Err(WatchFolderError::AlreadyExists(
                canonical.display().to_string(),
            ));
        }

        let id = generate_id();
        let entry = WatchFolderEntry {
            id: id.clone(),
            name,
            path: canonical,
            recursive,
            extensions: if extensions.is_empty() {
                default_extensions()
            } else {
                extensions
            },
            cleanup_after,
            processed_subdir: None,
            enabled: true,
            tags,
            group,
            created_at: Utc::now(),
            last_scanned: None,
        };
        self.folders.push(entry);
        Ok(id)
    }

    /// Remove a watch folder by ID.
    pub fn remove_folder(&mut self, id: &str) -> Result<(), WatchFolderError> {
        let pos = self
            .folders
            .iter()
            .position(|f| f.id == id)
            .ok_or_else(|| WatchFolderError::NotFound(id.to_string()))?;
        self.folders.remove(pos);
        Ok(())
    }

    /// Enable or disable a watch folder.
    pub fn set_enabled(&mut self, id: &str, enabled: bool) -> Result<(), WatchFolderError> {
        let folder = self
            .folders
            .iter_mut()
            .find(|f| f.id == id)
            .ok_or_else(|| WatchFolderError::NotFound(id.to_string()))?;
        folder.enabled = enabled;
        Ok(())
    }

    /// Get a list of all watch folders.
    pub fn list_folders(&self) -> &[WatchFolderEntry] {
        &self.folders
    }

    /// Get a single watch folder by ID.
    pub fn get_folder(&self, id: &str) -> Option<&WatchFolderEntry> {
        self.folders.iter().find(|f| f.id == id)
    }

    /// Scan a specific watch folder and return extracted URLs.
    pub async fn scan_folder(
        &mut self,
        id: &str,
    ) -> Result<WatchFolderScanResult, WatchFolderError> {
        let folder_idx = self
            .folders
            .iter()
            .position(|f| f.id == id)
            .ok_or_else(|| WatchFolderError::NotFound(id.to_string()))?;

        let folder = &self.folders[folder_idx];
        let (urls, files, errors) =
            scan_directory(&folder.path, folder.recursive, &folder.extensions).await?;

        // Track processed files
        for f in &files {
            self.processed_files.insert(f.display().to_string());
        }

        // Update last_scanned
        self.folders[folder_idx].last_scanned = Some(Utc::now());

        Ok(WatchFolderScanResult {
            files_scanned: files.len(),
            urls_extracted: urls.len(),
            urls_deduped: 0, // Dedup happens at DownloadManager level
            errors,
        })
    }

    /// Scan all enabled watch folders.
    pub async fn scan_all(&mut self) -> Vec<(String, WatchFolderScanResult)> {
        let mut results = Vec::new();
        let ids: Vec<(String, bool)> = self
            .folders
            .iter()
            .filter(|f| f.enabled)
            .map(|f| (f.id.clone(), f.recursive))
            .collect();

        for (id, _) in ids {
            match self.scan_folder(&id).await {
                Ok(result) => results.push((id, result)),
                Err(e) => {
                    warn!("Watch folder scan failed for {}: {}", id, e);
                }
            }
        }
        results
    }

    /// Get URLs from a scan result, with folder metadata for task creation.
    pub async fn scan_and_collect_urls(&mut self) -> Vec<WatchFolderUrl> {
        let mut collected = Vec::new();
        let ids: Vec<String> = self
            .folders
            .iter()
            .filter(|f| f.enabled)
            .map(|f| f.id.clone())
            .collect();

        for id in ids {
            let folder = match self.get_folder(&id) {
                Some(f) => f.clone(),
                None => continue,
            };

            match self.scan_folder(&id).await {
                Ok(result) => {
                    if result.urls_extracted > 0 {
                        // Re-scan to get actual URLs (scan_folder doesn't return them)
                        let (urls, files, _errors) =
                            scan_directory(&folder.path, folder.recursive, &folder.extensions)
                                .await
                                .unwrap_or_default();

                        for url in urls {
                            collected.push(WatchFolderUrl {
                                url,
                                folder_id: folder.id.clone(),
                                tags: folder.tags.clone(),
                                group: folder.group.clone(),
                            });
                        }

                        // Handle cleanup
                        if folder.cleanup_after {
                            for file in &files {
                                if let Some(subdir) = &folder.processed_subdir {
                                    let dest_dir = folder.path.join(subdir);
                                    let _ = tokio::fs::create_dir_all(&dest_dir).await;
                                    let dest = dest_dir.join(file.file_name().unwrap_or_default());
                                    let _ = tokio::fs::rename(file, &dest).await;
                                } else {
                                    let _ = tokio::fs::remove_file(file).await;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Watch folder scan failed for {}: {}", id, e);
                }
            }
        }
        collected
    }

    /// Save state to disk.
    pub fn save(&self, path: &Path) -> Result<(), WatchFolderError> {
        let state = WatchFolderState {
            folders: self.folders.clone(),
            auto_scan_config: Some(self.auto_scan_config.clone()),
        };
        save_watch_folders(path, &state)
    }

    /// Load state from disk.
    pub fn load(path: &Path) -> Result<Self, WatchFolderError> {
        let state = load_watch_folders(path)?;
        Ok(Self {
            folders: state.folders,
            processed_files: HashSet::new(),
            auto_scan_config: state.auto_scan_config.unwrap_or_default(),
        })
    }

    /// Set auto-scan configuration.
    pub fn set_auto_scan_config(&mut self, config: WatchFolderAutoScanConfig) {
        self.auto_scan_config = config;
    }

    /// Get auto-scan configuration.
    pub fn get_auto_scan_config(&self) -> &WatchFolderAutoScanConfig {
        &self.auto_scan_config
    }

    /// Check if auto-scan is due (interval has elapsed).
    pub fn is_auto_scan_due(&self) -> bool {
        if !self.auto_scan_config.enabled {
            return false;
        }
        match self.auto_scan_config.last_auto_scan {
            None => true,
            Some(last) => {
                let elapsed = Utc::now().signed_duration_since(last).num_seconds() as u64;
                elapsed >= self.auto_scan_config.interval_secs
            }
        }
    }

    /// Update last auto-scan timestamp to now.
    pub fn mark_auto_scan_complete(&mut self) {
        self.auto_scan_config.last_auto_scan = Some(Utc::now());
    }

    /// Get summary of all watch folders.
    pub fn summary(&self) -> WatchFolderSummary {
        let total = self.folders.len();
        let enabled = self.folders.iter().filter(|f| f.enabled).count();
        WatchFolderSummary {
            total_folders: total,
            enabled_folders: enabled,
            folders: self
                .folders
                .iter()
                .map(|f| WatchFolderInfo {
                    id: f.id.clone(),
                    name: f.name.clone(),
                    path: f.path.display().to_string(),
                    enabled: f.enabled,
                    recursive: f.recursive,
                    extensions: f.extensions.clone(),
                    last_scanned: f.last_scanned,
                })
                .collect(),
        }
    }
}

/// A URL extracted from a watch folder, with metadata for task creation.
#[derive(Debug, Clone)]
pub struct WatchFolderUrl {
    pub url: String,
    pub folder_id: String,
    pub tags: Vec<String>,
    pub group: Option<String>,
}

/// Summary of all watch folders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderSummary {
    pub total_folders: usize,
    pub enabled_folders: usize,
    pub folders: Vec<WatchFolderInfo>,
}

/// Info about a single watch folder for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderInfo {
    pub id: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub recursive: bool,
    pub extensions: Vec<String>,
    pub last_scanned: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_auto_scan_config_default() {
        let config = WatchFolderAutoScanConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.interval_secs, 300);
        assert!(config.last_auto_scan.is_none());
    }

    #[test]
    fn test_auto_scan_due_when_disabled() {
        let mgr = WatchFolderManager::new();
        assert!(!mgr.is_auto_scan_due());
    }

    #[test]
    fn test_auto_scan_due_when_enabled_no_last_scan() {
        let mut mgr = WatchFolderManager::new();
        mgr.set_auto_scan_config(WatchFolderAutoScanConfig {
            enabled: true,
            interval_secs: 300,
            last_auto_scan: None,
        });
        assert!(mgr.is_auto_scan_due());
    }

    #[test]
    fn test_auto_scan_due_when_interval_elapsed() {
        let mut mgr = WatchFolderManager::new();
        let past = Utc::now() - chrono::Duration::seconds(400);
        mgr.set_auto_scan_config(WatchFolderAutoScanConfig {
            enabled: true,
            interval_secs: 300,
            last_auto_scan: Some(past),
        });
        assert!(mgr.is_auto_scan_due());
    }

    #[test]
    fn test_auto_scan_not_due_when_recent() {
        let mut mgr = WatchFolderManager::new();
        let recent = Utc::now() - chrono::Duration::seconds(100);
        mgr.set_auto_scan_config(WatchFolderAutoScanConfig {
            enabled: true,
            interval_secs: 300,
            last_auto_scan: Some(recent),
        });
        assert!(!mgr.is_auto_scan_due());
    }

    #[test]
    fn test_mark_auto_scan_complete() {
        let mut mgr = WatchFolderManager::new();
        assert!(mgr.auto_scan_config.last_auto_scan.is_none());
        mgr.mark_auto_scan_complete();
        assert!(mgr.auto_scan_config.last_auto_scan.is_some());
    }

    #[test]
    fn test_auto_scan_config_persistence() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("watch_folders.json");

        let mut mgr = WatchFolderManager::new();
        mgr.set_auto_scan_config(WatchFolderAutoScanConfig {
            enabled: true,
            interval_secs: 600,
            last_auto_scan: Some(Utc::now()),
        });

        mgr.save(&config_path).unwrap();

        let loaded = WatchFolderManager::load(&config_path).unwrap();
        assert!(loaded.auto_scan_config.enabled);
        assert_eq!(loaded.auto_scan_config.interval_secs, 600);
        assert!(loaded.auto_scan_config.last_auto_scan.is_some());
    }

    #[test]
    fn test_extract_urls_from_text_basic() {
        let text = "Download this:\nhttps://example.com/file.zip\n\nAnd this:\nhttp://mirror.example.com/data.tar.gz";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://example.com/file.zip");
        assert_eq!(urls[1], "http://mirror.example.com/data.tar.gz");
    }

    #[test]
    fn test_extract_urls_magnet() {
        let text = "magnet:?xt=urn:btih:abc123&dn=test\nsome other text";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with("magnet:?"));
    }

    #[test]
    fn test_extract_urls_ed2k() {
        let text = "ed2k://|file|test.avi|1234567|abcdef|/";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with("ed2k://"));
    }

    #[test]
    fn test_extract_urls_dedup() {
        let text = "https://example.com/file.zip\nhttps://example.com/file.zip";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
    }

    #[test]
    fn test_extract_urls_skip_comments() {
        let text = "# This is a comment\nhttps://example.com/file.zip";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "https://example.com/file.zip");
    }

    #[test]
    fn test_extract_urls_from_mixed_text() {
        let text = "Check out these links:\n- https://example.com/a.zip (main mirror)\n- http://backup.example.com/b.zip [alternative]";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://example.com/a.zip");
        assert_eq!(urls[1], "http://backup.example.com/b.zip");
    }

    #[test]
    fn test_extract_urls_ftp() {
        let text = "ftp://files.example.com/pub/data.csv";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "ftp://files.example.com/pub/data.csv");
    }

    #[test]
    fn test_extract_urls_multiple_per_line() {
        let text = "mirror1: https://a.com/f.zip mirror2: https://b.com/f.zip";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 2);
    }

    #[test]
    fn test_extract_urls_no_urls() {
        let text = "Just some text with no links.";
        let urls = extract_urls_from_text(text);
        assert!(urls.is_empty());
    }

    #[test]
    fn test_extract_urls_empty() {
        let urls = extract_urls_from_text("");
        assert!(urls.is_empty());
    }

    #[test]
    fn test_matches_extension() {
        assert!(matches_extension(
            Path::new("test.txt"),
            &["txt".into(), "url".into()]
        ));
        assert!(matches_extension(Path::new("test.URL"), &["url".into()]));
        assert!(!matches_extension(Path::new("test.exe"), &["txt".into()]));
        assert!(matches_extension(Path::new("test.exe"), &[])); // empty = all
    }

    #[tokio::test]
    async fn test_scan_directory() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("links.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        writeln!(f, "https://example.com/file1.zip").unwrap();
        writeln!(f, "https://example.com/file2.zip").unwrap();

        let (urls, files, errors) = scan_directory(dir.path(), false, &["txt".into()])
            .await
            .unwrap();
        assert!(errors.is_empty());
        assert_eq!(urls.len(), 2);
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_scan_directory_recursive() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();

        let mut f = std::fs::File::create(sub.join("links.txt")).unwrap();
        writeln!(f, "https://example.com/deep.zip").unwrap();

        // Non-recursive should find nothing
        let (urls, _, _) = scan_directory(dir.path(), false, &["txt".into()])
            .await
            .unwrap();
        assert_eq!(urls.len(), 0);

        // Recursive should find it
        let (urls, _, _) = scan_directory(dir.path(), true, &["txt".into()])
            .await
            .unwrap();
        assert_eq!(urls.len(), 1);
    }

    #[tokio::test]
    async fn test_scan_directory_extension_filter() {
        let dir = TempDir::new().unwrap();

        let mut f = std::fs::File::create(dir.path().join("links.txt")).unwrap();
        writeln!(f, "https://example.com/a.zip").unwrap();

        let mut f = std::fs::File::create(dir.path().join("links.exe")).unwrap();
        writeln!(f, "https://example.com/b.zip").unwrap();

        let (urls, _, _) = scan_directory(dir.path(), false, &["txt".into()])
            .await
            .unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "https://example.com/a.zip");
    }

    #[test]
    fn test_watch_folder_manager_add_remove() {
        let dir = TempDir::new().unwrap();
        let mut mgr = WatchFolderManager::new();

        let id = mgr
            .add_folder(
                "Test".into(),
                dir.path().to_path_buf(),
                false,
                vec!["txt".into()],
                false,
                vec!["auto".into()],
                None,
            )
            .unwrap();

        assert_eq!(mgr.list_folders().len(), 1);
        assert!(mgr.get_folder(&id).is_some());

        mgr.remove_folder(&id).unwrap();
        assert_eq!(mgr.list_folders().len(), 0);
    }

    #[test]
    fn test_watch_folder_manager_duplicate_path() {
        let dir = TempDir::new().unwrap();
        let mut mgr = WatchFolderManager::new();

        mgr.add_folder(
            "Test".into(),
            dir.path().to_path_buf(),
            false,
            vec![],
            false,
            vec![],
            None,
        )
        .unwrap();

        let result = mgr.add_folder(
            "Test2".into(),
            dir.path().to_path_buf(),
            false,
            vec![],
            false,
            vec![],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_watch_folder_manager_invalid_path() {
        let mut mgr = WatchFolderManager::new();
        let result = mgr.add_folder(
            "Test".into(),
            PathBuf::from("/nonexistent/path/12345"),
            false,
            vec![],
            false,
            vec![],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_watch_folder_manager_set_enabled() {
        let dir = TempDir::new().unwrap();
        let mut mgr = WatchFolderManager::new();

        let id = mgr
            .add_folder(
                "Test".into(),
                dir.path().to_path_buf(),
                false,
                vec![],
                false,
                vec![],
                None,
            )
            .unwrap();

        assert!(mgr.get_folder(&id).unwrap().enabled);
        mgr.set_enabled(&id, false).unwrap();
        assert!(!mgr.get_folder(&id).unwrap().enabled);
    }

    #[tokio::test]
    async fn test_watch_folder_manager_scan() {
        let dir = TempDir::new().unwrap();
        let mut f = std::fs::File::create(dir.path().join("urls.txt")).unwrap();
        writeln!(f, "https://example.com/test.zip").unwrap();

        let mut mgr = WatchFolderManager::new();
        let id = mgr
            .add_folder(
                "Test".into(),
                dir.path().to_path_buf(),
                false,
                vec!["txt".into()],
                false,
                vec![],
                None,
            )
            .unwrap();

        let result = mgr.scan_folder(&id).await.unwrap();
        assert_eq!(result.urls_extracted, 1);
        assert_eq!(result.files_scanned, 1);
    }

    #[tokio::test]
    async fn test_watch_folder_cleanup_delete() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("urls.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        writeln!(f, "https://example.com/test.zip").unwrap();

        let mut mgr = WatchFolderManager::new();
        let id = mgr
            .add_folder(
                "Test".into(),
                dir.path().to_path_buf(),
                false,
                vec!["txt".into()],
                true, // cleanup_after = true
                vec![],
                None,
            )
            .unwrap();

        let urls = mgr.scan_and_collect_urls().await;
        assert_eq!(urls.len(), 1);
        // File should be deleted
        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn test_watch_folder_cleanup_move() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("urls.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        writeln!(f, "https://example.com/test.zip").unwrap();

        let mut mgr = WatchFolderManager::new();
        let id = mgr
            .add_folder(
                "Test".into(),
                dir.path().to_path_buf(),
                false,
                vec!["txt".into()],
                true, // cleanup_after = true
                vec![],
                None,
            )
            .unwrap();

        // Set processed_subdir
        let folder = mgr.folders.iter_mut().find(|f| f.id == id).unwrap();
        folder.processed_subdir = Some("processed".into());

        let urls = mgr.scan_and_collect_urls().await;
        assert_eq!(urls.len(), 1);
        // Original file should be gone
        assert!(!file_path.exists());
        // Should be in processed subdir
        assert!(dir.path().join("processed/urls.txt").exists());
    }

    #[test]
    fn test_watch_folder_persistence() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("watch_folders.json");

        let mut mgr = WatchFolderManager::new();
        let watch_dir = TempDir::new().unwrap();
        mgr.add_folder(
            "Downloads".into(),
            watch_dir.path().to_path_buf(),
            true,
            vec!["txt".into(), "url".into()],
            false,
            vec!["auto-import".into()],
            Some("watched".into()),
        )
        .unwrap();

        mgr.save(&config_path).unwrap();

        let loaded = WatchFolderManager::load(&config_path).unwrap();
        assert_eq!(loaded.list_folders().len(), 1);
        assert_eq!(loaded.list_folders()[0].name, "Downloads");
        assert!(loaded.list_folders()[0].recursive);
        assert_eq!(loaded.list_folders()[0].tags, vec!["auto-import"]);
        assert_eq!(loaded.list_folders()[0].group, Some("watched".into()));
    }

    #[test]
    fn test_watch_folder_summary() {
        let dir = TempDir::new().unwrap();
        let mut mgr = WatchFolderManager::new();
        mgr.add_folder(
            "A".into(),
            dir.path().to_path_buf(),
            false,
            vec![],
            false,
            vec![],
            None,
        )
        .unwrap();
        mgr.add_folder(
            "B".into(),
            dir.path().to_path_buf(),
            false,
            vec![],
            false,
            vec![],
            None,
        )
        .unwrap_err(); // duplicate

        let summary = mgr.summary();
        assert_eq!(summary.total_folders, 1);
        assert_eq!(summary.enabled_folders, 1);
    }

    #[test]
    fn test_default_extensions() {
        let ext = default_extensions();
        assert!(ext.contains(&"txt".to_string()));
        assert!(ext.contains(&"url".to_string()));
        assert!(ext.contains(&"m3u".to_string()));
    }

    #[test]
    fn test_extract_url_from_token_quoted() {
        assert_eq!(
            extract_url_from_token("\"https://example.com/file.zip\""),
            Some("https://example.com/file.zip".into())
        );
        assert_eq!(
            extract_url_from_token("<https://example.com/file.zip>"),
            Some("https://example.com/file.zip".into())
        );
    }

    #[test]
    fn test_extract_url_from_token_no_url() {
        assert_eq!(extract_url_from_token("just-a-word"), None);
        assert_eq!(extract_url_from_token(""), None);
    }
}
