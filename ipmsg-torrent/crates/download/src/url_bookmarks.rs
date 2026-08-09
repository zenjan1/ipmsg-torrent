//! Download URL Bookmarks (Phase 81)
//!
//! Named collections of URLs that can be quickly batch-imported as download tasks.
//! Unlike presets (which bundle task configuration), bookmarks are URL collections
//! that users can save and re-import on demand.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Error type for bookmark operations
#[derive(Debug)]
pub enum BookmarkError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    NotFound(String),
    EmptyBookmark(String),
    DuplicateName(String),
}

impl std::fmt::Display for BookmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
            Self::NotFound(name) => write!(f, "bookmark not found: {name}"),
            Self::EmptyBookmark(name) => write!(f, "bookmark has no URLs: {name}"),
            Self::DuplicateName(name) => write!(f, "bookmark name already exists: {name}"),
        }
    }
}

impl std::error::Error for BookmarkError {}

impl From<std::io::Error> for BookmarkError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for BookmarkError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

/// A single bookmarked URL with optional metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookmarkEntry {
    /// The URL to download
    pub url: String,
    /// Optional display name (defaults to URL)
    #[serde(default)]
    pub name: Option<String>,
    /// Optional tags to apply when imported
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional group to assign when imported
    #[serde(default)]
    pub group: Option<String>,
    /// Optional priority override (1-4)
    #[serde(default)]
    pub priority: Option<u8>,
}

impl BookmarkEntry {
    /// Create a simple URL-only bookmark entry
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            name: None,
            tags: Vec::new(),
            group: None,
            priority: None,
        }
    }

    /// Create a bookmark entry with a display name
    pub fn with_name(url: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            name: Some(name.into()),
            tags: Vec::new(),
            group: None,
            priority: None,
        }
    }

    /// Display label: name if set, otherwise URL
    pub fn display_label(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.url)
    }
}

/// A named collection of bookmarked URLs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UrlBookmark {
    /// Unique identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// The bookmarked URLs
    pub entries: Vec<BookmarkEntry>,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// When this bookmark was created
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    /// When this bookmark was last used for import
    #[serde(default)]
    pub last_used_at: Option<DateTime<Utc>>,
    /// Number of times this bookmark has been imported
    #[serde(default)]
    pub import_count: u32,
    /// Whether this bookmark is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl UrlBookmark {
    /// Create a new bookmark collection
    pub fn new(name: impl Into<String>, entries: Vec<BookmarkEntry>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            entries,
            description: None,
            created_at: Utc::now(),
            last_used_at: None,
            import_count: 0,
            enabled: true,
        }
    }

    /// Number of URLs in this bookmark
    pub fn url_count(&self) -> usize {
        self.entries.len()
    }

    /// Get all URLs as a simple string list
    pub fn urls(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.url.as_str()).collect()
    }
}

/// Summary of a bookmark for listing purposes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookmarkSummary {
    pub id: String,
    pub name: String,
    pub url_count: usize,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub import_count: u32,
    pub enabled: bool,
}

impl From<&UrlBookmark> for BookmarkSummary {
    fn from(bm: &UrlBookmark) -> Self {
        Self {
            id: bm.id.clone(),
            name: bm.name.clone(),
            url_count: bm.entries.len(),
            description: bm.description.clone(),
            created_at: bm.created_at,
            last_used_at: bm.last_used_at,
            import_count: bm.import_count,
            enabled: bm.enabled,
        }
    }
}

/// Result of importing a bookmark
#[derive(Debug, Clone)]
pub struct BookmarkImportResult {
    pub bookmark_name: String,
    pub urls_imported: usize,
    pub urls_skipped: usize,
    pub urls: Vec<String>,
}

// --- Persistence ---

const BOOKMARKS_FILE: &str = "url_bookmarks.json";

fn bookmarks_path(data_dir: &Path) -> PathBuf {
    data_dir.join(BOOKMARKS_FILE)
}

/// Save bookmarks to disk (atomic write)
pub fn save_bookmarks(bookmarks: &[UrlBookmark], data_dir: &Path) -> Result<(), BookmarkError> {
    let path = bookmarks_path(data_dir);
    let json = serde_json::to_string_pretty(bookmarks)?;
    // Atomic write: write to temp file then rename
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load bookmarks from disk
pub fn load_bookmarks(data_dir: &Path) -> Result<Vec<UrlBookmark>, BookmarkError> {
    let path = bookmarks_path(data_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(&path)?;
    let bookmarks: Vec<UrlBookmark> = serde_json::from_str(&content)?;
    Ok(bookmarks)
}

// --- Bookmark Manager operations ---

/// Add a new bookmark collection
pub fn add_bookmark(
    bookmarks: &mut Vec<UrlBookmark>,
    name: &str,
    entries: Vec<BookmarkEntry>,
) -> Result<UrlBookmark, BookmarkError> {
    // Check for duplicate name (case-insensitive)
    if bookmarks.iter().any(|b| b.name.eq_ignore_ascii_case(name)) {
        return Err(BookmarkError::DuplicateName(name.to_string()));
    }

    let bookmark = UrlBookmark::new(name, entries);
    bookmarks.push(bookmark.clone());
    Ok(bookmark)
}

/// Remove a bookmark by name
pub fn remove_bookmark(bookmarks: &mut Vec<UrlBookmark>, name: &str) -> Result<(), BookmarkError> {
    let idx = bookmarks
        .iter()
        .position(|b| b.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| BookmarkError::NotFound(name.to_string()))?;
    bookmarks.remove(idx);
    Ok(())
}

/// Get a bookmark by name
pub fn get_bookmark<'a>(bookmarks: &'a [UrlBookmark], name: &str) -> Option<&'a UrlBookmark> {
    bookmarks.iter().find(|b| b.name.eq_ignore_ascii_case(name))
}

/// Get a bookmark mutably by name
pub fn get_bookmark_mut<'a>(
    bookmarks: &'a mut [UrlBookmark],
    name: &str,
) -> Option<&'a mut UrlBookmark> {
    bookmarks
        .iter_mut()
        .find(|b| b.name.eq_ignore_ascii_case(name))
}

/// Add URLs to an existing bookmark
pub fn add_urls_to_book(
    bookmark: &mut UrlBookmark,
    urls: Vec<BookmarkEntry>,
) -> Result<(), BookmarkError> {
    if urls.is_empty() {
        return Ok(());
    }
    // Deduplicate: skip URLs already in the bookmark
    for entry in urls {
        if !bookmark.entries.iter().any(|e| e.url == entry.url) {
            bookmark.entries.push(entry);
        }
    }
    Ok(())
}

/// Remove a URL from a bookmark by URL string
pub fn remove_url_from_bookmark(
    bookmark: &mut UrlBookmark,
    url: &str,
) -> Result<(), BookmarkError> {
    let before = bookmark.entries.len();
    bookmark.entries.retain(|e| e.url != url);
    if bookmark.entries.len() == before {
        return Err(BookmarkError::NotFound(url.to_string()));
    }
    Ok(())
}

/// Mark a bookmark as used (updates last_used_at and import_count)
pub fn mark_bookmark_used(bookmark: &mut UrlBookmark) {
    bookmark.last_used_at = Some(Utc::now());
    bookmark.import_count += 1;
}

/// Parse a multi-line URL string into bookmark entries
pub fn parse_url_list(input: &str) -> Vec<BookmarkEntry> {
    input
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            // Support "name|url" or "name url" format, or just "url"
            if let Some((name, url)) = line.split_once('|') {
                let name = name.trim();
                let url = url.trim();
                if name.is_empty() {
                    BookmarkEntry::new(url)
                } else {
                    BookmarkEntry::with_name(url, name)
                }
            } else {
                BookmarkEntry::new(line)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_bookmark_entry_new() {
        let entry = BookmarkEntry::new("https://example.com/file.zip");
        assert_eq!(entry.url, "https://example.com/file.zip");
        assert!(entry.name.is_none());
        assert!(entry.tags.is_empty());
        assert_eq!(entry.display_label(), "https://example.com/file.zip");
    }

    #[test]
    fn test_bookmark_entry_with_name() {
        let entry = BookmarkEntry::with_name("https://example.com/f.zip", "My File");
        assert_eq!(entry.url, "https://example.com/f.zip");
        assert_eq!(entry.name.as_deref(), Some("My File"));
        assert_eq!(entry.display_label(), "My File");
    }

    #[test]
    fn test_url_bookmark_new() {
        let entries = vec![
            BookmarkEntry::new("https://a.com/1.zip"),
            BookmarkEntry::new("https://b.com/2.zip"),
        ];
        let bm = UrlBookmark::new("Test Collection", entries);
        assert_eq!(bm.name, "Test Collection");
        assert_eq!(bm.url_count(), 2);
        assert!(bm.enabled);
        assert_eq!(bm.import_count, 0);
        assert!(!bm.id.is_empty());
    }

    #[test]
    fn test_url_bookmark_urls() {
        let bm = UrlBookmark::new(
            "test",
            vec![
                BookmarkEntry::new("https://a.com/1"),
                BookmarkEntry::new("https://b.com/2"),
            ],
        );
        assert_eq!(bm.urls(), vec!["https://a.com/1", "https://b.com/2"]);
    }

    #[test]
    fn test_bookmark_summary_from() {
        let bm = UrlBookmark::new("My BM", vec![BookmarkEntry::new("https://x.com/f")]);
        let summary = BookmarkSummary::from(&bm);
        assert_eq!(summary.name, "My BM");
        assert_eq!(summary.url_count, 1);
        assert!(summary.enabled);
    }

    #[test]
    fn test_add_bookmark() {
        let mut bookmarks = Vec::new();
        let entries = vec![BookmarkEntry::new("https://a.com/1.zip")];
        let bm = add_bookmark(&mut bookmarks, "Test", entries).unwrap();
        assert_eq!(bm.name, "Test");
        assert_eq!(bookmarks.len(), 1);
    }

    #[test]
    fn test_add_bookmark_duplicate_name() {
        let mut bookmarks = Vec::new();
        add_bookmark(&mut bookmarks, "Test", vec![]).unwrap();
        let result = add_bookmark(&mut bookmarks, "test", vec![]);
        assert!(matches!(result, Err(BookmarkError::DuplicateName(_))));
    }

    #[test]
    fn test_remove_bookmark() {
        let mut bookmarks = Vec::new();
        add_bookmark(&mut bookmarks, "Test", vec![]).unwrap();
        assert_eq!(bookmarks.len(), 1);
        remove_bookmark(&mut bookmarks, "Test").unwrap();
        assert!(bookmarks.is_empty());
    }

    #[test]
    fn test_remove_bookmark_not_found() {
        let mut bookmarks: Vec<UrlBookmark> = Vec::new();
        let result = remove_bookmark(&mut bookmarks, "nonexistent");
        assert!(matches!(result, Err(BookmarkError::NotFound(_))));
    }

    #[test]
    fn test_get_bookmark() {
        let mut bookmarks = Vec::new();
        add_bookmark(
            &mut bookmarks,
            "Alpha",
            vec![BookmarkEntry::new("https://a.com")],
        )
        .unwrap();
        let found = get_bookmark(&bookmarks, "alpha");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alpha");
    }

    #[test]
    fn test_get_bookmark_case_insensitive() {
        let mut bookmarks = Vec::new();
        add_bookmark(&mut bookmarks, "MyBookmarks", vec![]).unwrap();
        assert!(get_bookmark(&bookmarks, "mybookmarks").is_some());
        assert!(get_bookmark(&bookmarks, "MYBOOKMARKS").is_some());
    }

    #[test]
    fn test_add_urls_to_bookmark() {
        let mut bm = UrlBookmark::new("test", vec![BookmarkEntry::new("https://a.com")]);
        add_urls_to_book(&mut bm, vec![BookmarkEntry::new("https://b.com")]).unwrap();
        assert_eq!(bm.url_count(), 2);
    }

    #[test]
    fn test_add_urls_deduplicates() {
        let mut bm = UrlBookmark::new("test", vec![BookmarkEntry::new("https://a.com")]);
        add_urls_to_book(
            &mut bm,
            vec![
                BookmarkEntry::new("https://a.com"), // duplicate
                BookmarkEntry::new("https://b.com"), // new
            ],
        )
        .unwrap();
        assert_eq!(bm.url_count(), 2); // only 2, not 3
    }

    #[test]
    fn test_remove_url_from_bookmark() {
        let mut bm = UrlBookmark::new(
            "test",
            vec![
                BookmarkEntry::new("https://a.com"),
                BookmarkEntry::new("https://b.com"),
            ],
        );
        remove_url_from_bookmark(&mut bm, "https://a.com").unwrap();
        assert_eq!(bm.url_count(), 1);
        assert_eq!(bm.entries[0].url, "https://b.com");
    }

    #[test]
    fn test_remove_url_not_found() {
        let mut bm = UrlBookmark::new("test", vec![BookmarkEntry::new("https://a.com")]);
        let result = remove_url_from_bookmark(&mut bm, "https://nonexistent.com");
        assert!(matches!(result, Err(BookmarkError::NotFound(_))));
    }

    #[test]
    fn test_mark_bookmark_used() {
        let mut bm = UrlBookmark::new("test", vec![]);
        assert_eq!(bm.import_count, 0);
        assert!(bm.last_used_at.is_none());
        mark_bookmark_used(&mut bm);
        assert_eq!(bm.import_count, 1);
        assert!(bm.last_used_at.is_some());
        mark_bookmark_used(&mut bm);
        assert_eq!(bm.import_count, 2);
    }

    #[test]
    fn test_parse_url_list_simple() {
        let input = "https://a.com/1.zip\nhttps://b.com/2.zip\n";
        let entries = parse_url_list(input);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].url, "https://a.com/1.zip");
        assert!(entries[0].name.is_none());
    }

    #[test]
    fn test_parse_url_list_with_names() {
        let input = "File A|https://a.com/1.zip\nFile B|https://b.com/2.zip\n";
        let entries = parse_url_list(input);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name.as_deref(), Some("File A"));
        assert_eq!(entries[0].url, "https://a.com/1.zip");
    }

    #[test]
    fn test_parse_url_list_skips_comments_and_blanks() {
        let input = "# This is a comment\nhttps://a.com\n\n# Another comment\nhttps://b.com\n";
        let entries = parse_url_list(input);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_parse_url_list_trims_whitespace() {
        let input = "  https://a.com  \n  https://b.com  \n";
        let entries = parse_url_list(input);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].url, "https://a.com");
    }

    #[test]
    fn test_save_and_load_bookmarks() {
        let tmp = TempDir::new().unwrap();
        let entries = vec![
            BookmarkEntry::new("https://a.com/1.zip"),
            BookmarkEntry::with_name("https://b.com/2.zip", "File B"),
        ];
        let bm = UrlBookmark::new("Test Collection", entries);
        let bookmarks = vec![bm.clone()];

        save_bookmarks(&bookmarks, tmp.path()).unwrap();
        let loaded = load_bookmarks(tmp.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Test Collection");
        assert_eq!(loaded[0].entries.len(), 2);
        assert_eq!(loaded[0].entries[0].url, "https://a.com/1.zip");
        assert_eq!(loaded[0].entries[1].name.as_deref(), Some("File B"));
    }

    #[test]
    fn test_load_bookmarks_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let loaded = load_bookmarks(tmp.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_bookmark_serialization_roundtrip() {
        let mut bm = UrlBookmark::new(
            "Roundtrip",
            vec![BookmarkEntry::new("https://example.com/file.zip")],
        );
        bm.description = Some("Test description".to_string());
        bm.import_count = 5;
        mark_bookmark_used(&mut bm);

        let json = serde_json::to_string(&bm).unwrap();
        let deserialized: UrlBookmark = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "Roundtrip");
        assert_eq!(
            deserialized.description.as_deref(),
            Some("Test description")
        );
        assert_eq!(deserialized.import_count, 6);
        assert!(deserialized.last_used_at.is_some());
    }

    #[test]
    fn test_bookmark_entry_with_tags_and_group() {
        let mut entry = BookmarkEntry::new("https://example.com/file.zip");
        entry.tags = vec!["linux".to_string(), "iso".to_string()];
        entry.group = Some("distros".to_string());
        entry.priority = Some(3);

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: BookmarkEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.tags, vec!["linux", "iso"]);
        assert_eq!(deserialized.group.as_deref(), Some("distros"));
        assert_eq!(deserialized.priority, Some(3));
    }

    #[test]
    fn test_multiple_bookmarks_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut bookmarks = Vec::new();
        add_bookmark(
            &mut bookmarks,
            "Linux ISOs",
            vec![
                BookmarkEntry::new("https://ubuntu.com/ubuntu.iso"),
                BookmarkEntry::new("https://fedora.org/fedora.iso"),
            ],
        )
        .unwrap();
        add_bookmark(
            &mut bookmarks,
            "Tools",
            vec![BookmarkEntry::new("https://tools.example.com/tool.zip")],
        )
        .unwrap();

        save_bookmarks(&bookmarks, tmp.path()).unwrap();
        let loaded = load_bookmarks(tmp.path()).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "Linux ISOs");
        assert_eq!(loaded[0].url_count(), 2);
        assert_eq!(loaded[1].name, "Tools");
        assert_eq!(loaded[1].url_count(), 1);
    }
}
