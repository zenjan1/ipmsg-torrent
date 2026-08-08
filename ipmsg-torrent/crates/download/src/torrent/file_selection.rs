//! Torrent file selection for multi-file torrents.
//!
//! Allows users to select which files to download from a multi-file torrent,
//! skipping unwanted files to save bandwidth and disk space.

use super::meta::TorrentMeta;
use serde::{Deserialize, Serialize};

/// Represents a file entry in a multi-file torrent with selection state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Index in the torrent's file list (0-based)
    pub index: usize,
    /// File path (relative to torrent root)
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Whether this file is selected for download
    pub selected: bool,
}

/// File selection mode for a torrent download.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum SelectionMode {
    /// Download all files (default)
    #[default]
    All,
    /// Download only selected files by index
    Selected(Vec<usize>),
    /// Download all files except excluded ones by index
    AllExcept(Vec<usize>),
}

/// Manages file selection for a multi-file torrent.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileSelection {
    /// Selection mode
    pub mode: SelectionMode,
}

impl FileSelection {
    /// Create a new file selection that downloads all files.
    pub fn all() -> Self {
        Self {
            mode: SelectionMode::All,
        }
    }

    /// Create a file selection that downloads only the specified files.
    pub fn selected(indices: Vec<usize>) -> Self {
        Self {
            mode: SelectionMode::Selected(indices),
        }
    }

    /// Create a file selection that downloads all files except the specified ones.
    pub fn all_except(indices: Vec<usize>) -> Self {
        Self {
            mode: SelectionMode::AllExcept(indices),
        }
    }

    /// Check if a file at the given index should be downloaded.
    pub fn is_selected(&self, file_index: usize) -> bool {
        match &self.mode {
            SelectionMode::All => true,
            SelectionMode::Selected(indices) => indices.contains(&file_index),
            SelectionMode::AllExcept(excluded) => !excluded.contains(&file_index),
        }
    }

    /// Get the list of selected file indices.
    pub fn selected_indices(&self, total_files: usize) -> Vec<usize> {
        match &self.mode {
            SelectionMode::All => (0..total_files).collect(),
            SelectionMode::Selected(indices) => indices.clone(),
            SelectionMode::AllExcept(excluded) => {
                (0..total_files).filter(|i| !excluded.contains(i)).collect()
            }
        }
    }

    /// Calculate total size of selected files.
    pub fn selected_size(&self, meta: &TorrentMeta) -> u64 {
        if meta.info.files.is_empty() {
            // Single-file torrent
            return meta.total_size();
        }

        meta.info
            .files
            .iter()
            .enumerate()
            .filter(|(i, _)| self.is_selected(*i))
            .map(|(_, f)| f.length)
            .sum()
    }

    /// Build file entries from torrent metadata with current selection state.
    pub fn file_entries(&self, meta: &TorrentMeta) -> Vec<FileEntry> {
        meta.info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| FileEntry {
                index: i,
                path: f.path.clone(),
                size: f.length,
                selected: self.is_selected(i),
            })
            .collect()
    }

    /// Check if this is a single-file torrent (selection not applicable).
    pub fn is_single_file(&self, meta: &TorrentMeta) -> bool {
        meta.info.files.is_empty()
    }

    /// Validate selection indices against total file count.
    /// Returns Err if any index is out of bounds.
    pub fn validate(&self, total_files: usize) -> Result<(), FileSelectionError> {
        match &self.mode {
            SelectionMode::All => Ok(()),
            SelectionMode::Selected(indices) => {
                for &idx in indices {
                    if idx >= total_files {
                        return Err(FileSelectionError::IndexOutOfBounds {
                            index: idx,
                            total: total_files,
                        });
                    }
                }
                if indices.is_empty() {
                    return Err(FileSelectionError::EmptySelection);
                }
                Ok(())
            }
            SelectionMode::AllExcept(excluded) => {
                for &idx in excluded {
                    if idx >= total_files {
                        return Err(FileSelectionError::IndexOutOfBounds {
                            index: idx,
                            total: total_files,
                        });
                    }
                }
                if excluded.len() >= total_files {
                    return Err(FileSelectionError::AllExcluded);
                }
                Ok(())
            }
        }
    }

    /// Parse a selection string like "0,2,4" or "all" or "except:1,3".
    pub fn parse(s: &str, total_files: usize) -> Result<Self, FileSelectionError> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("all") || s.is_empty() {
            return Ok(Self::all());
        }

        if let Some(rest) = s.strip_prefix("except:") {
            let indices = Self::parse_indices(rest)?;
            let selection = Self::all_except(indices);
            selection.validate(total_files)?;
            return Ok(selection);
        }

        let indices = Self::parse_indices(s)?;
        let selection = Self::selected(indices);
        selection.validate(total_files)?;
        Ok(selection)
    }

    fn parse_indices(s: &str) -> Result<Vec<usize>, FileSelectionError> {
        let mut indices = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let idx: usize = part
                .parse()
                .map_err(|_| FileSelectionError::InvalidIndex(part.to_string()))?;
            indices.push(idx);
        }
        Ok(indices)
    }
}

/// Errors related to file selection.
#[derive(Debug, thiserror::Error)]
pub enum FileSelectionError {
    #[error("file index {index} out of bounds (total: {total})")]
    IndexOutOfBounds { index: usize, total: usize },
    #[error("empty file selection (no files selected)")]
    EmptySelection,
    #[error("all files excluded (nothing to download)")]
    AllExcluded,
    #[error("invalid file index: {0}")]
    InvalidIndex(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::meta::{TorrentFile, TorrentInfo, TorrentMeta};

    fn make_test_meta(file_count: usize) -> TorrentMeta {
        let files: Vec<TorrentFile> = (0..file_count)
            .map(|i| TorrentFile {
                path: format!("file_{}.dat", i),
                length: 1024 * (i as u64 + 1),
            })
            .collect();

        let total_size: u64 = files.iter().map(|f| f.length).sum();

        TorrentMeta {
            info_hash: [0u8; 20],
            announce_list: vec![],
            announce: None,
            creation_date: None,
            comment: None,
            created_by: None,
            info: TorrentInfo {
                length: None,
                piece_length: 32768,
                pieces: vec![],
                name: "test_torrent".to_string(),
                files,
            },
        }
    }

    fn make_single_file_meta() -> TorrentMeta {
        TorrentMeta {
            info_hash: [0u8; 20],
            announce_list: vec![],
            announce: None,
            creation_date: None,
            comment: None,
            created_by: None,
            info: TorrentInfo {
                length: Some(10240),
                piece_length: 32768,
                pieces: vec![],
                name: "single.dat".to_string(),
                files: vec![],
            },
        }
    }

    #[test]
    fn test_selection_all() {
        let sel = FileSelection::all();
        assert!(sel.is_selected(0));
        assert!(sel.is_selected(5));
        assert_eq!(sel.selected_indices(3), vec![0, 1, 2]);
    }

    #[test]
    fn test_selection_selected() {
        let sel = FileSelection::selected(vec![0, 2, 4]);
        assert!(sel.is_selected(0));
        assert!(!sel.is_selected(1));
        assert!(sel.is_selected(2));
        assert!(!sel.is_selected(3));
        assert!(sel.is_selected(4));
        assert_eq!(sel.selected_indices(5), vec![0, 2, 4]);
    }

    #[test]
    fn test_selection_all_except() {
        let sel = FileSelection::all_except(vec![1, 3]);
        assert!(sel.is_selected(0));
        assert!(!sel.is_selected(1));
        assert!(sel.is_selected(2));
        assert!(!sel.is_selected(3));
        assert!(sel.is_selected(4));
        assert_eq!(sel.selected_indices(5), vec![0, 2, 4]);
    }

    #[test]
    fn test_selected_size() {
        let meta = make_test_meta(3); // sizes: 1024, 2048, 3072
        let sel = FileSelection::selected(vec![0, 2]);
        assert_eq!(sel.selected_size(&meta), 1024 + 3072);
    }

    #[test]
    fn test_selected_size_all() {
        let meta = make_test_meta(3);
        let sel = FileSelection::all();
        assert_eq!(sel.selected_size(&meta), 1024 + 2048 + 3072);
    }

    #[test]
    fn test_file_entries() {
        let meta = make_test_meta(3);
        let sel = FileSelection::selected(vec![1]);
        let entries = sel.file_entries(&meta);
        assert_eq!(entries.len(), 3);
        assert!(!entries[0].selected);
        assert!(entries[1].selected);
        assert!(!entries[2].selected);
        assert_eq!(entries[0].path, "file_0.dat");
    }

    #[test]
    fn test_single_file_detection() {
        let meta = make_single_file_meta();
        let sel = FileSelection::all();
        assert!(sel.is_single_file(&meta));

        let meta2 = make_test_meta(3);
        assert!(!sel.is_single_file(&meta2));
    }

    #[test]
    fn test_validate_ok() {
        let sel = FileSelection::selected(vec![0, 2]);
        assert!(sel.validate(3).is_ok());
    }

    #[test]
    fn test_validate_out_of_bounds() {
        let sel = FileSelection::selected(vec![0, 5]);
        let err = sel.validate(3).unwrap_err();
        assert!(matches!(err, FileSelectionError::IndexOutOfBounds { .. }));
    }

    #[test]
    fn test_validate_empty_selection() {
        let sel = FileSelection::selected(vec![]);
        let err = sel.validate(3).unwrap_err();
        assert!(matches!(err, FileSelectionError::EmptySelection));
    }

    #[test]
    fn test_validate_all_excluded() {
        let sel = FileSelection::all_except(vec![0, 1, 2]);
        let err = sel.validate(3).unwrap_err();
        assert!(matches!(err, FileSelectionError::AllExcluded));
    }

    #[test]
    fn test_validate_all_except_ok() {
        let sel = FileSelection::all_except(vec![1]);
        assert!(sel.validate(3).is_ok());
    }

    #[test]
    fn test_parse_all() {
        let sel = FileSelection::parse("all", 5).unwrap();
        assert_eq!(sel.mode, SelectionMode::All);
    }

    #[test]
    fn test_parse_empty() {
        let sel = FileSelection::parse("", 5).unwrap();
        assert_eq!(sel.mode, SelectionMode::All);
    }

    #[test]
    fn test_parse_selected() {
        let sel = FileSelection::parse("0,2,4", 5).unwrap();
        assert_eq!(sel.mode, SelectionMode::Selected(vec![0, 2, 4]));
    }

    #[test]
    fn test_parse_except() {
        let sel = FileSelection::parse("except:1,3", 5).unwrap();
        assert_eq!(sel.mode, SelectionMode::AllExcept(vec![1, 3]));
    }

    #[test]
    fn test_parse_invalid_index() {
        let err = FileSelection::parse("abc", 5).unwrap_err();
        assert!(matches!(err, FileSelectionError::InvalidIndex(_)));
    }

    #[test]
    fn test_parse_out_of_bounds() {
        let err = FileSelection::parse("0,10", 5).unwrap_err();
        assert!(matches!(err, FileSelectionError::IndexOutOfBounds { .. }));
    }

    #[test]
    fn test_default_is_all() {
        let sel = FileSelection::default();
        assert_eq!(sel.mode, SelectionMode::All);
        assert!(sel.is_selected(0));
    }

    #[test]
    fn test_single_file_size() {
        let meta = make_single_file_meta();
        let sel = FileSelection::all();
        assert_eq!(sel.selected_size(&meta), 10240);
    }
}
