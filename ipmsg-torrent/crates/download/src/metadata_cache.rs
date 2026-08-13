//! Metadata cache for magnet links
//!
//! Caches torrent metadata retrieved via BEP 0009 metadata exchange so that
//! repeated downloads of the same magnet link don't need to re-fetch metadata
//! from peers.
//!
//! ## Cache layout
//!
//! ```text
//! ~/.cache/ipmsg-torrent/metadata/<info_hash_hex>.torrent
//! ```
//!
//! Each file is the raw bencoded metadata (the info-dictionary bytes that
//! hash to the info_hash). An accompanying `.meta` sidecar stores the
//! display name, trackers, and creation timestamp for cache management.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Errors from metadata cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cache entry not found")]
    NotFound,
    #[error("invalid cache data: {0}")]
    Invalid(String),
}

/// Sidecar metadata stored alongside the cached torrent bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    /// Info hash as hex string (for validation).
    pub info_hash_hex: String,
    /// Display name from the magnet link (if any).
    pub display_name: Option<String>,
    /// Tracker URLs from the magnet link.
    pub trackers: Vec<String>,
    /// When this entry was created (Unix seconds).
    pub created_at: u64,
    /// Size of the metadata blob in bytes.
    pub metadata_size: u64,
}

/// Return the root cache directory for metadata.
///
/// Default: `~/.cache/ipmsg-torrent/metadata`
/// Honors `$XDG_CACHE_HOME` when set.
pub fn cache_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        return PathBuf::from(xdg).join("ipmsg-torrent").join("metadata");
    }
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("ipmsg-torrent")
        .join("metadata")
}

/// Compute the `.torrent` cache path for a given info hash.
pub fn torrent_cache_path(cache_dir: &Path, info_hash: &[u8; 20]) -> PathBuf {
    cache_dir.join(format!("{}.torrent", hex::encode(info_hash)))
}

/// Compute the `.meta` sidecar path for a given info hash.
pub fn meta_sidecar_path(cache_dir: &Path, info_hash: &[u8; 20]) -> PathBuf {
    cache_dir.join(format!("{}.meta", hex::encode(info_hash)))
}

/// Check whether a valid cache entry exists for the given info hash.
pub fn has_cached(cache_dir: &Path, info_hash: &[u8; 20]) -> bool {
    let torrent_path = torrent_cache_path(cache_dir, info_hash);
    let meta_path = meta_sidecar_path(cache_dir, info_hash);
    torrent_path.is_file() && meta_path.is_file()
}

/// Store metadata bytes and sidecar info in the cache.
///
/// Creates the cache directory if it doesn't exist.
pub fn save_metadata(
    cache_dir: &Path,
    info_hash: &[u8; 20],
    metadata_bytes: &[u8],
    display_name: Option<&str>,
    trackers: &[String],
) -> Result<(), CacheError> {
    std::fs::create_dir_all(cache_dir)?;

    let torrent_path = torrent_cache_path(cache_dir, info_hash);
    let meta_path = meta_sidecar_path(cache_dir, info_hash);

    // Write metadata blob
    std::fs::write(&torrent_path, metadata_bytes)?;

    // Write sidecar
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let meta = CacheMeta {
        info_hash_hex: hex::encode(info_hash),
        display_name: display_name.map(|s| s.to_string()),
        trackers: trackers.to_vec(),
        created_at: now,
        metadata_size: metadata_bytes.len() as u64,
    };

    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| CacheError::Invalid(format!("JSON encode: {e}")))?;
    std::fs::write(&meta_path, meta_json)?;

    tracing::info!(
        path = %torrent_path.display(),
        size = metadata_bytes.len(),
        "Metadata cached"
    );

    Ok(())
}

/// Load cached metadata bytes for the given info hash.
///
/// Returns `CacheError::NotFound` if no valid entry exists.
pub fn load_metadata(cache_dir: &Path, info_hash: &[u8; 20]) -> Result<Vec<u8>, CacheError> {
    let torrent_path = torrent_cache_path(cache_dir, info_hash);
    let meta_path = meta_sidecar_path(cache_dir, info_hash);

    if !torrent_path.is_file() || !meta_path.is_file() {
        return Err(CacheError::NotFound);
    }

    // Read and validate sidecar
    let meta_json = std::fs::read_to_string(&meta_path)?;
    let meta: CacheMeta = serde_json::from_str(&meta_json)
        .map_err(|e| CacheError::Invalid(format!("JSON decode: {e}")))?;

    // Validate info hash matches
    if meta.info_hash_hex != hex::encode(info_hash) {
        return Err(CacheError::Invalid("info hash mismatch".into()));
    }

    // Read metadata blob
    let data = std::fs::read(&torrent_path)?;

    // Validate size matches sidecar
    if data.len() as u64 != meta.metadata_size {
        tracing::warn!(
            "Cached metadata size mismatch (expected {}, got {}), re-fetching",
            meta.metadata_size,
            data.len()
        );
        // Clean up corrupt entry
        let _ = std::fs::remove_file(&torrent_path);
        let _ = std::fs::remove_file(&meta_path);
        return Err(CacheError::Invalid("size mismatch".into()));
    }

    tracing::info!(
        path = %torrent_path.display(),
        size = data.len(),
        "Metadata loaded from cache"
    );

    Ok(data)
}

/// Load the sidecar metadata (display name, trackers, etc.) without reading
/// the full metadata blob.
pub fn load_cache_meta(cache_dir: &Path, info_hash: &[u8; 20]) -> Result<CacheMeta, CacheError> {
    let meta_path = meta_sidecar_path(cache_dir, info_hash);
    if !meta_path.is_file() {
        return Err(CacheError::NotFound);
    }

    let meta_json = std::fs::read_to_string(&meta_path)?;
    let meta: CacheMeta = serde_json::from_str(&meta_json)
        .map_err(|e| CacheError::Invalid(format!("JSON decode: {e}")))?;

    if meta.info_hash_hex != hex::encode(info_hash) {
        return Err(CacheError::Invalid("info hash mismatch".into()));
    }

    Ok(meta)
}

/// Remove a cache entry for the given info hash.
pub fn remove_cached(cache_dir: &Path, info_hash: &[u8; 20]) -> Result<bool, CacheError> {
    let torrent_path = torrent_cache_path(cache_dir, info_hash);
    let meta_path = meta_sidecar_path(cache_dir, info_hash);

    let mut removed = false;
    if torrent_path.is_file() {
        std::fs::remove_file(&torrent_path)?;
        removed = true;
    }
    if meta_path.is_file() {
        std::fs::remove_file(&meta_path)?;
        removed = true;
    }

    Ok(removed)
}

/// List all cached info hashes.
pub fn list_cached(cache_dir: &Path) -> Vec<[u8; 20]> {
    let mut result = Vec::new();
    let entries = match std::fs::read_dir(cache_dir) {
        Ok(e) => e,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("torrent")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && let Ok(bytes) = hex::decode(stem)
            && bytes.len() == 20
        {
            let mut hash = [0u8; 20];
            hash.copy_from_slice(&bytes);
            result.push(hash);
        }
    }

    result
}

/// Remove cache entries older than `max_age_secs`.
///
/// Returns the number of entries removed.
pub fn evict_expired(cache_dir: &Path, max_age_secs: u64) -> Result<usize, CacheError> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut removed = 0;
    for hash in list_cached(cache_dir) {
        if let Ok(meta) = load_cache_meta(cache_dir, &hash)
            && now.saturating_sub(meta.created_at) > max_age_secs
        {
            remove_cached(cache_dir, &hash)?;
            removed += 1;
        }
    }

    if removed > 0 {
        tracing::info!(count = removed, "Evicted expired cache entries");
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_hash(byte: u8) -> [u8; 20] {
        [byte; 20]
    }

    #[test]
    fn test_cache_dir_default() {
        let dir = cache_dir();
        assert!(dir.to_str().unwrap().contains("ipmsg-torrent"));
        assert!(dir.to_str().unwrap().contains("metadata"));
    }

    #[test]
    fn test_cache_paths() {
        let dir = PathBuf::from("/tmp/test-cache");
        let hash = test_hash(0xAB);
        let torrent_path = torrent_cache_path(&dir, &hash);
        let meta_path = meta_sidecar_path(&dir, &hash);

        assert!(torrent_path.to_str().unwrap().contains(&hex::encode(hash)));
        assert!(torrent_path.extension().unwrap() == "torrent");
        assert!(meta_path.extension().unwrap() == "meta");
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x42);
        let metadata = b"d4:infod6:lengthi1024e4:name8:test.txt12:piece lengthi32768e6:pieces20:xxxxxxxxxxxxxxxxxxxxyyyee";

        save_metadata(
            &cache,
            &hash,
            metadata,
            Some("test.txt"),
            &["http://tracker.example.com/announce".to_string()],
        )
        .unwrap();

        assert!(has_cached(&cache, &hash));

        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, metadata);

        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some("test.txt".to_string()));
        assert_eq!(meta.trackers.len(), 1);
        assert_eq!(meta.metadata_size, metadata.len() as u64);
    }

    #[test]
    fn test_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0xFF);

        assert!(!has_cached(&cache, &hash));
        assert!(load_metadata(&cache, &hash).is_err());
        assert!(matches!(
            load_metadata(&cache, &hash).unwrap_err(),
            CacheError::NotFound
        ));
    }

    #[test]
    fn test_remove_cached() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x11);
        let metadata = b"test data";

        save_metadata(&cache, &hash, metadata, None, &[]).unwrap();
        assert!(has_cached(&cache, &hash));

        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(removed);
        assert!(!has_cached(&cache, &hash));
    }

    #[test]
    fn test_remove_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x22);

        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(!removed);
    }

    #[test]
    fn test_list_cached() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");

        let hash1 = test_hash(0x01);
        let hash2 = test_hash(0x02);

        save_metadata(&cache, &hash1, b"data1", None, &[]).unwrap();
        save_metadata(&cache, &hash2, b"data2", None, &[]).unwrap();

        let cached = list_cached(&cache);
        assert_eq!(cached.len(), 2);
        assert!(cached.contains(&hash1));
        assert!(cached.contains(&hash2));
    }

    #[test]
    fn test_list_cached_empty() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let cached = list_cached(&cache);
        assert!(cached.is_empty());
    }

    #[test]
    fn test_evict_expired() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x33);

        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();

        // Manually backdate the cache entry
        let meta_path = meta_sidecar_path(&cache, &hash);
        let meta_json = std::fs::read_to_string(&meta_path).unwrap();
        let mut meta: CacheMeta = serde_json::from_str(&meta_json).unwrap();
        meta.created_at = 1000; // Very old timestamp
        let updated_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(&meta_path, updated_json).unwrap();

        // Evict entries older than 1 hour
        let removed = evict_expired(&cache, 3600).unwrap();
        assert_eq!(removed, 1);
        assert!(!has_cached(&cache, &hash));
    }

    #[test]
    fn test_evict_keeps_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x44);

        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();

        // Evict entries older than 1 hour (our entry was just created)
        let removed = evict_expired(&cache, 3600).unwrap();
        assert_eq!(removed, 0);
        assert!(has_cached(&cache, &hash));
    }

    #[test]
    fn test_corrupt_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x55);

        save_metadata(&cache, &hash, b"original data", None, &[]).unwrap();

        // Corrupt the metadata file by replacing with shorter content
        let torrent_path = torrent_cache_path(&cache, &hash);
        std::fs::write(&torrent_path, b"short").unwrap();

        // Should detect corruption and clean up
        let result = load_metadata(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::Invalid(_)));

        // Corrupt files should be cleaned up
        assert!(!torrent_path.is_file());
    }

    #[test]
    fn test_info_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x66);

        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();

        // Tamper with the sidecar to change the info hash
        let meta_path = meta_sidecar_path(&cache, &hash);
        let mut meta: CacheMeta =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.info_hash_hex = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string();
        std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();

        let result = load_metadata(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::Invalid(_)));
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x77);

        save_metadata(&cache, &hash, b"first", Some("v1"), &[]).unwrap();
        save_metadata(&cache, &hash, b"second", Some("v2"), &[]).unwrap();

        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, b"second");

        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some("v2".to_string()));
    }

    // ===== Serialization =====

    #[test]
    fn test_cache_meta_serialization_roundtrip() {
        let meta = CacheMeta {
            info_hash_hex: "abcdef0123456789abcdef0123456789abcdef01".to_string(),
            display_name: Some("test_file.txt".to_string()),
            trackers: vec![
                "http://tracker1.example.com/announce".to_string(),
                "udp://tracker2.example.com:6969".to_string(),
            ],
            created_at: 1700000000,
            metadata_size: 4096,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: CacheMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.info_hash_hex, meta.info_hash_hex);
        assert_eq!(deserialized.display_name, meta.display_name);
        assert_eq!(deserialized.trackers, meta.trackers);
        assert_eq!(deserialized.created_at, meta.created_at);
        assert_eq!(deserialized.metadata_size, meta.metadata_size);
    }

    #[test]
    fn test_cache_meta_pretty_serialization() {
        let meta = CacheMeta {
            info_hash_hex: "0000000000000000000000000000000000000000".to_string(),
            display_name: None,
            trackers: vec![],
            created_at: 0,
            metadata_size: 0,
        };
        let pretty = serde_json::to_string_pretty(&meta).unwrap();
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("info_hash_hex"));
    }

    #[test]
    fn test_cache_meta_extra_fields_tolerance() {
        let json = r#"{
            "info_hash_hex": "abcdef0123456789abcdef0123456789abcdef01",
            "display_name": null,
            "trackers": [],
            "created_at": 1700000000,
            "metadata_size": 100,
            "extra_unknown_field": "should be ignored"
        }"#;
        let meta: CacheMeta = serde_json::from_str(json).unwrap();
        assert_eq!(
            meta.info_hash_hex,
            "abcdef0123456789abcdef0123456789abcdef01"
        );
        assert_eq!(meta.metadata_size, 100);
    }

    #[test]
    fn test_cache_meta_null_display_name() {
        let json = r#"{
            "info_hash_hex": "0000000000000000000000000000000000000000",
            "display_name": null,
            "trackers": [],
            "created_at": 0,
            "metadata_size": 0
        }"#;
        let meta: CacheMeta = serde_json::from_str(json).unwrap();
        assert!(meta.display_name.is_none());
    }

    #[test]
    fn test_cache_meta_empty_trackers() {
        let meta = CacheMeta {
            info_hash_hex: "0000000000000000000000000000000000000000".to_string(),
            display_name: None,
            trackers: vec![],
            created_at: 0,
            metadata_size: 0,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: CacheMeta = serde_json::from_str(&json).unwrap();
        assert!(deserialized.trackers.is_empty());
    }

    #[test]
    fn test_cache_meta_multiple_trackers_serialization() {
        let trackers = vec![
            "http://t1.example.com/a".to_string(),
            "udp://t2.example.com:6969".to_string(),
            "http://t3.example.com/announce".to_string(),
        ];
        let meta = CacheMeta {
            info_hash_hex: "1111111111111111111111111111111111111111".to_string(),
            display_name: Some("multi_tracker.torrent".to_string()),
            trackers: trackers.clone(),
            created_at: 1700000000,
            metadata_size: 2048,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: CacheMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.trackers, trackers);
    }

    // ===== Default values =====

    #[test]
    fn test_cache_meta_all_fields_present() {
        let meta = CacheMeta {
            info_hash_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            display_name: Some("file.mp4".to_string()),
            trackers: vec!["http://tracker.example.com/a".to_string()],
            created_at: 1700000000,
            metadata_size: 512,
        };
        assert_eq!(meta.info_hash_hex.len(), 40);
        assert!(meta.display_name.is_some());
        assert_eq!(meta.trackers.len(), 1);
        assert!(meta.created_at > 0);
        assert!(meta.metadata_size > 0);
    }

    // ===== Path computation =====

    #[test]
    fn test_torrent_cache_path_structure() {
        let dir = PathBuf::from("/cache");
        let hash = [0xAB; 20];
        let path = torrent_cache_path(&dir, &hash);
        assert_eq!(
            path,
            PathBuf::from("/cache/abababababababababababababababababababab.torrent")
        );
    }

    #[test]
    fn test_meta_sidecar_path_structure() {
        let dir = PathBuf::from("/cache");
        let hash = [0xCD; 20];
        let path = meta_sidecar_path(&dir, &hash);
        assert_eq!(
            path,
            PathBuf::from("/cache/cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd.meta")
        );
    }

    #[test]
    fn test_torrent_and_meta_paths_share_stem() {
        let dir = PathBuf::from("/cache");
        let hash = [0x12; 20];
        let torrent = torrent_cache_path(&dir, &hash);
        let meta = meta_sidecar_path(&dir, &hash);
        assert_eq!(torrent.file_stem(), meta.file_stem());
    }

    #[test]
    fn test_different_hashes_different_paths() {
        let dir = PathBuf::from("/cache");
        let hash1 = test_hash(0x01);
        let hash2 = test_hash(0x02);
        assert_ne!(
            torrent_cache_path(&dir, &hash1),
            torrent_cache_path(&dir, &hash2)
        );
        assert_ne!(
            meta_sidecar_path(&dir, &hash1),
            meta_sidecar_path(&dir, &hash2)
        );
    }

    // ===== has_cached =====

    #[test]
    fn test_has_cached_false_when_no_dir() {
        let dir = PathBuf::from("/nonexistent/path/cache");
        let hash = test_hash(0x01);
        assert!(!has_cached(&dir, &hash));
    }

    #[test]
    fn test_has_cached_false_when_only_torrent() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0xAA);
        // Only write torrent file, no meta sidecar
        let torrent_path = torrent_cache_path(&cache, &hash);
        std::fs::write(&torrent_path, b"data").unwrap();
        assert!(!has_cached(&cache, &hash));
    }

    #[test]
    fn test_has_cached_false_when_only_meta() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0xBB);
        // Only write meta file, no torrent
        let meta_path = meta_sidecar_path(&cache, &hash);
        let meta = CacheMeta {
            info_hash_hex: hex::encode(hash),
            display_name: None,
            trackers: vec![],
            created_at: 0,
            metadata_size: 4,
        };
        std::fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();
        assert!(!has_cached(&cache, &hash));
    }

    #[test]
    fn test_has_cached_true_when_both_exist() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0xCC);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        assert!(has_cached(&cache, &hash));
    }

    // ===== save_metadata =====

    #[test]
    fn test_save_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("nested").join("deep").join("metadata");
        let hash = test_hash(0x01);
        assert!(!cache.exists());
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        assert!(cache.exists());
        assert!(cache.is_dir());
    }

    #[test]
    fn test_save_with_no_display_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x02);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert!(meta.display_name.is_none());
    }

    #[test]
    fn test_save_with_display_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x03);
        save_metadata(&cache, &hash, b"data", Some("movie.mkv"), &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some("movie.mkv".to_string()));
    }

    #[test]
    fn test_save_with_empty_trackers() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x04);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert!(meta.trackers.is_empty());
    }

    #[test]
    fn test_save_with_multiple_trackers() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x05);
        let trackers = vec![
            "http://t1.example.com/a".to_string(),
            "udp://t2.example.com:6969".to_string(),
        ];
        save_metadata(&cache, &hash, b"data", None, &trackers).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.trackers, trackers);
    }

    #[test]
    fn test_save_metadata_size_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x06);
        let data = b"some metadata bytes here";
        save_metadata(&cache, &hash, data, None, &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.metadata_size, data.len() as u64);
    }

    #[test]
    fn test_save_empty_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x07);
        save_metadata(&cache, &hash, b"", None, &[]).unwrap();
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert!(loaded.is_empty());
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.metadata_size, 0);
    }

    #[test]
    fn test_save_large_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x08);
        let data = vec![0xABu8; 1024 * 1024]; // 1MB
        save_metadata(&cache, &hash, &data, None, &[]).unwrap();
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded.len(), 1024 * 1024);
    }

    #[test]
    fn test_save_info_hash_hex_in_meta() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB,
            0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67,
        ];
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.info_hash_hex, hex::encode(hash));
    }

    #[test]
    fn test_save_created_at_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x09);
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert!(meta.created_at >= before && meta.created_at <= after);
    }

    // ===== load_metadata =====

    #[test]
    fn test_load_metadata_corrupt_json_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0x10);
        // Write valid torrent but corrupt meta
        let torrent_path = torrent_cache_path(&cache, &hash);
        let meta_path = meta_sidecar_path(&cache, &hash);
        std::fs::write(&torrent_path, b"data").unwrap();
        std::fs::write(&meta_path, b"not valid json {{{").unwrap();
        let result = load_metadata(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::Invalid(_)));
    }

    #[test]
    fn test_load_metadata_missing_torrent_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x11);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        // Remove torrent file but keep meta
        let torrent_path = torrent_cache_path(&cache, &hash);
        std::fs::remove_file(&torrent_path).unwrap();
        let result = load_metadata(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::NotFound));
    }

    #[test]
    fn test_load_metadata_missing_meta_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x12);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        // Remove meta file but keep torrent
        let meta_path = meta_sidecar_path(&cache, &hash);
        std::fs::remove_file(&meta_path).unwrap();
        let result = load_metadata(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::NotFound));
    }

    // ===== load_cache_meta =====

    #[test]
    fn test_load_cache_meta_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x13);
        let result = load_cache_meta(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::NotFound));
    }

    #[test]
    fn test_load_cache_meta_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0x14);
        let meta_path = meta_sidecar_path(&cache, &hash);
        std::fs::write(&meta_path, b"corrupt{{{").unwrap();
        let result = load_cache_meta(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::Invalid(_)));
    }

    #[test]
    fn test_load_cache_meta_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x15);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        // Tamper with the hash in sidecar
        let meta_path = meta_sidecar_path(&cache, &hash);
        let mut meta: CacheMeta =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.info_hash_hex = "0000000000000000000000000000000000000000".to_string();
        std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        let result = load_cache_meta(&cache, &hash);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CacheError::Invalid(_)));
    }

    // ===== remove_cached =====

    #[test]
    fn test_remove_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x20);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let torrent_path = torrent_cache_path(&cache, &hash);
        let meta_path = meta_sidecar_path(&cache, &hash);
        assert!(torrent_path.is_file());
        assert!(meta_path.is_file());
        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(removed);
        assert!(!torrent_path.is_file());
        assert!(!meta_path.is_file());
    }

    #[test]
    fn test_remove_only_torrent_left() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0x21);
        // Only torrent file, no meta
        let torrent_path = torrent_cache_path(&cache, &hash);
        std::fs::write(&torrent_path, b"data").unwrap();
        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(removed);
        assert!(!torrent_path.is_file());
    }

    #[test]
    fn test_remove_only_meta_left() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let hash = test_hash(0x22);
        // Only meta file, no torrent
        let meta_path = meta_sidecar_path(&cache, &hash);
        let meta = CacheMeta {
            info_hash_hex: hex::encode(hash),
            display_name: None,
            trackers: vec![],
            created_at: 0,
            metadata_size: 0,
        };
        std::fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();
        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(removed);
        assert!(!meta_path.is_file());
    }

    // ===== list_cached =====

    #[test]
    fn test_list_cached_ignores_non_torrent_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        // Write a .txt file and a .meta file (should be ignored)
        std::fs::write(cache.join("random.txt"), b"ignore").unwrap();
        let hash = test_hash(0x30);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        let cached = list_cached(&cache);
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0], hash);
    }

    #[test]
    fn test_list_cached_ignores_invalid_hex_stems() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        // Write a .torrent file with non-hex stem
        std::fs::write(cache.join("not_hex.torrent"), b"data").unwrap();
        let cached = list_cached(&cache);
        assert!(cached.is_empty());
    }

    #[test]
    fn test_list_cached_ignores_short_hex_stems() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        // Write a .torrent file with hex stem but wrong length (10 bytes, not 20)
        std::fs::write(cache.join("aabbccddee.torrent"), b"data").unwrap();
        let cached = list_cached(&cache);
        assert!(cached.is_empty());
    }

    #[test]
    fn test_list_cached_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hashes: Vec<[u8; 20]> = (0..5).map(|i| test_hash(i)).collect();
        for h in &hashes {
            save_metadata(&cache, h, b"data", None, &[]).unwrap();
        }
        let cached = list_cached(&cache);
        assert_eq!(cached.len(), 5);
        for h in &hashes {
            assert!(cached.contains(h));
        }
    }

    // ===== evict_expired =====

    #[test]
    fn test_evict_expired_nonexistent_dir() {
        let dir = PathBuf::from("/nonexistent/cache/dir");
        let removed = evict_expired(&dir, 3600).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_evict_expired_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        std::fs::create_dir_all(&cache).unwrap();
        let removed = evict_expired(&cache, 3600).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_evict_expired_partial() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let old_hash = test_hash(0x40);
        let new_hash = test_hash(0x41);
        save_metadata(&cache, &old_hash, b"old", None, &[]).unwrap();
        save_metadata(&cache, &new_hash, b"new", None, &[]).unwrap();
        // Backdate only the old entry
        let meta_path = meta_sidecar_path(&cache, &old_hash);
        let mut meta: CacheMeta =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.created_at = 1000;
        std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        // Evict entries older than 1 hour
        let removed = evict_expired(&cache, 3600).unwrap();
        assert_eq!(removed, 1);
        assert!(!has_cached(&cache, &old_hash));
        assert!(has_cached(&cache, &new_hash));
    }

    #[test]
    fn test_evict_expired_zero_max_age() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x42);
        save_metadata(&cache, &hash, b"data", None, &[]).unwrap();
        // With max_age_secs=0, everything should be evicted (all entries are "older than 0 seconds")
        // Actually: now - created_at > 0 means anything created more than 0 seconds ago
        // Since we just created it, it might be 0 seconds old, so not evicted.
        // Let's backdate it slightly
        let meta_path = meta_sidecar_path(&cache, &hash);
        let mut meta: CacheMeta =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.created_at = 1000;
        std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        let removed = evict_expired(&cache, 0).unwrap();
        assert_eq!(removed, 1);
    }

    // ===== CacheError Display =====

    #[test]
    fn test_cache_error_io_display() {
        let err = CacheError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "access denied",
        ));
        let msg = format!("{err}");
        assert!(msg.contains("IO error"));
        assert!(msg.contains("access denied"));
    }

    #[test]
    fn test_cache_error_not_found_display() {
        let err = CacheError::NotFound;
        assert_eq!(format!("{err}"), "cache entry not found");
    }

    #[test]
    fn test_cache_error_invalid_display() {
        let err = CacheError::Invalid("bad data".to_string());
        let msg = format!("{err}");
        assert!(msg.contains("invalid cache data"));
        assert!(msg.contains("bad data"));
    }

    #[test]
    fn test_cache_error_io_debug() {
        let err = CacheError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let debug = format!("{err:?}");
        assert!(debug.contains("Io"));
    }

    #[test]
    fn test_cache_error_not_found_debug() {
        let err = CacheError::NotFound;
        let debug = format!("{err:?}");
        assert!(debug.contains("NotFound"));
    }

    #[test]
    fn test_cache_error_invalid_debug() {
        let err = CacheError::Invalid("test".to_string());
        let debug = format!("{err:?}");
        assert!(debug.contains("Invalid"));
    }

    // ===== CacheError From impls =====

    #[test]
    fn test_cache_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let cache_err: CacheError = io_err.into();
        assert!(matches!(cache_err, CacheError::Io(_)));
        assert!(format!("{cache_err}").contains("gone"));
    }

    // ===== CacheMeta Clone/Debug =====

    #[test]
    fn test_cache_meta_clone() {
        let meta = CacheMeta {
            info_hash_hex: "abcdef0123456789abcdef0123456789abcdef01".to_string(),
            display_name: Some("clone_test.txt".to_string()),
            trackers: vec!["http://tracker.example.com/a".to_string()],
            created_at: 1700000000,
            metadata_size: 4096,
        };
        let cloned = meta.clone();
        assert_eq!(cloned.info_hash_hex, meta.info_hash_hex);
        assert_eq!(cloned.display_name, meta.display_name);
        assert_eq!(cloned.trackers, meta.trackers);
        assert_eq!(cloned.created_at, meta.created_at);
        assert_eq!(cloned.metadata_size, meta.metadata_size);
    }

    #[test]
    fn test_cache_meta_debug() {
        let meta = CacheMeta {
            info_hash_hex: "0000000000000000000000000000000000000000".to_string(),
            display_name: None,
            trackers: vec![],
            created_at: 0,
            metadata_size: 0,
        };
        let debug = format!("{meta:?}");
        assert!(debug.contains("CacheMeta"));
        assert!(debug.contains("info_hash_hex"));
    }

    // ===== Edge cases =====

    #[test]
    fn test_all_zero_hash() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = [0u8; 20];
        save_metadata(&cache, &hash, b"zero hash data", None, &[]).unwrap();
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, b"zero hash data");
    }

    #[test]
    fn test_all_ff_hash() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = [0xFFu8; 20];
        save_metadata(&cache, &hash, b"ff hash data", None, &[]).unwrap();
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, b"ff hash data");
    }

    #[test]
    fn test_unicode_display_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x50);
        let name = "日本語ファイル名_🎉_тест";
        save_metadata(&cache, &hash, b"data", Some(name), &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some(name.to_string()));
    }

    #[test]
    fn test_special_chars_in_display_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x51);
        let name = "file with spaces & symbols <>.txt";
        save_metadata(&cache, &hash, b"data", Some(name), &[]).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some(name.to_string()));
    }

    #[test]
    fn test_many_trackers() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x52);
        let trackers: Vec<String> = (0..50)
            .map(|i| format!("http://tracker{i}.example.com/announce"))
            .collect();
        save_metadata(&cache, &hash, b"data", None, &trackers).unwrap();
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.trackers.len(), 50);
    }

    #[test]
    fn test_save_and_load_binary_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0x53);
        // Binary data with null bytes and high bytes
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        save_metadata(&cache, &hash, &data, None, &[]).unwrap();
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_concurrent_hash_distinct_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        // Store 10 entries with distinct hashes
        for i in 0..10u8 {
            let hash = test_hash(i);
            let data = format!("data_{i}");
            save_metadata(
                &cache,
                &hash,
                data.as_bytes(),
                Some(&format!("file_{i}")),
                &[],
            )
            .unwrap();
        }
        // Verify all 10 are independently accessible
        for i in 0..10u8 {
            let hash = test_hash(i);
            let loaded = load_metadata(&cache, &hash).unwrap();
            assert_eq!(loaded, format!("data_{i}").as_bytes());
            let meta = load_cache_meta(&cache, &hash).unwrap();
            assert_eq!(meta.display_name, Some(format!("file_{i}")));
        }
        // list_cached should find all 10
        let cached = list_cached(&cache);
        assert_eq!(cached.len(), 10);
    }

    // ===== cache_dir =====

    #[test]
    fn test_cache_dir_contains_metadata() {
        let dir = cache_dir();
        let dir_str = dir.to_str().unwrap();
        assert!(dir_str.ends_with("metadata") || dir_str.contains("metadata"));
    }

    #[test]
    fn test_cache_dir_contains_ipmsg_torrent() {
        let dir = cache_dir();
        let dir_str = dir.to_str().unwrap();
        assert!(dir_str.contains("ipmsg-torrent"));
    }

    // ===== Complete workflow =====

    #[test]
    fn test_complete_cache_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0xEE);
        let metadata = b"d4:infod6:lengthi2048e4:name12:example.mp412:piece lengthi262144e6:pieces20:aaaaaaaaaaaaaaaaaaaabbbb13:announce35:http://tracker.example.com/announceee";
        let trackers = vec![
            "http://tracker.example.com/announce".to_string(),
            "udp://backup.example.com:6969".to_string(),
        ];
        // 1. Save
        save_metadata(&cache, &hash, metadata, Some("example.mp4"), &trackers).unwrap();
        // 2. Verify exists
        assert!(has_cached(&cache, &hash));
        // 3. Load and validate
        let loaded = load_metadata(&cache, &hash).unwrap();
        assert_eq!(loaded, metadata);
        let meta = load_cache_meta(&cache, &hash).unwrap();
        assert_eq!(meta.display_name, Some("example.mp4".to_string()));
        assert_eq!(meta.trackers, trackers);
        assert_eq!(meta.metadata_size, metadata.len() as u64);
        assert_eq!(meta.info_hash_hex, hex::encode(hash));
        // 4. List
        let cached = list_cached(&cache);
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0], hash);
        // 5. Remove
        let removed = remove_cached(&cache, &hash).unwrap();
        assert!(removed);
        assert!(!has_cached(&cache, &hash));
        // 6. List empty
        let cached = list_cached(&cache);
        assert!(cached.is_empty());
    }

    #[test]
    fn test_overwrite_preserves_cache_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("metadata");
        let hash = test_hash(0xDD);
        // First version
        save_metadata(
            &cache,
            &hash,
            b"v1_data",
            Some("v1.txt"),
            &["http://t1.com/a".to_string()],
        )
        .unwrap();
        let m1 = load_cache_meta(&cache, &hash).unwrap();
        // Second version (overwrite)
        save_metadata(
            &cache,
            &hash,
            b"v2_data_longer",
            Some("v2.txt"),
            &["http://t2.com/a".to_string(), "http://t3.com/a".to_string()],
        )
        .unwrap();
        let m2 = load_cache_meta(&cache, &hash).unwrap();
        // Verify second version completely replaced first
        assert_eq!(load_metadata(&cache, &hash).unwrap(), b"v2_data_longer");
        assert_eq!(m2.display_name, Some("v2.txt".to_string()));
        assert_eq!(m2.trackers.len(), 2);
        assert_eq!(m2.metadata_size, 14); // len of "v2_data_longer"
        assert!(m2.created_at >= m1.created_at);
    }
}
