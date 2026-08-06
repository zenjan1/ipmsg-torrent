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
}
