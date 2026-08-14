//! Task Favorites/Pinning System
//!
//! Allows users to pin/favorite download tasks so they always appear at the top
//! of the queue and get priority scheduling. Favorites are persisted to disk.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Favorite task entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoriteTask {
    /// Task ID
    pub task_id: String,
    /// When the task was favorited
    pub favorited_at: DateTime<Utc>,
    /// Optional note/reason for favoriting
    pub note: Option<String>,
}

/// Favorites configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FavoritesConfig {
    /// Maximum number of favorites (0 = unlimited)
    pub max_favorites: usize,
}

/// Favorites manager
#[derive(Debug, Default)]
pub struct FavoritesManager {
    favorites: Vec<FavoriteTask>,
    config: FavoritesConfig,
}

impl FavoritesManager {
    /// Create new favorites manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Add task to favorites
    pub fn add_favorite(&mut self, task_id: String, note: Option<String>) -> Result<(), String> {
        // Check if already favorited
        if self.favorites.iter().any(|f| f.task_id == task_id) {
            return Err("Task already in favorites".to_string());
        }

        // Check max limit
        if self.config.max_favorites > 0 && self.favorites.len() >= self.config.max_favorites {
            return Err(format!(
                "Maximum favorites limit reached ({})",
                self.config.max_favorites
            ));
        }

        self.favorites.push(FavoriteTask {
            task_id,
            favorited_at: Utc::now(),
            note,
        });

        Ok(())
    }

    /// Remove task from favorites
    pub fn remove_favorite(&mut self, task_id: &str) -> bool {
        let initial_len = self.favorites.len();
        self.favorites.retain(|f| f.task_id != task_id);
        self.favorites.len() < initial_len
    }

    /// Check if task is favorited
    pub fn is_favorite(&self, task_id: &str) -> bool {
        self.favorites.iter().any(|f| f.task_id == task_id)
    }

    /// Get all favorite task IDs
    pub fn get_favorite_ids(&self) -> HashSet<String> {
        self.favorites.iter().map(|f| f.task_id.clone()).collect()
    }

    /// Get all favorites
    pub fn get_favorites(&self) -> &[FavoriteTask] {
        &self.favorites
    }

    /// Get favorites count
    pub fn count(&self) -> usize {
        self.favorites.len()
    }

    /// Set configuration
    pub fn set_config(&mut self, config: FavoritesConfig) {
        self.config = config;
    }

    /// Get configuration
    pub fn get_config(&self) -> &FavoritesConfig {
        &self.config
    }

    /// Save favorites to file
    pub fn save_to_file(&self, path: &Path) -> Result<(), String> {
        let json = serde_json::to_string_pretty(&self.favorites)
            .map_err(|e| format!("Failed to serialize favorites: {}", e))?;

        let temp_path = path.with_extension("json.tmp");
        let mut file = fs::File::create(&temp_path)
            .map_err(|e| format!("Failed to create temp file: {}", e))?;
        file.write_all(json.as_bytes())
            .map_err(|e| format!("Failed to write temp file: {}", e))?;

        fs::rename(&temp_path, path).map_err(|e| format!("Failed to rename temp file: {}", e))?;

        Ok(())
    }

    /// Load favorites from file
    pub fn load_from_file(&mut self, path: &Path) -> Result<(), String> {
        if !path.exists() {
            return Ok(());
        }

        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read favorites file: {}", e))?;

        self.favorites = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse favorites: {}", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    // ========== Original tests (preserved) ==========

    #[test]
    fn test_add_favorite() {
        let mut manager = FavoritesManager::new();
        assert_eq!(manager.count(), 0);

        manager
            .add_favorite("task1".to_string(), Some("Important".to_string()))
            .unwrap();
        assert_eq!(manager.count(), 1);
        assert!(manager.is_favorite("task1"));
    }

    #[test]
    fn test_add_duplicate() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();

        let result = manager.add_favorite("task1".to_string(), None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Task already in favorites");
    }

    #[test]
    fn test_remove_favorite() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        assert!(manager.is_favorite("task1"));

        let removed = manager.remove_favorite("task1");
        assert!(removed);
        assert!(!manager.is_favorite("task1"));
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut manager = FavoritesManager::new();
        let removed = manager.remove_favorite("task1");
        assert!(!removed);
    }

    #[test]
    fn test_get_favorite_ids() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();

        let ids = manager.get_favorite_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("task1"));
        assert!(ids.contains("task2"));
    }

    #[test]
    fn test_max_favorites_limit() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 2 });

        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();

        let result = manager.add_favorite("task3".to_string(), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Maximum favorites limit"));
    }

    #[test]
    fn test_unlimited_favorites() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 0 });

        for i in 0..100 {
            manager.add_favorite(format!("task{}", i), None).unwrap();
        }

        assert_eq!(manager.count(), 100);
    }

    #[test]
    fn test_save_and_load() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("task1".to_string(), Some("Note 1".to_string()))
            .unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();

        manager.save_to_file(path).unwrap();

        let mut loaded_manager = FavoritesManager::new();
        loaded_manager.load_from_file(path).unwrap();

        assert_eq!(loaded_manager.count(), 2);
        assert!(loaded_manager.is_favorite("task1"));
        assert!(loaded_manager.is_favorite("task2"));

        let favorites = loaded_manager.get_favorites();
        assert_eq!(favorites[0].task_id, "task1");
        assert_eq!(favorites[0].note, Some("Note 1".to_string()));
        assert_eq!(favorites[1].task_id, "task2");
        assert_eq!(favorites[1].note, None);
    }

    #[test]
    fn test_load_nonexistent_file() {
        let mut manager = FavoritesManager::new();
        let result = manager.load_from_file(Path::new("/nonexistent/path.json"));
        assert!(result.is_ok());
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_get_favorites() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();

        let favorites = manager.get_favorites();
        assert_eq!(favorites.len(), 2);
        assert_eq!(favorites[0].task_id, "task1");
        assert_eq!(favorites[1].task_id, "task2");
    }

    #[test]
    fn test_set_get_config() {
        let mut manager = FavoritesManager::new();
        assert_eq!(manager.get_config().max_favorites, 0);

        manager.set_config(FavoritesConfig { max_favorites: 10 });
        assert_eq!(manager.get_config().max_favorites, 10);
    }

    // ========== New comprehensive tests ==========

    // --- FavoriteTask struct tests ---

    #[test]
    fn test_favorite_task_serde_roundtrip_with_note() {
        let fav = FavoriteTask {
            task_id: "abc-123".to_string(),
            favorited_at: Utc::now(),
            note: Some("Important download".to_string()),
        };
        let json = serde_json::to_string(&fav).unwrap();
        let deserialized: FavoriteTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "abc-123");
        assert_eq!(deserialized.note, Some("Important download".to_string()));
    }

    #[test]
    fn test_favorite_task_serde_roundtrip_without_note() {
        let fav = FavoriteTask {
            task_id: "task-456".to_string(),
            favorited_at: Utc::now(),
            note: None,
        };
        let json = serde_json::to_string(&fav).unwrap();
        let deserialized: FavoriteTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-456");
        assert_eq!(deserialized.note, None);
    }

    #[test]
    fn test_favorite_task_serde_extra_fields_ignored() {
        let json = r#"{"task_id":"t1","favorited_at":"2026-01-01T00:00:00Z","note":null,"extra_field":"ignored"}"#;
        let deserialized: FavoriteTask = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.task_id, "t1");
        assert_eq!(deserialized.note, None);
    }

    #[test]
    fn test_favorite_task_clone() {
        let fav = FavoriteTask {
            task_id: "clone-test".to_string(),
            favorited_at: Utc::now(),
            note: Some("Clone me".to_string()),
        };
        let cloned = fav.clone();
        assert_eq!(cloned.task_id, fav.task_id);
        assert_eq!(cloned.note, fav.note);
        assert_eq!(cloned.favorited_at, fav.favorited_at);
    }

    #[test]
    fn test_favorite_task_debug() {
        let fav = FavoriteTask {
            task_id: "debug-test".to_string(),
            favorited_at: Utc::now(),
            note: None,
        };
        let debug_str = format!("{:?}", fav);
        assert!(debug_str.contains("debug-test"));
        assert!(debug_str.contains("FavoriteTask"));
    }

    // --- FavoritesConfig tests ---

    #[test]
    fn test_favorites_config_default() {
        let config = FavoritesConfig::default();
        assert_eq!(config.max_favorites, 0);
    }

    #[test]
    fn test_favorites_config_serde_roundtrip() {
        let config = FavoritesConfig { max_favorites: 42 };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: FavoritesConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_favorites, 42);
    }

    #[test]
    fn test_favorites_config_extra_fields_ignored() {
        let json = r#"{"max_favorites":5,"unknown_field":true}"#;
        let config: FavoritesConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_favorites, 5);
    }

    #[test]
    fn test_favorites_config_clone() {
        let config = FavoritesConfig { max_favorites: 10 };
        let cloned = config.clone();
        assert_eq!(cloned.max_favorites, config.max_favorites);
    }

    #[test]
    fn test_favorites_config_debug() {
        let config = FavoritesConfig { max_favorites: 7 };
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("7"));
        assert!(debug_str.contains("FavoritesConfig"));
    }

    // --- FavoritesManager constructor tests ---

    #[test]
    fn test_manager_new_empty() {
        let manager = FavoritesManager::new();
        assert_eq!(manager.count(), 0);
        assert!(manager.get_favorites().is_empty());
        assert!(manager.get_favorite_ids().is_empty());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let manager = FavoritesManager::default();
        assert_eq!(manager.count(), 0);
        assert_eq!(manager.get_config().max_favorites, 0);
    }

    // --- add_favorite boundary tests ---

    #[test]
    fn test_add_favorite_with_empty_task_id() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("".to_string(), None).unwrap();
        assert!(manager.is_favorite(""));
        assert_eq!(manager.count(), 1);
    }

    #[test]
    fn test_add_favorite_with_unicode_task_id() {
        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("任务-中文".to_string(), Some("中文任务".to_string()))
            .unwrap();
        assert!(manager.is_favorite("任务-中文"));
    }

    #[test]
    fn test_add_favorite_with_emoji_task_id() {
        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("🚀-download".to_string(), None)
            .unwrap();
        assert!(manager.is_favorite("🚀-download"));
    }

    #[test]
    fn test_add_favorite_preserves_order() {
        let mut manager = FavoritesManager::new();
        for i in 0..10 {
            manager.add_favorite(format!("task{}", i), None).unwrap();
        }
        let favorites = manager.get_favorites();
        for (idx, fav) in favorites.iter().enumerate() {
            assert_eq!(fav.task_id, format!("task{}", idx));
        }
    }

    #[test]
    fn test_add_favorite_with_long_note() {
        let mut manager = FavoritesManager::new();
        let long_note = "x".repeat(10_000);
        manager
            .add_favorite("task1".to_string(), Some(long_note.clone()))
            .unwrap();
        let favorites = manager.get_favorites();
        assert_eq!(favorites[0].note.as_ref().unwrap().len(), 10_000);
    }

    #[test]
    fn test_add_favorite_duplicate_error_message() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        let err = manager.add_favorite("task1".to_string(), None).unwrap_err();
        assert_eq!(err, "Task already in favorites");
    }

    #[test]
    fn test_add_favorite_max_limit_error_message() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 1 });
        manager.add_favorite("task1".to_string(), None).unwrap();
        let err = manager.add_favorite("task2".to_string(), None).unwrap_err();
        assert!(err.contains("1"));
        assert!(err.contains("Maximum favorites limit"));
    }

    #[test]
    fn test_add_favorite_max_favorites_1() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 1 });
        manager.add_favorite("task1".to_string(), None).unwrap();
        let result = manager.add_favorite("task2".to_string(), None);
        assert!(result.is_err());
        assert_eq!(manager.count(), 1);
    }

    // --- remove_favorite boundary tests ---

    #[test]
    fn test_remove_favorite_from_empty() {
        let mut manager = FavoritesManager::new();
        assert!(!manager.remove_favorite("anything"));
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_remove_favorite_idempotent() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        assert!(manager.remove_favorite("task1"));
        assert!(!manager.remove_favorite("task1"));
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_remove_favorite_preserves_others() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();
        manager.add_favorite("task3".to_string(), None).unwrap();

        manager.remove_favorite("task2");
        assert_eq!(manager.count(), 2);
        assert!(!manager.is_favorite("task2"));
        assert!(manager.is_favorite("task1"));
        assert!(manager.is_favorite("task3"));
    }

    #[test]
    fn test_remove_favorite_empty_task_id() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("".to_string(), None).unwrap();
        assert!(manager.remove_favorite(""));
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_remove_favorite_unicode() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("中文任务".to_string(), None).unwrap();
        assert!(manager.remove_favorite("中文任务"));
        assert_eq!(manager.count(), 0);
    }

    // --- is_favorite boundary tests ---

    #[test]
    fn test_is_favorite_empty_manager() {
        let manager = FavoritesManager::new();
        assert!(!manager.is_favorite("anything"));
    }

    #[test]
    fn test_is_favorite_after_removal() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.remove_favorite("task1");
        assert!(!manager.is_favorite("task1"));
    }

    #[test]
    fn test_is_favorite_partial_match() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        // "task" is a substring of "task1" but should not match
        assert!(!manager.is_favorite("task"));
        assert!(!manager.is_favorite("task12"));
    }

    // --- get_favorite_ids tests ---

    #[test]
    fn test_get_favorite_ids_empty() {
        let manager = FavoritesManager::new();
        let ids = manager.get_favorite_ids();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_get_favorite_ids_single() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("only-one".to_string(), None).unwrap();
        let ids = manager.get_favorite_ids();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("only-one"));
    }

    #[test]
    fn test_get_favorite_ids_many() {
        let mut manager = FavoritesManager::new();
        for i in 0..50 {
            manager.add_favorite(format!("task-{}", i), None).unwrap();
        }
        let ids = manager.get_favorite_ids();
        assert_eq!(ids.len(), 50);
        for i in 0..50 {
            assert!(ids.contains(&format!("task-{}", i)));
        }
    }

    // --- count tests ---

    #[test]
    fn test_count_zero_initially() {
        let manager = FavoritesManager::new();
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_count_after_add_and_remove() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("a".to_string(), None).unwrap();
        manager.add_favorite("b".to_string(), None).unwrap();
        assert_eq!(manager.count(), 2);
        manager.remove_favorite("a");
        assert_eq!(manager.count(), 1);
    }

    // --- get_favorites tests ---

    #[test]
    fn test_get_favorites_empty_slice() {
        let manager = FavoritesManager::new();
        assert!(manager.get_favorites().is_empty());
    }

    #[test]
    fn test_get_favorites_returns_correct_order() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("first".to_string(), None).unwrap();
        manager.add_favorite("second".to_string(), None).unwrap();
        manager.add_favorite("third".to_string(), None).unwrap();

        let favs = manager.get_favorites();
        assert_eq!(favs[0].task_id, "first");
        assert_eq!(favs[1].task_id, "second");
        assert_eq!(favs[2].task_id, "third");
    }

    // --- set_config / get_config tests ---

    #[test]
    fn test_set_config_overwrite() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 5 });
        assert_eq!(manager.get_config().max_favorites, 5);
        manager.set_config(FavoritesConfig { max_favorites: 100 });
        assert_eq!(manager.get_config().max_favorites, 100);
    }

    #[test]
    fn test_set_config_does_not_clear_favorites() {
        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();
        manager.set_config(FavoritesConfig { max_favorites: 1 });
        // Favorites should still be there even if over limit
        assert_eq!(manager.count(), 2);
    }

    #[test]
    fn test_get_config_returns_reference() {
        let mut manager = FavoritesManager::new();
        let config = manager.get_config();
        assert_eq!(config.max_favorites, 0);
    }

    // --- Persistence tests ---

    #[test]
    fn test_save_creates_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        // Remove the file so save_to_file creates it fresh
        let _ = std::fs::remove_file(&path);

        let manager = FavoritesManager::new();
        manager.save_to_file(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_save_empty_favorites() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let manager = FavoritesManager::new();
        manager.save_to_file(path).unwrap();

        let mut loaded = FavoritesManager::new();
        loaded.load_from_file(path).unwrap();
        assert_eq!(loaded.count(), 0);
    }

    #[test]
    fn test_save_overwrites_previous() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.save_to_file(path).unwrap();

        // Now overwrite with different data
        let mut manager2 = FavoritesManager::new();
        manager2.add_favorite("task2".to_string(), None).unwrap();
        manager2.add_favorite("task3".to_string(), None).unwrap();
        manager2.save_to_file(path).unwrap();

        let mut loaded = FavoritesManager::new();
        loaded.load_from_file(path).unwrap();
        assert_eq!(loaded.count(), 2);
        assert!(!loaded.is_favorite("task1"));
        assert!(loaded.is_favorite("task2"));
        assert!(loaded.is_favorite("task3"));
    }

    #[test]
    fn test_save_no_tmp_file_left() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let tmp_path = path.with_extension("json.tmp");

        let manager = FavoritesManager::new();
        manager.save_to_file(path).unwrap();

        // The temp file should have been renamed, not left behind
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_load_corrupt_json() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        std::fs::write(path, "not valid json {{{").unwrap();

        let mut manager = FavoritesManager::new();
        let result = manager.load_from_file(path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to parse favorites"));
    }

    #[test]
    fn test_load_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        std::fs::write(path, "").unwrap();

        let mut manager = FavoritesManager::new();
        let result = manager.load_from_file(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_empty_json_array() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        std::fs::write(path, "[]").unwrap();

        let mut manager = FavoritesManager::new();
        manager.load_from_file(path).unwrap();
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_persistence_with_unicode() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("任务-1".to_string(), Some("重要下载".to_string()))
            .unwrap();
        manager
            .add_favorite("🎬-movie".to_string(), Some("电影".to_string()))
            .unwrap();
        manager.save_to_file(path).unwrap();

        let mut loaded = FavoritesManager::new();
        loaded.load_from_file(path).unwrap();
        assert_eq!(loaded.count(), 2);
        assert!(loaded.is_favorite("任务-1"));
        assert!(loaded.is_favorite("🎬-movie"));
        let favs = loaded.get_favorites();
        assert_eq!(favs[0].note, Some("重要下载".to_string()));
        assert_eq!(favs[1].note, Some("电影".to_string()));
    }

    #[test]
    fn test_persistence_full_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 50 });
        for i in 0..20 {
            let note = if i % 2 == 0 {
                Some(format!("Note {}", i))
            } else {
                None
            };
            manager.add_favorite(format!("task-{}", i), note).unwrap();
        }
        manager.save_to_file(path).unwrap();

        let mut loaded = FavoritesManager::new();
        loaded.load_from_file(path).unwrap();
        assert_eq!(loaded.count(), 20);
        for i in 0..20 {
            assert!(loaded.is_favorite(&format!("task-{}", i)));
            let favs = loaded.get_favorites();
            let fav = favs
                .iter()
                .find(|f| f.task_id == format!("task-{}", i))
                .unwrap();
            if i % 2 == 0 {
                assert_eq!(fav.note, Some(format!("Note {}", i)));
            } else {
                assert_eq!(fav.note, None);
            }
        }
    }

    // --- FavoritesManager Debug trait ---

    #[test]
    fn test_manager_debug() {
        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("debug-task".to_string(), None)
            .unwrap();
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("FavoritesManager"));
        assert!(debug_str.contains("debug-task"));
    }

    // --- Complex workflow tests ---

    #[test]
    fn test_complete_lifecycle() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Create manager, add favorites
        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("task-a".to_string(), Some("First".to_string()))
            .unwrap();
        manager
            .add_favorite("task-b".to_string(), Some("Second".to_string()))
            .unwrap();
        manager.add_favorite("task-c".to_string(), None).unwrap();
        assert_eq!(manager.count(), 3);

        // Save and reload
        manager.save_to_file(path).unwrap();
        let mut loaded = FavoritesManager::new();
        loaded.load_from_file(path).unwrap();
        assert_eq!(loaded.count(), 3);

        // Remove one
        loaded.remove_favorite("task-b");
        assert_eq!(loaded.count(), 2);
        assert!(!loaded.is_favorite("task-b"));
        assert!(loaded.is_favorite("task-a"));
        assert!(loaded.is_favorite("task-c"));

        // Add another
        loaded.add_favorite("task-d".to_string(), None).unwrap();
        assert_eq!(loaded.count(), 3);

        // Save again and verify
        loaded.save_to_file(path).unwrap();
        let mut reloaded = FavoritesManager::new();
        reloaded.load_from_file(path).unwrap();
        assert_eq!(reloaded.count(), 3);
        assert!(!reloaded.is_favorite("task-b"));
        assert!(reloaded.is_favorite("task-d"));
    }

    #[test]
    fn test_multiple_managers_independent() {
        let mut m1 = FavoritesManager::new();
        let mut m2 = FavoritesManager::new();

        m1.add_favorite("task1".to_string(), None).unwrap();
        m2.add_favorite("task2".to_string(), None).unwrap();

        assert!(m1.is_favorite("task1"));
        assert!(!m1.is_favorite("task2"));
        assert!(m2.is_favorite("task2"));
        assert!(!m2.is_favorite("task1"));
    }

    #[test]
    fn test_add_remove_readd_same_task() {
        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("task1".to_string(), Some("First time".to_string()))
            .unwrap();
        manager.remove_favorite("task1");
        manager
            .add_favorite("task1".to_string(), Some("Second time".to_string()))
            .unwrap();
        assert_eq!(manager.count(), 1);
        assert!(manager.is_favorite("task1"));
        let favs = manager.get_favorites();
        assert_eq!(favs[0].note, Some("Second time".to_string()));
    }

    #[test]
    fn test_max_favorites_then_remove_then_add() {
        let mut manager = FavoritesManager::new();
        manager.set_config(FavoritesConfig { max_favorites: 2 });

        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.add_favorite("task2".to_string(), None).unwrap();
        assert!(manager.add_favorite("task3".to_string(), None).is_err());

        manager.remove_favorite("task1");
        // Now we can add task3
        manager.add_favorite("task3".to_string(), None).unwrap();
        assert_eq!(manager.count(), 2);
        assert!(manager.is_favorite("task2"));
        assert!(manager.is_favorite("task3"));
    }

    // --- Edge case: large number of favorites ---

    #[test]
    fn test_many_favorites() {
        let mut manager = FavoritesManager::new();
        for i in 0..500 {
            manager.add_favorite(format!("task-{}", i), None).unwrap();
        }
        assert_eq!(manager.count(), 500);
        assert!(manager.is_favorite("task-250"));
        assert!(!manager.is_favorite("task-500"));
    }

    // --- Save to specific paths ---

    #[test]
    fn test_save_to_nested_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("subdir").join("favorites.json");

        // Parent dir doesn't exist yet, save should fail gracefully
        let manager = FavoritesManager::new();
        let result = manager.save_to_file(&path);
        // The parent directory doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_directory_instead_of_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path();

        let mut manager = FavoritesManager::new();
        // Reading a directory should fail
        let result = manager.load_from_file(path);
        assert!(result.is_err());
    }

    // --- Config interaction with add ---

    #[test]
    fn test_config_change_affects_subsequent_adds() {
        let mut manager = FavoritesManager::new();
        // Start with no limit
        for i in 0..5 {
            manager.add_favorite(format!("task{}", i), None).unwrap();
        }
        assert_eq!(manager.count(), 5);

        // Set limit to 5 - should prevent adding more
        manager.set_config(FavoritesConfig { max_favorites: 5 });
        assert!(manager.add_favorite("task-new".to_string(), None).is_err());

        // Increase limit
        manager.set_config(FavoritesConfig { max_favorites: 10 });
        manager.add_favorite("task-new".to_string(), None).unwrap();
        assert_eq!(manager.count(), 6);
    }

    // --- Serialization format verification ---

    #[test]
    fn test_save_format_is_pretty_json() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager.add_favorite("task1".to_string(), None).unwrap();
        manager.save_to_file(path).unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        // Pretty JSON should contain newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    #[test]
    fn test_saved_json_contains_task_id() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut manager = FavoritesManager::new();
        manager
            .add_favorite("unique-task-id-12345".to_string(), None)
            .unwrap();
        manager.save_to_file(path).unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("unique-task-id-12345"));
    }
}
