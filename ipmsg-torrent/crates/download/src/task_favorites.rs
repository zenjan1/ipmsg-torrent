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
    use tempfile::NamedTempFile;

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
}
