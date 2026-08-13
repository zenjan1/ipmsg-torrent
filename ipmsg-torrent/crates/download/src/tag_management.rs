// Phase 125: Tag Management System
// Manages tags across all download tasks: rename, merge, aliases, orphan cleanup, usage stats.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing;

/// Information about a single tag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagInfo {
    /// The canonical tag name
    pub name: String,
    /// Number of tasks currently using this tag
    pub usage_count: u32,
    /// Timestamp (Unix seconds) when this tag was last used (added to a task)
    pub last_used_at: Option<u64>,
    /// Timestamp (Unix seconds) when this tag was created
    pub created_at: u64,
    /// Optional emoji or color label for UI display
    pub label: Option<String>,
}

/// Alias mapping: alias -> canonical tag name
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TagAliasMap {
    pub aliases: HashMap<String, String>,
}

/// Configuration for tag management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagManagementConfig {
    /// Enable automatic orphan tag cleanup
    pub auto_cleanup_orphans: bool,
    /// Number of seconds after which an unused tag is considered orphan (default 7 days)
    pub orphan_threshold_secs: u64,
    /// Enable tag alias resolution (default true)
    pub enable_aliases: bool,
    /// Maximum number of tags allowed (0 = unlimited)
    pub max_tags: usize,
}

impl Default for TagManagementConfig {
    fn default() -> Self {
        Self {
            auto_cleanup_orphans: false,
            orphan_threshold_secs: 7 * 24 * 3600, // 7 days
            enable_aliases: true,
            max_tags: 0,
        }
    }
}

/// Summary of tag management status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagManagementSummary {
    /// Total unique tags
    pub total_tags: usize,
    /// Tags with zero usage (orphans)
    pub orphan_tags: Vec<String>,
    /// Total alias mappings
    pub alias_count: usize,
    /// Top 10 most used tags
    pub top_tags: Vec<(String, u32)>,
    /// Tags never used (created but never applied)
    pub unused_tags: Vec<String>,
    /// Configuration
    pub config: TagManagementConfig,
}

/// Actions the tag manager can perform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TagAction {
    /// Rename a tag across all tasks
    Renamed {
        old: String,
        new: String,
        affected_tasks: usize,
    },
    /// Merge source tag into target tag
    Merged {
        source: String,
        target: String,
        affected_tasks: usize,
    },
    /// Cleaned up orphan tags
    OrphansCleaned { removed: Vec<String> },
    /// Added an alias
    AliasAdded { alias: String, canonical: String },
    /// Removed an alias
    AliasRemoved { alias: String },
}

/// The tag manager
#[derive(Debug)]
pub struct TagManager {
    /// All known tags with metadata
    tags: Arc<RwLock<HashMap<String, TagInfo>>>,
    /// Alias mappings: alias -> canonical name
    aliases: Arc<RwLock<TagAliasMap>>,
    /// Configuration
    config: Arc<RwLock<TagManagementConfig>>,
    /// Data directory for persistence
    data_dir: std::path::PathBuf,
}

impl TagManager {
    /// Create a new TagManager
    pub fn new(data_dir: &Path) -> Self {
        Self {
            tags: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(TagAliasMap::default())),
            config: Arc::new(RwLock::new(TagManagementConfig::default())),
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Restore from disk
    pub async fn restore(&self) {
        // Load config
        let config_path = self.data_dir.join("tag_management_config.json");
        if config_path.exists() {
            match tokio::fs::read_to_string(&config_path).await {
                Ok(content) => match serde_json::from_str::<TagManagementConfig>(&content) {
                    Ok(cfg) => {
                        *self.config.write().await = cfg;
                        tracing::info!("Loaded tag management config");
                    }
                    Err(e) => tracing::warn!("Failed to parse tag management config: {}", e),
                },
                Err(e) => tracing::warn!("Failed to read tag management config: {}", e),
            }
        }

        // Load tags
        let tags_path = self.data_dir.join("tag_management.json");
        if tags_path.exists() {
            match tokio::fs::read_to_string(&tags_path).await {
                Ok(content) => {
                    #[derive(Deserialize)]
                    struct TagData {
                        #[serde(default)]
                        tags: HashMap<String, TagInfo>,
                        #[serde(default)]
                        aliases: TagAliasMap,
                    }
                    match serde_json::from_str::<TagData>(&content) {
                        Ok(data) => {
                            *self.tags.write().await = data.tags;
                            *self.aliases.write().await = data.aliases;
                            tracing::info!("Loaded tag management data");
                        }
                        Err(e) => tracing::warn!("Failed to parse tag management data: {}", e),
                    }
                }
                Err(e) => tracing::warn!("Failed to read tag management data: {}", e),
            }
        }
    }

    /// Persist to disk
    async fn save(&self) {
        let tags = self.tags.read().await.clone();
        let aliases = self.aliases.read().await.clone();

        #[derive(Serialize)]
        struct TagData<'a> {
            tags: &'a HashMap<String, TagInfo>,
            aliases: &'a TagAliasMap,
        }

        let data = TagData {
            tags: &tags,
            aliases: &aliases,
        };
        let path = self.data_dir.join("tag_management.json");
        if let Ok(json) = serde_json::to_string_pretty(&data) {
            let _ = tokio::fs::write(&path, json).await;
        }
    }

    /// Save config to disk
    async fn save_config(&self) {
        let config = self.config.read().await.clone();
        let path = self.data_dir.join("tag_management_config.json");
        if let Ok(json) = serde_json::to_string_pretty(&config) {
            let _ = tokio::fs::write(&path, json).await;
        }
    }

    /// Resolve a tag name through aliases (returns canonical name)
    pub async fn resolve_alias(&self, tag: &str) -> String {
        let aliases = self.aliases.read().await;
        let config = self.config.read().await;
        if config.enable_aliases {
            aliases
                .aliases
                .get(tag)
                .cloned()
                .unwrap_or_else(|| tag.to_string())
        } else {
            tag.to_string()
        }
    }

    /// Register a tag usage (called when a tag is added to a task)
    pub async fn register_usage(&self, tag: &str, now: u64) {
        let canonical = self.resolve_alias(tag).await;
        let mut tags = self.tags.write().await;
        let info = tags.entry(canonical.clone()).or_insert_with(|| TagInfo {
            name: canonical,
            usage_count: 0,
            last_used_at: None,
            created_at: now,
            label: None,
        });
        info.usage_count += 1;
        info.last_used_at = Some(now);
        drop(tags);
        let _ = self.save().await;
    }

    /// Register multiple tag usages at once
    pub async fn register_usages(&self, tags_list: &[String], now: u64) {
        let mut tags = self.tags.write().await;
        let aliases = self.aliases.read().await;
        let config = self.config.read().await;

        for tag in tags_list {
            let canonical = if config.enable_aliases {
                aliases
                    .aliases
                    .get(tag.as_str())
                    .cloned()
                    .unwrap_or_else(|| tag.clone())
            } else {
                tag.clone()
            };

            let info = tags.entry(canonical.clone()).or_insert_with(|| TagInfo {
                name: canonical,
                usage_count: 0,
                last_used_at: None,
                created_at: now,
                label: None,
            });
            info.usage_count += 1;
            info.last_used_at = Some(now);
        }
        drop(tags);
        drop(aliases);
        drop(config);
        let _ = self.save().await;
    }

    /// Unregister a tag usage (called when a tag is removed from a task)
    pub async fn unregister_usage(&self, tag: &str) {
        let canonical = self.resolve_alias(tag).await;
        let mut tags = self.tags.write().await;
        if let Some(info) = tags.get_mut(&canonical) {
            info.usage_count = info.usage_count.saturating_sub(1);
        }
        drop(tags);
        let _ = self.save().await;
    }

    /// Unregister multiple tag usages at once
    pub async fn unregister_usages(&self, tags_list: &[String]) {
        let mut tags = self.tags.write().await;
        let aliases = self.aliases.read().await;
        let config = self.config.read().await;

        for tag in tags_list {
            let canonical = if config.enable_aliases {
                aliases
                    .aliases
                    .get(tag.as_str())
                    .cloned()
                    .unwrap_or_else(|| tag.clone())
            } else {
                tag.clone()
            };

            if let Some(info) = tags.get_mut(&canonical) {
                info.usage_count = info.usage_count.saturating_sub(1);
            }
        }
        drop(tags);
        drop(aliases);
        drop(config);
        let _ = self.save().await;
    }

    /// Sync tag usage counts from actual task data (authoritative source)
    pub async fn sync_from_tasks(&self, task_tags: &[Vec<String>], now: u64) {
        let mut tags = self.tags.write().await;
        let aliases = self.aliases.read().await;
        let config = self.config.read().await;

        // Count actual usage
        let mut counts: HashMap<String, u32> = HashMap::new();
        for task_tag_list in task_tags {
            for tag in task_tag_list {
                let canonical = if config.enable_aliases {
                    aliases
                        .aliases
                        .get(tag.as_str())
                        .cloned()
                        .unwrap_or_else(|| tag.clone())
                } else {
                    tag.clone()
                };
                *counts.entry(canonical).or_insert(0) += 1;
            }
        }

        // Update existing tags and add new ones
        for (name, count) in &counts {
            let info = tags.entry(name.clone()).or_insert_with(|| TagInfo {
                name: name.clone(),
                usage_count: 0,
                last_used_at: None,
                created_at: now,
                label: None,
            });
            info.usage_count = *count;
            if info.last_used_at.is_none() {
                info.last_used_at = Some(now);
            }
        }

        // Mark tags not in use as 0 (don't delete, just update count)
        for (name, info) in tags.iter_mut() {
            if !counts.contains_key(name) {
                info.usage_count = 0;
            }
        }

        drop(tags);
        drop(aliases);
        drop(config);
        let _ = self.save().await;
    }

    /// Get info about a specific tag (exact name lookup, no alias resolution)
    pub async fn get_tag_info(&self, tag: &str) -> Option<TagInfo> {
        let tags = self.tags.read().await;
        tags.get(tag).cloned()
    }

    /// Get all known tags with their info
    pub async fn get_all_tags(&self) -> Vec<TagInfo> {
        let tags = self.tags.read().await;
        let mut result: Vec<TagInfo> = tags.values().cloned().collect();
        result.sort_by_key(|t| std::cmp::Reverse(t.usage_count));
        result
    }

    /// Get orphan tags (tags with usage_count == 0)
    pub async fn get_orphan_tags(&self) -> Vec<String> {
        let tags = self.tags.read().await;
        tags.values()
            .filter(|t| t.usage_count == 0)
            .map(|t| t.name.clone())
            .collect()
    }

    /// Get unused tags (tags that were created but never used, i.e., last_used_at is None)
    pub async fn get_unused_tags(&self) -> Vec<String> {
        let tags = self.tags.read().await;
        tags.values()
            .filter(|t| t.last_used_at.is_none())
            .map(|t| t.name.clone())
            .collect()
    }

    /// Rename a tag (returns the old info if it existed)
    pub async fn rename_tag(&self, old_name: &str, new_name: &str) -> Option<TagInfo> {
        let mut tags = self.tags.write().await;

        // Can't rename to an existing tag (use merge instead)
        if tags.contains_key(new_name) {
            return None;
        }

        if let Some(mut info) = tags.remove(old_name) {
            info.name = new_name.to_string();
            tags.insert(new_name.to_string(), info.clone());
            drop(tags);
            let _ = self.save().await;
            tracing::info!(old = old_name, new = new_name, "Renamed tag");
            Some(info)
        } else {
            None
        }
    }

    /// Merge source tag into target tag.
    /// Returns the source tag info if merge was successful.
    /// After merge, the source tag is removed and its usage count is added to target.
    pub async fn merge_tags(&self, source: &str, target: &str) -> Option<TagInfo> {
        if source == target {
            return None;
        }

        let mut tags = self.tags.write().await;

        let source_info = tags.remove(source)?;

        // Add source usage to target
        let target_info = tags.entry(target.to_string()).or_insert_with(|| TagInfo {
            name: target.to_string(),
            usage_count: 0,
            last_used_at: None,
            created_at: source_info.created_at,
            label: None,
        });

        target_info.usage_count += source_info.usage_count;

        // Update last_used_at to the more recent one
        match (target_info.last_used_at, source_info.last_used_at) {
            (Some(t), Some(s)) => target_info.last_used_at = Some(t.max(s)),
            (None, Some(s)) => target_info.last_used_at = Some(s),
            _ => {}
        }

        // Add source as an alias to target (if aliases enabled)
        drop(tags);
        let mut aliases = self.aliases.write().await;
        aliases
            .aliases
            .insert(source.to_string(), target.to_string());
        drop(aliases);

        let _ = self.save().await;
        tracing::info!(source = source, target = target, "Merged tags");
        Some(source_info)
    }

    /// Add an alias for a tag
    pub async fn add_alias(&self, alias: &str, canonical: &str) -> bool {
        if alias == canonical {
            return false;
        }

        let mut aliases = self.aliases.write().await;
        aliases
            .aliases
            .insert(alias.to_string(), canonical.to_string());
        drop(aliases);

        let _ = self.save().await;
        tracing::info!(alias = alias, canonical = canonical, "Added tag alias");
        true
    }

    /// Remove an alias
    pub async fn remove_alias(&self, alias: &str) -> bool {
        let mut aliases = self.aliases.write().await;
        let removed = aliases.aliases.remove(alias).is_some();
        drop(aliases);

        if removed {
            let _ = self.save().await;
            tracing::info!(alias = alias, "Removed tag alias");
        }
        removed
    }

    /// Get all aliases
    pub async fn get_aliases(&self) -> HashMap<String, String> {
        self.aliases.read().await.aliases.clone()
    }

    /// Set label (emoji/color) for a tag
    pub async fn set_tag_label(&self, tag: &str, label: Option<String>) -> bool {
        let canonical = self.resolve_alias(tag).await;
        let mut tags = self.tags.write().await;
        if let Some(info) = tags.get_mut(&canonical) {
            info.label = label;
            drop(tags);
            let _ = self.save().await;
            true
        } else {
            false
        }
    }

    /// Clean up orphan tags (usage_count == 0)
    pub async fn cleanup_orphans(&self) -> Vec<String> {
        let mut tags = self.tags.write().await;
        let orphans: Vec<String> = tags
            .values()
            .filter(|t| t.usage_count == 0)
            .map(|t| t.name.clone())
            .collect();

        for name in &orphans {
            tags.remove(name);
        }

        // Also remove aliases pointing to removed tags
        let mut aliases = self.aliases.write().await;
        aliases.aliases.retain(|_, v| !orphans.contains(v));
        drop(aliases);

        drop(tags);
        if !orphans.is_empty() {
            let _ = self.save().await;
            tracing::info!(count = orphans.len(), "Cleaned up orphan tags");
        }
        orphans
    }

    /// Delete a specific tag (regardless of usage)
    pub async fn delete_tag(&self, tag: &str) -> bool {
        let canonical = self.resolve_alias(tag).await;
        let mut tags = self.tags.write().await;
        let removed = tags.remove(&canonical).is_some();
        drop(tags);

        if removed {
            // Remove aliases pointing to this tag
            let mut aliases = self.aliases.write().await;
            aliases.aliases.retain(|_, v| v != &canonical);
            drop(aliases);
            let _ = self.save().await;
        }
        removed
    }

    /// Get summary
    pub async fn get_summary(&self) -> TagManagementSummary {
        let tags = self.tags.read().await;
        let aliases = self.aliases.read().await;
        let config = self.config.read().await;

        let orphan_tags: Vec<String> = tags
            .values()
            .filter(|t| t.usage_count == 0)
            .map(|t| t.name.clone())
            .collect();

        let unused_tags: Vec<String> = tags
            .values()
            .filter(|t| t.last_used_at.is_none())
            .map(|t| t.name.clone())
            .collect();

        let mut all_tags: Vec<(String, u32)> = tags
            .values()
            .map(|t| (t.name.clone(), t.usage_count))
            .collect();
        all_tags.sort_by_key(|a| std::cmp::Reverse(a.1));
        let top_tags = all_tags.into_iter().take(10).collect();

        TagManagementSummary {
            total_tags: tags.len(),
            orphan_tags,
            alias_count: aliases.aliases.len(),
            top_tags,
            unused_tags,
            config: config.clone(),
        }
    }

    /// Get config
    pub async fn get_config(&self) -> TagManagementConfig {
        self.config.read().await.clone()
    }

    /// Set config
    pub async fn set_config(&self, config: TagManagementConfig) {
        *self.config.write().await = config;
        let _ = self.save_config().await;
    }

    /// Format summary for display
    pub fn format_summary(summary: &TagManagementSummary) -> String {
        let mut out = String::new();
        out.push_str("📋 Tag Management Summary\n");
        out.push_str(&format!("  Total tags: {}\n", summary.total_tags));
        out.push_str(&format!("  Orphan tags: {}\n", summary.orphan_tags.len()));
        out.push_str(&format!("  Alias mappings: {}\n", summary.alias_count));
        out.push_str(&format!(
            "  Auto-cleanup orphans: {}\n",
            if summary.config.auto_cleanup_orphans {
                "enabled"
            } else {
                "disabled"
            }
        ));

        if !summary.top_tags.is_empty() {
            out.push_str("\n  Top tags:\n");
            for (name, count) in &summary.top_tags {
                out.push_str(&format!("    {} ({} tasks)\n", name, count));
            }
        }

        if !summary.orphan_tags.is_empty() {
            out.push_str("\n  Orphan tags (0 usage):\n");
            for name in summary.orphan_tags.iter().take(20) {
                out.push_str(&format!("    - {}\n", name));
            }
            if summary.orphan_tags.len() > 20 {
                out.push_str(&format!(
                    "    ... and {} more\n",
                    summary.orphan_tags.len() - 20
                ));
            }
        }

        if !summary.unused_tags.is_empty() {
            out.push_str("\n  Unused tags (never applied):\n");
            for name in summary.unused_tags.iter().take(10) {
                out.push_str(&format!("    - {}\n", name));
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("ipmsg_tag_test").join(name);
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    #[tokio::test]
    async fn test_register_and_get_tag_info() {
        let dir = test_dir("reg_info");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;

        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.name, "movies");
        assert_eq!(info.usage_count, 2);
        assert_eq!(info.last_used_at, Some(1001));
        assert_eq!(info.created_at, 1000);
    }

    #[tokio::test]
    async fn test_unregister_usage() {
        let dir = test_dir("unreg");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.unregister_usage("movies").await;

        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 1);
    }

    #[tokio::test]
    async fn test_unregister_below_zero() {
        let dir = test_dir("unreg_zero");
        let mgr = TagManager::new(&dir);
        mgr.unregister_usage("nonexistent").await;
        // Should not panic, count stays at 0
    }

    #[tokio::test]
    async fn test_rename_tag() {
        let dir = test_dir("rename");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movie", 1000).await;

        let info = mgr.rename_tag("movie", "movies").await.unwrap();
        assert_eq!(info.name, "movies");
        assert_eq!(info.usage_count, 1);

        // Old name should be gone
        assert!(mgr.get_tag_info("movie").await.is_none());
        // New name should exist
        assert!(mgr.get_tag_info("movies").await.is_some());
    }

    #[tokio::test]
    async fn test_rename_to_existing_fails() {
        let dir = test_dir("rename_fail");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movie", 1000).await;
        mgr.register_usage("films", 1001).await;

        let result = mgr.rename_tag("movie", "films").await;
        assert!(result.is_none());
        // Both tags should still exist
        assert!(mgr.get_tag_info("movie").await.is_some());
        assert!(mgr.get_tag_info("films").await.is_some());
    }

    #[tokio::test]
    async fn test_rename_nonexistent() {
        let dir = test_dir("rename_noexist");
        let mgr = TagManager::new(&dir);
        let result = mgr.rename_tag("nonexistent", "new").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_merge_tags() {
        let dir = test_dir("merge");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movie", 1000).await;
        mgr.register_usage("movie", 1001).await;
        mgr.register_usage("movies", 1002).await;

        let source_info = mgr.merge_tags("movie", "movies").await.unwrap();
        assert_eq!(source_info.name, "movie");
        assert_eq!(source_info.usage_count, 2);

        // Source should be gone
        assert!(mgr.get_tag_info("movie").await.is_none());
        // Target should have combined count
        let target = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(target.usage_count, 3); // 2 from movie + 1 from movies

        // "movie" should now be an alias for "movies"
        let resolved = mgr.resolve_alias("movie").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_merge_same_tag() {
        let dir = test_dir("merge_same");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        let result = mgr.merge_tags("movies", "movies").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_merge_nonexistent_source() {
        let dir = test_dir("merge_nosrc");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        let result = mgr.merge_tags("nonexistent", "movies").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_add_and_resolve_alias() {
        let dir = test_dir("alias");
        let mgr = TagManager::new(&dir);

        assert!(mgr.add_alias("movie", "movies").await);
        let resolved = mgr.resolve_alias("movie").await;
        assert_eq!(resolved, "movies");

        // Non-alias should resolve to itself
        let resolved2 = mgr.resolve_alias("films").await;
        assert_eq!(resolved2, "films");
    }

    #[tokio::test]
    async fn test_remove_alias() {
        let dir = test_dir("rm_alias");
        let mgr = TagManager::new(&dir);

        mgr.add_alias("movie", "movies").await;
        assert!(mgr.remove_alias("movie").await);

        let resolved = mgr.resolve_alias("movie").await;
        assert_eq!(resolved, "movie"); // No longer aliased
    }

    #[tokio::test]
    async fn test_remove_nonexistent_alias() {
        let dir = test_dir("rm_alias_noexist");
        let mgr = TagManager::new(&dir);
        assert!(!mgr.remove_alias("nonexistent").await);
    }

    #[tokio::test]
    async fn test_alias_same_name() {
        let dir = test_dir("alias_same");
        let mgr = TagManager::new(&dir);
        assert!(!mgr.add_alias("movies", "movies").await);
    }

    #[tokio::test]
    async fn test_get_orphan_tags() {
        let dir = test_dir("orphans");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("old_tag", 1000).await;
        mgr.unregister_usage("old_tag").await;

        let orphans = mgr.get_orphan_tags().await;
        assert!(orphans.contains(&"old_tag".to_string()));
        assert!(!orphans.contains(&"movies".to_string()));
    }

    #[tokio::test]
    async fn test_cleanup_orphans() {
        let dir = test_dir("cleanup");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("old1", 1000).await;
        mgr.register_usage("old2", 1000).await;
        mgr.unregister_usage("old1").await;
        mgr.unregister_usage("old2").await;

        let removed = mgr.cleanup_orphans().await;
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"old1".to_string()));
        assert!(removed.contains(&"old2".to_string()));

        // movies should still exist
        assert!(mgr.get_tag_info("movies").await.is_some());
        // orphans should be gone
        assert!(mgr.get_tag_info("old1").await.is_none());
        assert!(mgr.get_tag_info("old2").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_tag() {
        let dir = test_dir("delete");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        assert!(mgr.delete_tag("movies").await);
        assert!(mgr.get_tag_info("movies").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_tag() {
        let dir = test_dir("delete_noexist");
        let mgr = TagManager::new(&dir);
        assert!(!mgr.delete_tag("nonexistent").await);
    }

    #[tokio::test]
    async fn test_set_tag_label() {
        let dir = test_dir("label");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        assert!(mgr.set_tag_label("movies", Some("🎬".to_string())).await);
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, Some("🎬".to_string()));

        // Clear label
        assert!(mgr.set_tag_label("movies", None).await);
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, None);
    }

    #[tokio::test]
    async fn test_set_label_nonexistent() {
        let dir = test_dir("label_noexist");
        let mgr = TagManager::new(&dir);
        assert!(
            !mgr.set_tag_label("nonexistent", Some("🎬".to_string()))
                .await
        );
    }

    #[tokio::test]
    async fn test_get_all_tags_sorted() {
        let dir = test_dir("all_tags");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("low", 1000).await;
        mgr.register_usage("high", 1001).await;
        mgr.register_usage("high", 1002).await;
        mgr.register_usage("high", 1003).await;
        mgr.register_usage("mid", 1004).await;
        mgr.register_usage("mid", 1005).await;

        let all = mgr.get_all_tags().await;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].name, "high");
        assert_eq!(all[0].usage_count, 3);
        assert_eq!(all[1].name, "mid");
        assert_eq!(all[1].usage_count, 2);
        assert_eq!(all[2].name, "low");
        assert_eq!(all[2].usage_count, 1);
    }

    #[tokio::test]
    async fn test_get_summary() {
        let dir = test_dir("summary");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("old", 1000).await;
        mgr.unregister_usage("old").await;
        mgr.add_alias("film", "movies").await;

        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 2);
        assert_eq!(summary.orphan_tags.len(), 1);
        assert!(summary.orphan_tags.contains(&"old".to_string()));
        assert_eq!(summary.alias_count, 1);
        assert_eq!(summary.top_tags[0].0, "movies");
        assert_eq!(summary.top_tags[0].1, 2);
    }

    #[tokio::test]
    async fn test_config_persistence() {
        let dir = test_dir("config_persist");
        let mgr = TagManager::new(&dir);

        let mut config = TagManagementConfig::default();
        config.auto_cleanup_orphans = true;
        config.orphan_threshold_secs = 3600;
        config.max_tags = 50;
        mgr.set_config(config).await;

        // Create new manager and restore
        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        let loaded = mgr2.get_config().await;
        assert!(loaded.auto_cleanup_orphans);
        assert_eq!(loaded.orphan_threshold_secs, 3600);
        assert_eq!(loaded.max_tags, 50);
    }

    #[tokio::test]
    async fn test_data_persistence() {
        let dir = test_dir("data_persist");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.add_alias("film", "movies").await;

        // Create new manager and restore
        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        let info = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 2);

        let resolved = mgr2.resolve_alias("film").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_sync_from_tasks() {
        let dir = test_dir("sync");
        let mgr = TagManager::new(&dir);

        // Pre-register some tags
        mgr.register_usage("old_tag", 1000).await;
        mgr.register_usage("movies", 1000).await;

        // Sync from actual task data
        let task_tags = vec![
            vec!["movies".to_string(), "action".to_string()],
            vec!["movies".to_string()],
            vec!["comedy".to_string()],
        ];
        mgr.sync_from_tasks(&task_tags, 2000).await;

        // movies should have count 2
        let movies = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 2);

        // action should have count 1
        let action = mgr.get_tag_info("action").await.unwrap();
        assert_eq!(action.usage_count, 1);

        // comedy should have count 1
        let comedy = mgr.get_tag_info("comedy").await.unwrap();
        assert_eq!(comedy.usage_count, 1);

        // old_tag should have count 0 (not in tasks)
        let old = mgr.get_tag_info("old_tag").await.unwrap();
        assert_eq!(old.usage_count, 0);
    }

    #[tokio::test]
    async fn test_register_usages_batch() {
        let dir = test_dir("batch_reg");
        let mgr = TagManager::new(&dir);

        let tags = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        mgr.register_usages(&tags, 1000).await;

        assert_eq!(mgr.get_tag_info("a").await.unwrap().usage_count, 1);
        assert_eq!(mgr.get_tag_info("b").await.unwrap().usage_count, 1);
        assert_eq!(mgr.get_tag_info("c").await.unwrap().usage_count, 1);
    }

    #[tokio::test]
    async fn test_unregister_usages_batch() {
        let dir = test_dir("batch_unreg");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("a", 1000).await;
        mgr.register_usage("a", 1001).await;
        mgr.register_usage("b", 1002).await;

        let tags = vec!["a".to_string(), "b".to_string()];
        mgr.unregister_usages(&tags).await;

        assert_eq!(mgr.get_tag_info("a").await.unwrap().usage_count, 1);
        assert_eq!(mgr.get_tag_info("b").await.unwrap().usage_count, 0);
    }

    #[tokio::test]
    async fn test_alias_disabled_in_config() {
        let dir = test_dir("alias_disabled");
        let mgr = TagManager::new(&dir);

        let mut config = TagManagementConfig::default();
        config.enable_aliases = false;
        mgr.set_config(config).await;

        mgr.add_alias("movie", "movies").await;

        // When aliases are disabled, resolve returns the input as-is
        let resolved = mgr.resolve_alias("movie").await;
        assert_eq!(resolved, "movie");
    }

    #[tokio::test]
    async fn test_get_unused_tags() {
        let dir = test_dir("unused");
        let mgr = TagManager::new(&dir);

        // Register and then unregister (has last_used_at set)
        mgr.register_usage("used_then_removed", 1000).await;
        mgr.unregister_usage("used_then_removed").await;

        // Manually add a tag without usage (simulating a tag created but never used)
        {
            let mut tags = mgr.tags.write().await;
            tags.insert(
                "never_used".to_string(),
                TagInfo {
                    name: "never_used".to_string(),
                    usage_count: 0,
                    last_used_at: None,
                    created_at: 1000,
                    label: None,
                },
            );
        }

        let unused = mgr.get_unused_tags().await;
        assert!(unused.contains(&"never_used".to_string()));
        // "used_then_removed" has last_used_at set, so it's not "unused"
        assert!(!unused.contains(&"used_then_removed".to_string()));
    }

    #[tokio::test]
    async fn test_format_summary() {
        let summary = TagManagementSummary {
            total_tags: 5,
            orphan_tags: vec!["old1".to_string(), "old2".to_string()],
            alias_count: 2,
            top_tags: vec![("movies".to_string(), 10), ("music".to_string(), 5)],
            unused_tags: vec!["never".to_string()],
            config: TagManagementConfig::default(),
        };

        let formatted = TagManager::format_summary(&summary);
        assert!(formatted.contains("Tag Management Summary"));
        assert!(formatted.contains("Total tags: 5"));
        assert!(formatted.contains("Orphan tags: 2"));
        assert!(formatted.contains("movies (10 tasks)"));
        assert!(formatted.contains("Alias mappings: 2"));
    }

    #[tokio::test]
    async fn test_merge_creates_alias_for_resolve() {
        let dir = test_dir("merge_alias_resolve");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movie", 1000).await;
        mgr.register_usage("movie", 1001).await;
        mgr.register_usage("films", 1002).await;

        // Register usage through alias after merge
        mgr.merge_tags("movie", "films").await;

        // Registering usage via alias "movie" should increment "films"
        mgr.register_usage("movie", 1003).await;

        let films = mgr.get_tag_info("films").await.unwrap();
        assert_eq!(films.usage_count, 4); // 2 original films(1) + movie(2) + 1 via alias
    }

    #[tokio::test]
    async fn test_cleanup_orphans_removes_aliases() {
        let dir = test_dir("cleanup_aliases");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("old", 1000).await;
        mgr.add_alias("film", "old").await;
        mgr.unregister_usage("old").await;

        let removed = mgr.cleanup_orphans().await;
        assert!(removed.contains(&"old".to_string()));

        // Alias should also be cleaned up
        let aliases = mgr.get_aliases().await;
        assert!(!aliases.contains_key("film"));
    }

    #[tokio::test]
    async fn test_delete_removes_related_aliases() {
        let dir = test_dir("delete_aliases");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movies", 1000).await;
        mgr.add_alias("film", "movies").await;
        mgr.add_alias("movie", "movies").await;

        mgr.delete_tag("movies").await;

        let aliases = mgr.get_aliases().await;
        assert!(!aliases.contains_key("film"));
        assert!(!aliases.contains_key("movie"));
    }
}
