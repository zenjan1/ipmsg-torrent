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

    // ===== Phase 212: Comprehensive Test Coverage =====

    // --- TagInfo serialization roundtrip ---
    #[test]
    fn test_taginfo_serde_roundtrip() {
        let info = TagInfo {
            name: "movies".to_string(),
            usage_count: 42,
            last_used_at: Some(1700000000),
            created_at: 1699000000,
            label: Some("🎬".to_string()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "movies");
        assert_eq!(deser.usage_count, 42);
        assert_eq!(deser.last_used_at, Some(1700000000));
        assert_eq!(deser.created_at, 1699000000);
        assert_eq!(deser.label, Some("🎬".to_string()));
    }

    #[test]
    fn test_taginfo_serde_none_label() {
        let info = TagInfo {
            name: "music".to_string(),
            usage_count: 0,
            last_used_at: None,
            created_at: 1000,
            label: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.label, None);
        assert_eq!(deser.last_used_at, None);
    }

    #[test]
    fn test_taginfo_serde_extra_fields_ignored() {
        let json = r#"{"name":"x","usage_count":1,"created_at":100,"last_used_at":null,"label":null,"extra_field":true}"#;
        let deser: TagInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deser.name, "x");
        assert_eq!(deser.usage_count, 1);
    }

    #[test]
    fn test_taginfo_clone_debug() {
        let info = TagInfo {
            name: "test".to_string(),
            usage_count: 5,
            last_used_at: Some(100),
            created_at: 50,
            label: Some("🎵".to_string()),
        };
        let cloned = info.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.usage_count, 5);
        // Debug works
        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("test"));
    }

    // --- TagAliasMap serialization ---
    #[test]
    fn test_tag_alias_map_serde_roundtrip() {
        let mut map = TagAliasMap::default();
        map.aliases.insert("film".to_string(), "movies".to_string());
        map.aliases.insert("song".to_string(), "music".to_string());
        let json = serde_json::to_string(&map).unwrap();
        let deser: TagAliasMap = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.aliases.len(), 2);
        assert_eq!(deser.aliases.get("film").unwrap(), "movies");
    }

    #[test]
    fn test_tag_alias_map_default_empty() {
        let map = TagAliasMap::default();
        assert!(map.aliases.is_empty());
    }

    #[test]
    fn test_tag_alias_map_extra_fields() {
        let json = r#"{"aliases":{"a":"b"},"future_field":42}"#;
        let deser: TagAliasMap = serde_json::from_str(json).unwrap();
        assert_eq!(deser.aliases.get("a").unwrap(), "b");
    }

    // --- TagManagementConfig serialization ---
    #[test]
    fn test_config_serde_roundtrip() {
        let config = TagManagementConfig {
            auto_cleanup_orphans: true,
            orphan_threshold_secs: 3600,
            enable_aliases: false,
            max_tags: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: TagManagementConfig = serde_json::from_str(&json).unwrap();
        assert!(deser.auto_cleanup_orphans);
        assert_eq!(deser.orphan_threshold_secs, 3600);
        assert!(!deser.enable_aliases);
        assert_eq!(deser.max_tags, 100);
    }

    #[test]
    fn test_config_default_values() {
        let config = TagManagementConfig::default();
        assert!(!config.auto_cleanup_orphans);
        assert_eq!(config.orphan_threshold_secs, 7 * 24 * 3600);
        assert!(config.enable_aliases);
        assert_eq!(config.max_tags, 0);
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = TagManagementConfig {
            auto_cleanup_orphans: true,
            orphan_threshold_secs: 86400,
            enable_aliases: true,
            max_tags: 50,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let deser: TagManagementConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.max_tags, 50);
    }

    #[test]
    fn test_config_extra_fields_ignored() {
        let json = r#"{"auto_cleanup_orphans":false,"orphan_threshold_secs":100,"enable_aliases":true,"max_tags":0,"new_field":"hello"}"#;
        let deser: TagManagementConfig = serde_json::from_str(json).unwrap();
        assert_eq!(deser.orphan_threshold_secs, 100);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = TagManagementConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_tags, config.max_tags);
        let debug = format!("{:?}", config);
        assert!(debug.contains("TagManagementConfig"));
    }

    // --- TagManagementSummary serialization ---
    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = TagManagementSummary {
            total_tags: 10,
            orphan_tags: vec!["old".to_string()],
            alias_count: 3,
            top_tags: vec![("movies".to_string(), 50)],
            unused_tags: vec!["new".to_string()],
            config: TagManagementConfig::default(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deser: TagManagementSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.total_tags, 10);
        assert_eq!(deser.orphan_tags, vec!["old"]);
        assert_eq!(deser.top_tags.len(), 1);
    }

    #[test]
    fn test_summary_empty() {
        let summary = TagManagementSummary {
            total_tags: 0,
            orphan_tags: vec![],
            alias_count: 0,
            top_tags: vec![],
            unused_tags: vec![],
            config: TagManagementConfig::default(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deser: TagManagementSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.total_tags, 0);
        assert!(deser.top_tags.is_empty());
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = TagManagementSummary {
            total_tags: 1,
            orphan_tags: vec![],
            alias_count: 0,
            top_tags: vec![("a".to_string(), 1)],
            unused_tags: vec![],
            config: TagManagementConfig::default(),
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_tags, 1);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("TagManagementSummary"));
    }

    // --- TagAction serialization ---
    #[test]
    fn test_tag_action_renamed_serde() {
        let action = TagAction::Renamed {
            old: "movie".to_string(),
            new: "movies".to_string(),
            affected_tasks: 5,
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::Renamed {
                old,
                new,
                affected_tasks,
            } => {
                assert_eq!(old, "movie");
                assert_eq!(new, "movies");
                assert_eq!(affected_tasks, 5);
            }
            _ => panic!("Expected Renamed variant"),
        }
    }

    #[test]
    fn test_tag_action_merged_serde() {
        let action = TagAction::Merged {
            source: "movie".to_string(),
            target: "movies".to_string(),
            affected_tasks: 10,
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::Merged {
                source,
                target,
                affected_tasks,
            } => {
                assert_eq!(source, "movie");
                assert_eq!(target, "movies");
                assert_eq!(affected_tasks, 10);
            }
            _ => panic!("Expected Merged variant"),
        }
    }

    #[test]
    fn test_tag_action_orphans_cleaned_serde() {
        let action = TagAction::OrphansCleaned {
            removed: vec!["old1".to_string(), "old2".to_string()],
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::OrphansCleaned { removed } => {
                assert_eq!(removed.len(), 2);
            }
            _ => panic!("Expected OrphansCleaned variant"),
        }
    }

    #[test]
    fn test_tag_action_alias_added_serde() {
        let action = TagAction::AliasAdded {
            alias: "film".to_string(),
            canonical: "movies".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("AliasAdded"));
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::AliasAdded { alias, canonical } => {
                assert_eq!(alias, "film");
                assert_eq!(canonical, "movies");
            }
            _ => panic!("Expected AliasAdded"),
        }
    }

    #[test]
    fn test_tag_action_alias_removed_serde() {
        let action = TagAction::AliasRemoved {
            alias: "film".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::AliasRemoved { alias } => {
                assert_eq!(alias, "film");
            }
            _ => panic!("Expected AliasRemoved"),
        }
    }

    #[test]
    fn test_tag_action_clone_debug() {
        let action = TagAction::Renamed {
            old: "a".to_string(),
            new: "b".to_string(),
            affected_tasks: 3,
        };
        let cloned = action.clone();
        let debug = format!("{:?}", action);
        assert!(debug.contains("Renamed"));
        match cloned {
            TagAction::Renamed { old, .. } => assert_eq!(old, "a"),
            _ => panic!("Clone failed"),
        }
    }

    // --- TagManager::new() ---
    #[tokio::test]
    async fn test_new_manager_empty_state() {
        let dir = test_dir("new_empty");
        let mgr = TagManager::new(&dir);
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
        let aliases = mgr.get_aliases().await;
        assert!(aliases.is_empty());
        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 0);
    }

    // --- resolve_alias edge cases ---
    #[tokio::test]
    async fn test_resolve_alias_empty_string() {
        let dir = test_dir("resolve_empty");
        let mgr = TagManager::new(&dir);
        let resolved = mgr.resolve_alias("").await;
        assert_eq!(resolved, "");
    }

    #[tokio::test]
    async fn test_resolve_alias_unicode() {
        let dir = test_dir("resolve_unicode");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("电影", "movies").await;
        let resolved = mgr.resolve_alias("电影").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_resolve_alias_emoji() {
        let dir = test_dir("resolve_emoji");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("🎬", "movies").await;
        let resolved = mgr.resolve_alias("🎬").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_resolve_alias_chain_not_transitive() {
        let dir = test_dir("resolve_chain");
        let mgr = TagManager::new(&dir);
        // a -> b, b -> c => resolving "a" should give "b" (not "c")
        mgr.add_alias("a", "b").await;
        mgr.add_alias("b", "c").await;
        let resolved = mgr.resolve_alias("a").await;
        assert_eq!(resolved, "b");
    }

    // --- register_usage edge cases ---
    #[tokio::test]
    async fn test_register_usage_empty_tag() {
        let dir = test_dir("reg_empty");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("", 1000).await;
        let info = mgr.get_tag_info("").await.unwrap();
        assert_eq!(info.usage_count, 1);
    }

    #[tokio::test]
    async fn test_register_usage_unicode() {
        let dir = test_dir("reg_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("日本語", 1000).await;
        mgr.register_usage("中文", 1001).await;
        let info = mgr.get_tag_info("日本語").await.unwrap();
        assert_eq!(info.usage_count, 1);
        let info2 = mgr.get_tag_info("中文").await.unwrap();
        assert_eq!(info2.usage_count, 1);
    }

    #[tokio::test]
    async fn test_register_usage_via_alias() {
        let dir = test_dir("reg_alias");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        mgr.register_usage("film", 1000).await;
        // Should resolve to "movies" and increment that
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 1);
        // "film" should not be a separate tag
        assert!(mgr.get_tag_info("film").await.is_none());
    }

    #[tokio::test]
    async fn test_register_usage_large_count() {
        let dir = test_dir("reg_large");
        let mgr = TagManager::new(&dir);
        for _ in 0..1000 {
            mgr.register_usage("popular", 1000).await;
        }
        let info = mgr.get_tag_info("popular").await.unwrap();
        assert_eq!(info.usage_count, 1000);
    }

    // --- register_usages batch edge cases ---
    #[tokio::test]
    async fn test_register_usages_empty_list() {
        let dir = test_dir("batch_empty");
        let mgr = TagManager::new(&dir);
        let empty: Vec<String> = vec![];
        mgr.register_usages(&empty, 1000).await;
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_register_usages_with_aliases() {
        let dir = test_dir("batch_alias");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        let tags = vec!["film".to_string(), "music".to_string()];
        mgr.register_usages(&tags, 1000).await;
        // "film" should resolve to "movies"
        let movies = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 1);
        let music = mgr.get_tag_info("music").await.unwrap();
        assert_eq!(music.usage_count, 1);
    }

    #[tokio::test]
    async fn test_register_usages_duplicate_tags() {
        let dir = test_dir("batch_dup");
        let mgr = TagManager::new(&dir);
        let tags = vec!["movies".to_string(), "movies".to_string()];
        mgr.register_usages(&tags, 1000).await;
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 2);
    }

    // --- unregister_usages batch edge cases ---
    #[tokio::test]
    async fn test_unregister_usages_empty_list() {
        let dir = test_dir("unbatch_empty");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        let empty: Vec<String> = vec![];
        mgr.unregister_usages(&empty).await;
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 1); // unchanged
    }

    #[tokio::test]
    async fn test_unregister_usages_nonexistent() {
        let dir = test_dir("unbatch_noexist");
        let mgr = TagManager::new(&dir);
        let tags = vec!["nonexistent".to_string()];
        mgr.unregister_usages(&tags).await; // should not panic
    }

    #[tokio::test]
    async fn test_unregister_usages_saturating() {
        let dir = test_dir("unbatch_sat");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await; // count = 1
        let tags = vec![
            "movies".to_string(),
            "movies".to_string(),
            "movies".to_string(),
        ];
        mgr.unregister_usages(&tags).await;
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 0); // saturating_sub, not underflow
    }

    // --- sync_from_tasks edge cases ---
    #[tokio::test]
    async fn test_sync_from_tasks_empty() {
        let dir = test_dir("sync_empty");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("old", 1000).await;
        let task_tags: Vec<Vec<String>> = vec![];
        mgr.sync_from_tasks(&task_tags, 2000).await;
        let info = mgr.get_tag_info("old").await.unwrap();
        assert_eq!(info.usage_count, 0); // no tasks use it
    }

    #[tokio::test]
    async fn test_sync_from_tasks_with_aliases() {
        let dir = test_dir("sync_alias");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        let task_tags = vec![vec!["film".to_string()]];
        mgr.sync_from_tasks(&task_tags, 2000).await;
        // "film" resolves to "movies"
        let movies = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 1);
    }

    #[tokio::test]
    async fn test_sync_from_tasks_preserves_existing() {
        let dir = test_dir("sync_preserve");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("existing", 500).await;
        mgr.set_tag_label("existing", Some("🏷️".to_string())).await;

        let task_tags = vec![vec!["existing".to_string()]];
        mgr.sync_from_tasks(&task_tags, 2000).await;

        let info = mgr.get_tag_info("existing").await.unwrap();
        assert_eq!(info.usage_count, 1);
        assert_eq!(info.label, Some("🏷️".to_string())); // preserved
    }

    // --- rename_tag edge cases ---
    #[tokio::test]
    async fn test_rename_tag_unicode() {
        let dir = test_dir("rename_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("电影", 1000).await;
        let result = mgr.rename_tag("电影", "movies").await;
        assert!(result.is_some());
        assert!(mgr.get_tag_info("电影").await.is_none());
        assert!(mgr.get_tag_info("movies").await.is_some());
    }

    #[tokio::test]
    async fn test_rename_preserves_label() {
        let dir = test_dir("rename_label");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.set_tag_label("movies", Some("🎬".to_string())).await;
        mgr.rename_tag("movies", "films").await;
        let info = mgr.get_tag_info("films").await.unwrap();
        assert_eq!(info.label, Some("🎬".to_string()));
    }

    #[tokio::test]
    async fn test_rename_preserves_usage_count() {
        let dir = test_dir("rename_count");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("movies", 1002).await;
        mgr.rename_tag("movies", "films").await;
        let info = mgr.get_tag_info("films").await.unwrap();
        assert_eq!(info.usage_count, 3);
    }

    // --- merge_tags edge cases ---
    #[tokio::test]
    async fn test_merge_preserves_last_used_at() {
        let dir = test_dir("merge_timestamp");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movie", 5000).await; // created_at=5000, last_used=5000
        mgr.register_usage("films", 3000).await; // created_at=3000, last_used=3000
        mgr.merge_tags("movie", "films").await;
        let info = mgr.get_tag_info("films").await.unwrap();
        // last_used_at should be max(5000, 3000) = 5000
        assert_eq!(info.last_used_at, Some(5000));
    }

    #[tokio::test]
    async fn test_merge_target_has_no_last_used() {
        let dir = test_dir("merge_no_last");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("source", 5000).await;
        // Create target without usage
        {
            let mut tags = mgr.tags.write().await;
            tags.insert(
                "target".to_string(),
                TagInfo {
                    name: "target".to_string(),
                    usage_count: 0,
                    last_used_at: None,
                    created_at: 1000,
                    label: None,
                },
            );
        }
        mgr.merge_tags("source", "target").await;
        let info = mgr.get_tag_info("target").await.unwrap();
        // Should get source's last_used_at
        assert_eq!(info.last_used_at, Some(5000));
        assert_eq!(info.usage_count, 1);
    }

    #[tokio::test]
    async fn test_merge_unicode() {
        let dir = test_dir("merge_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("电影", 1000).await;
        mgr.register_usage("影视", 1001).await;
        mgr.merge_tags("电影", "影视").await;
        assert!(mgr.get_tag_info("电影").await.is_none());
        let info = mgr.get_tag_info("影视").await.unwrap();
        assert_eq!(info.usage_count, 2);
    }

    // --- add_alias / remove_alias edge cases ---
    #[tokio::test]
    async fn test_add_alias_overwrite() {
        let dir = test_dir("alias_overwrite");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        // Overwrite the alias
        mgr.add_alias("film", "music").await;
        let resolved = mgr.resolve_alias("film").await;
        assert_eq!(resolved, "music");
    }

    #[tokio::test]
    async fn test_remove_alias_unicode() {
        let dir = test_dir("rm_alias_uni");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("电影", "movies").await;
        assert!(mgr.remove_alias("电影").await);
        assert!(!mgr.remove_alias("电影").await); // already removed
    }

    #[tokio::test]
    async fn test_get_aliases_empty() {
        let dir = test_dir("aliases_empty");
        let mgr = TagManager::new(&dir);
        let aliases = mgr.get_aliases().await;
        assert!(aliases.is_empty());
    }

    #[tokio::test]
    async fn test_get_aliases_multiple() {
        let dir = test_dir("aliases_multi");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("a", "x").await;
        mgr.add_alias("b", "y").await;
        mgr.add_alias("c", "z").await;
        let aliases = mgr.get_aliases().await;
        assert_eq!(aliases.len(), 3);
    }

    // --- set_tag_label edge cases ---
    #[tokio::test]
    async fn test_set_label_unicode() {
        let dir = test_dir("label_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        assert!(mgr.set_tag_label("movies", Some("🎬🍿".to_string())).await);
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, Some("🎬🍿".to_string()));
    }

    #[tokio::test]
    async fn test_set_label_via_alias() {
        let dir = test_dir("label_alias");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.add_alias("film", "movies").await;
        // Setting label via alias should resolve to canonical
        assert!(mgr.set_tag_label("film", Some("🎬".to_string())).await);
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, Some("🎬".to_string()));
    }

    #[tokio::test]
    async fn test_set_label_nonexistent_via_alias() {
        let dir = test_dir("label_noexist_alias");
        let mgr = TagManager::new(&dir);
        assert!(
            !mgr.set_tag_label("nonexistent", Some("🎬".to_string()))
                .await
        );
    }

    // --- get_orphan_tags edge cases ---
    #[tokio::test]
    async fn test_get_orphan_tags_empty() {
        let dir = test_dir("orphan_empty");
        let mgr = TagManager::new(&dir);
        let orphans = mgr.get_orphan_tags().await;
        assert!(orphans.is_empty());
    }

    #[tokio::test]
    async fn test_get_orphan_tags_all_orphan() {
        let dir = test_dir("orphan_all");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("a", 1000).await;
        mgr.register_usage("b", 1001).await;
        mgr.unregister_usage("a").await;
        mgr.unregister_usage("b").await;
        let orphans = mgr.get_orphan_tags().await;
        assert_eq!(orphans.len(), 2);
    }

    // --- cleanup_orphans edge cases ---
    #[tokio::test]
    async fn test_cleanup_orphans_empty() {
        let dir = test_dir("cleanup_empty");
        let mgr = TagManager::new(&dir);
        let removed = mgr.cleanup_orphans().await;
        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_orphans_no_orphans() {
        let dir = test_dir("cleanup_none");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("music", 1001).await;
        let removed = mgr.cleanup_orphans().await;
        assert!(removed.is_empty());
        // Tags still exist
        assert!(mgr.get_tag_info("movies").await.is_some());
        assert!(mgr.get_tag_info("music").await.is_some());
    }

    // --- delete_tag edge cases ---
    #[tokio::test]
    async fn test_delete_tag_via_alias() {
        let dir = test_dir("delete_alias");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.add_alias("film", "movies").await;
        // Delete via alias should resolve and delete canonical
        assert!(mgr.delete_tag("film").await);
        assert!(mgr.get_tag_info("movies").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_tag_unicode() {
        let dir = test_dir("delete_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("电影", 1000).await;
        assert!(mgr.delete_tag("电影").await);
        assert!(mgr.get_tag_info("电影").await.is_none());
    }

    // --- get_all_tags edge cases ---
    #[tokio::test]
    async fn test_get_all_tags_empty() {
        let dir = test_dir("all_empty");
        let mgr = TagManager::new(&dir);
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_get_all_tags_single() {
        let dir = test_dir("all_single");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        let all = mgr.get_all_tags().await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "movies");
    }

    // --- get_unused_tags edge cases ---
    #[tokio::test]
    async fn test_get_unused_tags_empty() {
        let dir = test_dir("unused_empty");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        let unused = mgr.get_unused_tags().await;
        assert!(unused.is_empty());
    }

    #[tokio::test]
    async fn test_get_unused_tags_all_unused() {
        let dir = test_dir("unused_all");
        let mgr = TagManager::new(&dir);
        // Create tags without usage
        {
            let mut tags = mgr.tags.write().await;
            tags.insert(
                "a".to_string(),
                TagInfo {
                    name: "a".to_string(),
                    usage_count: 0,
                    last_used_at: None,
                    created_at: 1000,
                    label: None,
                },
            );
            tags.insert(
                "b".to_string(),
                TagInfo {
                    name: "b".to_string(),
                    usage_count: 0,
                    last_used_at: None,
                    created_at: 1001,
                    label: None,
                },
            );
        }
        let unused = mgr.get_unused_tags().await;
        assert_eq!(unused.len(), 2);
    }

    // --- get_summary edge cases ---
    #[tokio::test]
    async fn test_get_summary_empty() {
        let dir = test_dir("summary_empty");
        let mgr = TagManager::new(&dir);
        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 0);
        assert!(summary.orphan_tags.is_empty());
        assert!(summary.top_tags.is_empty());
        assert!(summary.unused_tags.is_empty());
        assert_eq!(summary.alias_count, 0);
    }

    #[tokio::test]
    async fn test_get_summary_top_tags_limit() {
        let dir = test_dir("summary_top");
        let mgr = TagManager::new(&dir);
        // Create 15 tags with different usage counts
        for i in 0..15 {
            let tag_name = format!("tag{:02}", i);
            for j in 0..(i as u32 + 1) {
                mgr.register_usage(&tag_name, 1000 + j as u64).await;
            }
        }
        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 15);
        // top_tags should be limited to 10
        assert_eq!(summary.top_tags.len(), 10);
        // First should be highest usage
        assert_eq!(summary.top_tags[0].1, 15);
    }

    // --- format_summary edge cases ---
    #[test]
    fn test_format_summary_empty() {
        let summary = TagManagementSummary {
            total_tags: 0,
            orphan_tags: vec![],
            alias_count: 0,
            top_tags: vec![],
            unused_tags: vec![],
            config: TagManagementConfig::default(),
        };
        let formatted = TagManager::format_summary(&summary);
        assert!(formatted.contains("Tag Management Summary"));
        assert!(formatted.contains("Total tags: 0"));
        assert!(formatted.contains("Orphan tags: 0"));
        // No "Top tags" section when empty
        assert!(!formatted.contains("Top tags:"));
    }

    #[test]
    fn test_format_summary_with_all_sections() {
        let summary = TagManagementSummary {
            total_tags: 10,
            orphan_tags: vec!["old1".to_string(), "old2".to_string()],
            alias_count: 3,
            top_tags: vec![("movies".to_string(), 50), ("music".to_string(), 30)],
            unused_tags: vec!["never".to_string()],
            config: TagManagementConfig {
                auto_cleanup_orphans: true,
                orphan_threshold_secs: 3600,
                enable_aliases: true,
                max_tags: 100,
            },
        };
        let formatted = TagManager::format_summary(&summary);
        assert!(formatted.contains("enabled")); // auto_cleanup
        assert!(formatted.contains("movies (50 tasks)"));
        assert!(formatted.contains("music (30 tasks)"));
        assert!(formatted.contains("Orphan tags (0 usage):"));
        assert!(formatted.contains("old1"));
        assert!(formatted.contains("Unused tags (never applied):"));
        assert!(formatted.contains("never"));
    }

    #[test]
    fn test_format_summary_many_orphans_truncated() {
        let orphans: Vec<String> = (0..30).map(|i| format!("old{}", i)).collect();
        let summary = TagManagementSummary {
            total_tags: 30,
            orphan_tags: orphans,
            alias_count: 0,
            top_tags: vec![],
            unused_tags: vec![],
            config: TagManagementConfig::default(),
        };
        let formatted = TagManager::format_summary(&summary);
        // Should show first 20 and "... and X more"
        assert!(formatted.contains("... and 10 more"));
    }

    #[test]
    fn test_format_summary_many_unused_truncated() {
        let unused: Vec<String> = (0..20).map(|i| format!("unused{}", i)).collect();
        let summary = TagManagementSummary {
            total_tags: 20,
            orphan_tags: vec![],
            alias_count: 0,
            top_tags: vec![],
            unused_tags: unused,
            config: TagManagementConfig::default(),
        };
        let formatted = TagManager::format_summary(&summary);
        // Should show first 10
        assert!(formatted.contains("unused0"));
        assert!(formatted.contains("unused9"));
    }

    // --- Persistence edge cases ---
    #[tokio::test]
    async fn test_restore_missing_files() {
        let dir = test_dir("restore_missing");
        let mgr = TagManager::new(&dir);
        mgr.restore().await; // Should not panic
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_restore_corrupt_config() {
        let dir = test_dir("restore_bad_config");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management_config.json"), "not valid json {{{").unwrap();
        let mgr = TagManager::new(&dir);
        mgr.restore().await; // Should not panic, falls back to default
        let config = mgr.get_config().await;
        assert_eq!(config.max_tags, 0); // default
    }

    #[tokio::test]
    async fn test_restore_corrupt_data() {
        let dir = test_dir("restore_bad_data");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management.json"), "corrupt data").unwrap();
        let mgr = TagManager::new(&dir);
        mgr.restore().await; // Should not panic
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_restore_empty_config_file() {
        let dir = test_dir("restore_empty_config");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management_config.json"), "").unwrap();
        let mgr = TagManager::new(&dir);
        mgr.restore().await; // Should not panic
    }

    #[tokio::test]
    async fn test_restore_empty_data_file() {
        let dir = test_dir("restore_empty_data");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management.json"), "").unwrap();
        let mgr = TagManager::new(&dir);
        mgr.restore().await; // Should not panic
    }

    #[tokio::test]
    async fn test_persistence_overwrite() {
        let dir = test_dir("persist_overwrite");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;

        // Overwrite with new data
        mgr.register_usage("music", 2000).await;

        // Restore and verify latest state
        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;
        let movies = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 2);
        let music = mgr2.get_tag_info("music").await.unwrap();
        assert_eq!(music.usage_count, 1);
    }

    #[tokio::test]
    async fn test_persistence_full_roundtrip() {
        let dir = test_dir("persist_full");
        let mgr = TagManager::new(&dir);

        // Set up complex state
        let mut config = TagManagementConfig::default();
        config.auto_cleanup_orphans = true;
        config.max_tags = 200;
        mgr.set_config(config).await;

        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("music", 1002).await;
        mgr.add_alias("film", "movies").await;
        mgr.set_tag_label("movies", Some("🎬".to_string())).await;

        // Restore into fresh manager
        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        // Verify everything
        let loaded_config = mgr2.get_config().await;
        assert!(loaded_config.auto_cleanup_orphans);
        assert_eq!(loaded_config.max_tags, 200);

        let movies = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 2);
        assert_eq!(movies.label, Some("🎬".to_string()));

        let resolved = mgr2.resolve_alias("film").await;
        assert_eq!(resolved, "movies");
    }

    // --- Complex workflows ---
    #[tokio::test]
    async fn test_full_lifecycle() {
        let dir = test_dir("lifecycle");
        let mgr = TagManager::new(&dir);

        // 1. Register tags
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("action", 1002).await;
        mgr.register_usage("old", 1003).await;

        // 2. Add alias
        mgr.add_alias("film", "movies").await;

        // 3. Set label
        mgr.set_tag_label("movies", Some("🎬".to_string())).await;

        // 4. Unregister orphan
        mgr.unregister_usage("old").await;

        // 5. Verify state
        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 3);
        assert_eq!(summary.orphan_tags.len(), 1);

        // 6. Cleanup orphans
        let removed = mgr.cleanup_orphans().await;
        assert_eq!(removed.len(), 1);

        // 7. Merge
        mgr.merge_tags("action", "movies").await;
        let movies = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 3); // 2 movies + 1 action

        // 8. Rename
        mgr.rename_tag("movies", "cinema").await;
        assert!(mgr.get_tag_info("movies").await.is_none());
        let cinema = mgr.get_tag_info("cinema").await.unwrap();
        assert_eq!(cinema.usage_count, 3);
        assert_eq!(cinema.label, Some("🎬".to_string()));

        // 9. Verify alias still works through merge+rename chain
        let resolved = mgr.resolve_alias("film").await;
        assert_eq!(resolved, "movies"); // alias still points to old name
    }

    #[tokio::test]
    async fn test_multi_tag_independent_operations() {
        let dir = test_dir("multi_independent");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("a", 1000).await;
        mgr.register_usage("b", 1001).await;
        mgr.register_usage("c", 1002).await;

        // Delete "b" shouldn't affect "a" or "c"
        mgr.delete_tag("b").await;

        assert!(mgr.get_tag_info("a").await.is_some());
        assert!(mgr.get_tag_info("b").await.is_none());
        assert!(mgr.get_tag_info("c").await.is_some());
    }

    #[tokio::test]
    async fn test_register_unregister_alternating() {
        let dir = test_dir("alternating");
        let mgr = TagManager::new(&dir);

        for _ in 0..50 {
            mgr.register_usage("toggle", 1000).await;
        }
        for _ in 0..30 {
            mgr.unregister_usage("toggle").await;
        }
        let info = mgr.get_tag_info("toggle").await.unwrap();
        assert_eq!(info.usage_count, 20);
    }

    // --- Config management ---
    #[tokio::test]
    async fn test_set_config_persists() {
        let dir = test_dir("config_persist2");
        let mgr = TagManager::new(&dir);

        let mut config = TagManagementConfig::default();
        config.orphan_threshold_secs = 999;
        config.enable_aliases = false;
        mgr.set_config(config).await;

        // Verify config file was written
        let content = std::fs::read_to_string(dir.join("tag_management_config.json")).unwrap();
        assert!(content.contains("999"));
        assert!(content.contains("false"));
    }

    #[tokio::test]
    async fn test_get_config_returns_current() {
        let dir = test_dir("config_current");
        let mgr = TagManager::new(&dir);

        let config = mgr.get_config().await;
        assert!(!config.auto_cleanup_orphans);

        let mut new_config = config.clone();
        new_config.auto_cleanup_orphans = true;
        mgr.set_config(new_config).await;

        let loaded = mgr.get_config().await;
        assert!(loaded.auto_cleanup_orphans);
    }

    // --- max_tags config ---
    #[tokio::test]
    async fn test_config_max_tags_zero_unlimited() {
        let dir = test_dir("max_zero");
        let mgr = TagManager::new(&dir);
        let config = mgr.get_config().await;
        assert_eq!(config.max_tags, 0); // 0 = unlimited
    }

    #[tokio::test]
    async fn test_config_max_tags_custom() {
        let dir = test_dir("max_custom");
        let mgr = TagManager::new(&dir);
        let mut config = TagManagementConfig::default();
        config.max_tags = 50;
        mgr.set_config(config).await;
        let loaded = mgr.get_config().await;
        assert_eq!(loaded.max_tags, 50);
    }

    // --- Data file structure ---
    #[tokio::test]
    async fn test_save_creates_data_file() {
        let dir = test_dir("save_creates");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("test", 1000).await;
        assert!(dir.join("tag_management.json").exists());
    }

    #[tokio::test]
    async fn test_save_config_creates_config_file() {
        let dir = test_dir("save_config_creates");
        let mgr = TagManager::new(&dir);
        let config = TagManagementConfig::default();
        mgr.set_config(config).await;
        assert!(dir.join("tag_management_config.json").exists());
    }

    // ===== Phase 235: Comprehensive Test Coverage =====

    // --- TagInfo additional serde tests ---
    #[test]
    fn test_p235_taginfo_serde_pretty() {
        let info = TagInfo {
            name: "movies".to_string(),
            usage_count: 10,
            last_used_at: Some(1000),
            created_at: 500,
            label: Some("🎬".to_string()),
        };
        let json = serde_json::to_string_pretty(&info).unwrap();
        assert!(json.contains('\n'));
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "movies");
        assert_eq!(deser.usage_count, 10);
    }

    #[test]
    fn test_p235_taginfo_serde_missing_optional_fields() {
        let json = r#"{"name":"test","usage_count":5,"created_at":100}"#;
        let deser: TagInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deser.name, "test");
        assert_eq!(deser.usage_count, 5);
        assert_eq!(deser.last_used_at, None);
        assert_eq!(deser.label, None);
    }

    #[test]
    fn test_p235_taginfo_zero_usage_count() {
        let info = TagInfo {
            name: "zero".to_string(),
            usage_count: 0,
            last_used_at: None,
            created_at: 0,
            label: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.usage_count, 0);
        assert_eq!(deser.created_at, 0);
    }

    #[test]
    fn test_p235_taginfo_max_values() {
        let info = TagInfo {
            name: "max".to_string(),
            usage_count: u32::MAX,
            last_used_at: Some(u64::MAX),
            created_at: u64::MAX,
            label: Some("🎬".to_string()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.usage_count, u32::MAX);
        assert_eq!(deser.last_used_at, Some(u64::MAX));
    }

    #[test]
    fn test_p235_taginfo_unicode_name() {
        let info = TagInfo {
            name: "日本語テスト".to_string(),
            usage_count: 1,
            last_used_at: Some(100),
            created_at: 50,
            label: Some("🏷️".to_string()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "日本語テスト");
    }

    #[test]
    fn test_p235_taginfo_emoji_name() {
        let info = TagInfo {
            name: "🎬🎵📚".to_string(),
            usage_count: 3,
            last_used_at: Some(100),
            created_at: 50,
            label: Some("✨".to_string()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: TagInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "🎬🎵📚");
    }

    // --- TagAliasMap additional tests ---
    #[test]
    fn test_p235_tag_alias_map_serde_pretty() {
        let mut map = TagAliasMap::default();
        map.aliases.insert("film".to_string(), "movies".to_string());
        let json = serde_json::to_string_pretty(&map).unwrap();
        assert!(json.contains('\n'));
        let deser: TagAliasMap = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.aliases.len(), 1);
    }

    #[test]
    fn test_p235_tag_alias_map_clone_debug() {
        let mut map = TagAliasMap::default();
        map.aliases.insert("a".to_string(), "b".to_string());
        let cloned = map.clone();
        assert_eq!(cloned.aliases.len(), 1);
        let debug = format!("{:?}", map);
        assert!(debug.contains("TagAliasMap"));
    }

    #[test]
    fn test_p235_tag_alias_map_multiple_aliases() {
        let mut map = TagAliasMap::default();
        for i in 0..100 {
            map.aliases
                .insert(format!("alias{}", i), format!("tag{}", i));
        }
        let json = serde_json::to_string(&map).unwrap();
        let deser: TagAliasMap = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.aliases.len(), 100);
    }

    // --- TagManagementConfig additional tests ---
    #[test]
    fn test_p235_config_serde_missing_fields() {
        // TagManagementConfig requires all fields (no #[serde(default)] on struct)
        // Test that partial JSON fails to deserialize
        let json = r#"{"auto_cleanup_orphans":true}"#;
        let result = serde_json::from_str::<TagManagementConfig>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_p235_config_zero_values() {
        let config = TagManagementConfig {
            auto_cleanup_orphans: false,
            orphan_threshold_secs: 0,
            enable_aliases: false,
            max_tags: 0,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: TagManagementConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.orphan_threshold_secs, 0);
        assert_eq!(deser.max_tags, 0);
    }

    #[test]
    fn test_p235_config_max_values() {
        let config = TagManagementConfig {
            auto_cleanup_orphans: true,
            orphan_threshold_secs: u64::MAX,
            enable_aliases: true,
            max_tags: usize::MAX,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: TagManagementConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.orphan_threshold_secs, u64::MAX);
        assert_eq!(deser.max_tags, usize::MAX);
    }

    // --- TagManagementSummary additional tests ---
    #[test]
    fn test_p235_summary_serde_pretty() {
        let summary = TagManagementSummary {
            total_tags: 5,
            orphan_tags: vec!["old".to_string()],
            alias_count: 2,
            top_tags: vec![("movies".to_string(), 10)],
            unused_tags: vec!["new".to_string()],
            config: TagManagementConfig::default(),
        };
        let json = serde_json::to_string_pretty(&summary).unwrap();
        assert!(json.contains('\n'));
        let deser: TagManagementSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.total_tags, 5);
    }

    #[test]
    fn test_p235_summary_extra_fields_ignored() {
        let json = r#"{"total_tags":10,"orphan_tags":[],"alias_count":0,"top_tags":[],"unused_tags":[],"config":{"auto_cleanup_orphans":false,"orphan_threshold_secs":604800,"enable_aliases":true,"max_tags":0},"future_field":true}"#;
        let deser: TagManagementSummary = serde_json::from_str(json).unwrap();
        assert_eq!(deser.total_tags, 10);
    }

    #[test]
    fn test_p235_summary_unicode_content() {
        let summary = TagManagementSummary {
            total_tags: 3,
            orphan_tags: vec!["孤立标签".to_string()],
            alias_count: 1,
            top_tags: vec![("电影".to_string(), 100), ("音乐".to_string(), 50)],
            unused_tags: vec!["🆕".to_string()],
            config: TagManagementConfig::default(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deser: TagManagementSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.top_tags[0].0, "电影");
        assert_eq!(deser.orphan_tags[0], "孤立标签");
    }

    // --- TagAction additional tests ---
    #[test]
    fn test_p235_tag_action_all_variants_serde() {
        let actions = vec![
            TagAction::Renamed {
                old: "a".to_string(),
                new: "b".to_string(),
                affected_tasks: 5,
            },
            TagAction::Merged {
                source: "x".to_string(),
                target: "y".to_string(),
                affected_tasks: 10,
            },
            TagAction::OrphansCleaned {
                removed: vec!["old1".to_string(), "old2".to_string()],
            },
            TagAction::AliasAdded {
                alias: "film".to_string(),
                canonical: "movies".to_string(),
            },
            TagAction::AliasRemoved {
                alias: "film".to_string(),
            },
        ];

        for action in actions {
            let json = serde_json::to_string(&action).unwrap();
            let _deser: TagAction = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_p235_tag_action_unicode_content() {
        let action = TagAction::Renamed {
            old: "电影".to_string(),
            new: "影视".to_string(),
            affected_tasks: 3,
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::Renamed { old, new, .. } => {
                assert_eq!(old, "电影");
                assert_eq!(new, "影视");
            }
            _ => panic!("Expected Renamed"),
        }
    }

    #[test]
    fn test_p235_tag_action_zero_affected_tasks() {
        let action = TagAction::Renamed {
            old: "a".to_string(),
            new: "b".to_string(),
            affected_tasks: 0,
        };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::Renamed { affected_tasks, .. } => {
                assert_eq!(affected_tasks, 0);
            }
            _ => panic!("Expected Renamed"),
        }
    }

    #[test]
    fn test_p235_tag_action_empty_removed_list() {
        let action = TagAction::OrphansCleaned { removed: vec![] };
        let json = serde_json::to_string(&action).unwrap();
        let deser: TagAction = serde_json::from_str(&json).unwrap();
        match deser {
            TagAction::OrphansCleaned { removed } => {
                assert!(removed.is_empty());
            }
            _ => panic!("Expected OrphansCleaned"),
        }
    }

    // --- TagManager additional async tests ---

    #[tokio::test]
    async fn test_p235_resolve_alias_disabled_returns_input() {
        let dir = test_dir("alias_disabled_resolve");
        let mgr = TagManager::new(&dir);
        let mut config = TagManagementConfig::default();
        config.enable_aliases = false;
        mgr.set_config(config).await;

        mgr.add_alias("film", "movies").await;
        let resolved = mgr.resolve_alias("film").await;
        assert_eq!(resolved, "film");
    }

    #[tokio::test]
    async fn test_p235_register_usage_creates_tag_with_correct_timestamps() {
        let dir = test_dir("reg_timestamps");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 12345).await;

        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.created_at, 12345);
        assert_eq!(info.last_used_at, Some(12345));
    }

    #[tokio::test]
    async fn test_p235_register_usage_updates_last_used_only() {
        let dir = test_dir("reg_update_last");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 2000).await;

        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.created_at, 1000);
        assert_eq!(info.last_used_at, Some(2000));
        assert_eq!(info.usage_count, 2);
    }

    #[tokio::test]
    async fn test_p235_unregister_nonexistent_tag_no_panic() {
        let dir = test_dir("unreg_nonexistent");
        let mgr = TagManager::new(&dir);
        mgr.unregister_usage("nonexistent").await;
        assert!(mgr.get_tag_info("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_p235_register_usages_with_unicode() {
        let dir = test_dir("batch_unicode");
        let mgr = TagManager::new(&dir);
        let tags = vec!["电影".to_string(), "音乐".to_string(), "📚".to_string()];
        mgr.register_usages(&tags, 1000).await;

        assert!(mgr.get_tag_info("电影").await.is_some());
        assert!(mgr.get_tag_info("音乐").await.is_some());
        assert!(mgr.get_tag_info("📚").await.is_some());
    }

    #[tokio::test]
    async fn test_p235_sync_from_tasks_with_unicode() {
        let dir = test_dir("sync_unicode");
        let mgr = TagManager::new(&dir);
        let task_tags = vec![
            vec!["电影".to_string(), "动作".to_string()],
            vec!["电影".to_string()],
        ];
        mgr.sync_from_tasks(&task_tags, 2000).await;

        let movies = mgr.get_tag_info("电影").await.unwrap();
        assert_eq!(movies.usage_count, 2);
        let action = mgr.get_tag_info("动作").await.unwrap();
        assert_eq!(action.usage_count, 1);
    }

    #[tokio::test]
    async fn test_p235_sync_from_tasks_preserves_labels() {
        let dir = test_dir("sync_preserve_labels");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.set_tag_label("movies", Some("🎬".to_string())).await;

        let task_tags = vec![vec!["movies".to_string()]];
        mgr.sync_from_tasks(&task_tags, 2000).await;

        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, Some("🎬".to_string()));
    }

    #[tokio::test]
    async fn test_p235_rename_to_same_name() {
        let dir = test_dir("rename_same");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        let result = mgr.rename_tag("movies", "movies").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_p235_rename_nonexistent_to_existing() {
        let dir = test_dir("rename_noexist_to_exist");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;

        let result = mgr.rename_tag("nonexistent", "movies").await;
        assert!(result.is_none());
        assert!(mgr.get_tag_info("movies").await.is_some());
    }

    #[tokio::test]
    async fn test_p235_merge_updates_target_created_at() {
        let dir = test_dir("merge_created_at");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("source", 5000).await;
        {
            let mut tags = mgr.tags.write().await;
            tags.insert(
                "target".to_string(),
                TagInfo {
                    name: "target".to_string(),
                    usage_count: 1,
                    last_used_at: Some(6000),
                    created_at: 6000,
                    label: None,
                },
            );
        }
        mgr.merge_tags("source", "target").await;
        let info = mgr.get_tag_info("target").await.unwrap();
        assert_eq!(info.created_at, 6000);
    }

    #[tokio::test]
    async fn test_p235_merge_both_have_no_last_used() {
        let dir = test_dir("merge_no_last_used");
        let mgr = TagManager::new(&dir);
        {
            let mut tags = mgr.tags.write().await;
            tags.insert(
                "source".to_string(),
                TagInfo {
                    name: "source".to_string(),
                    usage_count: 1,
                    last_used_at: None,
                    created_at: 1000,
                    label: None,
                },
            );
            tags.insert(
                "target".to_string(),
                TagInfo {
                    name: "target".to_string(),
                    usage_count: 1,
                    last_used_at: None,
                    created_at: 2000,
                    label: None,
                },
            );
        }
        mgr.merge_tags("source", "target").await;
        let info = mgr.get_tag_info("target").await.unwrap();
        assert_eq!(info.last_used_at, None);
        assert_eq!(info.usage_count, 2);
    }

    #[tokio::test]
    async fn test_p235_add_alias_unicode() {
        let dir = test_dir("alias_unicode");
        let mgr = TagManager::new(&dir);
        assert!(mgr.add_alias("电影", "movies").await);
        let resolved = mgr.resolve_alias("电影").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_p235_add_alias_emoji() {
        let dir = test_dir("alias_emoji");
        let mgr = TagManager::new(&dir);
        assert!(mgr.add_alias("🎬", "movies").await);
        let resolved = mgr.resolve_alias("🎬").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_p235_remove_alias_nonexistent() {
        let dir = test_dir("rm_alias_nonexistent");
        let mgr = TagManager::new(&dir);
        assert!(!mgr.remove_alias("nonexistent").await);
    }

    #[tokio::test]
    async fn test_p235_get_aliases_multiple() {
        let dir = test_dir("aliases_multiple");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        mgr.add_alias("song", "music").await;
        mgr.add_alias("book", "reading").await;

        let aliases = mgr.get_aliases().await;
        assert_eq!(aliases.len(), 3);
        assert_eq!(aliases.get("film").unwrap(), "movies");
        assert_eq!(aliases.get("song").unwrap(), "music");
        assert_eq!(aliases.get("book").unwrap(), "reading");
    }

    #[tokio::test]
    async fn test_p235_set_tag_label_via_alias() {
        let dir = test_dir("label_via_alias");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.add_alias("film", "movies").await;

        assert!(mgr.set_tag_label("film", Some("🎬".to_string())).await);
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.label, Some("🎬".to_string()));
    }

    #[tokio::test]
    async fn test_p235_set_tag_label_nonexistent_via_alias() {
        let dir = test_dir("label_noexist_alias");
        let mgr = TagManager::new(&dir);
        mgr.add_alias("film", "movies").await;
        assert!(!mgr.set_tag_label("film", Some("🎬".to_string())).await);
    }

    #[tokio::test]
    async fn test_p235_get_orphan_tags_empty() {
        let dir = test_dir("orphans_empty");
        let mgr = TagManager::new(&dir);
        let orphans = mgr.get_orphan_tags().await;
        assert!(orphans.is_empty());
    }

    #[tokio::test]
    async fn test_p235_get_orphan_tags_multiple() {
        let dir = test_dir("orphans_multiple");
        let mgr = TagManager::new(&dir);
        {
            let mut tags = mgr.tags.write().await;
            for i in 0..10 {
                tags.insert(
                    format!("orphan{}", i),
                    TagInfo {
                        name: format!("orphan{}", i),
                        usage_count: 0,
                        last_used_at: None,
                        created_at: 1000,
                        label: None,
                    },
                );
            }
        }
        let orphans = mgr.get_orphan_tags().await;
        assert_eq!(orphans.len(), 10);
    }

    #[tokio::test]
    async fn test_p235_cleanup_orphans_empty() {
        let dir = test_dir("cleanup_empty");
        let mgr = TagManager::new(&dir);
        let removed = mgr.cleanup_orphans().await;
        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn test_p235_cleanup_orphans_preserves_used() {
        let dir = test_dir("cleanup_preserves");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("used", 1000).await;
        mgr.register_usage("unused", 1000).await;
        mgr.unregister_usage("unused").await;

        let removed = mgr.cleanup_orphans().await;
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&"unused".to_string()));
        assert!(mgr.get_tag_info("used").await.is_some());
    }

    #[tokio::test]
    async fn test_p235_delete_tag_via_alias() {
        let dir = test_dir("delete_via_alias");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.add_alias("film", "movies").await;

        assert!(mgr.delete_tag("film").await);
        assert!(mgr.get_tag_info("movies").await.is_none());
    }

    #[tokio::test]
    async fn test_p235_delete_tag_nonexistent() {
        let dir = test_dir("delete_nonexistent2");
        let mgr = TagManager::new(&dir);
        assert!(!mgr.delete_tag("nonexistent").await);
    }

    #[tokio::test]
    async fn test_p235_get_all_tags_empty() {
        let dir = test_dir("all_tags_empty");
        let mgr = TagManager::new(&dir);
        let all = mgr.get_all_tags().await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_p235_get_all_tags_many() {
        let dir = test_dir("all_tags_many");
        let mgr = TagManager::new(&dir);
        for i in 0..50 {
            mgr.register_usage(&format!("tag{}", i), 1000).await;
        }
        let all = mgr.get_all_tags().await;
        assert_eq!(all.len(), 50);
    }

    #[tokio::test]
    async fn test_p235_get_unused_tags_empty() {
        let dir = test_dir("unused_empty");
        let mgr = TagManager::new(&dir);
        let unused = mgr.get_unused_tags().await;
        assert!(unused.is_empty());
    }

    #[tokio::test]
    async fn test_p235_get_summary_empty() {
        let dir = test_dir("summary_empty");
        let mgr = TagManager::new(&dir);
        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 0);
        assert!(summary.orphan_tags.is_empty());
        assert!(summary.top_tags.is_empty());
        assert!(summary.unused_tags.is_empty());
    }

    #[tokio::test]
    async fn test_p235_get_summary_top_tags_limit() {
        let dir = test_dir("summary_limit");
        let mgr = TagManager::new(&dir);
        for i in 0..20 {
            for _ in 0..(20 - i) {
                mgr.register_usage(&format!("tag{}", i), 1000).await;
            }
        }
        let summary = mgr.get_summary().await;
        assert_eq!(summary.top_tags.len(), 10);
        assert_eq!(summary.top_tags[0].0, "tag0");
        assert_eq!(summary.top_tags[0].1, 20);
    }

    #[test]
    fn test_p235_format_summary_empty() {
        let summary = TagManagementSummary {
            total_tags: 0,
            orphan_tags: vec![],
            alias_count: 0,
            top_tags: vec![],
            unused_tags: vec![],
            config: TagManagementConfig::default(),
        };
        let formatted = TagManager::format_summary(&summary);
        assert!(formatted.contains("Total tags: 0"));
        assert!(formatted.contains("Orphan tags: 0"));
    }

    #[test]
    fn test_p235_format_summary_with_all_sections() {
        let summary = TagManagementSummary {
            total_tags: 100,
            orphan_tags: (0..25).map(|i| format!("orphan{}", i)).collect(),
            alias_count: 50,
            top_tags: vec![("movies".to_string(), 1000), ("music".to_string(), 500)],
            unused_tags: (0..15).map(|i| format!("unused{}", i)).collect(),
            config: TagManagementConfig {
                auto_cleanup_orphans: true,
                ..Default::default()
            },
        };
        let formatted = TagManager::format_summary(&summary);
        assert!(formatted.contains("Total tags: 100"));
        assert!(formatted.contains("Orphan tags: 25"));
        assert!(formatted.contains("Alias mappings: 50"));
        assert!(formatted.contains("enabled"));
        assert!(formatted.contains("movies (1000 tasks)"));
        assert!(formatted.contains("... and 5 more"));
        assert!(formatted.contains("Unused tags"));
    }

    // --- Persistence tests ---

    #[tokio::test]
    async fn test_p235_restore_missing_files() {
        let dir = test_dir("restore_missing");
        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        assert!(mgr.get_all_tags().await.is_empty());
    }

    #[tokio::test]
    async fn test_p235_restore_corrupt_config_json() {
        let dir = test_dir("restore_corrupt_config");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management_config.json"), "not valid json").unwrap();

        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        let config = mgr.get_config().await;
        assert!(!config.auto_cleanup_orphans);
    }

    #[tokio::test]
    async fn test_p235_restore_corrupt_data_json() {
        let dir = test_dir("restore_corrupt_data");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management.json"), "{invalid json}").unwrap();

        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        assert!(mgr.get_all_tags().await.is_empty());
    }

    #[tokio::test]
    async fn test_p235_restore_empty_config_file() {
        let dir = test_dir("restore_empty_config");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management_config.json"), "").unwrap();

        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        let config = mgr.get_config().await;
        assert!(!config.auto_cleanup_orphans);
    }

    #[tokio::test]
    async fn test_p235_restore_empty_data_file() {
        let dir = test_dir("restore_empty_data");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("tag_management.json"), "").unwrap();

        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        assert!(mgr.get_all_tags().await.is_empty());
    }

    #[tokio::test]
    async fn test_p235_restore_partial_data() {
        let dir = test_dir("restore_partial");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(
            dir.join("tag_management.json"),
            r#"{"tags":{"movies":{"name":"movies","usage_count":5,"created_at":1000}}}"#,
        )
        .unwrap();

        let mgr = TagManager::new(&dir);
        mgr.restore().await;
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 5);
        assert!(mgr.get_aliases().await.is_empty());
    }

    #[tokio::test]
    async fn test_p235_persistence_unicode_roundtrip() {
        let dir = test_dir("persist_unicode");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("电影", 1000).await;
        mgr.register_usage("音乐", 1001).await;
        mgr.add_alias("影视", "电影").await;
        mgr.set_tag_label("电影", Some("🎬".to_string())).await;

        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        let movies = mgr2.get_tag_info("电影").await.unwrap();
        assert_eq!(movies.usage_count, 1);
        assert_eq!(movies.label, Some("🎬".to_string()));

        let resolved = mgr2.resolve_alias("影视").await;
        assert_eq!(resolved, "电影");
    }

    #[tokio::test]
    async fn test_p235_persistence_emoji_roundtrip() {
        let dir = test_dir("persist_emoji");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("🎬", 1000).await;
        mgr.add_alias("🎥", "🎬").await;

        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        assert!(mgr2.get_tag_info("🎬").await.is_some());
        let resolved = mgr2.resolve_alias("🎥").await;
        assert_eq!(resolved, "🎬");
    }

    #[tokio::test]
    async fn test_p235_save_overwrites_existing() {
        let dir = test_dir("save_overwrite");
        let mgr = TagManager::new(&dir);
        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("movies", 1002).await;

        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;
        let info = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 3);
    }

    #[tokio::test]
    async fn test_p235_config_save_overwrites_existing() {
        let dir = test_dir("config_overwrite");
        let mgr = TagManager::new(&dir);

        let mut config1 = TagManagementConfig::default();
        config1.max_tags = 10;
        mgr.set_config(config1).await;

        let mut config2 = TagManagementConfig::default();
        config2.max_tags = 100;
        mgr.set_config(config2).await;

        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;
        let loaded = mgr2.get_config().await;
        assert_eq!(loaded.max_tags, 100);
    }

    // --- Complex workflow tests ---

    #[tokio::test]
    async fn test_p235_full_lifecycle() {
        let dir = test_dir("lifecycle");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("movies", 1001).await;
        mgr.register_usage("music", 1002).await;

        mgr.add_alias("film", "movies").await;
        mgr.add_alias("song", "music").await;

        mgr.set_tag_label("movies", Some("🎬".to_string())).await;
        mgr.set_tag_label("music", Some("🎵".to_string())).await;

        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 2);
        assert_eq!(summary.alias_count, 2);

        let mgr2 = TagManager::new(&dir);
        mgr2.restore().await;

        let movies = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 2);
        assert_eq!(movies.label, Some("🎬".to_string()));

        let resolved = mgr2.resolve_alias("film").await;
        assert_eq!(resolved, "movies");

        mgr2.merge_tags("music", "movies").await;
        let movies_after = mgr2.get_tag_info("movies").await.unwrap();
        assert_eq!(movies_after.usage_count, 3);

        mgr2.rename_tag("movies", "media").await;
        assert!(mgr2.get_tag_info("movies").await.is_none());
        assert!(mgr2.get_tag_info("media").await.is_some());

        mgr2.delete_tag("media").await;
        assert!(mgr2.get_tag_info("media").await.is_none());
    }

    #[tokio::test]
    async fn test_p235_multi_tag_independent_operations() {
        let dir = test_dir("multi_independent2");
        let mgr = TagManager::new(&dir);

        for i in 0..10 {
            mgr.register_usage(&format!("tag{}", i), 1000).await;
        }

        mgr.delete_tag("tag0").await;
        mgr.delete_tag("tag5").await;

        for i in 1..5 {
            assert!(mgr.get_tag_info(&format!("tag{}", i)).await.is_some());
        }
        for i in 6..10 {
            assert!(mgr.get_tag_info(&format!("tag{}", i)).await.is_some());
        }
    }

    #[tokio::test]
    async fn test_p235_register_unregister_alternating() {
        let dir = test_dir("alternating2");
        let mgr = TagManager::new(&dir);

        for _ in 0..100 {
            mgr.register_usage("toggle", 1000).await;
        }
        for _ in 0..50 {
            mgr.unregister_usage("toggle").await;
        }

        let info = mgr.get_tag_info("toggle").await.unwrap();
        assert_eq!(info.usage_count, 50);
    }

    #[tokio::test]
    async fn test_p235_alias_chain_behavior() {
        let dir = test_dir("alias_chain");
        let mgr = TagManager::new(&dir);

        mgr.add_alias("a", "b").await;
        mgr.add_alias("b", "c").await;

        let resolved = mgr.resolve_alias("a").await;
        assert_eq!(resolved, "b");

        let resolved2 = mgr.resolve_alias("b").await;
        assert_eq!(resolved2, "c");

        mgr.register_usage("a", 1000).await;
        let b_info = mgr.get_tag_info("b").await.unwrap();
        assert_eq!(b_info.usage_count, 1);
    }

    #[tokio::test]
    async fn test_p235_merge_then_register_via_alias() {
        let dir = test_dir("merge_register_alias");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movie", 1000).await;
        mgr.register_usage("movies", 1001).await;

        mgr.merge_tags("movie", "movies").await;

        mgr.register_usage("movie", 1002).await;

        let movies = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(movies.usage_count, 3);
    }

    #[tokio::test]
    async fn test_p235_cleanup_orphans_then_add_alias() {
        let dir = test_dir("cleanup_then_alias");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movies", 1000).await;
        mgr.register_usage("old", 1001).await;
        mgr.unregister_usage("old").await;

        mgr.cleanup_orphans().await;

        mgr.add_alias("old", "movies").await;

        let resolved = mgr.resolve_alias("old").await;
        assert_eq!(resolved, "movies");
    }

    #[tokio::test]
    async fn test_p235_delete_then_recreate() {
        let dir = test_dir("delete_recreate");
        let mgr = TagManager::new(&dir);

        mgr.register_usage("movies", 1000).await;
        mgr.set_tag_label("movies", Some("🎬".to_string())).await;

        mgr.delete_tag("movies").await;
        assert!(mgr.get_tag_info("movies").await.is_none());

        mgr.register_usage("movies", 2000).await;
        let info = mgr.get_tag_info("movies").await.unwrap();
        assert_eq!(info.usage_count, 1);
        assert_eq!(info.created_at, 2000);
        assert_eq!(info.label, None);
    }

    #[tokio::test]
    async fn test_p235_config_change_affects_alias_resolution() {
        let dir = test_dir("config_affects_alias");
        let mgr = TagManager::new(&dir);

        mgr.add_alias("film", "movies").await;
        let resolved1 = mgr.resolve_alias("film").await;
        assert_eq!(resolved1, "movies");

        let mut config = TagManagementConfig::default();
        config.enable_aliases = false;
        mgr.set_config(config).await;

        let resolved2 = mgr.resolve_alias("film").await;
        assert_eq!(resolved2, "film");

        let mut config2 = TagManagementConfig::default();
        config2.enable_aliases = true;
        mgr.set_config(config2).await;

        let resolved3 = mgr.resolve_alias("film").await;
        assert_eq!(resolved3, "movies");
    }

    #[tokio::test]
    async fn test_p235_many_tags_performance() {
        let dir = test_dir("many_tags");
        let mgr = TagManager::new(&dir);

        for i in 0..1000 {
            mgr.register_usage(&format!("tag{}", i), 1000).await;
        }

        let all = mgr.get_all_tags().await;
        assert_eq!(all.len(), 1000);

        let summary = mgr.get_summary().await;
        assert_eq!(summary.total_tags, 1000);
    }

    #[tokio::test]
    async fn test_p235_many_aliases_performance() {
        let dir = test_dir("many_aliases");
        let mgr = TagManager::new(&dir);

        for i in 0..1000 {
            mgr.add_alias(&format!("alias{}", i), "target").await;
        }

        let aliases = mgr.get_aliases().await;
        assert_eq!(aliases.len(), 1000);
    }
}
