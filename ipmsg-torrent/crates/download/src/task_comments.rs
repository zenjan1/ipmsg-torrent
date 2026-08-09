//! Per-task user comments system.
//!
//! Allows users to add timestamped comments to download tasks for tracking
//! observations, debugging notes, or status updates over time.
//!
//! Unlike the single `notes` field (a one-shot description) or `task_activity`
//! (system-generated events), this module provides a chronological comment
//! thread authored by the user.
//!
//! Features:
//! - Add/remove/list comments per task
//! - Comments have timestamp, author (optional), and text content
//! - Configurable maximum comments per task (ring buffer eviction)
//! - Persistence to `task_comments.json`
//! - Search across all task comments

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tokio::fs;

/// Default maximum comments per task
const DEFAULT_MAX_COMMENTS_PER_TASK: usize = 50;

/// Default maximum total comments across all tasks
const DEFAULT_MAX_TOTAL_COMMENTS: usize = 5000;

/// Errors from task comment operations.
#[derive(Error, Debug)]
pub enum TaskCommentError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    #[error("Comment not found: {0}")]
    CommentNotFound(String),
    #[error("Comment text is empty")]
    EmptyComment,
}

/// A single user comment on a download task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskComment {
    /// Unique comment ID (UUID v4)
    pub id: String,
    /// Task ID this comment belongs to
    pub task_id: String,
    /// Comment text content (non-empty, trimmed)
    pub text: String,
    /// Optional author/name (defaults to "user" if empty)
    pub author: Option<String>,
    /// When the comment was created
    pub created_at: DateTime<Utc>,
    /// Optional tags for categorizing comments (e.g., "bug", "note", "important")
    pub tags: Vec<String>,
}

/// Configuration for the task comments system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCommentsConfig {
    /// Maximum comments per task (oldest evicted when exceeded)
    pub max_comments_per_task: usize,
    /// Maximum total comments across all tasks
    pub max_total_comments: usize,
}

impl Default for TaskCommentsConfig {
    fn default() -> Self {
        Self {
            max_comments_per_task: DEFAULT_MAX_COMMENTS_PER_TASK,
            max_total_comments: DEFAULT_MAX_TOTAL_COMMENTS,
        }
    }
}

/// Summary of comments for a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCommentSummary {
    /// Task ID
    pub task_id: String,
    /// Total number of comments for this task
    pub comment_count: usize,
    /// Most recent comment (if any)
    pub latest_comment: Option<TaskComment>,
    /// All comments in chronological order (oldest first)
    pub comments: Vec<TaskComment>,
}

/// Result of searching comments across all tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentSearchResult {
    /// Search query used
    pub query: String,
    /// Matching comments grouped by task
    pub matches: Vec<TaskCommentSummary>,
    /// Total number of matching comments
    pub total_matches: usize,
}

/// Manages per-task user comments with persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCommentsManager {
    /// Comments indexed by task_id -> Vec<Comment> (chronological order)
    comments: HashMap<String, Vec<TaskComment>>,
    /// Configuration
    #[serde(default)]
    config: TaskCommentsConfig,
}

impl Default for TaskCommentsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskCommentsManager {
    /// Create a new empty comments manager.
    pub fn new() -> Self {
        Self {
            comments: HashMap::new(),
            config: TaskCommentsConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: TaskCommentsConfig) -> Self {
        Self {
            comments: HashMap::new(),
            config,
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &TaskCommentsConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: TaskCommentsConfig) {
        self.config = config;
        // Enforce new limits by evicting oldest comments
        self.enforce_limits();
    }

    /// Add a comment to a task.
    /// Returns the created comment.
    pub fn add_comment(
        &mut self,
        task_id: &str,
        text: &str,
        author: Option<&str>,
        tags: Vec<String>,
    ) -> Result<TaskComment, TaskCommentError> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(TaskCommentError::EmptyComment);
        }

        let comment = TaskComment {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task_id.to_string(),
            text: trimmed.to_string(),
            author: author
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty()),
            created_at: Utc::now(),
            tags: tags.into_iter().filter(|t| !t.is_empty()).collect(),
        };

        let task_comments = self.comments.entry(task_id.to_string()).or_default();
        task_comments.push(comment.clone());

        // Enforce per-task limit
        if task_comments.len() > self.config.max_comments_per_task {
            let excess = task_comments.len() - self.config.max_comments_per_task;
            task_comments.drain(..excess);
        }

        // Enforce global limit
        self.enforce_total_limit();

        Ok(comment)
    }

    /// Remove a comment by ID from any task.
    pub fn remove_comment(&mut self, comment_id: &str) -> Result<TaskComment, TaskCommentError> {
        for (_task_id, comments) in self.comments.iter_mut() {
            if let Some(pos) = comments.iter().position(|c| c.id == comment_id) {
                return Ok(comments.remove(pos));
            }
        }
        Err(TaskCommentError::CommentNotFound(comment_id.to_string()))
    }

    /// Get all comments for a task (chronological order, oldest first).
    pub fn get_comments(&self, task_id: &str) -> Vec<&TaskComment> {
        self.comments
            .get(task_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// Get a summary of comments for a task.
    pub fn get_comment_summary(&self, task_id: &str) -> TaskCommentSummary {
        let comments = self.comments.get(task_id).cloned().unwrap_or_default();
        let latest = comments.last().cloned();
        TaskCommentSummary {
            task_id: task_id.to_string(),
            comment_count: comments.len(),
            latest_comment: latest,
            comments,
        }
    }

    /// List all task IDs that have comments.
    pub fn list_tasks_with_comments(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .comments
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Get comment counts per task.
    pub fn get_comment_counts(&self) -> HashMap<String, usize> {
        self.comments
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, v)| (k.clone(), v.len()))
            .collect()
    }

    /// Search comments across all tasks by text content (case-insensitive).
    pub fn search_comments(&self, query: &str) -> CommentSearchResult {
        let query_lower = query.to_lowercase();
        let mut matches = Vec::new();
        let mut total = 0;

        for (task_id, comments) in &self.comments {
            let matching: Vec<TaskComment> = comments
                .iter()
                .filter(|c| {
                    c.text.to_lowercase().contains(&query_lower)
                        || c.author
                            .as_ref()
                            .map(|a| a.to_lowercase().contains(&query_lower))
                            .unwrap_or(false)
                        || c.tags
                            .iter()
                            .any(|t| t.to_lowercase().contains(&query_lower))
                })
                .cloned()
                .collect();

            if !matching.is_empty() {
                total += matching.len();
                let latest = matching.last().cloned();
                matches.push(TaskCommentSummary {
                    task_id: task_id.clone(),
                    comment_count: matching.len(),
                    latest_comment: latest,
                    comments: matching,
                });
            }
        }

        // Sort by task_id for deterministic output
        matches.sort_by(|a, b| a.task_id.cmp(&b.task_id));

        CommentSearchResult {
            query: query.to_string(),
            matches,
            total_matches: total,
        }
    }

    /// Search comments by tag.
    pub fn search_by_tag(&self, tag: &str) -> Vec<TaskComment> {
        let tag_lower = tag.to_lowercase();
        let mut results = Vec::new();
        for comments in self.comments.values() {
            for comment in comments {
                if comment.tags.iter().any(|t| t.to_lowercase() == tag_lower) {
                    results.push(comment.clone());
                }
            }
        }
        results.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        results
    }

    /// Get total number of comments across all tasks.
    pub fn total_comment_count(&self) -> usize {
        self.comments.values().map(|v| v.len()).sum()
    }

    /// Remove all comments for a task (e.g., when task is deleted).
    pub fn clear_task_comments(&mut self, task_id: &str) -> usize {
        self.comments.remove(task_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Enforce per-task limits by removing oldest comments.
    fn enforce_limits(&mut self) {
        for comments in self.comments.values_mut() {
            if comments.len() > self.config.max_comments_per_task {
                let excess = comments.len() - self.config.max_comments_per_task;
                comments.drain(..excess);
            }
        }
        self.enforce_total_limit();
    }

    /// Enforce global total limit by removing oldest comments across all tasks.
    fn enforce_total_limit(&mut self) {
        let total: usize = self.comments.values().map(|v| v.len()).sum();
        if total <= self.config.max_total_comments {
            return;
        }

        // Collect all comments with their task_id, sorted by created_at
        let mut all_comments: Vec<(String, usize, DateTime<Utc>)> = Vec::new();
        for (task_id, comments) in &self.comments {
            for (idx, comment) in comments.iter().enumerate() {
                all_comments.push((task_id.clone(), idx, comment.created_at));
            }
        }
        all_comments.sort_by(|a, b| a.2.cmp(&b.2));

        // Remove oldest until under limit
        let to_remove = total - self.config.max_total_comments;
        for (task_id, _idx, _created) in all_comments.into_iter().take(to_remove) {
            if let Some(comments) = self.comments.get_mut(&task_id) {
                if !comments.is_empty() {
                    comments.remove(0);
                }
            }
        }

        // Clean up empty entries
        self.comments.retain(|_, v| !v.is_empty());
    }

    /// Save comments to disk (atomic write).
    pub async fn save(&self, path: &Path) -> Result<(), TaskCommentError> {
        let json = serde_json::to_string_pretty(self)?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, json.as_bytes()).await?;
        fs::rename(&tmp_path, path).await?;
        Ok(())
    }

    /// Load comments from disk.
    pub async fn load(path: &Path) -> Result<Self, TaskCommentError> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let json = fs::read_to_string(path).await?;
        let manager: Self = serde_json::from_str(&json)?;
        Ok(manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_manager_empty() {
        let mgr = TaskCommentsManager::new();
        assert_eq!(mgr.total_comment_count(), 0);
        assert!(mgr.list_tasks_with_comments().is_empty());
    }

    #[test]
    fn test_add_comment() {
        let mut mgr = TaskCommentsManager::new();
        let comment = mgr
            .add_comment(
                "task1",
                "Hello world",
                Some("alice"),
                vec!["note".to_string()],
            )
            .unwrap();
        assert_eq!(comment.task_id, "task1");
        assert_eq!(comment.text, "Hello world");
        assert_eq!(comment.author, Some("alice".to_string()));
        assert_eq!(comment.tags, vec!["note"]);
        assert_eq!(mgr.total_comment_count(), 1);
    }

    #[test]
    fn test_add_comment_trims_whitespace() {
        let mut mgr = TaskCommentsManager::new();
        let comment = mgr
            .add_comment("task1", "  spaced out  ", None, vec![])
            .unwrap();
        assert_eq!(comment.text, "spaced out");
    }

    #[test]
    fn test_add_empty_comment_rejected() {
        let mut mgr = TaskCommentsManager::new();
        assert!(mgr.add_comment("task1", "", None, vec![]).is_err());
        assert!(mgr.add_comment("task1", "   ", None, vec![]).is_err());
    }

    #[test]
    fn test_add_comment_empty_author_becomes_none() {
        let mut mgr = TaskCommentsManager::new();
        let comment = mgr
            .add_comment("task1", "text", Some("  "), vec![])
            .unwrap();
        assert_eq!(comment.author, None);
    }

    #[test]
    fn test_add_comment_empty_tags_filtered() {
        let mut mgr = TaskCommentsManager::new();
        let comment = mgr
            .add_comment(
                "task1",
                "text",
                None,
                vec!["".to_string(), "good".to_string()],
            )
            .unwrap();
        assert_eq!(comment.tags, vec!["good"]);
    }

    #[test]
    fn test_get_comments_chronological() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "first", None, vec![]).unwrap();
        mgr.add_comment("task1", "second", None, vec![]).unwrap();
        mgr.add_comment("task1", "third", None, vec![]).unwrap();

        let comments = mgr.get_comments("task1");
        assert_eq!(comments.len(), 3);
        assert_eq!(comments[0].text, "first");
        assert_eq!(comments[1].text, "second");
        assert_eq!(comments[2].text, "third");
    }

    #[test]
    fn test_get_comments_nonexistent_task() {
        let mgr = TaskCommentsManager::new();
        assert!(mgr.get_comments("nonexistent").is_empty());
    }

    #[test]
    fn test_remove_comment() {
        let mut mgr = TaskCommentsManager::new();
        let c1 = mgr.add_comment("task1", "first", None, vec![]).unwrap();
        let _c2 = mgr.add_comment("task1", "second", None, vec![]).unwrap();

        let removed = mgr.remove_comment(&c1.id).unwrap();
        assert_eq!(removed.text, "first");
        assert_eq!(mgr.total_comment_count(), 1);

        let remaining = mgr.get_comments("task1");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].text, "second");
    }

    #[test]
    fn test_remove_comment_not_found() {
        let mut mgr = TaskCommentsManager::new();
        assert!(mgr.remove_comment("nonexistent").is_err());
    }

    #[test]
    fn test_per_task_limit_evicts_oldest() {
        let config = TaskCommentsConfig {
            max_comments_per_task: 3,
            max_total_comments: 100,
        };
        let mut mgr = TaskCommentsManager::with_config(config);

        mgr.add_comment("task1", "c1", None, vec![]).unwrap();
        mgr.add_comment("task1", "c2", None, vec![]).unwrap();
        mgr.add_comment("task1", "c3", None, vec![]).unwrap();
        mgr.add_comment("task1", "c4", None, vec![]).unwrap();

        let comments = mgr.get_comments("task1");
        assert_eq!(comments.len(), 3);
        // Oldest (c1) should be evicted
        assert_eq!(comments[0].text, "c2");
        assert_eq!(comments[1].text, "c3");
        assert_eq!(comments[2].text, "c4");
    }

    #[test]
    fn test_global_limit_evicts_oldest_across_tasks() {
        let config = TaskCommentsConfig {
            max_comments_per_task: 10,
            max_total_comments: 4,
        };
        let mut mgr = TaskCommentsManager::with_config(config);

        mgr.add_comment("task1", "t1-c1", None, vec![]).unwrap();
        mgr.add_comment("task1", "t1-c2", None, vec![]).unwrap();
        mgr.add_comment("task2", "t2-c1", None, vec![]).unwrap();
        mgr.add_comment("task2", "t2-c2", None, vec![]).unwrap();
        // This 5th comment should trigger eviction of the oldest
        mgr.add_comment("task3", "t3-c1", None, vec![]).unwrap();

        assert_eq!(mgr.total_comment_count(), 4);
        // t1-c1 (oldest) should be evicted
        let t1_comments = mgr.get_comments("task1");
        assert_eq!(t1_comments.len(), 1);
        assert_eq!(t1_comments[0].text, "t1-c2");
    }

    #[test]
    fn test_get_comment_summary() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "first", None, vec![]).unwrap();
        mgr.add_comment("task1", "second", None, vec![]).unwrap();

        let summary = mgr.get_comment_summary("task1");
        assert_eq!(summary.task_id, "task1");
        assert_eq!(summary.comment_count, 2);
        assert_eq!(summary.latest_comment.as_ref().unwrap().text, "second");
        assert_eq!(summary.comments.len(), 2);
    }

    #[test]
    fn test_get_comment_summary_empty_task() {
        let mgr = TaskCommentsManager::new();
        let summary = mgr.get_comment_summary("nonexistent");
        assert_eq!(summary.comment_count, 0);
        assert!(summary.latest_comment.is_none());
        assert!(summary.comments.is_empty());
    }

    #[test]
    fn test_list_tasks_with_comments() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task-b", "comment", None, vec![]).unwrap();
        mgr.add_comment("task-a", "comment", None, vec![]).unwrap();
        mgr.add_comment("task-c", "comment", None, vec![]).unwrap();

        let ids = mgr.list_tasks_with_comments();
        assert_eq!(ids, vec!["task-a", "task-b", "task-c"]);
    }

    #[test]
    fn test_get_comment_counts() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "c1", None, vec![]).unwrap();
        mgr.add_comment("task1", "c2", None, vec![]).unwrap();
        mgr.add_comment("task2", "c3", None, vec![]).unwrap();

        let counts = mgr.get_comment_counts();
        assert_eq!(counts.get("task1"), Some(&2));
        assert_eq!(counts.get("task2"), Some(&1));
    }

    #[test]
    fn test_search_comments_by_text() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "Bug found in download", None, vec![])
            .unwrap();
        mgr.add_comment("task1", "Fixed the bug", None, vec![])
            .unwrap();
        mgr.add_comment("task2", "All good here", None, vec![])
            .unwrap();

        let result = mgr.search_comments("bug");
        assert_eq!(result.total_matches, 2);
        assert_eq!(result.matches.len(), 1); // only task1 has matches
        assert_eq!(result.matches[0].task_id, "task1");
    }

    #[test]
    fn test_search_comments_by_author() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "hello", Some("Alice"), vec![])
            .unwrap();
        mgr.add_comment("task2", "world", Some("Bob"), vec![])
            .unwrap();

        let result = mgr.search_comments("alice");
        assert_eq!(result.total_matches, 1);
        assert_eq!(result.matches[0].task_id, "task1");
    }

    #[test]
    fn test_search_comments_by_tag() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "note1", None, vec!["important".to_string()])
            .unwrap();
        mgr.add_comment("task2", "note2", None, vec!["bug".to_string()])
            .unwrap();

        let results = mgr.search_by_tag("important");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].task_id, "task1");
    }

    #[test]
    fn test_search_by_tag_case_insensitive() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "note", None, vec!["Important".to_string()])
            .unwrap();

        let results = mgr.search_by_tag("IMPORTANT");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_clear_task_comments() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "c1", None, vec![]).unwrap();
        mgr.add_comment("task1", "c2", None, vec![]).unwrap();
        mgr.add_comment("task2", "c3", None, vec![]).unwrap();

        let removed = mgr.clear_task_comments("task1");
        assert_eq!(removed, 2);
        assert_eq!(mgr.total_comment_count(), 1);
        assert!(mgr.get_comments("task1").is_empty());
    }

    #[test]
    fn test_clear_nonexistent_task() {
        let mut mgr = TaskCommentsManager::new();
        let removed = mgr.clear_task_comments("nonexistent");
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_config_update_enforces_limits() {
        let mut mgr = TaskCommentsManager::new();
        for i in 0..10 {
            mgr.add_comment("task1", &format!("comment {i}"), None, vec![])
                .unwrap();
        }
        assert_eq!(mgr.get_comments("task1").len(), 10);

        // Reduce limit
        mgr.set_config(TaskCommentsConfig {
            max_comments_per_task: 5,
            max_total_comments: 100,
        });

        assert_eq!(mgr.get_comments("task1").len(), 5);
        // Oldest should be evicted, so first remaining is "comment 5"
        assert_eq!(mgr.get_comments("task1")[0].text, "comment 5");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "hello", Some("alice"), vec!["tag1".to_string()])
            .unwrap();
        mgr.add_comment("task1", "world", None, vec![]).unwrap();

        let json = serde_json::to_string(&mgr).unwrap();
        let loaded: TaskCommentsManager = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.total_comment_count(), 2);
        assert_eq!(loaded.get_comments("task1")[0].text, "hello");
        assert_eq!(loaded.get_comments("task1")[1].text, "world");
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comments.json");

        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "persistent comment", None, vec![])
            .unwrap();
        mgr.save(&path).await.unwrap();

        let loaded = TaskCommentsManager::load(&path).await.unwrap();
        assert_eq!(loaded.total_comment_count(), 1);
        assert_eq!(loaded.get_comments("task1")[0].text, "persistent comment");
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let loaded = TaskCommentsManager::load(&path).await.unwrap();
        assert_eq!(loaded.total_comment_count(), 0);
    }

    #[test]
    fn test_multiple_tasks_independent() {
        let mut mgr = TaskCommentsManager::new();
        mgr.add_comment("task1", "t1 comment", None, vec![])
            .unwrap();
        mgr.add_comment("task2", "t2 comment", None, vec![])
            .unwrap();
        mgr.add_comment("task3", "t3 comment", None, vec![])
            .unwrap();

        assert_eq!(mgr.total_comment_count(), 3);
        assert_eq!(mgr.get_comments("task1").len(), 1);
        assert_eq!(mgr.get_comments("task2").len(), 1);
        assert_eq!(mgr.get_comments("task3").len(), 1);

        mgr.clear_task_comments("task2");
        assert_eq!(mgr.total_comment_count(), 2);
        assert!(mgr.get_comments("task2").is_empty());
    }
}
