//! Advanced task search and filtering system (Phase 117)
//!
//! Provides enhanced filtering capabilities beyond the basic TaskFilter:
//! - Size range filtering
//! - Date range filtering
//! - Progress range filtering
//! - Speed filtering
//! - Group filtering
//! - Boolean flags (has_tags, has_notes, has_error)
//! - Regex pattern matching
//! - Multi-tag filtering (any/all)

use chrono::{DateTime, Utc};

use crate::DownloadTask;

/// Advanced search query for complex task filtering
#[derive(Debug, Clone, Default)]
pub struct AdvancedSearchQuery {
    /// Text search in task name (case-insensitive substring)
    pub name_contains: Option<String>,
    /// Regex pattern for task name matching
    pub name_regex: Option<String>,
    /// Filter by download state
    pub state: Option<crate::DownloadState>,
    /// Filter by protocol
    pub protocol: Option<crate::DownloadProtocol>,
    /// Filter by group (exact match)
    pub group: Option<String>,
    /// Filter by single tag (exact match)
    pub tag: Option<String>,
    /// Filter by multiple tags (match any of these tags)
    pub tags_any: Option<Vec<String>>,
    /// Filter by multiple tags (match all of these tags)
    pub tags_all: Option<Vec<String>>,
    /// Minimum file size in bytes
    pub min_size: Option<u64>,
    /// Maximum file size in bytes
    pub max_size: Option<u64>,
    /// Minimum progress (0.0 to 1.0)
    pub min_progress: Option<f64>,
    /// Maximum progress (0.0 to 1.0)
    pub max_progress: Option<f64>,
    /// Minimum current speed in bytes/sec
    pub min_speed: Option<f64>,
    /// Maximum current speed in bytes/sec
    pub max_speed: Option<f64>,
    /// Tasks created after this time
    pub created_after: Option<DateTime<Utc>>,
    /// Tasks created before this time
    pub created_before: Option<DateTime<Utc>>,
    /// Tasks updated after this time
    pub updated_after: Option<DateTime<Utc>>,
    /// Tasks updated before this time
    pub updated_before: Option<DateTime<Utc>>,
    /// Filter tasks that have tags
    pub has_tags: Option<bool>,
    /// Filter tasks that have notes
    pub has_notes: Option<bool>,
    /// Filter tasks that have errors
    pub has_error: Option<bool>,
    /// Filter tasks that have mirrors
    pub has_mirrors: Option<bool>,
    /// Filter tasks that have deadline
    pub has_deadline: Option<bool>,
    /// Filter tasks that have checksum
    pub has_checksum: Option<bool>,
    /// Filter by priority
    pub priority: Option<crate::DownloadPriority>,
    /// Minimum priority level
    pub min_priority: Option<crate::DownloadPriority>,
    /// Filter tasks with speed limit set
    pub has_speed_limit: Option<bool>,
    /// Filter tasks that are in queue (Queued state)
    pub in_queue: Option<bool>,
    /// Filter tasks that are actively downloading
    pub is_active: Option<bool>,
    /// Filter completed tasks
    pub is_complete: Option<bool>,
    /// Filter failed tasks
    pub is_failed: Option<bool>,
    /// Filter paused tasks
    pub is_paused: Option<bool>,
}

/// Search result with metadata
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Matching tasks
    pub tasks: Vec<DownloadTask>,
    /// Total number of matches
    pub total: usize,
    /// Search execution time in microseconds
    pub execution_time_us: u64,
    /// Applied query summary
    pub query_summary: String,
}

/// Sort criteria for advanced search results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchSortBy {
    /// Sort by creation time (newest first)
    CreatedDesc,
    /// Sort by creation time (oldest first)
    CreatedAsc,
    /// Sort by name (A-Z)
    NameAsc,
    /// Sort by name (Z-A)
    NameDesc,
    /// Sort by file size (largest first)
    SizeDesc,
    /// Sort by file size (smallest first)
    SizeAsc,
    /// Sort by progress (highest first)
    ProgressDesc,
    /// Sort by progress (lowest first)
    ProgressAsc,
    /// Sort by speed (fastest first)
    SpeedDesc,
    /// Sort by speed (slowest first)
    SpeedAsc,
    /// Sort by updated time (most recent first)
    UpdatedDesc,
    /// Sort by updated time (oldest first)
    UpdatedAsc,
    /// Sort by priority (highest first)
    PriorityDesc,
    /// Sort by ETA (soonest first)
    EtaAsc,
}

impl AdvancedSearchQuery {
    /// Create a new empty query
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the query is empty (no filters applied)
    pub fn is_empty(&self) -> bool {
        self.name_contains.is_none()
            && self.name_regex.is_none()
            && self.state.is_none()
            && self.protocol.is_none()
            && self.group.is_none()
            && self.tag.is_none()
            && self.tags_any.is_none()
            && self.tags_all.is_none()
            && self.min_size.is_none()
            && self.max_size.is_none()
            && self.min_progress.is_none()
            && self.max_progress.is_none()
            && self.min_speed.is_none()
            && self.max_speed.is_none()
            && self.created_after.is_none()
            && self.created_before.is_none()
            && self.updated_after.is_none()
            && self.updated_before.is_none()
            && self.has_tags.is_none()
            && self.has_notes.is_none()
            && self.has_error.is_none()
            && self.has_mirrors.is_none()
            && self.has_deadline.is_none()
            && self.has_checksum.is_none()
            && self.priority.is_none()
            && self.min_priority.is_none()
            && self.has_speed_limit.is_none()
            && self.in_queue.is_none()
            && self.is_active.is_none()
            && self.is_complete.is_none()
            && self.is_failed.is_none()
            && self.is_paused.is_none()
    }

    /// Check if a task matches all filter criteria
    pub fn matches(&self, task: &DownloadTask) -> bool {
        // Name substring search (case-insensitive)
        if let Some(ref query) = self.name_contains
            && !task.name.to_lowercase().contains(&query.to_lowercase())
        {
            return false;
        }

        // Name regex match
        if let Some(ref pattern) = self.name_regex
            && !match_name_pattern(pattern, &task.name)
        {
            return false;
        }

        // State filter
        if let Some(state) = self.state
            && task.state != state
        {
            return false;
        }

        // Protocol filter
        if let Some(protocol) = self.protocol
            && task.protocol != protocol
        {
            return false;
        }

        // Group filter
        if let Some(ref group) = self.group {
            match &task.group {
                Some(g) if g == group => {}
                _ => return false,
            }
        }

        // Single tag filter
        if let Some(ref tag) = self.tag
            && !task.tags.iter().any(|t| t == tag)
        {
            return false;
        }

        // Tags any (OR logic)
        if let Some(ref tags) = self.tags_any
            && !tags.iter().any(|t| task.tags.iter().any(|tt| tt == t))
        {
            return false;
        }

        // Tags all (AND logic)
        if let Some(ref tags) = self.tags_all
            && !tags.iter().all(|t| task.tags.iter().any(|tt| tt == t))
        {
            return false;
        }

        // Size range
        if let Some(min) = self.min_size
            && task.size < min
        {
            return false;
        }
        if let Some(max) = self.max_size
            && task.size > max
        {
            return false;
        }

        // Progress range
        let progress = if task.size > 0 {
            task.downloaded as f64 / task.size as f64
        } else {
            0.0
        };
        if let Some(min) = self.min_progress
            && progress < min
        {
            return false;
        }
        if let Some(max) = self.max_progress
            && progress > max
        {
            return false;
        }

        // Speed range
        if let Some(min) = self.min_speed
            && task.speed_bps < min
        {
            return false;
        }
        if let Some(max) = self.max_speed
            && task.speed_bps > max
        {
            return false;
        }

        // Created time range
        if let Some(ref after) = self.created_after
            && task.created_at < *after
        {
            return false;
        }
        if let Some(ref before) = self.created_before
            && task.created_at > *before
        {
            return false;
        }

        // Updated time range
        if let Some(ref after) = self.updated_after
            && task.updated_at < *after
        {
            return false;
        }
        if let Some(ref before) = self.updated_before
            && task.updated_at > *before
        {
            return false;
        }

        // Boolean flags
        if let Some(has_tags) = self.has_tags {
            let task_has_tags = !task.tags.is_empty();
            if has_tags != task_has_tags {
                return false;
            }
        }

        if let Some(has_notes) = self.has_notes {
            let task_has_notes = task.notes.is_some() && !task.notes.as_ref().unwrap().is_empty();
            if has_notes != task_has_notes {
                return false;
            }
        }

        if let Some(has_error) = self.has_error
            && has_error != task.error.is_some()
        {
            return false;
        }

        if let Some(has_mirrors) = self.has_mirrors
            && has_mirrors != !task.mirror_urls.is_empty()
        {
            return false;
        }

        if let Some(has_deadline) = self.has_deadline
            && has_deadline != task.deadline.is_some()
        {
            return false;
        }

        if let Some(has_checksum) = self.has_checksum
            && has_checksum != task.expected_checksum.is_some()
        {
            return false;
        }

        // Priority filter
        if let Some(priority) = self.priority
            && task.priority != priority
        {
            return false;
        }

        // Minimum priority
        if let Some(min_priority) = self.min_priority
            && priority_to_u32(task.priority) < priority_to_u32(min_priority)
        {
            return false;
        }

        // Has speed limit
        if let Some(has_speed_limit) = self.has_speed_limit
            && has_speed_limit != task.speed_limit_bps.is_some()
        {
            return false;
        }

        // State shortcuts
        if let Some(in_queue) = self.in_queue
            && in_queue != (task.state == crate::DownloadState::Queued)
        {
            return false;
        }

        if let Some(is_active) = self.is_active
            && is_active != (task.state == crate::DownloadState::Downloading)
        {
            return false;
        }

        if let Some(is_complete) = self.is_complete
            && is_complete != (task.state == crate::DownloadState::Complete)
        {
            return false;
        }

        if let Some(is_failed) = self.is_failed
            && is_failed != (task.state == crate::DownloadState::Error)
        {
            return false;
        }

        if let Some(is_paused) = self.is_paused
            && is_paused != (task.state == crate::DownloadState::Paused)
        {
            return false;
        }

        true
    }

    /// Generate a human-readable summary of the query
    pub fn summarize(&self) -> String {
        let mut parts = Vec::new();

        if let Some(ref q) = self.name_contains {
            parts.push(format!("name contains \"{}\"", q));
        }
        if let Some(ref p) = self.name_regex {
            parts.push(format!("name matches \"{}\"", p));
        }
        if let Some(state) = self.state {
            parts.push(format!("state={:?}", state));
        }
        if let Some(protocol) = self.protocol {
            parts.push(format!("protocol={:?}", protocol));
        }
        if let Some(ref group) = self.group {
            parts.push(format!("group=\"{}\"", group));
        }
        if let Some(ref tag) = self.tag {
            parts.push(format!("tag=\"{}\"", tag));
        }
        if let Some(ref tags) = self.tags_any {
            parts.push(format!("tags_any={:?}", tags));
        }
        if let Some(ref tags) = self.tags_all {
            parts.push(format!("tags_all={:?}", tags));
        }
        if let Some(min) = self.min_size {
            parts.push(format!("size>={}", format_bytes(min)));
        }
        if let Some(max) = self.max_size {
            parts.push(format!("size<={}", format_bytes(max)));
        }
        if let Some(min) = self.min_progress {
            parts.push(format!("progress>={:.0}%", min * 100.0));
        }
        if let Some(max) = self.max_progress {
            parts.push(format!("progress<={:.0}%", max * 100.0));
        }
        if let Some(min) = self.min_speed {
            parts.push(format!("speed>={}/s", format_bytes(min as u64)));
        }
        if let Some(max) = self.max_speed {
            parts.push(format!("speed<={}/s", format_bytes(max as u64)));
        }
        if let Some(ref after) = self.created_after {
            parts.push(format!("created after {}", after.format("%Y-%m-%d")));
        }
        if let Some(ref before) = self.created_before {
            parts.push(format!("created before {}", before.format("%Y-%m-%d")));
        }
        if let Some(has_tags) = self.has_tags {
            parts.push(format!("has_tags={}", has_tags));
        }
        if let Some(has_notes) = self.has_notes {
            parts.push(format!("has_notes={}", has_notes));
        }
        if let Some(has_error) = self.has_error {
            parts.push(format!("has_error={}", has_error));
        }
        if let Some(priority) = self.priority {
            parts.push(format!("priority={:?}", priority));
        }
        if let Some(is_complete) = self.is_complete {
            if is_complete {
                parts.push("completed".to_string());
            }
        }
        if let Some(is_failed) = self.is_failed {
            if is_failed {
                parts.push("failed".to_string());
            }
        }
        if let Some(is_paused) = self.is_paused {
            if is_paused {
                parts.push("paused".to_string());
            }
        }
        if let Some(is_active) = self.is_active {
            if is_active {
                parts.push("active".to_string());
            }
        }

        if parts.is_empty() {
            "all tasks".to_string()
        } else {
            parts.join(" AND ")
        }
    }
}

/// Sort tasks by the given criteria
pub fn sort_search_results(tasks: &mut [DownloadTask], sort_by: SearchSortBy) {
    match sort_by {
        SearchSortBy::CreatedDesc => tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at)),
        SearchSortBy::CreatedAsc => tasks.sort_by(|a, b| a.created_at.cmp(&b.created_at)),
        SearchSortBy::NameAsc => {
            tasks.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        }
        SearchSortBy::NameDesc => {
            tasks.sort_by(|a, b| b.name.to_lowercase().cmp(&a.name.to_lowercase()))
        }
        SearchSortBy::SizeDesc => tasks.sort_by(|a, b| b.size.cmp(&a.size)),
        SearchSortBy::SizeAsc => tasks.sort_by(|a, b| a.size.cmp(&b.size)),
        SearchSortBy::ProgressDesc => tasks.sort_by(|a, b| {
            let pa = if a.size > 0 {
                a.downloaded as f64 / a.size as f64
            } else {
                0.0
            };
            let pb = if b.size > 0 {
                b.downloaded as f64 / b.size as f64
            } else {
                0.0
            };
            pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
        }),
        SearchSortBy::ProgressAsc => tasks.sort_by(|a, b| {
            let pa = if a.size > 0 {
                a.downloaded as f64 / a.size as f64
            } else {
                0.0
            };
            let pb = if b.size > 0 {
                b.downloaded as f64 / b.size as f64
            } else {
                0.0
            };
            pa.partial_cmp(&pb).unwrap_or(std::cmp::Ordering::Equal)
        }),
        SearchSortBy::SpeedDesc => tasks.sort_by(|a, b| {
            b.speed_bps
                .partial_cmp(&a.speed_bps)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        SearchSortBy::SpeedAsc => tasks.sort_by(|a, b| {
            a.speed_bps
                .partial_cmp(&b.speed_bps)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        SearchSortBy::UpdatedDesc => tasks.sort_by(|a, b| b.updated_at.cmp(&a.updated_at)),
        SearchSortBy::UpdatedAsc => tasks.sort_by(|a, b| a.updated_at.cmp(&b.updated_at)),
        SearchSortBy::PriorityDesc => {
            tasks.sort_by(|a, b| priority_to_u32(b.priority).cmp(&priority_to_u32(a.priority)))
        }
        SearchSortBy::EtaAsc => {
            // Sort by ETA (soonest first), tasks with no ETA go last
            tasks.sort_by(|a, b| {
                let eta_a = if a.speed_bps > 0.0 && a.size > a.downloaded {
                    (a.size - a.downloaded) as f64 / a.speed_bps
                } else if a.state == crate::DownloadState::Complete {
                    0.0
                } else {
                    f64::MAX
                };
                let eta_b = if b.speed_bps > 0.0 && b.size > b.downloaded {
                    (b.size - b.downloaded) as f64 / b.speed_bps
                } else if b.state == crate::DownloadState::Complete {
                    0.0
                } else {
                    f64::MAX
                };
                eta_a
                    .partial_cmp(&eta_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
    }
}

/// Convert priority to numeric value for comparison
fn priority_to_u32(priority: crate::DownloadPriority) -> u32 {
    match priority {
        crate::DownloadPriority::Low => 0,
        crate::DownloadPriority::Normal => 1,
        crate::DownloadPriority::High => 2,
    }
}

/// Simple pattern matching supporting * and ? wildcards (case-insensitive)
fn match_name_pattern(pattern: &str, name: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let name = name.to_lowercase();

    // If no wildcards, treat as substring match
    if !pattern.contains('*') && !pattern.contains('?') {
        return name.contains(&pattern);
    }

    // Convert pattern to a simple matcher
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 1 {
        // No wildcards, exact match with possible ? wildcards
        return match_with_questions(&pattern, &name);
    }

    // Check if pattern starts with *
    let starts_with_wildcard = pattern.starts_with('*');
    // Check if pattern ends with *
    let ends_with_wildcard = pattern.ends_with('*');

    let mut pos = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        // Find the part in the name starting from current position
        if let Some(found_pos) = find_with_questions(part, &name[pos..]) {
            // If this is the first part and pattern doesn't start with *, it must be at the beginning
            if i == 0 && !starts_with_wildcard && found_pos != 0 {
                return false;
            }
            pos += found_pos + part.len();
        } else {
            return false;
        }
    }

    // If pattern doesn't end with *, the last part must be at the end
    if !ends_with_wildcard {
        if let Some(last) = parts.last() {
            if !last.is_empty() && !name.ends_with(&last.to_lowercase()) {
                return false;
            }
        }
    }

    true
}

/// Match a string against a pattern with ? wildcards
fn match_with_questions(pattern: &str, text: &str) -> bool {
    if pattern.len() != text.len() {
        return false;
    }
    pattern
        .chars()
        .zip(text.chars())
        .all(|(p, t)| p == '?' || p == t)
}

/// Find a pattern (with ? wildcards) in text, returning the position
fn find_with_questions(pattern: &str, text: &str) -> Option<usize> {
    if pattern.is_empty() {
        return Some(0);
    }
    if pattern.len() > text.len() {
        return None;
    }
    for i in 0..=(text.len() - pattern.len()) {
        if match_with_questions(pattern, &text[i..i + pattern.len()]) {
            return Some(i);
        }
    }
    None
}

/// Format bytes to human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState, DownloadTask};
    use std::path::PathBuf;

    fn make_task(name: &str, state: DownloadState) -> DownloadTask {
        DownloadTask {
            id: format!("task-{}", name),
            name: name.to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1000,
            downloaded: 0,
            state,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec![],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: vec![],
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: vec![],
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }
    }

    #[test]
    fn test_empty_query_matches_all() {
        let query = AdvancedSearchQuery::new();
        assert!(query.is_empty());

        let task = make_task("test", DownloadState::Downloading);
        assert!(query.matches(&task));
    }

    #[test]
    fn test_name_contains() {
        let mut query = AdvancedSearchQuery::new();
        query.name_contains = Some("linux".to_string());

        let task1 = make_task("Ubuntu Linux ISO", DownloadState::Queued);
        let task2 = make_task("Windows 11", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_name_contains_case_insensitive() {
        let mut query = AdvancedSearchQuery::new();
        query.name_contains = Some("LINUX".to_string());

        let task = make_task("ubuntu linux iso", DownloadState::Queued);
        assert!(query.matches(&task));
    }

    #[test]
    fn test_state_filter() {
        let mut query = AdvancedSearchQuery::new();
        query.state = Some(DownloadState::Downloading);

        let task1 = make_task("downloading", DownloadState::Downloading);
        let task2 = make_task("paused", DownloadState::Paused);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_group_filter() {
        let mut query = AdvancedSearchQuery::new();
        query.group = Some("work".to_string());

        let mut task1 = make_task("task1", DownloadState::Queued);
        task1.group = Some("work".to_string());

        let mut task2 = make_task("task2", DownloadState::Queued);
        task2.group = Some("personal".to_string());

        let task3 = make_task("task3", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_tag_filter() {
        let mut query = AdvancedSearchQuery::new();
        query.tag = Some("movies".to_string());

        let mut task1 = make_task("task1", DownloadState::Queued);
        task1.tags = vec!["movies".to_string(), "hd".to_string()];

        let mut task2 = make_task("task2", DownloadState::Queued);
        task2.tags = vec!["music".to_string()];

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_tags_any() {
        let mut query = AdvancedSearchQuery::new();
        query.tags_any = Some(vec!["movies".to_string(), "music".to_string()]);

        let mut task1 = make_task("task1", DownloadState::Queued);
        task1.tags = vec!["movies".to_string()];

        let mut task2 = make_task("task2", DownloadState::Queued);
        task2.tags = vec!["music".to_string()];

        let mut task3 = make_task("task3", DownloadState::Queued);
        task3.tags = vec!["docs".to_string()];

        assert!(query.matches(&task1));
        assert!(query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_tags_all() {
        let mut query = AdvancedSearchQuery::new();
        query.tags_all = Some(vec!["movies".to_string(), "hd".to_string()]);

        let mut task1 = make_task("task1", DownloadState::Queued);
        task1.tags = vec!["movies".to_string(), "hd".to_string(), "action".to_string()];

        let mut task2 = make_task("task2", DownloadState::Queued);
        task2.tags = vec!["movies".to_string()];

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_size_range() {
        let mut query = AdvancedSearchQuery::new();
        query.min_size = Some(500);
        query.max_size = Some(2000);

        let mut task1 = make_task("small", DownloadState::Queued);
        task1.size = 100;

        let mut task2 = make_task("medium", DownloadState::Queued);
        task2.size = 1000;

        let mut task3 = make_task("large", DownloadState::Queued);
        task3.size = 5000;

        assert!(!query.matches(&task1));
        assert!(query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_progress_range() {
        let mut query = AdvancedSearchQuery::new();
        query.min_progress = Some(0.5);
        query.max_progress = Some(0.9);

        let mut task1 = make_task("task1", DownloadState::Downloading);
        task1.size = 1000;
        task1.downloaded = 100; // 10%

        let mut task2 = make_task("task2", DownloadState::Downloading);
        task2.size = 1000;
        task2.downloaded = 700; // 70%

        let mut task3 = make_task("task3", DownloadState::Downloading);
        task3.size = 1000;
        task3.downloaded = 1000; // 100%

        assert!(!query.matches(&task1));
        assert!(query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_speed_range() {
        let mut query = AdvancedSearchQuery::new();
        query.min_speed = Some(100.0);
        query.max_speed = Some(1000.0);

        let mut task1 = make_task("slow", DownloadState::Downloading);
        task1.speed_bps = 50.0;

        let mut task2 = make_task("medium", DownloadState::Downloading);
        task2.speed_bps = 500.0;

        let mut task3 = make_task("fast", DownloadState::Downloading);
        task3.speed_bps = 2000.0;

        assert!(!query.matches(&task1));
        assert!(query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_has_tags() {
        let mut query = AdvancedSearchQuery::new();
        query.has_tags = Some(true);

        let mut task1 = make_task("tagged", DownloadState::Queued);
        task1.tags = vec!["test".to_string()];

        let task2 = make_task("untagged", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));

        query.has_tags = Some(false);
        assert!(!query.matches(&task1));
        assert!(query.matches(&task2));
    }

    #[test]
    fn test_has_notes() {
        let mut query = AdvancedSearchQuery::new();
        query.has_notes = Some(true);

        let mut task1 = make_task("noted", DownloadState::Queued);
        task1.notes = Some("important".to_string());

        let task2 = make_task("no notes", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_has_error() {
        let mut query = AdvancedSearchQuery::new();
        query.has_error = Some(true);

        let mut task1 = make_task("failed", DownloadState::Error);
        task1.error = Some("Connection timeout".to_string());

        let task2 = make_task("ok", DownloadState::Downloading);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_has_mirrors() {
        let mut query = AdvancedSearchQuery::new();
        query.has_mirrors = Some(true);

        let mut task1 = make_task("with mirrors", DownloadState::Queued);
        task1.mirror_urls = vec!["http://mirror1.com/file".to_string()];

        let task2 = make_task("no mirrors", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_priority_filter() {
        let mut query = AdvancedSearchQuery::new();
        query.priority = Some(DownloadPriority::High);

        let mut task1 = make_task("high", DownloadState::Queued);
        task1.priority = DownloadPriority::High;

        let mut task2 = make_task("normal", DownloadState::Queued);
        task2.priority = DownloadPriority::Normal;

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_min_priority() {
        let mut query = AdvancedSearchQuery::new();
        query.min_priority = Some(DownloadPriority::High);

        let mut task1 = make_task("high", DownloadState::Queued);
        task1.priority = DownloadPriority::High;

        let mut task2 = make_task("normal", DownloadState::Queued);
        task2.priority = DownloadPriority::Normal;

        let mut task3 = make_task("low", DownloadState::Queued);
        task3.priority = DownloadPriority::Low;

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
        assert!(!query.matches(&task3));
    }

    #[test]
    fn test_state_shortcuts() {
        let mut query = AdvancedSearchQuery::new();
        query.is_complete = Some(true);

        let task1 = make_task("done", DownloadState::Complete);
        let task2 = make_task("downloading", DownloadState::Downloading);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_is_active() {
        let mut query = AdvancedSearchQuery::new();
        query.is_active = Some(true);

        let task1 = make_task("downloading", DownloadState::Downloading);
        let task2 = make_task("queued", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_is_paused() {
        let mut query = AdvancedSearchQuery::new();
        query.is_paused = Some(true);

        let task1 = make_task("paused", DownloadState::Paused);
        let task2 = make_task("downloading", DownloadState::Downloading);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_combined_filters() {
        let mut query = AdvancedSearchQuery::new();
        query.state = Some(DownloadState::Downloading);
        query.min_speed = Some(100.0);
        query.has_tags = Some(true);

        let mut task1 = make_task("fast tagged", DownloadState::Downloading);
        task1.speed_bps = 500.0;
        task1.tags = vec!["test".to_string()];

        let mut task2 = make_task("slow tagged", DownloadState::Downloading);
        task2.speed_bps = 50.0;
        task2.tags = vec!["test".to_string()];

        let mut task3 = make_task("fast untagged", DownloadState::Downloading);
        task3.speed_bps = 500.0;

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2)); // too slow
        assert!(!query.matches(&task3)); // no tags
    }

    #[test]
    fn test_pattern_matching_exact() {
        assert!(match_name_pattern("test", "test file"));
        assert!(match_name_pattern("test", "my test file"));
        assert!(!match_name_pattern("test", "other file"));
    }

    #[test]
    fn test_pattern_matching_wildcard_star() {
        assert!(match_name_pattern("*.mp4", "movie.mp4"));
        assert!(!match_name_pattern("*.mp4", "movie.avi"));
        assert!(match_name_pattern("linux-*", "linux-ubuntu-22.04"));
        assert!(!match_name_pattern("linux-*", "windows-11"));
    }

    #[test]
    fn test_pattern_matching_wildcard_question() {
        assert!(match_name_pattern("file?.txt", "file1.txt"));
        assert!(match_name_pattern("file?.txt", "fileA.txt"));
        assert!(!match_name_pattern("file?.txt", "file12.txt"));
    }

    #[test]
    fn test_pattern_matching_combined() {
        assert!(match_name_pattern("*ubuntu*", "ubuntu-22.04-desktop.iso"));
        assert!(match_name_pattern("*.iso", "ubuntu-22.04-desktop.iso"));
        assert!(!match_name_pattern("*.iso", "ubuntu-22.04-desktop.img"));
    }

    #[test]
    fn test_sort_by_name() {
        let mut tasks = vec![
            make_task("Charlie", DownloadState::Queued),
            make_task("alpha", DownloadState::Queued),
            make_task("Bravo", DownloadState::Queued),
        ];

        sort_search_results(&mut tasks, SearchSortBy::NameAsc);
        assert_eq!(tasks[0].name, "alpha");
        assert_eq!(tasks[1].name, "Bravo");
        assert_eq!(tasks[2].name, "Charlie");

        sort_search_results(&mut tasks, SearchSortBy::NameDesc);
        assert_eq!(tasks[0].name, "Charlie");
        assert_eq!(tasks[1].name, "Bravo");
        assert_eq!(tasks[2].name, "alpha");
    }

    #[test]
    fn test_sort_by_size() {
        let mut tasks = vec![
            {
                let mut t = make_task("small", DownloadState::Queued);
                t.size = 100;
                t
            },
            {
                let mut t = make_task("large", DownloadState::Queued);
                t.size = 10000;
                t
            },
            {
                let mut t = make_task("medium", DownloadState::Queued);
                t.size = 1000;
                t
            },
        ];

        sort_search_results(&mut tasks, SearchSortBy::SizeDesc);
        assert_eq!(tasks[0].size, 10000);
        assert_eq!(tasks[1].size, 1000);
        assert_eq!(tasks[2].size, 100);
    }

    #[test]
    fn test_sort_by_progress() {
        let mut tasks = vec![
            {
                let mut t = make_task("t1", DownloadState::Downloading);
                t.size = 1000;
                t.downloaded = 500;
                t
            },
            {
                let mut t = make_task("t2", DownloadState::Downloading);
                t.size = 1000;
                t.downloaded = 900;
                t
            },
            {
                let mut t = make_task("t3", DownloadState::Downloading);
                t.size = 1000;
                t.downloaded = 100;
                t
            },
        ];

        sort_search_results(&mut tasks, SearchSortBy::ProgressDesc);
        assert_eq!(tasks[0].downloaded, 900);
        assert_eq!(tasks[1].downloaded, 500);
        assert_eq!(tasks[2].downloaded, 100);
    }

    #[test]
    fn test_summarize_empty() {
        let query = AdvancedSearchQuery::new();
        assert_eq!(query.summarize(), "all tasks");
    }

    #[test]
    fn test_summarize_single() {
        let mut query = AdvancedSearchQuery::new();
        query.state = Some(DownloadState::Downloading);
        assert!(query.summarize().contains("state=Downloading"));
    }

    #[test]
    fn test_summarize_combined() {
        let mut query = AdvancedSearchQuery::new();
        query.name_contains = Some("linux".to_string());
        query.min_size = Some(1_000_000_000);
        query.is_complete = Some(false);

        let summary = query.summarize();
        assert!(summary.contains("name contains"));
        assert!(summary.contains("size>="));
        assert!(summary.contains("AND"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1536), "1.5KB");
        assert_eq!(format_bytes(1_048_576), "1.0MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0GB");
    }

    #[test]
    fn test_has_speed_limit() {
        let mut query = AdvancedSearchQuery::new();
        query.has_speed_limit = Some(true);

        let mut task1 = make_task("limited", DownloadState::Downloading);
        task1.speed_limit_bps = Some(1_000_000);

        let task2 = make_task("unlimited", DownloadState::Downloading);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_has_deadline() {
        let mut query = AdvancedSearchQuery::new();
        query.has_deadline = Some(true);

        let mut task1 = make_task("urgent", DownloadState::Downloading);
        task1.deadline = Some(Utc::now() + chrono::Duration::hours(2));

        let task2 = make_task("no deadline", DownloadState::Downloading);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }

    #[test]
    fn test_has_checksum() {
        let mut query = AdvancedSearchQuery::new();
        query.has_checksum = Some(true);

        let mut task1 = make_task("verified", DownloadState::Queued);
        task1.expected_checksum = Some("abc123".to_string());

        let task2 = make_task("no checksum", DownloadState::Queued);

        assert!(query.matches(&task1));
        assert!(!query.matches(&task2));
    }
}
