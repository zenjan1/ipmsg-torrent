//! Download History Analytics (Phase 128)
//!
//! Analyzes download history to provide insights and statistics:
//! - Success/failure rates over time
//! - Protocol distribution
//! - Average download sizes
//! - Top domains/sources
//! - Time-based trends (daily/weekly/monthly)
//! - Tag usage statistics
//!
//! Features:
//! - Configurable analysis periods
//! - Protocol breakdown with percentages
//! - Success rate tracking
//! - Average/median download size calculation
//! - Domain extraction from source URLs
//! - Tag frequency analysis
//! - Persistent configuration

use crate::download_history::{HistoryEntry, HistoryOutcome};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Analytics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryAnalyticsConfig {
    /// Enable analytics generation
    pub enabled: bool,
    /// Default analysis period in days (default: 30)
    pub default_period_days: i64,
    /// Maximum entries to analyze (default: 10000)
    pub max_entries: usize,
    /// Include tag statistics
    pub include_tag_stats: bool,
    /// Include domain statistics
    pub include_domain_stats: bool,
}

impl Default for HistoryAnalyticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_period_days: 30,
            max_entries: 10000,
            include_tag_stats: true,
            include_domain_stats: true,
        }
    }
}

/// Protocol statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProtocolStats {
    /// Number of downloads with this protocol
    pub count: usize,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Percentage of total downloads (0.0 - 100.0)
    pub percentage: f64,
    /// Average download size in bytes
    pub avg_size: u64,
}

/// Outcome statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OutcomeStats {
    /// Number of successful downloads
    pub completed: usize,
    /// Number of failed downloads
    pub failed: usize,
    /// Success rate percentage (0.0 - 100.0)
    pub success_rate: f64,
    /// Total bytes successfully downloaded
    pub completed_bytes: u64,
    /// Total bytes attempted (including failed)
    pub attempted_bytes: u64,
}

/// Domain statistics entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainStats {
    /// Domain name (e.g., "example.com")
    pub domain: String,
    /// Number of downloads from this domain
    pub count: usize,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Success count
    pub success_count: usize,
}

/// Tag statistics entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagStats {
    /// Tag name
    pub tag: String,
    /// Number of downloads with this tag
    pub count: usize,
    /// Total bytes downloaded
    pub total_bytes: u64,
}

/// Size distribution bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeBucket {
    /// Bucket label (e.g., "< 1MB", "1-10MB", etc.)
    pub label: String,
    /// Number of downloads in this bucket
    pub count: usize,
}

/// Time period statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodStats {
    /// Period start date
    pub start_date: DateTime<Utc>,
    /// Period end date
    pub end_date: DateTime<Utc>,
    /// Number of days in period
    pub days: i64,
    /// Downloads per day average
    pub avg_downloads_per_day: f64,
    /// Bytes downloaded per day average
    pub avg_bytes_per_day: u64,
}

/// Complete analytics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryAnalyticsSummary {
    /// Analysis period start
    pub period_start: DateTime<Utc>,
    /// Analysis period end
    pub period_end: DateTime<Utc>,
    /// Total number of entries analyzed
    pub total_entries: usize,
    /// Protocol breakdown
    pub protocol_stats: HashMap<String, ProtocolStats>,
    /// Outcome statistics
    pub outcome_stats: OutcomeStats,
    /// Average download size in bytes
    pub avg_download_size: u64,
    /// Median download size in bytes
    pub median_download_size: u64,
    /// Largest download in bytes
    pub largest_download: u64,
    /// Smallest download in bytes
    pub smallest_download: u64,
    /// Size distribution buckets
    pub size_distribution: Vec<SizeBucket>,
    /// Top domains by download count
    pub top_domains: Vec<DomainStats>,
    /// Tag frequency statistics
    pub tag_stats: Vec<TagStats>,
    /// Time period statistics
    pub period_stats: PeriodStats,
}

/// Analytics manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryAnalyticsManager {
    /// Configuration
    pub config: HistoryAnalyticsConfig,
}

impl Default for HistoryAnalyticsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryAnalyticsManager {
    /// Create a new analytics manager with default configuration
    pub fn new() -> Self {
        Self {
            config: HistoryAnalyticsConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: HistoryAnalyticsConfig) -> Self {
        Self { config }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &HistoryAnalyticsConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: HistoryAnalyticsConfig) {
        self.config = config;
    }

    /// Generate analytics summary from history entries
    pub fn analyze(&self, entries: &[HistoryEntry]) -> HistoryAnalyticsSummary {
        let now = Utc::now();
        let period_days = self.config.default_period_days;
        let period_start = now - Duration::days(period_days);

        // Filter entries within period and limit count
        let mut filtered: Vec<&HistoryEntry> = entries
            .iter()
            .filter(|e| e.finished_at >= period_start)
            .take(self.config.max_entries)
            .collect();

        // Sort by finished_at descending
        filtered.sort_by_key(|e| std::cmp::Reverse(e.finished_at));

        let total_entries = filtered.len();

        // Protocol statistics
        let mut protocol_counts: HashMap<String, usize> = HashMap::new();
        let mut protocol_bytes: HashMap<String, u64> = HashMap::new();

        // Outcome statistics
        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut completed_bytes = 0u64;
        let mut attempted_bytes = 0u64;

        // Size tracking
        let mut sizes: Vec<u64> = Vec::new();

        // Domain tracking
        let mut domain_counts: HashMap<String, usize> = HashMap::new();
        let mut domain_bytes: HashMap<String, u64> = HashMap::new();
        let mut domain_success: HashMap<String, usize> = HashMap::new();

        // Tag tracking
        let mut tag_counts: HashMap<String, usize> = HashMap::new();
        let mut tag_bytes: HashMap<String, u64> = HashMap::new();

        for entry in &filtered {
            // Protocol stats
            let proto_str = format!("{:?}", entry.protocol);
            *protocol_counts.entry(proto_str.clone()).or_insert(0) += 1;
            *protocol_bytes.entry(proto_str).or_insert(0) += entry.downloaded;

            // Outcome stats
            match entry.outcome {
                HistoryOutcome::Completed => {
                    completed += 1;
                    completed_bytes += entry.downloaded;
                }
                HistoryOutcome::Failed => {
                    failed += 1;
                }
            }
            attempted_bytes += entry.downloaded;

            // Size tracking
            sizes.push(entry.size);

            // Domain extraction (simple heuristic from source_url if available)
            #[allow(clippy::collapsible_if)]
            if self.config.include_domain_stats {
                if let Some(domain) = extract_domain_from_entry(entry) {
                    *domain_counts.entry(domain.clone()).or_insert(0) += 1;
                    *domain_bytes.entry(domain.clone()).or_insert(0) += entry.downloaded;
                    if entry.outcome == HistoryOutcome::Completed {
                        *domain_success.entry(domain).or_insert(0) += 1;
                    }
                }
            }

            // Tag stats
            if self.config.include_tag_stats {
                for tag in &entry.tags {
                    *tag_counts.entry(tag.clone()).or_insert(0) += 1;
                    *tag_bytes.entry(tag.clone()).or_insert(0) += entry.downloaded;
                }
            }
        }

        // Build protocol stats with percentages
        let mut protocol_stats: HashMap<String, ProtocolStats> = HashMap::new();
        for (proto, count) in protocol_counts {
            let bytes = protocol_bytes.get(&proto).copied().unwrap_or(0);
            let percentage = if total_entries > 0 {
                (count as f64 / total_entries as f64) * 100.0
            } else {
                0.0
            };
            let avg_size = if count > 0 { bytes / count as u64 } else { 0 };

            protocol_stats.insert(
                proto,
                ProtocolStats {
                    count,
                    total_bytes: bytes,
                    percentage,
                    avg_size,
                },
            );
        }

        // Outcome stats
        let success_rate = if total_entries > 0 {
            (completed as f64 / total_entries as f64) * 100.0
        } else {
            0.0
        };
        let outcome_stats = OutcomeStats {
            completed,
            failed,
            success_rate,
            completed_bytes,
            attempted_bytes,
        };

        // Size statistics
        sizes.sort();
        let avg_download_size = if !sizes.is_empty() {
            sizes.iter().sum::<u64>() / sizes.len() as u64
        } else {
            0
        };
        let median_download_size = if !sizes.is_empty() {
            let mid = sizes.len() / 2;
            if sizes.len().is_multiple_of(2) {
                (sizes[mid - 1] + sizes[mid]) / 2
            } else {
                sizes[mid]
            }
        } else {
            0
        };
        let largest_download = sizes.last().copied().unwrap_or(0);
        let smallest_download = sizes.first().copied().unwrap_or(0);

        // Size distribution buckets
        let size_distribution = compute_size_distribution(&sizes);

        // Top domains
        let mut top_domains: Vec<DomainStats> = domain_counts
            .into_iter()
            .map(|(domain, count)| {
                let total_bytes = domain_bytes.get(&domain).copied().unwrap_or(0);
                let success_count = domain_success.get(&domain).copied().unwrap_or(0);
                DomainStats {
                    domain,
                    count,
                    total_bytes,
                    success_count,
                }
            })
            .collect();
        top_domains.sort_by_key(|d| std::cmp::Reverse(d.count));
        top_domains.truncate(10);

        // Tag stats
        let mut tag_stats: Vec<TagStats> = tag_counts
            .into_iter()
            .map(|(tag, count)| {
                let total_bytes = tag_bytes.get(&tag).copied().unwrap_or(0);
                TagStats {
                    tag,
                    count,
                    total_bytes,
                }
            })
            .collect();
        tag_stats.sort_by_key(|t| std::cmp::Reverse(t.count));

        // Period stats
        let avg_downloads_per_day = if period_days > 0 {
            total_entries as f64 / period_days as f64
        } else {
            0.0
        };
        let avg_bytes_per_day = if period_days > 0 {
            completed_bytes / period_days as u64
        } else {
            0
        };
        let period_stats = PeriodStats {
            start_date: period_start,
            end_date: now,
            days: period_days,
            avg_downloads_per_day,
            avg_bytes_per_day,
        };

        HistoryAnalyticsSummary {
            period_start,
            period_end: now,
            total_entries,
            protocol_stats,
            outcome_stats,
            avg_download_size,
            median_download_size,
            largest_download,
            smallest_download,
            size_distribution,
            top_domains,
            tag_stats,
            period_stats,
        }
    }

    /// Format summary as human-readable string
    pub fn format_summary(&self, summary: &HistoryAnalyticsSummary) -> String {
        let mut out = String::new();

        out.push_str("📊 Download History Analytics\n");
        out.push_str(&format!(
            "Period: {} to {} ({} days)\n",
            summary.period_start.format("%Y-%m-%d"),
            summary.period_end.format("%Y-%m-%d"),
            summary.period_stats.days
        ));
        out.push_str(&format!("Total entries: {}\n\n", summary.total_entries));

        // Outcome stats
        out.push_str("📈 Outcomes:\n");
        out.push_str(&format!(
            "  ✅ Completed: {} ({:.1}%)\n",
            summary.outcome_stats.completed, summary.outcome_stats.success_rate
        ));
        out.push_str(&format!("  ❌ Failed: {}\n", summary.outcome_stats.failed));
        out.push_str(&format!(
            "  💾 Data downloaded: {}\n\n",
            format_bytes(summary.outcome_stats.completed_bytes)
        ));

        // Protocol breakdown
        out.push_str("🔗 Protocols:\n");
        let mut protos: Vec<_> = summary.protocol_stats.iter().collect();
        protos.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.count));
        for (proto, stats) in protos {
            out.push_str(&format!(
                "  {}: {} ({:.1}%) - avg {}\n",
                proto,
                stats.count,
                stats.percentage,
                format_bytes(stats.avg_size)
            ));
        }
        out.push('\n');

        // Size statistics
        out.push_str("📦 Size Statistics:\n");
        out.push_str(&format!(
            "  Average: {}\n",
            format_bytes(summary.avg_download_size)
        ));
        out.push_str(&format!(
            "  Median: {}\n",
            format_bytes(summary.median_download_size)
        ));
        out.push_str(&format!(
            "  Largest: {}\n",
            format_bytes(summary.largest_download)
        ));
        out.push_str(&format!(
            "  Smallest: {}\n\n",
            format_bytes(summary.smallest_download)
        ));

        // Size distribution
        if !summary.size_distribution.is_empty() {
            out.push_str("📊 Size Distribution:\n");
            for bucket in &summary.size_distribution {
                if bucket.count > 0 {
                    out.push_str(&format!("  {}: {}\n", bucket.label, bucket.count));
                }
            }
            out.push('\n');
        }

        // Top domains
        if !summary.top_domains.is_empty() {
            out.push_str("🌐 Top Domains:\n");
            for domain in summary.top_domains.iter().take(5) {
                let success_rate = if domain.count > 0 {
                    (domain.success_count as f64 / domain.count as f64) * 100.0
                } else {
                    0.0
                };
                out.push_str(&format!(
                    "  {}: {} downloads ({:.0}% success) - {}\n",
                    domain.domain,
                    domain.count,
                    success_rate,
                    format_bytes(domain.total_bytes)
                ));
            }
            out.push('\n');
        }

        // Tag stats
        if !summary.tag_stats.is_empty() {
            out.push_str("🏷️  Top Tags:\n");
            for tag in summary.tag_stats.iter().take(5) {
                out.push_str(&format!(
                    "  {}: {} downloads - {}\n",
                    tag.tag,
                    tag.count,
                    format_bytes(tag.total_bytes)
                ));
            }
            out.push('\n');
        }

        // Period averages
        out.push_str("📅 Daily Averages:\n");
        out.push_str(&format!(
            "  Downloads/day: {:.1}\n",
            summary.period_stats.avg_downloads_per_day
        ));
        out.push_str(&format!(
            "  Data/day: {}\n",
            format_bytes(summary.period_stats.avg_bytes_per_day)
        ));

        out
    }
}

/// Extract domain from a history entry (simple heuristic)
fn extract_domain_from_entry(_entry: &HistoryEntry) -> Option<String> {
    // HistoryEntry doesn't have source_url, so we use name as fallback
    // In real usage, we'd need to extend HistoryEntry or use a different approach
    // For now, return None since we can't extract domain from name alone
    None
}

/// Compute size distribution buckets
fn compute_size_distribution(sizes: &[u64]) -> Vec<SizeBucket> {
    let buckets = [
        ("< 1MB", 0, 1_000_000),
        ("1-10MB", 1_000_000, 10_000_000),
        ("10-100MB", 10_000_000, 100_000_000),
        ("100MB-1GB", 100_000_000, 1_000_000_000),
        ("1-10GB", 1_000_000_000, 10_000_000_000),
        ("> 10GB", 10_000_000_000, u64::MAX),
    ];

    buckets
        .iter()
        .map(|(label, min, max)| {
            let count = sizes.iter().filter(|&&s| s >= *min && s < *max).count();
            SizeBucket {
                label: label.to_string(),
                count,
            }
        })
        .collect()
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Save analytics configuration to disk
pub fn save_analytics_config(
    config: &HistoryAnalyticsConfig,
    data_dir: &Path,
) -> Result<(), HistoryAnalyticsError> {
    let path = data_dir.join("history_analytics_config.json");
    let json = serde_json::to_string(config)
        .map_err(|e| HistoryAnalyticsError::Serialize(e.to_string()))?;
    std::fs::write(&path, json).map_err(|e| HistoryAnalyticsError::Io(e.to_string()))?;
    Ok(())
}

/// Load analytics configuration from disk
pub fn load_analytics_config(
    data_dir: &Path,
) -> Result<Option<HistoryAnalyticsConfig>, HistoryAnalyticsError> {
    let path = data_dir.join("history_analytics_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let json =
        std::fs::read_to_string(&path).map_err(|e| HistoryAnalyticsError::Io(e.to_string()))?;
    let config =
        serde_json::from_str(&json).map_err(|e| HistoryAnalyticsError::Serialize(e.to_string()))?;
    Ok(Some(config))
}

/// Error type for analytics operations
#[derive(Debug, thiserror::Error)]
pub enum HistoryAnalyticsError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("Serialization error: {0}")]
    Serialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download_history::{HistoryEntry, HistoryProtocol};
    use chrono::Utc;
    use std::path::PathBuf;

    fn make_entry(
        name: &str,
        protocol: HistoryProtocol,
        outcome: HistoryOutcome,
        size: u64,
        downloaded: u64,
        finished_at: DateTime<Utc>,
    ) -> HistoryEntry {
        HistoryEntry {
            task_id: format!("task-{}", name),
            name: name.to_string(),
            protocol,
            outcome,
            size,
            downloaded,
            save_path: PathBuf::from("/tmp"),
            error: None,
            created_at: finished_at - Duration::hours(1),
            finished_at,
            tags: Vec::new(),
        }
    }

    fn make_entry_with_tags(
        name: &str,
        protocol: HistoryProtocol,
        outcome: HistoryOutcome,
        size: u64,
        downloaded: u64,
        finished_at: DateTime<Utc>,
        tags: Vec<String>,
    ) -> HistoryEntry {
        HistoryEntry {
            task_id: format!("task-{}", name),
            name: name.to_string(),
            protocol,
            outcome,
            size,
            downloaded,
            save_path: PathBuf::from("/tmp"),
            error: None,
            created_at: finished_at - Duration::hours(1),
            finished_at,
            tags,
        }
    }

    #[test]
    fn test_default_config() {
        let config = HistoryAnalyticsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_period_days, 30);
        assert_eq!(config.max_entries, 10000);
        assert!(config.include_tag_stats);
        assert!(config.include_domain_stats);
    }

    #[test]
    fn test_empty_analytics() {
        let manager = HistoryAnalyticsManager::new();
        let entries: Vec<HistoryEntry> = Vec::new();
        let summary = manager.analyze(&entries);

        assert_eq!(summary.total_entries, 0);
        assert_eq!(summary.outcome_stats.completed, 0);
        assert_eq!(summary.outcome_stats.failed, 0);
        assert_eq!(summary.avg_download_size, 0);
        assert!(summary.protocol_stats.is_empty());
    }

    #[test]
    fn test_basic_analytics() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![
            make_entry(
                "file1.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                1000,
                1000,
                now - Duration::days(1),
            ),
            make_entry(
                "file2.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                2000,
                2000,
                now - Duration::days(2),
            ),
            make_entry(
                "file3.txt",
                HistoryProtocol::Torrent,
                HistoryOutcome::Failed,
                3000,
                1500,
                now - Duration::days(3),
            ),
        ];

        let summary = manager.analyze(&entries);

        assert_eq!(summary.total_entries, 3);
        assert_eq!(summary.outcome_stats.completed, 2);
        assert_eq!(summary.outcome_stats.failed, 1);
        assert!((summary.outcome_stats.success_rate - 66.67).abs() < 0.1);

        // Protocol stats
        assert!(summary.protocol_stats.contains_key("Xunlei"));
        assert!(summary.protocol_stats.contains_key("Torrent"));
        assert_eq!(summary.protocol_stats["Xunlei"].count, 2);
        assert_eq!(summary.protocol_stats["Torrent"].count, 1);

        // Size stats
        assert_eq!(summary.largest_download, 3000);
        assert_eq!(summary.smallest_download, 1000);
    }

    #[test]
    fn test_size_distribution() {
        let sizes = vec![
            500_000,       // < 1MB
            5_000_000,     // 1-10MB
            50_000_000,    // 10-100MB
            500_000_000,   // 100MB-1GB
            5_000_000_000, // 1-10GB
        ];

        let buckets = compute_size_distribution(&sizes);

        assert_eq!(buckets.len(), 6);
        assert_eq!(buckets[0].count, 1); // < 1MB
        assert_eq!(buckets[1].count, 1); // 1-10MB
        assert_eq!(buckets[2].count, 1); // 10-100MB
        assert_eq!(buckets[3].count, 1); // 100MB-1GB
        assert_eq!(buckets[4].count, 1); // 1-10GB
        assert_eq!(buckets[5].count, 0); // > 10GB
    }

    #[test]
    fn test_median_calculation_odd() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![
            make_entry(
                "small.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                100,
                100,
                now - Duration::days(1),
            ),
            make_entry(
                "medium.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                500,
                500,
                now - Duration::days(2),
            ),
            make_entry(
                "large.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                1000,
                1000,
                now - Duration::days(3),
            ),
        ];

        let summary = manager.analyze(&entries);

        // Median of [100, 500, 1000] should be 500
        assert_eq!(summary.median_download_size, 500);
    }

    #[test]
    fn test_median_calculation_even() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![
            make_entry(
                "a.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                100,
                100,
                now - Duration::days(1),
            ),
            make_entry(
                "b.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                200,
                200,
                now - Duration::days(2),
            ),
            make_entry(
                "c.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                400,
                400,
                now - Duration::days(3),
            ),
            make_entry(
                "d.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                800,
                800,
                now - Duration::days(4),
            ),
        ];

        let summary = manager.analyze(&entries);

        // Median of [100, 200, 400, 800] should be (200 + 400) / 2 = 300
        assert_eq!(summary.median_download_size, 300);
    }

    #[test]
    fn test_tag_statistics() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![
            make_entry_with_tags(
                "file1.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                1000,
                1000,
                now - Duration::days(1),
                vec!["video".to_string(), "hd".to_string()],
            ),
            make_entry_with_tags(
                "file2.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                2000,
                2000,
                now - Duration::days(2),
                vec!["video".to_string()],
            ),
            make_entry_with_tags(
                "file3.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                500,
                500,
                now - Duration::days(3),
                vec!["audio".to_string()],
            ),
        ];

        let summary = manager.analyze(&entries);

        assert!(!summary.tag_stats.is_empty());
        // "video" should appear twice, others once
        let video_tag = summary.tag_stats.iter().find(|t| t.tag == "video");
        assert!(video_tag.is_some());
        assert_eq!(video_tag.unwrap().count, 2);
    }

    #[test]
    fn test_period_filtering() {
        let mut config = HistoryAnalyticsConfig::default();
        config.default_period_days = 7; // Only last 7 days

        let manager = HistoryAnalyticsManager::with_config(config);
        let now = Utc::now();

        let entries = vec![
            make_entry(
                "recent.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                1000,
                1000,
                now - Duration::days(3),
            ),
            make_entry(
                "old.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                2000,
                2000,
                now - Duration::days(10), // Outside 7-day window
            ),
        ];

        let summary = manager.analyze(&entries);

        // Only the recent entry should be counted
        assert_eq!(summary.total_entries, 1);
        assert_eq!(summary.outcome_stats.completed, 1);
    }

    #[test]
    fn test_max_entries_limit() {
        let mut config = HistoryAnalyticsConfig::default();
        config.max_entries = 5;

        let manager = HistoryAnalyticsManager::with_config(config);
        let now = Utc::now();

        let entries: Vec<HistoryEntry> = (0..10)
            .map(|i| {
                make_entry(
                    &format!("file{}.txt", i),
                    HistoryProtocol::Xunlei,
                    HistoryOutcome::Completed,
                    1000,
                    1000,
                    now - Duration::days(i as i64),
                )
            })
            .collect();

        let summary = manager.analyze(&entries);

        // Should only analyze first 5 entries
        assert_eq!(summary.total_entries, 5);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024u64.pow(4)), "1.00 TB");
    }

    #[test]
    fn test_format_summary() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![make_entry(
            "file1.txt",
            HistoryProtocol::Xunlei,
            HistoryOutcome::Completed,
            1000,
            1000,
            now - Duration::days(1),
        )];

        let summary = manager.analyze(&entries);
        let formatted = manager.format_summary(&summary);

        assert!(formatted.contains("Download History Analytics"));
        assert!(formatted.contains("Completed: 1"));
        assert!(formatted.contains("Xunlei"));
    }

    #[test]
    fn test_config_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let config = HistoryAnalyticsConfig {
            enabled: false,
            default_period_days: 7,
            max_entries: 500,
            include_tag_stats: false,
            include_domain_stats: false,
        };

        // Save
        save_analytics_config(&config, data_dir).unwrap();

        // Load
        let loaded = load_analytics_config(data_dir).unwrap().unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.default_period_days, config.default_period_days);
        assert_eq!(loaded.max_entries, config.max_entries);
        assert_eq!(loaded.include_tag_stats, config.include_tag_stats);
        assert_eq!(loaded.include_domain_stats, config.include_domain_stats);
    }

    #[test]
    fn test_load_nonexistent_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let result = load_analytics_config(data_dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_outcome_bytes_tracking() {
        let manager = HistoryAnalyticsManager::new();
        let now = Utc::now();

        let entries = vec![
            make_entry(
                "success.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Completed,
                1000,
                1000,
                now - Duration::days(1),
            ),
            make_entry(
                "failed.txt",
                HistoryProtocol::Xunlei,
                HistoryOutcome::Failed,
                2000,
                500, // Partial download before failure
                now - Duration::days(2),
            ),
        ];

        let summary = manager.analyze(&entries);

        assert_eq!(summary.outcome_stats.completed_bytes, 1000);
        assert_eq!(summary.outcome_stats.attempted_bytes, 1500);
    }
}
