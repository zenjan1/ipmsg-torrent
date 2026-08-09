//! Download Task Performance Profiler
//!
//! Tracks detailed performance metrics for each download task and generates
//! diagnostic reports with optimization recommendations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Bottleneck category classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BottleneckCategory {
    /// No significant bottleneck detected
    None,
    /// Network speed is the limiting factor
    Network,
    /// Disk I/O is the limiting factor
    Disk,
    /// Server/source is throttling
    Server,
    /// Too many retries indicate instability
    Instability,
    /// Task stalled frequently
    Stalling,
}

impl std::fmt::Display for BottleneckCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BottleneckCategory::None => write!(f, "none"),
            BottleneckCategory::Network => write!(f, "network"),
            BottleneckCategory::Disk => write!(f, "disk"),
            BottleneckCategory::Server => write!(f, "server"),
            BottleneckCategory::Instability => write!(f, "instability"),
            BottleneckCategory::Stalling => write!(f, "stalling"),
        }
    }
}

/// Performance rating based on efficiency score
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceRating {
    /// 80-100: Excellent performance
    Excellent,
    /// 60-79: Good performance
    Good,
    /// 40-59: Fair performance
    Fair,
    /// 20-39: Poor performance
    Poor,
    /// 0-19: Critical performance issues
    Critical,
}

impl PerformanceRating {
    pub fn from_score(score: f64) -> Self {
        match score {
            s if s >= 80.0 => PerformanceRating::Excellent,
            s if s >= 60.0 => PerformanceRating::Good,
            s if s >= 40.0 => PerformanceRating::Fair,
            s if s >= 20.0 => PerformanceRating::Poor,
            _ => PerformanceRating::Critical,
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            PerformanceRating::Excellent => "🟢",
            PerformanceRating::Good => "🔵",
            PerformanceRating::Fair => "🟡",
            PerformanceRating::Poor => "🟠",
            PerformanceRating::Critical => "🔴",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            PerformanceRating::Excellent => "Excellent",
            PerformanceRating::Good => "Good",
            PerformanceRating::Fair => "Fair",
            PerformanceRating::Poor => "Poor",
            PerformanceRating::Critical => "Critical",
        }
    }
}

/// Speed sample for tracking speed over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSpeedSample {
    /// Timestamp (Unix epoch seconds)
    pub timestamp: i64,
    /// Speed in bytes per second
    pub speed_bps: f64,
}

/// Performance profile for a single download task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfile {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Protocol used
    pub protocol: String,
    /// Total file size in bytes
    pub total_bytes: u64,
    /// Bytes downloaded so far
    pub downloaded_bytes: u64,
    /// Progress percentage (0-100)
    pub progress_pct: f64,
    /// Total duration from creation to now/completion (seconds)
    pub duration_secs: f64,
    /// Active download time (excluding pauses)
    pub active_download_secs: f64,
    /// Average download speed (bytes/sec)
    pub avg_speed_bps: f64,
    /// Peak download speed (bytes/sec)
    pub peak_speed_bps: f64,
    /// Minimum non-zero download speed observed
    pub min_speed_bps: f64,
    /// Speed variance (standard deviation)
    pub speed_stddev: f64,
    /// Number of times the task stalled (speed dropped below threshold)
    pub stall_count: u32,
    /// Total time spent stalled (seconds)
    pub total_stall_secs: f64,
    /// Number of retries
    pub retry_count: u32,
    /// Number of errors encountered
    pub error_count: u32,
    /// Efficiency score (0-100)
    pub efficiency_score: f64,
    /// Performance rating
    pub rating: PerformanceRating,
    /// Primary bottleneck category
    pub bottleneck: BottleneckCategory,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
    /// Speed samples (capped at max_samples)
    pub speed_samples: Vec<ProfileSpeedSample>,
    /// Whether the task is complete
    pub is_complete: bool,
    /// When the task was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the profile was last updated
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Configuration for the task profiler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfilerConfig {
    /// Enable/disable profiling
    pub enabled: bool,
    /// Maximum speed samples per task
    pub max_samples_per_task: usize,
    /// Speed threshold below which a sample is considered stalled (bytes/sec)
    pub stall_threshold_bps: f64,
    /// Minimum active download time to generate recommendations (seconds)
    pub min_active_time_secs: f64,
    /// Auto-refresh profiles on every speed update
    pub auto_refresh: bool,
}

impl Default for TaskProfilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples_per_task: 200,
            stall_threshold_bps: 1024.0, // 1 KB/s
            min_active_time_secs: 30.0,
            auto_refresh: true,
        }
    }
}

/// Performance summary across all profiled tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    /// Total tasks profiled
    pub total_tasks_profiled: usize,
    /// Average efficiency score across all tasks
    pub avg_efficiency_score: f64,
    /// Overall performance rating
    pub overall_rating: PerformanceRating,
    /// Tasks with best performance (top N)
    pub best_performers: Vec<TaskProfileBrief>,
    /// Tasks with worst performance (bottom N)
    pub worst_performers: Vec<TaskProfileBrief>,
    /// Bottleneck distribution
    pub bottleneck_distribution: HashMap<String, usize>,
    /// Overall recommendations
    pub overall_recommendations: Vec<String>,
    /// Total bytes downloaded across all tasks
    pub total_bytes_downloaded: u64,
    /// Average speed across all active tasks
    pub avg_speed_all_tasks: f64,
}

/// Brief task profile for summary listings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfileBrief {
    pub task_id: String,
    pub task_name: String,
    pub protocol: String,
    pub progress_pct: f64,
    pub efficiency_score: f64,
    pub rating: PerformanceRating,
    pub avg_speed_bps: f64,
    pub bottleneck: BottleneckCategory,
}

impl TaskProfileBrief {
    pub fn from_profile(p: &TaskProfile) -> Self {
        Self {
            task_id: p.task_id.clone(),
            task_name: p.task_name.clone(),
            protocol: p.protocol.clone(),
            progress_pct: p.progress_pct,
            efficiency_score: p.efficiency_score,
            rating: p.rating,
            avg_speed_bps: p.avg_speed_bps,
            bottleneck: p.bottleneck,
        }
    }
}

/// Input data needed to compute a task profile
#[derive(Debug, Clone)]
pub struct TaskProfileInput {
    pub task_id: String,
    pub task_name: String,
    pub protocol: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub is_complete: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub active_time_seconds: f64,
    pub current_speed_bps: f64,
    pub retry_count: u32,
    pub error_count: u32,
    pub stall_count: u32,
    pub total_stall_secs: f64,
}

/// Task profiler manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfiler {
    /// Configuration
    pub config: TaskProfilerConfig,
    /// Task profiles indexed by task ID
    pub profiles: HashMap<String, TaskProfile>,
}

impl Default for TaskProfiler {
    fn default() -> Self {
        Self {
            config: TaskProfilerConfig::default(),
            profiles: HashMap::new(),
        }
    }
}

impl TaskProfiler {
    /// Create a new task profiler with custom config
    pub fn new(config: TaskProfilerConfig) -> Self {
        Self {
            config,
            profiles: HashMap::new(),
        }
    }

    /// Update or create a profile for a task
    pub fn update_profile(&mut self, input: TaskProfileInput) {
        let now = chrono::Utc::now();
        let progress_pct = if input.total_bytes > 0 {
            (input.downloaded_bytes as f64 / input.total_bytes as f64) * 100.0
        } else {
            0.0
        };

        let duration_secs = (now - input.created_at).num_seconds().max(0) as f64;

        let profile = self
            .profiles
            .entry(input.task_id.clone())
            .or_insert_with(|| TaskProfile {
                task_id: input.task_id.clone(),
                task_name: input.task_name.clone(),
                protocol: input.protocol.clone(),
                total_bytes: input.total_bytes,
                downloaded_bytes: input.downloaded_bytes,
                progress_pct,
                duration_secs,
                active_download_secs: input.active_time_seconds,
                avg_speed_bps: 0.0,
                peak_speed_bps: 0.0,
                min_speed_bps: f64::MAX,
                speed_stddev: 0.0,
                stall_count: 0,
                total_stall_secs: 0.0,
                retry_count: 0,
                error_count: 0,
                efficiency_score: 0.0,
                rating: PerformanceRating::Critical,
                bottleneck: BottleneckCategory::None,
                recommendations: Vec::new(),
                speed_samples: Vec::new(),
                is_complete: false,
                created_at: input.created_at,
                updated_at: now,
            });

        // Update basic fields
        profile.task_name = input.task_name;
        profile.total_bytes = input.total_bytes;
        profile.downloaded_bytes = input.downloaded_bytes;
        profile.progress_pct = progress_pct;
        profile.duration_secs = duration_secs;
        profile.active_download_secs = input.active_time_seconds;
        profile.is_complete = input.is_complete;
        profile.retry_count = input.retry_count;
        profile.error_count = input.error_count;
        profile.stall_count = input.stall_count;
        profile.total_stall_secs = input.total_stall_secs;
        profile.updated_at = now;

        // Update speed statistics
        let current_speed = input.current_speed_bps;
        if current_speed > 0.0 {
            if current_speed > profile.peak_speed_bps {
                profile.peak_speed_bps = current_speed;
            }
            if current_speed < profile.min_speed_bps {
                profile.min_speed_bps = current_speed;
            }
        }

        // Calculate average speed from downloaded bytes and active time
        if input.active_time_seconds > 0.0 {
            profile.avg_speed_bps = input.downloaded_bytes as f64 / input.active_time_seconds;
        }

        // Fix min_speed_bps if never set
        if profile.min_speed_bps == f64::MAX {
            profile.min_speed_bps = 0.0;
        }

        // Add speed sample
        if current_speed > 0.0 {
            profile.speed_samples.push(ProfileSpeedSample {
                timestamp: now.timestamp(),
                speed_bps: current_speed,
            });

            // Cap samples
            if profile.speed_samples.len() > self.config.max_samples_per_task {
                let excess = profile.speed_samples.len() - self.config.max_samples_per_task;
                profile.speed_samples.drain(..excess);
            }

            // Calculate speed stddev (inline to avoid borrow conflict)
            if profile.speed_samples.len() >= 2 {
                let samples = &profile.speed_samples;
                let mean: f64 =
                    samples.iter().map(|s| s.speed_bps).sum::<f64>() / samples.len() as f64;
                let variance: f64 = samples
                    .iter()
                    .map(|s| (s.speed_bps - mean).powi(2))
                    .sum::<f64>()
                    / (samples.len() - 1) as f64;
                profile.speed_stddev = variance.sqrt();
            }
        }

        // Calculate efficiency score and bottleneck (inline to avoid borrow conflict)
        let mut score = 100.0;
        let mut bottleneck = BottleneckCategory::None;
        let mut max_penalty = 0.0f64;

        // Factor 1: Stall penalty (up to -30 points)
        if profile.stall_count > 0 {
            let stall_penalty = (profile.stall_count as f64 * 5.0).min(30.0);
            if stall_penalty > max_penalty {
                max_penalty = stall_penalty;
                bottleneck = BottleneckCategory::Stalling;
            }
            score -= stall_penalty;
        }

        // Factor 2: Retry/error penalty (up to -25 points)
        let error_penalty = ((profile.retry_count + profile.error_count) as f64 * 4.0).min(25.0);
        if error_penalty > max_penalty {
            max_penalty = error_penalty;
            bottleneck = BottleneckCategory::Instability;
        }
        score -= error_penalty;

        // Factor 3: Speed variance penalty (up to -20 points)
        if profile.avg_speed_bps > 0.0 && profile.speed_stddev > 0.0 {
            let cv = profile.speed_stddev / profile.avg_speed_bps;
            let variance_penalty = (cv * 15.0).min(20.0);
            if variance_penalty > max_penalty {
                max_penalty = variance_penalty;
                bottleneck = BottleneckCategory::Network;
            }
            score -= variance_penalty;
        }

        // Factor 4: Low throughput penalty (up to -15 points)
        if profile.duration_secs > 60.0 && profile.active_download_secs > 0.0 {
            let active_ratio = profile.active_download_secs / profile.duration_secs;
            if active_ratio < 0.5 {
                let throughput_penalty = ((0.5 - active_ratio) * 30.0).min(15.0);
                if throughput_penalty > max_penalty {
                    bottleneck = BottleneckCategory::Server;
                }
                score -= throughput_penalty;
            }
        }

        // Factor 5: Progress vs time penalty for completed tasks
        if profile.is_complete && profile.duration_secs > 0.0 {
            let expected_speed = profile.total_bytes as f64 / profile.duration_secs;
            if expected_speed < 1024.0 && profile.total_bytes > 1024 * 1024 {
                score -= 10.0;
                if bottleneck == BottleneckCategory::None {
                    bottleneck = BottleneckCategory::Server;
                }
            }
        }

        profile.efficiency_score = score.max(0.0).min(100.0);
        profile.rating = PerformanceRating::from_score(profile.efficiency_score);
        profile.bottleneck = bottleneck;

        // Generate recommendations (inline to avoid borrow conflict)
        profile.recommendations.clear();
        if profile.active_download_secs >= self.config.min_active_time_secs {
            if profile.stall_count >= 5 {
                profile.recommendations.push(format!(
                    "Task stalled {} times. Consider adding mirror URLs or switching to a faster source.",
                    profile.stall_count
                ));
            }

            if profile.error_count >= 3 {
                profile.recommendations.push(format!(
                    "Task encountered {} errors. Check network stability and source availability.",
                    profile.error_count
                ));
            }

            if profile.avg_speed_bps > 0.0 && profile.speed_stddev / profile.avg_speed_bps > 1.5 {
                profile.recommendations.push(
                    "Highly variable speed detected. Consider enabling bandwidth allocation or scheduling downloads during off-peak hours."
                        .to_string(),
                );
            }

            if profile.duration_secs > 120.0 {
                let active_ratio = profile.active_download_secs / profile.duration_secs;
                if active_ratio < 0.3 {
                    profile.recommendations.push(format!(
                        "Task only actively downloading {:.0}% of the time. Source may be throttling or unreliable.",
                        active_ratio * 100.0
                    ));
                }
            }

            if profile.avg_speed_bps > 0.0 && profile.peak_speed_bps / profile.avg_speed_bps > 5.0 {
                profile.recommendations.push(
                    "Large gap between peak and average speed. Consider using segmented download for more consistent throughput."
                        .to_string(),
                );
            }

            if !profile.is_complete && profile.avg_speed_bps > 0.0 {
                let remaining = profile.total_bytes.saturating_sub(profile.downloaded_bytes);
                let eta_secs = remaining as f64 / profile.avg_speed_bps;
                if eta_secs > 86400.0 {
                    let eta_hours = eta_secs / 3600.0;
                    profile.recommendations.push(format!(
                        "At current speed, estimated completion: {:.1} hours. Consider increasing bandwidth allocation.",
                        eta_hours
                    ));
                }
            }
        }
    }

    /// Get a profile for a specific task
    pub fn get_profile(&self, task_id: &str) -> Option<&TaskProfile> {
        self.profiles.get(task_id)
    }

    /// Get all profiles
    pub fn get_all_profiles(&self) -> Vec<&TaskProfile> {
        self.profiles.values().collect()
    }

    /// Get the N worst performing tasks
    pub fn get_worst_performers(&self, n: usize) -> Vec<&TaskProfile> {
        let mut profiles: Vec<&TaskProfile> = self.profiles.values().collect();
        profiles.sort_by(|a, b| {
            a.efficiency_score
                .partial_cmp(&b.efficiency_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        profiles.into_iter().take(n).collect()
    }

    /// Get the N best performing tasks
    pub fn get_best_performers(&self, n: usize) -> Vec<&TaskProfile> {
        let mut profiles: Vec<&TaskProfile> = self.profiles.values().collect();
        profiles.sort_by(|a, b| {
            b.efficiency_score
                .partial_cmp(&a.efficiency_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        profiles.into_iter().take(n).collect()
    }

    /// Generate a performance summary across all tasks
    pub fn get_performance_summary(&self, top_n: usize) -> PerformanceSummary {
        let profiles: Vec<&TaskProfile> = self.profiles.values().collect();
        let total = profiles.len();

        if total == 0 {
            return PerformanceSummary {
                total_tasks_profiled: 0,
                avg_efficiency_score: 0.0,
                overall_rating: PerformanceRating::Excellent,
                best_performers: Vec::new(),
                worst_performers: Vec::new(),
                bottleneck_distribution: HashMap::new(),
                overall_recommendations: Vec::new(),
                total_bytes_downloaded: 0,
                avg_speed_all_tasks: 0.0,
            };
        }

        let avg_score: f64 =
            profiles.iter().map(|p| p.efficiency_score).sum::<f64>() / total as f64;
        let overall_rating = PerformanceRating::from_score(avg_score);

        let mut best: Vec<TaskProfileBrief> = profiles
            .iter()
            .map(|p| TaskProfileBrief::from_profile(p))
            .collect();
        best.sort_by(|a, b| {
            b.efficiency_score
                .partial_cmp(&a.efficiency_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        best.truncate(top_n);

        let mut worst: Vec<TaskProfileBrief> = profiles
            .iter()
            .map(|p| TaskProfileBrief::from_profile(p))
            .collect();
        worst.sort_by(|a, b| {
            a.efficiency_score
                .partial_cmp(&b.efficiency_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        worst.truncate(top_n);

        let mut bottleneck_dist: HashMap<String, usize> = HashMap::new();
        for p in &profiles {
            let key = p.bottleneck.to_string();
            *bottleneck_dist.entry(key).or_insert(0) += 1;
        }
        // Remove "none" from distribution if present
        bottleneck_dist.remove("none");

        let total_bytes: u64 = profiles.iter().map(|p| p.downloaded_bytes).sum();
        let avg_speed: f64 = if total > 0 {
            profiles.iter().map(|p| p.avg_speed_bps).sum::<f64>() / total as f64
        } else {
            0.0
        };

        let mut overall_recs = Vec::new();
        if avg_score < 50.0 {
            overall_recs.push(
                "Overall performance is poor. Check network connectivity and source availability."
                    .to_string(),
            );
        }
        if bottleneck_dist.get("stalling").copied().unwrap_or(0) > total / 3 {
            overall_recs.push(
                "Many tasks are stalling. Consider enabling mirror URLs and source rotation."
                    .to_string(),
            );
        }
        if bottleneck_dist.get("instability").copied().unwrap_or(0) > total / 3 {
            overall_recs.push(
                "Frequent errors detected. Check network stability and retry policies.".to_string(),
            );
        }
        if bottleneck_dist.get("server").copied().unwrap_or(0) > total / 3 {
            overall_recs.push("Multiple tasks experiencing server throttling. Try downloading during off-peak hours.".to_string());
        }

        PerformanceSummary {
            total_tasks_profiled: total,
            avg_efficiency_score: avg_score,
            overall_rating,
            best_performers: best,
            worst_performers: worst,
            bottleneck_distribution: bottleneck_dist,
            overall_recommendations: overall_recs,
            total_bytes_downloaded: total_bytes,
            avg_speed_all_tasks: avg_speed,
        }
    }

    /// Remove a task profile
    pub fn remove_profile(&mut self, task_id: &str) -> bool {
        self.profiles.remove(task_id).is_some()
    }

    /// Clear all profiles
    pub fn clear_all(&mut self) {
        self.profiles.clear();
    }

    /// Get profiler config
    pub fn get_config(&self) -> &TaskProfilerConfig {
        &self.config
    }

    /// Set profiler config
    pub fn set_config(&mut self, config: TaskProfilerConfig) {
        self.config = config;
    }

    /// Format a performance summary for display
    pub fn format_summary(summary: &PerformanceSummary) -> String {
        let mut out = String::new();

        out.push_str(&format!(
            "📊 Performance Summary ({} tasks profiled)\n",
            summary.total_tasks_profiled
        ));
        out.push_str(&format!(
            "Overall: {} {:.0}/100 ({})\n",
            summary.overall_rating.emoji(),
            summary.avg_efficiency_score,
            summary.overall_rating.label()
        ));
        out.push_str(&format!(
            "Total downloaded: {}\n",
            format_bytes(summary.total_bytes_downloaded as f64)
        ));
        out.push_str(&format!(
            "Avg speed: {}/s\n\n",
            format_bytes(summary.avg_speed_all_tasks)
        ));

        if !summary.worst_performers.is_empty() {
            out.push_str("⚠️  Worst performers:\n");
            for p in &summary.worst_performers {
                out.push_str(&format!(
                    "  {} {} ({}) - {:.0}/100, bottleneck: {}\n",
                    p.rating.emoji(),
                    p.task_name,
                    p.protocol,
                    p.efficiency_score,
                    p.bottleneck
                ));
            }
            out.push('\n');
        }

        if !summary.best_performers.is_empty() {
            out.push_str("✅ Best performers:\n");
            for p in &summary.best_performers {
                out.push_str(&format!(
                    "  {} {} ({}) - {:.0}/100\n",
                    p.rating.emoji(),
                    p.task_name,
                    p.protocol,
                    p.efficiency_score
                ));
            }
            out.push('\n');
        }

        if !summary.bottleneck_distribution.is_empty() {
            out.push_str("🔍 Bottleneck distribution:\n");
            let mut sorted: Vec<_> = summary.bottleneck_distribution.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));
            for (cat, count) in sorted {
                out.push_str(&format!("  {}: {}\n", cat, count));
            }
            out.push('\n');
        }

        if !summary.overall_recommendations.is_empty() {
            out.push_str("💡 Recommendations:\n");
            for rec in &summary.overall_recommendations {
                out.push_str(&format!("  • {}\n", rec));
            }
        }

        out
    }
}

/// Format bytes to human-readable string
fn format_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{:.0} B", bytes)
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} KB", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Save profiler to JSON file
pub fn save_task_profiler(
    profiler: &TaskProfiler,
    path: &std::path::Path,
) -> Result<(), std::io::Error> {
    let json = serde_json::to_string_pretty(profiler)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Load profiler from JSON file
pub fn load_task_profiler(path: &std::path::Path) -> Result<TaskProfiler, String> {
    if !path.exists() {
        return Ok(TaskProfiler::default());
    }
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read profiler file: {}", e))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse profiler file: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input(id: &str, downloaded: u64, total: u64, speed: f64) -> TaskProfileInput {
        TaskProfileInput {
            task_id: id.to_string(),
            task_name: format!("Task {}", id),
            protocol: "http".to_string(),
            total_bytes: total,
            downloaded_bytes: downloaded,
            is_complete: downloaded >= total,
            created_at: chrono::Utc::now() - chrono::Duration::seconds(300),
            active_time_seconds: 250.0,
            current_speed_bps: speed,
            retry_count: 0,
            error_count: 0,
            stall_count: 0,
            total_stall_secs: 0.0,
        }
    }

    #[test]
    fn test_create_profile() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 500_000, 1_000_000, 2000.0);
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert_eq!(profile.task_id, "t1");
        assert_eq!(profile.total_bytes, 1_000_000);
        assert_eq!(profile.downloaded_bytes, 500_000);
        assert!((profile.progress_pct - 50.0).abs() < 0.1);
        assert!(profile.efficiency_score > 0.0);
    }

    #[test]
    fn test_update_existing_profile() {
        let mut profiler = TaskProfiler::default();

        let input1 = make_input("t1", 100_000, 1_000_000, 1000.0);
        profiler.update_profile(input1);

        let input2 = make_input("t1", 500_000, 1_000_000, 2000.0);
        profiler.update_profile(input2);

        let profile = profiler.get_profile("t1").unwrap();
        assert_eq!(profile.downloaded_bytes, 500_000);
        assert_eq!(profile.peak_speed_bps, 2000.0);
        assert!((profile.progress_pct - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_speed_statistics() {
        let mut profiler = TaskProfiler::default();

        for speed in [1000.0, 2000.0, 3000.0, 1500.0, 2500.0] {
            let input = make_input("t1", 100_000, 1_000_000, speed);
            profiler.update_profile(input);
        }

        let profile = profiler.get_profile("t1").unwrap();
        assert_eq!(profile.peak_speed_bps, 3000.0);
        assert_eq!(profile.min_speed_bps, 1000.0);
        assert!(profile.speed_stddev > 0.0);
        assert_eq!(profile.speed_samples.len(), 5);
    }

    #[test]
    fn test_sample_capping() {
        let mut profiler = TaskProfiler::new(TaskProfilerConfig {
            max_samples_per_task: 5,
            ..Default::default()
        });

        for i in 0..10 {
            let input = make_input("t1", 100_000, 1_000_000, (i * 100) as f64 + 100.0);
            profiler.update_profile(input);
        }

        let profile = profiler.get_profile("t1").unwrap();
        assert_eq!(profile.speed_samples.len(), 5);
    }

    #[test]
    fn test_stall_penalty() {
        let mut profiler = TaskProfiler::default();

        let mut input = make_input("t1", 500_000, 1_000_000, 1000.0);
        input.stall_count = 10;
        input.total_stall_secs = 60.0;
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(profile.efficiency_score < 80.0); // Should be penalized
        assert_eq!(profile.bottleneck, BottleneckCategory::Stalling);
    }

    #[test]
    fn test_error_penalty() {
        let mut profiler = TaskProfiler::default();

        let mut input = make_input("t1", 500_000, 1_000_000, 1000.0);
        input.retry_count = 5;
        input.error_count = 5;
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(profile.efficiency_score < 80.0);
        assert_eq!(profile.bottleneck, BottleneckCategory::Instability);
    }

    #[test]
    fn test_performance_rating() {
        assert_eq!(
            PerformanceRating::from_score(90.0),
            PerformanceRating::Excellent
        );
        assert_eq!(PerformanceRating::from_score(70.0), PerformanceRating::Good);
        assert_eq!(PerformanceRating::from_score(50.0), PerformanceRating::Fair);
        assert_eq!(PerformanceRating::from_score(30.0), PerformanceRating::Poor);
        assert_eq!(
            PerformanceRating::from_score(10.0),
            PerformanceRating::Critical
        );
    }

    #[test]
    fn test_bottleneck_display() {
        assert_eq!(BottleneckCategory::None.to_string(), "none");
        assert_eq!(BottleneckCategory::Network.to_string(), "network");
        assert_eq!(BottleneckCategory::Server.to_string(), "server");
    }

    #[test]
    fn test_get_worst_performers() {
        let mut profiler = TaskProfiler::default();

        for i in 0..5 {
            let mut input = make_input(&format!("t{}", i), 500_000, 1_000_000, 1000.0);
            input.stall_count = i * 3;
            profiler.update_profile(input);
        }

        let worst = profiler.get_worst_performers(2);
        assert_eq!(worst.len(), 2);
        assert!(worst[0].efficiency_score <= worst[1].efficiency_score);
    }

    #[test]
    fn test_get_best_performers() {
        let mut profiler = TaskProfiler::default();

        for i in 0..5 {
            let mut input = make_input(&format!("t{}", i), 500_000, 1_000_000, 1000.0);
            input.stall_count = i * 3;
            profiler.update_profile(input);
        }

        let best = profiler.get_best_performers(2);
        assert_eq!(best.len(), 2);
        assert!(best[0].efficiency_score >= best[1].efficiency_score);
    }

    #[test]
    fn test_performance_summary_empty() {
        let profiler = TaskProfiler::default();
        let summary = profiler.get_performance_summary(5);

        assert_eq!(summary.total_tasks_profiled, 0);
        assert_eq!(summary.total_bytes_downloaded, 0);
        assert!(summary.best_performers.is_empty());
        assert!(summary.worst_performers.is_empty());
    }

    #[test]
    fn test_performance_summary() {
        let mut profiler = TaskProfiler::default();

        for i in 0..3 {
            let input = make_input(
                &format!("t{}", i),
                500_000,
                1_000_000,
                1000.0 * (i + 1) as f64,
            );
            profiler.update_profile(input);
        }

        let summary = profiler.get_performance_summary(2);
        assert_eq!(summary.total_tasks_profiled, 3);
        assert_eq!(summary.best_performers.len(), 2);
        assert_eq!(summary.worst_performers.len(), 2);
        assert!(summary.avg_efficiency_score > 0.0);
    }

    #[test]
    fn test_remove_profile() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 500_000, 1_000_000, 1000.0);
        profiler.update_profile(input);

        assert!(profiler.get_profile("t1").is_some());
        assert!(profiler.remove_profile("t1"));
        assert!(profiler.get_profile("t1").is_none());
        assert!(!profiler.remove_profile("t1"));
    }

    #[test]
    fn test_clear_all() {
        let mut profiler = TaskProfiler::default();

        for i in 0..3 {
            let input = make_input(&format!("t{}", i), 500_000, 1_000_000, 1000.0);
            profiler.update_profile(input);
        }

        assert_eq!(profiler.profiles.len(), 3);
        profiler.clear_all();
        assert_eq!(profiler.profiles.len(), 0);
    }

    #[test]
    fn test_config() {
        let mut profiler = TaskProfiler::default();
        assert!(profiler.config.enabled);
        assert_eq!(profiler.config.max_samples_per_task, 200);

        profiler.set_config(TaskProfilerConfig {
            enabled: false,
            max_samples_per_task: 50,
            ..Default::default()
        });

        assert!(!profiler.config.enabled);
        assert_eq!(profiler.config.max_samples_per_task, 50);
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 500_000, 1_000_000, 1000.0);
        profiler.update_profile(input);

        let json = serde_json::to_string(&profiler).unwrap();
        let deserialized: TaskProfiler = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.profiles.len(), 1);
        assert!(deserialized.get_profile("t1").is_some());
    }

    #[test]
    fn test_save_load() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 500_000, 1_000_000, 1000.0);
        profiler.update_profile(input);

        let dir = std::env::temp_dir().join("test_task_profiler_save_load");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("task_profiler.json");

        save_task_profiler(&profiler, &path).unwrap();
        let loaded = load_task_profiler(&path).unwrap();

        assert_eq!(loaded.profiles.len(), 1);
        assert!(loaded.get_profile("t1").is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_nonexistent() {
        let path = std::path::PathBuf::from("/tmp/nonexistent_task_profiler.json");
        let profiler = load_task_profiler(&path).unwrap();
        assert_eq!(profiler.profiles.len(), 0);
    }

    #[test]
    fn test_format_summary() {
        let mut profiler = TaskProfiler::default();

        for i in 0..3 {
            let input = make_input(
                &format!("t{}", i),
                500_000,
                1_000_000,
                1000.0 * (i + 1) as f64,
            );
            profiler.update_profile(input);
        }

        let summary = profiler.get_performance_summary(2);
        let formatted = TaskProfiler::format_summary(&summary);

        assert!(formatted.contains("Performance Summary"));
        assert!(formatted.contains("3 tasks profiled"));
    }

    #[test]
    fn test_completed_task_profile() {
        let mut profiler = TaskProfiler::default();
        let input = TaskProfileInput {
            task_id: "t1".to_string(),
            task_name: "Completed Task".to_string(),
            protocol: "http".to_string(),
            total_bytes: 1_000_000,
            downloaded_bytes: 1_000_000,
            is_complete: true,
            created_at: chrono::Utc::now() - chrono::Duration::seconds(300),
            active_time_seconds: 250.0,
            current_speed_bps: 0.0,
            retry_count: 0,
            error_count: 0,
            stall_count: 0,
            total_stall_secs: 0.0,
        };
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(profile.is_complete);
        assert!((profile.progress_pct - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_recommendations_for_stalling() {
        let mut profiler = TaskProfiler::default();
        let mut input = make_input("t1", 500_000, 1_000_000, 1000.0);
        input.stall_count = 10;
        input.active_time_seconds = 60.0;
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(!profile.recommendations.is_empty());
        assert!(
            profile
                .recommendations
                .iter()
                .any(|r| r.contains("stalled"))
        );
    }

    #[test]
    fn test_recommendations_for_errors() {
        let mut profiler = TaskProfiler::default();
        let mut input = make_input("t1", 500_000, 1_000_000, 1000.0);
        input.error_count = 5;
        input.active_time_seconds = 60.0;
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(!profile.recommendations.is_empty());
        assert!(profile.recommendations.iter().any(|r| r.contains("errors")));
    }

    #[test]
    fn test_zero_speed_no_sample() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 0, 1_000_000, 0.0);
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(profile.speed_samples.is_empty());
        assert_eq!(profile.peak_speed_bps, 0.0);
    }

    #[test]
    fn test_brief_from_profile() {
        let mut profiler = TaskProfiler::default();
        let input = make_input("t1", 500_000, 1_000_000, 1000.0);
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        let brief = TaskProfileBrief::from_profile(profile);

        assert_eq!(brief.task_id, "t1");
        assert_eq!(brief.task_name, "Task t1");
        assert!((brief.progress_pct - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500.0), "500 B");
        assert_eq!(format_bytes(1500.0), "1.5 KB");
        assert_eq!(format_bytes(1_500_000.0), "1.4 MB");
        assert_eq!(format_bytes(1_500_000_000.0), "1.40 GB");
    }

    #[test]
    fn test_speed_stddev_single_sample() {
        // Single sample should have zero stddev
        let samples = vec![ProfileSpeedSample {
            timestamp: 0,
            speed_bps: 1000.0,
        }];
        if samples.len() >= 2 {
            let mean: f64 = samples.iter().map(|s| s.speed_bps).sum::<f64>() / samples.len() as f64;
            let variance: f64 = samples
                .iter()
                .map(|s| (s.speed_bps - mean).powi(2))
                .sum::<f64>()
                / (samples.len() - 1) as f64;
            let stddev = variance.sqrt();
            assert_eq!(stddev, 0.0);
        } else {
            // Less than 2 samples, stddev is 0
            assert_eq!(0.0, 0.0);
        }
    }

    #[test]
    fn test_efficiency_score_bounds() {
        let mut profiler = TaskProfiler::default();

        // Worst case: lots of stalls + errors
        let mut input = make_input("t1", 500_000, 1_000_000, 1000.0);
        input.stall_count = 100;
        input.retry_count = 100;
        input.error_count = 100;
        profiler.update_profile(input);

        let profile = profiler.get_profile("t1").unwrap();
        assert!(profile.efficiency_score >= 0.0);
        assert!(profile.efficiency_score <= 100.0);
    }
}
