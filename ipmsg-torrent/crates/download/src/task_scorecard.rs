//! Download Task Scorecard System (Phase 139)
//!
//! Aggregates performance data from multiple subsystems into a unified
//! per-task scorecard with composite scoring and actionable recommendations.
//!
//! Data sources integrated:
//! - Task Profiler: efficiency score, bottleneck detection, speed stats
//! - Speed Anomaly Detector: anomaly count and severity
//! - Source Reliability Tracker: domain reliability score and tier
//! - Download metadata: progress, retries, errors, duration
//!
//! Features:
//! - Composite score (0-100) with configurable weights per dimension
//! - Letter grade (A+ through F) mapping
//! - Per-dimension breakdown (speed, reliability, stability, progress, efficiency)
//! - Actionable recommendations aggregated from all sources
//! - Bulk scorecard generation with summary statistics
//! - Persistence to `task_scorecard_config.json`
//!
//! ## Score Dimensions
//!
//! | Dimension     | Default Weight | Description                          |
//! |---------------|---------------|--------------------------------------|
//! | Efficiency    | 0.30          | From profiler efficiency score       |
//! | Speed         | 0.25          | Based on avg speed vs peak ratio     |
//! | Stability     | 0.20          | Based on stalls, retries, errors     |
//! | Reliability   | 0.15          | Source domain reliability score      |
//! | Progress      | 0.10          | Completion percentage                |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Errors from task scorecard operations.
#[derive(Debug, thiserror::Error)]
pub enum ScorecardError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Letter grade for task performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LetterGrade {
    /// 97-100: Exceptional
    APlus,
    /// 93-96: Excellent
    A,
    /// 90-92: Very Good
    AMinus,
    /// 87-89: Good
    BPlus,
    /// 83-86: Above Average
    B,
    /// 80-82: Solid
    BMinus,
    /// 77-79: Decent
    CPlus,
    /// 73-76: Average
    C,
    /// 70-72: Below Average
    CMinus,
    /// 67-69: Poor
    DPlus,
    /// 63-66: Below Average
    D,
    /// 60-62: Barely Passing
    DMinus,
    /// Below 60: Failing
    F,
}

impl LetterGrade {
    /// Convert numeric score (0-100) to letter grade.
    pub fn from_score(score: f64) -> Self {
        match score {
            s if s >= 97.0 => Self::APlus,
            s if s >= 93.0 => Self::A,
            s if s >= 90.0 => Self::AMinus,
            s if s >= 87.0 => Self::BPlus,
            s if s >= 83.0 => Self::B,
            s if s >= 80.0 => Self::BMinus,
            s if s >= 77.0 => Self::CPlus,
            s if s >= 73.0 => Self::C,
            s if s >= 70.0 => Self::CMinus,
            s if s >= 67.0 => Self::DPlus,
            s if s >= 63.0 => Self::D,
            s if s >= 60.0 => Self::DMinus,
            _ => Self::F,
        }
    }

    /// Emoji representation of the grade.
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::APlus => "🏆",
            Self::A => "🌟",
            Self::AMinus => "✨",
            Self::BPlus => "👍",
            Self::B => "✅",
            Self::BMinus => "📊",
            Self::CPlus => "📉",
            Self::C => "⚠️",
            Self::CMinus => "🔻",
            Self::DPlus => "🟠",
            Self::D => "🔴",
            Self::DMinus => "⛔",
            Self::F => "💀",
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::APlus => "A+ (Exceptional)",
            Self::A => "A (Excellent)",
            Self::AMinus => "A- (Very Good)",
            Self::BPlus => "B+ (Good)",
            Self::B => "B (Above Average)",
            Self::BMinus => "B- (Solid)",
            Self::CPlus => "C+ (Decent)",
            Self::C => "C (Average)",
            Self::CMinus => "C- (Below Average)",
            Self::DPlus => "D+ (Poor)",
            Self::D => "D (Below Average)",
            Self::DMinus => "D- (Barely Passing)",
            Self::F => "F (Failing)",
        }
    }
}

impl std::fmt::Display for LetterGrade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Score weights configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreWeights {
    /// Weight for efficiency dimension (from profiler). Default: 0.30
    pub efficiency: f64,
    /// Weight for speed dimension (avg/peak ratio). Default: 0.25
    pub speed: f64,
    /// Weight for stability dimension (stalls/retries/errors). Default: 0.20
    pub stability: f64,
    /// Weight for reliability dimension (source domain score). Default: 0.15
    pub reliability: f64,
    /// Weight for progress dimension (completion %). Default: 0.10
    pub progress: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            efficiency: 0.30,
            speed: 0.25,
            stability: 0.20,
            reliability: 0.15,
            progress: 0.10,
        }
    }
}

impl ScoreWeights {
    /// Validate that weights sum to approximately 1.0.
    pub fn is_valid(&self) -> bool {
        let sum = self.efficiency + self.speed + self.stability + self.reliability + self.progress;
        (sum - 1.0).abs() < 0.01
    }

    /// Normalize weights to sum to 1.0.
    pub fn normalize(&mut self) {
        let sum = self.efficiency + self.speed + self.stability + self.reliability + self.progress;
        if sum > 0.0 {
            self.efficiency /= sum;
            self.speed /= sum;
            self.stability /= sum;
            self.reliability /= sum;
            self.progress /= sum;
        }
    }
}

/// Configuration for the task scorecard system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScorecardConfig {
    /// Enable scorecard generation.
    pub enabled: bool,
    /// Score weights per dimension.
    pub weights: ScoreWeights,
    /// Minimum samples before generating scorecards.
    pub min_samples: usize,
    /// Maximum scorecards to retain.
    pub max_scorecards: usize,
    /// Include recommendations in scorecards.
    pub include_recommendations: bool,
    /// Maximum recommendations per scorecard.
    pub max_recommendations: usize,
}

impl Default for ScorecardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            weights: ScoreWeights::default(),
            min_samples: 2,
            max_scorecards: 500,
            include_recommendations: true,
            max_recommendations: 10,
        }
    }
}

/// Individual dimension score breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionScore {
    /// Dimension name.
    pub name: String,
    /// Raw score (0-100).
    pub score: f64,
    /// Weight applied.
    pub weight: f64,
    /// Weighted contribution to composite.
    pub weighted_score: f64,
    /// Brief assessment.
    pub assessment: String,
}

/// Input data for generating a task scorecard.
#[derive(Debug, Clone, Default)]
pub struct ScorecardInput {
    /// Task ID.
    pub task_id: String,
    /// Task name.
    pub task_name: String,
    /// Protocol (http/torrent/ed2k/p2p).
    pub protocol: String,
    /// Source domain (for reliability lookup).
    pub source_domain: Option<String>,
    /// Total file size in bytes.
    pub total_bytes: u64,
    /// Bytes downloaded.
    pub downloaded_bytes: u64,
    /// Progress percentage (0-100).
    pub progress_pct: f64,
    /// Average download speed (bytes/sec).
    pub avg_speed_bps: f64,
    /// Peak download speed (bytes/sec).
    pub peak_speed_bps: f64,
    /// Efficiency score from profiler (0-100).
    pub efficiency_score: f64,
    /// Stall count.
    pub stall_count: u32,
    /// Retry count.
    pub retry_count: u32,
    /// Error count.
    pub error_count: u32,
    /// Duration in seconds.
    pub duration_secs: f64,
    /// Whether task is complete.
    pub is_complete: bool,
    /// Source reliability score (0.0-1.0), if available.
    pub source_reliability_score: Option<f64>,
    /// Number of speed anomalies detected.
    pub anomaly_count: u32,
    /// Bottleneck category from profiler.
    pub bottleneck: Option<String>,
    /// Existing recommendations from profiler.
    pub profiler_recommendations: Vec<String>,
}

/// A complete task scorecard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScorecard {
    /// Task ID.
    pub task_id: String,
    /// Task name.
    pub task_name: String,
    /// Protocol.
    pub protocol: String,
    /// Composite score (0-100).
    pub composite_score: f64,
    /// Letter grade.
    pub grade: LetterGrade,
    /// Dimension breakdowns.
    pub dimensions: Vec<DimensionScore>,
    /// Aggregated recommendations.
    pub recommendations: Vec<String>,
    /// When the scorecard was generated.
    pub generated_at: chrono::DateTime<chrono::Utc>,
    /// Source domain (if available).
    pub source_domain: Option<String>,
    /// Source reliability tier (if available).
    pub source_reliability_tier: Option<String>,
    /// Bottleneck category (if detected).
    pub bottleneck: Option<String>,
    /// Quick summary line.
    pub summary: String,
}

/// Summary statistics across all scorecards.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScorecardSummary {
    /// Total scorecards generated.
    pub total_scorecards: usize,
    /// Average composite score.
    pub avg_score: f64,
    /// Highest scoring task.
    pub best_task: Option<TaskBrief>,
    /// Lowest scoring task.
    pub worst_task: Option<TaskBrief>,
    /// Grade distribution.
    pub grade_distribution: HashMap<String, usize>,
    /// Average dimension scores.
    pub avg_dimensions: HashMap<String, f64>,
    /// Top recommendations across all tasks.
    pub top_recommendations: Vec<(String, usize)>,
    /// Tasks with failing grades.
    pub failing_count: usize,
    /// Tasks with excellent grades (A- or above).
    pub excellent_count: usize,
}

/// Brief task info for summary listings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskBrief {
    /// Task ID.
    pub task_id: String,
    /// Task name.
    pub task_name: String,
    /// Composite score.
    pub score: f64,
    /// Letter grade.
    pub grade: LetterGrade,
}

/// The main scorecard manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScorecardManager {
    /// Configuration.
    pub config: ScorecardConfig,
    /// Generated scorecards indexed by task_id.
    scorecards: HashMap<String, TaskScorecard>,
}

impl Default for TaskScorecardManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskScorecardManager {
    /// Create a new scorecard manager with default config.
    pub fn new() -> Self {
        Self {
            config: ScorecardConfig::default(),
            scorecards: HashMap::new(),
        }
    }

    /// Create with custom config.
    pub fn with_config(config: ScorecardConfig) -> Self {
        Self {
            config,
            scorecards: HashMap::new(),
        }
    }

    /// Generate a scorecard for a task based on input data.
    pub fn generate_scorecard(&mut self, input: &ScorecardInput) -> Option<TaskScorecard> {
        if !self.config.enabled {
            return None;
        }

        let weights = &self.config.weights;

        // Calculate dimension scores
        let efficiency_score = input.efficiency_score.clamp(0.0, 100.0);

        let speed_score = self.calculate_speed_score(input);
        let stability_score = self.calculate_stability_score(input);
        let reliability_score = self.calculate_reliability_score(input);
        let progress_score = input.progress_pct.clamp(0.0, 100.0);

        // Build dimension breakdowns
        let dimensions = vec![
            DimensionScore {
                name: "Efficiency".to_string(),
                score: efficiency_score,
                weight: weights.efficiency,
                weighted_score: efficiency_score * weights.efficiency,
                assessment: Self::assess_efficiency(efficiency_score),
            },
            DimensionScore {
                name: "Speed".to_string(),
                score: speed_score,
                weight: weights.speed,
                weighted_score: speed_score * weights.speed,
                assessment: Self::assess_speed(speed_score),
            },
            DimensionScore {
                name: "Stability".to_string(),
                score: stability_score,
                weight: weights.stability,
                weighted_score: stability_score * weights.stability,
                assessment: Self::assess_stability(stability_score),
            },
            DimensionScore {
                name: "Reliability".to_string(),
                score: reliability_score,
                weight: weights.reliability,
                weighted_score: reliability_score * weights.reliability,
                assessment: Self::assess_reliability(reliability_score),
            },
            DimensionScore {
                name: "Progress".to_string(),
                score: progress_score,
                weight: weights.progress,
                weighted_score: progress_score * weights.progress,
                assessment: Self::assess_progress(progress_score),
            },
        ];

        // Composite score
        let composite_score: f64 = dimensions.iter().map(|d| d.weighted_score).sum();
        let grade = LetterGrade::from_score(composite_score);

        // Aggregate recommendations
        let mut recommendations = Vec::new();
        if self.config.include_recommendations {
            // From profiler
            for rec in &input.profiler_recommendations {
                if recommendations.len() < self.config.max_recommendations {
                    recommendations.push(rec.clone());
                }
            }
            // Dimension-based recommendations
            self.add_dimension_recommendations(&dimensions, input, &mut recommendations);
            recommendations.truncate(self.config.max_recommendations);
        }

        // Source reliability tier
        let source_reliability_tier = input.source_reliability_score.map(|score| match score {
            s if s >= 0.8 => "Excellent".to_string(),
            s if s >= 0.6 => "Good".to_string(),
            s if s >= 0.4 => "Fair".to_string(),
            s if s >= 0.2 => "Poor".to_string(),
            _ => "Unreliable".to_string(),
        });

        // Summary line
        let progress_str = if input.is_complete {
            "Complete".to_string()
        } else {
            format!("{:.1}% done", input.progress_pct)
        };
        let summary = format!(
            "{} {} — Score: {:.1}/100 ({}) | {} | Bottleneck: {}",
            grade.emoji(),
            input.task_name,
            composite_score,
            grade.label(),
            progress_str,
            input.bottleneck.as_deref().unwrap_or("None detected")
        );

        let scorecard = TaskScorecard {
            task_id: input.task_id.clone(),
            task_name: input.task_name.clone(),
            protocol: input.protocol.clone(),
            composite_score,
            grade,
            dimensions,
            recommendations,
            generated_at: chrono::Utc::now(),
            source_domain: input.source_domain.clone(),
            source_reliability_tier,
            bottleneck: input.bottleneck.clone(),
            summary,
        };

        // Enforce max scorecards
        self.scorecards
            .insert(input.task_id.clone(), scorecard.clone());
        self.enforce_max_limit();

        Some(scorecard)
    }

    /// Calculate speed dimension score based on avg/peak ratio.
    fn calculate_speed_score(&self, input: &ScorecardInput) -> f64 {
        if input.peak_speed_bps <= 0.0 {
            return 50.0; // neutral if no data
        }
        let ratio = input.avg_speed_bps / input.peak_speed_bps;
        // ratio of 1.0 = perfect consistency = 100
        // ratio of 0.5 = moderate = 50
        // ratio of 0.0 = terrible = 0
        (ratio * 100.0).clamp(0.0, 100.0)
    }

    /// Calculate stability dimension score based on stalls, retries, errors.
    fn calculate_stability_score(&self, input: &ScorecardInput) -> f64 {
        let mut score = 100.0;

        // Penalty for stalls: -10 per stall, max -40
        score -= (input.stall_count as f64 * 10.0).min(40.0);

        // Penalty for retries: -8 per retry, max -30
        score -= (input.retry_count as f64 * 8.0).min(30.0);

        // Penalty for errors: -15 per error, max -30
        score -= (input.error_count as f64 * 15.0).min(30.0);

        score.clamp(0.0, 100.0)
    }

    /// Calculate reliability dimension score from source reliability data.
    fn calculate_reliability_score(&self, input: &ScorecardInput) -> f64 {
        match input.source_reliability_score {
            Some(score) => (score * 100.0).clamp(0.0, 100.0),
            None => 70.0, // neutral default when no data
        }
    }

    /// Assess efficiency dimension.
    fn assess_efficiency(score: f64) -> String {
        match score {
            s if s >= 90.0 => "Excellent resource utilization".to_string(),
            s if s >= 75.0 => "Good efficiency with minor room for improvement".to_string(),
            s if s >= 60.0 => "Moderate efficiency, some optimization needed".to_string(),
            s if s >= 40.0 => "Below average efficiency, significant waste".to_string(),
            _ => "Very poor efficiency, immediate action needed".to_string(),
        }
    }

    /// Assess speed dimension.
    fn assess_speed(score: f64) -> String {
        match score {
            s if s >= 90.0 => "Consistently fast downloads".to_string(),
            s if s >= 75.0 => "Good speed consistency".to_string(),
            s if s >= 60.0 => "Moderate speed variation".to_string(),
            s if s >= 40.0 => "Significant speed fluctuations".to_string(),
            _ => "Highly inconsistent speeds".to_string(),
        }
    }

    /// Assess stability dimension.
    fn assess_stability(score: f64) -> String {
        match score {
            s if s >= 90.0 => "Very stable, no issues detected".to_string(),
            s if s >= 75.0 => "Mostly stable with minor interruptions".to_string(),
            s if s >= 60.0 => "Some stability concerns".to_string(),
            s if s >= 40.0 => "Frequent interruptions detected".to_string(),
            _ => "Highly unstable, many failures".to_string(),
        }
    }

    /// Assess reliability dimension.
    fn assess_reliability(score: f64) -> String {
        match score {
            s if s >= 90.0 => "Highly reliable source".to_string(),
            s if s >= 75.0 => "Reliable source with occasional issues".to_string(),
            s if s >= 60.0 => "Moderately reliable source".to_string(),
            s if s >= 40.0 => "Unreliable source, consider alternatives".to_string(),
            _ => "Very unreliable source".to_string(),
        }
    }

    /// Assess progress dimension.
    fn assess_progress(score: f64) -> String {
        match score {
            s if s >= 100.0 => "Download complete".to_string(),
            s if s >= 75.0 => "Nearly complete".to_string(),
            s if s >= 50.0 => "Good progress".to_string(),
            s if s >= 25.0 => "Making progress".to_string(),
            _ => "Early stage".to_string(),
        }
    }

    /// Add dimension-based recommendations.
    fn add_dimension_recommendations(
        &self,
        dimensions: &[DimensionScore],
        input: &ScorecardInput,
        recommendations: &mut Vec<String>,
    ) {
        for dim in dimensions {
            if recommendations.len() >= self.config.max_recommendations {
                break;
            }
            match dim.name.as_str() {
                "Speed" if dim.score < 60.0 => {
                    recommendations.push(
                        "Speed inconsistency detected — consider adding mirror sources".to_string(),
                    );
                }
                "Stability" if dim.score < 60.0 => {
                    if input.stall_count > 0 {
                        recommendations.push(format!(
                            "Task stalled {} times — check network stability and server response",
                            input.stall_count
                        ));
                    }
                    if input.retry_count > 0
                        && recommendations.len() < self.config.max_recommendations
                    {
                        recommendations.push(format!(
                            "Retried {} times — consider switching to a more reliable source",
                            input.retry_count
                        ));
                    }
                }
                "Reliability" if dim.score < 60.0 => {
                    if let Some(ref domain) = input.source_domain {
                        recommendations.push(format!(
                            "Source domain '{}' has poor reliability — try alternative mirrors",
                            domain
                        ));
                    } else {
                        recommendations.push(
                            "Source reliability is low — consider using trusted mirrors"
                                .to_string(),
                        );
                    }
                }
                "Efficiency" if dim.score < 50.0 => {
                    recommendations.push(
                        "Very low efficiency — check for bandwidth limits or throttling"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
    }

    /// Enforce max scorecards limit by removing oldest entries.
    fn enforce_max_limit(&mut self) {
        if self.scorecards.len() <= self.config.max_scorecards {
            return;
        }
        let mut entries: Vec<(String, chrono::DateTime<chrono::Utc>)> = self
            .scorecards
            .iter()
            .map(|(k, v)| (k.clone(), v.generated_at))
            .collect();
        entries.sort_by_key(|(_, t)| *t);
        let to_remove = self.scorecards.len() - self.config.max_scorecards;
        for (id, _) in entries.into_iter().take(to_remove) {
            self.scorecards.remove(&id);
        }
    }

    /// Get scorecard for a specific task.
    pub fn get_scorecard(&self, task_id: &str) -> Option<&TaskScorecard> {
        self.scorecards.get(task_id)
    }

    /// Get all scorecards.
    pub fn get_all_scorecards(&self) -> Vec<&TaskScorecard> {
        let mut cards: Vec<_> = self.scorecards.values().collect();
        cards.sort_by(|a, b| {
            b.composite_score
                .partial_cmp(&a.composite_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        cards
    }

    /// Get scorecards sorted by score (best first).
    pub fn get_top_performers(&self, n: usize) -> Vec<&TaskScorecard> {
        let all = self.get_all_scorecards();
        all.into_iter().take(n).collect()
    }

    /// Get scorecards sorted by score (worst first).
    pub fn get_worst_performers(&self, n: usize) -> Vec<&TaskScorecard> {
        let mut cards: Vec<_> = self.scorecards.values().collect();
        cards.sort_by(|a, b| {
            a.composite_score
                .partial_cmp(&b.composite_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        cards.into_iter().take(n).collect()
    }

    /// Get scorecards filtered by grade.
    pub fn get_by_grade(&self, grade: LetterGrade) -> Vec<&TaskScorecard> {
        self.scorecards
            .values()
            .filter(|s| s.grade == grade)
            .collect()
    }

    /// Get scorecards for failing tasks (grade D or below).
    pub fn get_failing_tasks(&self) -> Vec<&TaskScorecard> {
        self.scorecards
            .values()
            .filter(|s| {
                matches!(
                    s.grade,
                    LetterGrade::DPlus | LetterGrade::D | LetterGrade::DMinus | LetterGrade::F
                )
            })
            .collect()
    }

    /// Remove scorecard for a task.
    pub fn remove_scorecard(&mut self, task_id: &str) -> bool {
        self.scorecards.remove(task_id).is_some()
    }

    /// Clear all scorecards.
    pub fn clear_all(&mut self) {
        self.scorecards.clear();
    }

    /// Generate summary statistics across all scorecards.
    pub fn get_summary(&self) -> ScorecardSummary {
        if self.scorecards.is_empty() {
            return ScorecardSummary::default();
        }

        let total = self.scorecards.len();
        let scores: Vec<f64> = self
            .scorecards
            .values()
            .map(|s| s.composite_score)
            .collect();
        let avg_score = scores.iter().sum::<f64>() / total as f64;

        let best = self.scorecards.values().max_by(|a, b| {
            a.composite_score
                .partial_cmp(&b.composite_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let worst = self.scorecards.values().min_by(|a, b| {
            a.composite_score
                .partial_cmp(&b.composite_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Grade distribution
        let mut grade_dist: HashMap<String, usize> = HashMap::new();
        for card in self.scorecards.values() {
            let grade_key = format!("{:?}", card.grade);
            *grade_dist.entry(grade_key).or_insert(0) += 1;
        }

        // Average dimension scores
        let mut dim_totals: HashMap<String, (f64, usize)> = HashMap::new();
        for card in self.scorecards.values() {
            for dim in &card.dimensions {
                let entry = dim_totals.entry(dim.name.clone()).or_insert((0.0, 0));
                entry.0 += dim.score;
                entry.1 += 1;
            }
        }
        let avg_dimensions: HashMap<String, f64> = dim_totals
            .into_iter()
            .map(|(name, (total, count))| (name, total / count as f64))
            .collect();

        // Top recommendations
        let mut rec_counts: HashMap<String, usize> = HashMap::new();
        for card in self.scorecards.values() {
            for rec in &card.recommendations {
                *rec_counts.entry(rec.clone()).or_insert(0) += 1;
            }
        }
        let mut top_recommendations: Vec<(String, usize)> = rec_counts.into_iter().collect();
        top_recommendations.sort_by_key(|b| std::cmp::Reverse(b.1));
        top_recommendations.truncate(10);

        let failing_count = self
            .scorecards
            .values()
            .filter(|s| {
                matches!(
                    s.grade,
                    LetterGrade::DPlus | LetterGrade::D | LetterGrade::DMinus | LetterGrade::F
                )
            })
            .count();

        let excellent_count = self
            .scorecards
            .values()
            .filter(|s| {
                matches!(
                    s.grade,
                    LetterGrade::APlus | LetterGrade::A | LetterGrade::AMinus
                )
            })
            .count();

        ScorecardSummary {
            total_scorecards: total,
            avg_score,
            best_task: best.map(|c| TaskBrief {
                task_id: c.task_id.clone(),
                task_name: c.task_name.clone(),
                score: c.composite_score,
                grade: c.grade,
            }),
            worst_task: worst.map(|c| TaskBrief {
                task_id: c.task_id.clone(),
                task_name: c.task_name.clone(),
                score: c.composite_score,
                grade: c.grade,
            }),
            grade_distribution: grade_dist,
            avg_dimensions,
            top_recommendations,
            failing_count,
            excellent_count,
        }
    }

    /// Format summary into human-readable text.
    pub fn format_summary(summary: &ScorecardSummary) -> String {
        if summary.total_scorecards == 0 {
            return "📊 No task scorecards available.".to_string();
        }

        let mut out = String::new();
        out.push_str(&format!(
            "📊 Task Scorecard Summary ({} tasks)\n",
            summary.total_scorecards
        ));
        out.push_str(&format!("Average Score: {:.1}/100\n", summary.avg_score));
        out.push_str(&format!(
            "Excellent (A-+): {} | Failing (D+/F): {}\n",
            summary.excellent_count, summary.failing_count
        ));

        if let Some(ref best) = summary.best_task {
            out.push_str(&format!(
                "\n🏆 Best: {} ({:.1} — {})\n",
                best.task_name, best.score, best.grade
            ));
        }
        if let Some(ref worst) = summary.worst_task {
            out.push_str(&format!(
                "💀 Worst: {} ({:.1} — {})\n",
                worst.task_name, worst.score, worst.grade
            ));
        }

        if !summary.avg_dimensions.is_empty() {
            out.push_str("\n📈 Average Dimension Scores:\n");
            let mut dims: Vec<_> = summary.avg_dimensions.iter().collect();
            dims.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
            for (name, score) in dims {
                let bar = Self::mini_bar(*score);
                out.push_str(&format!(
                    "  {} {}: {:.0}/100 {}\n",
                    Self::dim_emoji(name),
                    name,
                    score,
                    bar
                ));
            }
        }

        if !summary.top_recommendations.is_empty() {
            out.push_str("\n💡 Top Recommendations:\n");
            for (rec, count) in summary.top_recommendations.iter().take(5) {
                out.push_str(&format!("  • {} ({} tasks)\n", rec, count));
            }
        }

        out
    }

    /// Mini progress bar for dimension scores.
    fn mini_bar(score: f64) -> String {
        let filled = (score / 10.0).round() as usize;
        let empty = 10_usize.saturating_sub(filled);
        format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
    }

    /// Emoji for dimension name.
    fn dim_emoji(name: &str) -> &'static str {
        match name {
            "Efficiency" => "⚡",
            "Speed" => "🚀",
            "Stability" => "🛡️",
            "Reliability" => "🔗",
            "Progress" => "📊",
            _ => "📋",
        }
    }

    /// Get config reference.
    pub fn get_config(&self) -> &ScorecardConfig {
        &self.config
    }

    /// Set config.
    pub fn set_config(&mut self, config: ScorecardConfig) {
        self.config = config;
    }

    /// Get number of scorecards.
    pub fn scorecard_count(&self) -> usize {
        self.scorecards.len()
    }

    /// Save config to file.
    pub async fn save_config(&self, path: &Path) -> Result<(), ScorecardError> {
        let json = serde_json::to_string_pretty(&self.config)?;
        tokio::fs::write(path, json).await?;
        Ok(())
    }

    /// Load config from file.
    pub async fn load_config(path: &Path) -> Result<ScorecardConfig, ScorecardError> {
        let json = tokio::fs::read_to_string(path).await?;
        let config: ScorecardConfig = serde_json::from_str(&json)?;
        Ok(config)
    }

    /// Save scorecards to file.
    pub async fn save_data(&self, path: &Path) -> Result<(), ScorecardError> {
        let json = serde_json::to_string_pretty(&self.scorecards)?;
        tokio::fs::write(path, json).await?;
        Ok(())
    }

    /// Load scorecards from file.
    pub async fn load_data(&mut self, path: &Path) -> Result<(), ScorecardError> {
        let json = tokio::fs::read_to_string(path).await?;
        self.scorecards = serde_json::from_str(&json)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> ScorecardInput {
        ScorecardInput {
            task_id: "task-1".to_string(),
            task_name: "test-download.zip".to_string(),
            protocol: "http".to_string(),
            source_domain: Some("example.com".to_string()),
            total_bytes: 1_000_000_000,
            downloaded_bytes: 500_000_000,
            progress_pct: 50.0,
            avg_speed_bps: 5_000_000.0,
            peak_speed_bps: 8_000_000.0,
            efficiency_score: 75.0,
            stall_count: 2,
            retry_count: 1,
            error_count: 0,
            duration_secs: 300.0,
            is_complete: false,
            source_reliability_score: Some(0.85),
            anomaly_count: 0,
            bottleneck: Some("Network".to_string()),
            profiler_recommendations: vec!["Consider adding mirror sources".to_string()],
        }
    }

    #[test]
    fn test_letter_grade_from_score() {
        assert_eq!(LetterGrade::from_score(98.0), LetterGrade::APlus);
        assert_eq!(LetterGrade::from_score(94.0), LetterGrade::A);
        assert_eq!(LetterGrade::from_score(91.0), LetterGrade::AMinus);
        assert_eq!(LetterGrade::from_score(88.0), LetterGrade::BPlus);
        assert_eq!(LetterGrade::from_score(84.0), LetterGrade::B);
        assert_eq!(LetterGrade::from_score(81.0), LetterGrade::BMinus);
        assert_eq!(LetterGrade::from_score(78.0), LetterGrade::CPlus);
        assert_eq!(LetterGrade::from_score(74.0), LetterGrade::C);
        assert_eq!(LetterGrade::from_score(71.0), LetterGrade::CMinus);
        assert_eq!(LetterGrade::from_score(68.0), LetterGrade::DPlus);
        assert_eq!(LetterGrade::from_score(64.0), LetterGrade::D);
        assert_eq!(LetterGrade::from_score(61.0), LetterGrade::DMinus);
        assert_eq!(LetterGrade::from_score(50.0), LetterGrade::F);
        assert_eq!(LetterGrade::from_score(0.0), LetterGrade::F);
    }

    #[test]
    fn test_letter_grade_display() {
        assert!(LetterGrade::APlus.emoji().contains("🏆"));
        assert!(LetterGrade::F.emoji().contains("💀"));
        assert!(LetterGrade::A.label().contains("Excellent"));
        assert!(LetterGrade::F.label().contains("Failing"));
    }

    #[test]
    fn test_default_weights_valid() {
        let weights = ScoreWeights::default();
        assert!(weights.is_valid());
    }

    #[test]
    fn test_weights_normalize() {
        let mut weights = ScoreWeights {
            efficiency: 2.0,
            speed: 2.0,
            stability: 2.0,
            reliability: 2.0,
            progress: 2.0,
        };
        assert!(!weights.is_valid());
        weights.normalize();
        assert!(weights.is_valid());
        assert!((weights.efficiency - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_generate_scorecard() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();

        assert_eq!(card.task_id, "task-1");
        assert!(card.composite_score > 0.0);
        assert!(card.composite_score <= 100.0);
        assert_eq!(card.dimensions.len(), 5);
        assert!(!card.recommendations.is_empty());
        assert!(card.summary.contains("test-download.zip"));
    }

    #[test]
    fn test_scorecard_disabled() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            enabled: false,
            ..Default::default()
        });
        let input = sample_input();
        assert!(manager.generate_scorecard(&input).is_none());
    }

    #[test]
    fn test_speed_score_calculation() {
        let manager = TaskScorecardManager::new();
        // Perfect consistency
        let input = ScorecardInput {
            avg_speed_bps: 5_000_000.0,
            peak_speed_bps: 5_000_000.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!((score - 100.0).abs() < 0.01);

        // 50% consistency
        let input = ScorecardInput {
            avg_speed_bps: 2_500_000.0,
            peak_speed_bps: 5_000_000.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!((score - 50.0).abs() < 0.01);

        // Zero peak
        let input = ScorecardInput {
            avg_speed_bps: 0.0,
            peak_speed_bps: 0.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!((score - 50.0).abs() < 0.01); // neutral
    }

    #[test]
    fn test_stability_score_calculation() {
        let manager = TaskScorecardManager::new();

        // Perfect stability
        let input = ScorecardInput::default();
        let score = manager.calculate_stability_score(&input);
        assert!((score - 100.0).abs() < 0.01);

        // Some issues
        let input = ScorecardInput {
            stall_count: 2,
            retry_count: 1,
            error_count: 1,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        // 100 - 20 (stalls) - 8 (retry) - 15 (error) = 57
        assert!((score - 57.0).abs() < 0.01);

        // Max penalties
        let input = ScorecardInput {
            stall_count: 100,
            retry_count: 100,
            error_count: 100,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!(score == 0.0);
    }

    #[test]
    fn test_reliability_score_with_data() {
        let manager = TaskScorecardManager::new();

        let input = ScorecardInput {
            source_reliability_score: Some(0.9),
            ..Default::default()
        };
        let score = manager.calculate_reliability_score(&input);
        assert!((score - 90.0).abs() < 0.01);

        // No data → neutral
        let input = ScorecardInput {
            source_reliability_score: None,
            ..Default::default()
        };
        let score = manager.calculate_reliability_score(&input);
        assert!((score - 70.0).abs() < 0.01);
    }

    #[test]
    fn test_get_scorecard() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);

        assert!(manager.get_scorecard("task-1").is_some());
        assert!(manager.get_scorecard("nonexistent").is_none());
    }

    #[test]
    fn test_get_all_scorecards_sorted() {
        let mut manager = TaskScorecardManager::new();

        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                task_name: format!("download-{}.zip", i),
                efficiency_score: (i as f64) * 20.0,
                progress_pct: (i as f64) * 20.0,
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }

        let all = manager.get_all_scorecards();
        assert_eq!(all.len(), 5);
        // Should be sorted best first
        for i in 0..all.len() - 1 {
            assert!(all[i].composite_score >= all[i + 1].composite_score);
        }
    }

    #[test]
    fn test_top_and_worst_performers() {
        let mut manager = TaskScorecardManager::new();

        for i in 0..10 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                task_name: format!("file-{}.zip", i),
                efficiency_score: (i as f64) * 10.0,
                progress_pct: (i as f64) * 10.0,
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }

        let top = manager.get_top_performers(3);
        assert_eq!(top.len(), 3);
        assert!(top[0].composite_score >= top[1].composite_score);

        let worst = manager.get_worst_performers(3);
        assert_eq!(worst.len(), 3);
        assert!(worst[0].composite_score <= worst[1].composite_score);
    }

    #[test]
    fn test_get_failing_tasks() {
        let mut manager = TaskScorecardManager::new();

        // Good task
        let good = ScorecardInput {
            task_id: "good".to_string(),
            efficiency_score: 95.0,
            progress_pct: 100.0,
            source_reliability_score: Some(0.95),
            ..Default::default()
        };
        manager.generate_scorecard(&good);

        // Bad task
        let bad = ScorecardInput {
            task_id: "bad".to_string(),
            efficiency_score: 10.0,
            progress_pct: 5.0,
            stall_count: 20,
            retry_count: 10,
            error_count: 5,
            source_reliability_score: Some(0.1),
            ..Default::default()
        };
        manager.generate_scorecard(&bad);

        let failing = manager.get_failing_tasks();
        assert!(failing.iter().any(|c| c.task_id == "bad"));
    }

    #[test]
    fn test_remove_and_clear() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);
        assert_eq!(manager.scorecard_count(), 1);

        assert!(manager.remove_scorecard("task-1"));
        assert_eq!(manager.scorecard_count(), 0);

        manager.generate_scorecard(&input);
        manager.clear_all();
        assert_eq!(manager.scorecard_count(), 0);
    }

    #[test]
    fn test_max_scorecards_limit() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            max_scorecards: 5,
            ..Default::default()
        });

        for i in 0..10 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                task_name: format!("file-{}.zip", i),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }

        assert!(manager.scorecard_count() <= 5);
    }

    #[test]
    fn test_summary_generation() {
        let mut manager = TaskScorecardManager::new();

        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                task_name: format!("file-{}.zip", i),
                efficiency_score: 50.0 + (i as f64) * 10.0,
                progress_pct: 20.0 + (i as f64) * 15.0,
                source_reliability_score: Some(0.5 + (i as f64) * 0.1),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_scorecards, 5);
        assert!(summary.avg_score > 0.0);
        assert!(summary.best_task.is_some());
        assert!(summary.worst_task.is_some());
        assert!(!summary.avg_dimensions.is_empty());
    }

    #[test]
    fn test_empty_summary() {
        let manager = TaskScorecardManager::new();
        let summary = manager.get_summary();
        assert_eq!(summary.total_scorecards, 0);
        assert!(summary.best_task.is_none());
    }

    #[test]
    fn test_format_summary() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);

        let summary = manager.get_summary();
        let formatted = TaskScorecardManager::format_summary(&summary);
        assert!(formatted.contains("Task Scorecard Summary"));
        assert!(formatted.contains("Average Score"));
    }

    #[test]
    fn test_format_empty_summary() {
        let summary = ScorecardSummary::default();
        let formatted = TaskScorecardManager::format_summary(&summary);
        assert!(formatted.contains("No task scorecards"));
    }

    #[test]
    fn test_config_persistence() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let manager = TaskScorecardManager::new();
            let path = std::path::Path::new("/tmp/test_scorecard_config.json");

            manager.save_config(path).await.unwrap();
            let loaded = TaskScorecardManager::load_config(path).await.unwrap();
            assert_eq!(loaded.enabled, manager.config.enabled);

            std::fs::remove_file(path).ok();
        });
    }

    #[test]
    fn test_data_persistence() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScorecardManager::new();
            let input = sample_input();
            manager.generate_scorecard(&input);

            let path = std::path::Path::new("/tmp/test_scorecard_data.json");
            manager.save_data(path).await.unwrap();

            let mut loaded = TaskScorecardManager::new();
            loaded.load_data(path).await.unwrap();
            assert_eq!(loaded.scorecard_count(), 1);
            assert!(loaded.get_scorecard("task-1").is_some());

            std::fs::remove_file(path).ok();
        });
    }

    #[test]
    fn test_recommendations_truncation() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            max_recommendations: 3,
            ..Default::default()
        });

        let input = ScorecardInput {
            task_id: "task-rec".to_string(),
            task_name: "rec-test.zip".to_string(),
            efficiency_score: 20.0,
            stall_count: 10,
            retry_count: 5,
            error_count: 3,
            source_reliability_score: Some(0.2),
            profiler_recommendations: vec![
                "Rec 1".to_string(),
                "Rec 2".to_string(),
                "Rec 3".to_string(),
                "Rec 4".to_string(),
                "Rec 5".to_string(),
            ],
            ..Default::default()
        };

        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.recommendations.len() <= 3);
    }

    #[test]
    fn test_source_reliability_tier_mapping() {
        let mut manager = TaskScorecardManager::new();

        let input = ScorecardInput {
            task_id: "rel-task".to_string(),
            task_name: "rel-test.zip".to_string(),
            source_reliability_score: Some(0.85),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert_eq!(card.source_reliability_tier.as_deref(), Some("Excellent"));

        let input2 = ScorecardInput {
            task_id: "rel-task2".to_string(),
            task_name: "rel-test2.zip".to_string(),
            source_reliability_score: Some(0.15),
            ..Default::default()
        };
        let card2 = manager.generate_scorecard(&input2).unwrap();
        assert_eq!(card2.source_reliability_tier.as_deref(), Some("Unreliable"));
    }

    #[test]
    fn test_mini_bar() {
        let bar = TaskScorecardManager::mini_bar(70.0);
        assert!(bar.contains("█"));
        assert!(bar.contains("░"));
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
    }

    #[test]
    fn test_grade_distribution_in_summary() {
        let mut manager = TaskScorecardManager::new();

        // Generate tasks with varying quality
        let excellent = ScorecardInput {
            task_id: "excellent".to_string(),
            efficiency_score: 98.0,
            progress_pct: 100.0,
            avg_speed_bps: 5_000_000.0,
            peak_speed_bps: 5_000_000.0,
            source_reliability_score: Some(0.99),
            ..Default::default()
        };
        manager.generate_scorecard(&excellent);

        let poor = ScorecardInput {
            task_id: "poor".to_string(),
            efficiency_score: 15.0,
            progress_pct: 10.0,
            stall_count: 50,
            retry_count: 20,
            error_count: 10,
            source_reliability_score: Some(0.05),
            ..Default::default()
        };
        manager.generate_scorecard(&poor);

        let summary = manager.get_summary();
        assert!(!summary.grade_distribution.is_empty());
        assert!(summary.excellent_count >= 1);
        assert!(summary.failing_count >= 1);
    }

    #[test]
    fn test_no_recommendations_when_disabled() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            include_recommendations: false,
            ..Default::default()
        });

        let input = ScorecardInput {
            task_id: "no-rec".to_string(),
            efficiency_score: 20.0,
            stall_count: 10,
            profiler_recommendations: vec!["Should not appear".to_string()],
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.recommendations.is_empty());
    }

    #[test]
    fn test_dimension_assessments() {
        // Just verify they don't panic and return non-empty strings
        assert!(!TaskScorecardManager::assess_efficiency(50.0).is_empty());
        assert!(!TaskScorecardManager::assess_speed(50.0).is_empty());
        assert!(!TaskScorecardManager::assess_stability(50.0).is_empty());
        assert!(!TaskScorecardManager::assess_reliability(50.0).is_empty());
        assert!(!TaskScorecardManager::assess_progress(50.0).is_empty());
    }
}
