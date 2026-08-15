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

    // ==================== LetterGrade serde ====================

    #[test]
    fn test_letter_grade_serde_roundtrip_all_variants() {
        let grades = vec![
            LetterGrade::APlus,
            LetterGrade::A,
            LetterGrade::AMinus,
            LetterGrade::BPlus,
            LetterGrade::B,
            LetterGrade::BMinus,
            LetterGrade::CPlus,
            LetterGrade::C,
            LetterGrade::CMinus,
            LetterGrade::DPlus,
            LetterGrade::D,
            LetterGrade::DMinus,
            LetterGrade::F,
        ];
        for grade in &grades {
            let json = serde_json::to_string(grade).unwrap();
            let deserialized: LetterGrade = serde_json::from_str(&json).unwrap();
            assert_eq!(*grade, deserialized, "roundtrip failed for {:?}", grade);
        }
    }

    #[test]
    fn test_letter_grade_serde_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&LetterGrade::APlus).unwrap(),
            "\"a_plus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::AMinus).unwrap(),
            "\"a_minus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::BPlus).unwrap(),
            "\"b_plus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::BMinus).unwrap(),
            "\"b_minus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::CPlus).unwrap(),
            "\"c_plus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::CMinus).unwrap(),
            "\"c_minus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::DPlus).unwrap(),
            "\"d_plus\""
        );
        assert_eq!(
            serde_json::to_string(&LetterGrade::DMinus).unwrap(),
            "\"d_minus\""
        );
        assert_eq!(serde_json::to_string(&LetterGrade::F).unwrap(), "\"f\"");
    }

    #[test]
    fn test_letter_grade_serde_extra_fields_ignored() {
        // Enum variants deserialize from snake_case strings
        let json = r#""a_plus""#;
        let grade: LetterGrade = serde_json::from_str(json).unwrap();
        assert_eq!(grade, LetterGrade::APlus);
    }

    // ==================== LetterGrade traits ====================

    #[test]
    fn test_letter_grade_clone_copy() {
        let grade = LetterGrade::APlus;
        let cloned = grade;
        let copied = grade;
        assert_eq!(grade, cloned);
        assert_eq!(grade, copied);
    }

    #[test]
    fn test_letter_grade_debug() {
        let grade = LetterGrade::BPlus;
        let debug_str = format!("{:?}", grade);
        assert_eq!(debug_str, "BPlus");
    }

    #[test]
    fn test_letter_grade_eq() {
        assert_eq!(LetterGrade::A, LetterGrade::A);
        assert_ne!(LetterGrade::A, LetterGrade::B);
        assert_ne!(LetterGrade::APlus, LetterGrade::AMinus);
    }

    // ==================== LetterGrade from_score boundaries ====================

    #[test]
    fn test_letter_grade_from_score_exact_boundaries() {
        assert_eq!(LetterGrade::from_score(97.0), LetterGrade::APlus);
        assert_eq!(LetterGrade::from_score(96.99), LetterGrade::A);
        assert_eq!(LetterGrade::from_score(93.0), LetterGrade::A);
        assert_eq!(LetterGrade::from_score(92.99), LetterGrade::AMinus);
        assert_eq!(LetterGrade::from_score(90.0), LetterGrade::AMinus);
        assert_eq!(LetterGrade::from_score(89.99), LetterGrade::BPlus);
        assert_eq!(LetterGrade::from_score(87.0), LetterGrade::BPlus);
        assert_eq!(LetterGrade::from_score(86.99), LetterGrade::B);
        assert_eq!(LetterGrade::from_score(83.0), LetterGrade::B);
        assert_eq!(LetterGrade::from_score(82.99), LetterGrade::BMinus);
        assert_eq!(LetterGrade::from_score(80.0), LetterGrade::BMinus);
        assert_eq!(LetterGrade::from_score(79.99), LetterGrade::CPlus);
        assert_eq!(LetterGrade::from_score(77.0), LetterGrade::CPlus);
        assert_eq!(LetterGrade::from_score(76.99), LetterGrade::C);
        assert_eq!(LetterGrade::from_score(73.0), LetterGrade::C);
        assert_eq!(LetterGrade::from_score(72.99), LetterGrade::CMinus);
        assert_eq!(LetterGrade::from_score(70.0), LetterGrade::CMinus);
        assert_eq!(LetterGrade::from_score(69.99), LetterGrade::DPlus);
        assert_eq!(LetterGrade::from_score(67.0), LetterGrade::DPlus);
        assert_eq!(LetterGrade::from_score(66.99), LetterGrade::D);
        assert_eq!(LetterGrade::from_score(63.0), LetterGrade::D);
        assert_eq!(LetterGrade::from_score(62.99), LetterGrade::DMinus);
        assert_eq!(LetterGrade::from_score(60.0), LetterGrade::DMinus);
        assert_eq!(LetterGrade::from_score(59.99), LetterGrade::F);
    }

    #[test]
    fn test_letter_grade_from_score_negative() {
        assert_eq!(LetterGrade::from_score(-10.0), LetterGrade::F);
    }

    #[test]
    fn test_letter_grade_from_score_over_100() {
        assert_eq!(LetterGrade::from_score(150.0), LetterGrade::APlus);
    }

    // ==================== LetterGrade emoji all variants ====================

    #[test]
    fn test_letter_grade_emoji_all_variants() {
        assert_eq!(LetterGrade::APlus.emoji(), "🏆");
        assert_eq!(LetterGrade::A.emoji(), "🌟");
        assert_eq!(LetterGrade::AMinus.emoji(), "✨");
        assert_eq!(LetterGrade::BPlus.emoji(), "👍");
        assert_eq!(LetterGrade::B.emoji(), "✅");
        assert_eq!(LetterGrade::BMinus.emoji(), "📊");
        assert_eq!(LetterGrade::CPlus.emoji(), "📉");
        assert_eq!(LetterGrade::C.emoji(), "⚠️");
        assert_eq!(LetterGrade::CMinus.emoji(), "🔻");
        assert_eq!(LetterGrade::DPlus.emoji(), "🟠");
        assert_eq!(LetterGrade::D.emoji(), "🔴");
        assert_eq!(LetterGrade::DMinus.emoji(), "⛔");
        assert_eq!(LetterGrade::F.emoji(), "💀");
    }

    // ==================== LetterGrade label all variants ====================

    #[test]
    fn test_letter_grade_label_all_variants() {
        assert!(LetterGrade::APlus.label().contains("Exceptional"));
        assert!(LetterGrade::A.label().contains("Excellent"));
        assert!(LetterGrade::AMinus.label().contains("Very Good"));
        assert!(LetterGrade::BPlus.label().contains("Good"));
        assert!(LetterGrade::B.label().contains("Above Average"));
        assert!(LetterGrade::BMinus.label().contains("Solid"));
        assert!(LetterGrade::CPlus.label().contains("Decent"));
        assert!(LetterGrade::C.label().contains("Average"));
        assert!(LetterGrade::CMinus.label().contains("Below Average"));
        assert!(LetterGrade::DPlus.label().contains("Poor"));
        assert!(LetterGrade::D.label().contains("Below Average"));
        assert!(LetterGrade::DMinus.label().contains("Barely Passing"));
        assert!(LetterGrade::F.label().contains("Failing"));
    }

    // ==================== LetterGrade Display ====================

    #[test]
    fn test_letter_grade_display_all_variants() {
        let display = format!("{}", LetterGrade::APlus);
        assert!(display.contains("Exceptional"));
        let display = format!("{}", LetterGrade::F);
        assert!(display.contains("Failing"));
        let display = format!("{}", LetterGrade::B);
        assert!(display.contains("Above Average"));
    }

    // ==================== ScoreWeights serde ====================

    #[test]
    fn test_score_weights_serde_roundtrip() {
        let weights = ScoreWeights::default();
        let json = serde_json::to_string(&weights).unwrap();
        let deserialized: ScoreWeights = serde_json::from_str(&json).unwrap();
        assert!((deserialized.efficiency - weights.efficiency).abs() < f64::EPSILON);
        assert!((deserialized.speed - weights.speed).abs() < f64::EPSILON);
        assert!((deserialized.stability - weights.stability).abs() < f64::EPSILON);
        assert!((deserialized.reliability - weights.reliability).abs() < f64::EPSILON);
        assert!((deserialized.progress - weights.progress).abs() < f64::EPSILON);
    }

    #[test]
    fn test_score_weights_serde_custom_values() {
        let weights = ScoreWeights {
            efficiency: 0.5,
            speed: 0.2,
            stability: 0.1,
            reliability: 0.1,
            progress: 0.1,
        };
        let json = serde_json::to_string(&weights).unwrap();
        let deserialized: ScoreWeights = serde_json::from_str(&json).unwrap();
        assert!((deserialized.efficiency - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_score_weights_serde_extra_fields_ignored() {
        let json = r#"{"efficiency":0.3,"speed":0.25,"stability":0.2,"reliability":0.15,"progress":0.1,"extra":true}"#;
        let weights: ScoreWeights = serde_json::from_str(json).unwrap();
        assert!(weights.is_valid());
    }

    // ==================== ScoreWeights is_valid ====================

    #[test]
    fn test_score_weights_is_valid_true() {
        let weights = ScoreWeights::default();
        assert!(weights.is_valid());
    }

    #[test]
    fn test_score_weights_is_valid_false() {
        let weights = ScoreWeights {
            efficiency: 1.0,
            speed: 1.0,
            stability: 1.0,
            reliability: 1.0,
            progress: 1.0,
        };
        assert!(!weights.is_valid());
    }

    #[test]
    fn test_score_weights_is_valid_near_one() {
        let weights = ScoreWeights {
            efficiency: 0.2,
            speed: 0.2,
            stability: 0.2,
            reliability: 0.2,
            progress: 0.2001,
        };
        assert!(weights.is_valid()); // within 0.01 tolerance
    }

    // ==================== ScoreWeights normalize ====================

    #[test]
    fn test_score_weights_normalize_already_valid() {
        let mut weights = ScoreWeights::default();
        weights.normalize();
        assert!(weights.is_valid());
    }

    #[test]
    fn test_score_weights_normalize_zero_sum() {
        let mut weights = ScoreWeights {
            efficiency: 0.0,
            speed: 0.0,
            stability: 0.0,
            reliability: 0.0,
            progress: 0.0,
        };
        weights.normalize(); // should not panic
        assert!(!weights.is_valid()); // still invalid (NaN or zero)
    }

    #[test]
    fn test_score_weights_normalize_uneven() {
        let mut weights = ScoreWeights {
            efficiency: 10.0,
            speed: 5.0,
            stability: 3.0,
            reliability: 1.0,
            progress: 1.0,
        };
        weights.normalize();
        assert!(weights.is_valid());
        assert!((weights.efficiency - 10.0 / 20.0).abs() < 0.01);
        assert!((weights.speed - 5.0 / 20.0).abs() < 0.01);
    }

    // ==================== ScoreWeights Clone/Debug ====================

    #[test]
    fn test_score_weights_clone() {
        let weights = ScoreWeights::default();
        let cloned = weights.clone();
        assert!((cloned.efficiency - weights.efficiency).abs() < f64::EPSILON);
    }

    #[test]
    fn test_score_weights_debug() {
        let weights = ScoreWeights::default();
        let debug = format!("{:?}", weights);
        assert!(debug.contains("efficiency"));
        assert!(debug.contains("speed"));
    }

    // ==================== ScorecardConfig serde ====================

    #[test]
    fn test_scorecard_config_serde_roundtrip() {
        let config = ScorecardConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ScorecardConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.min_samples, config.min_samples);
        assert_eq!(deserialized.max_scorecards, config.max_scorecards);
    }

    #[test]
    fn test_scorecard_config_serde_pretty() {
        let config = ScorecardConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: ScorecardConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
    }

    #[test]
    fn test_scorecard_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"weights":{"efficiency":0.3,"speed":0.25,"stability":0.2,"reliability":0.15,"progress":0.1},"min_samples":2,"max_scorecards":500,"include_recommendations":true,"max_recommendations":10,"unknown_field":"ignored"}"#;
        let config: ScorecardConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
    }

    // ==================== ScorecardConfig Clone/Debug ====================

    #[test]
    fn test_scorecard_config_clone() {
        let config = ScorecardConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.max_scorecards, config.max_scorecards);
    }

    #[test]
    fn test_scorecard_config_debug() {
        let config = ScorecardConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("enabled"));
        assert!(debug.contains("ScorecardConfig"));
    }

    // ==================== DimensionScore serde ====================

    #[test]
    fn test_dimension_score_serde_roundtrip() {
        let dim = DimensionScore {
            name: "Speed".to_string(),
            score: 85.5,
            weight: 0.25,
            weighted_score: 21.375,
            assessment: "Good speed consistency".to_string(),
        };
        let json = serde_json::to_string(&dim).unwrap();
        let deserialized: DimensionScore = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "Speed");
        assert!((deserialized.score - 85.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_dimension_score_clone_debug() {
        let dim = DimensionScore {
            name: "Efficiency".to_string(),
            score: 90.0,
            weight: 0.3,
            weighted_score: 27.0,
            assessment: "Excellent".to_string(),
        };
        let cloned = dim.clone();
        assert_eq!(cloned.name, dim.name);
        let debug = format!("{:?}", dim);
        assert!(debug.contains("DimensionScore"));
    }

    // ==================== ScorecardInput ====================

    #[test]
    fn test_scorecard_input_default() {
        let input = ScorecardInput::default();
        assert_eq!(input.task_id, "");
        assert_eq!(input.total_bytes, 0);
        assert_eq!(input.progress_pct, 0.0);
        assert!(input.source_reliability_score.is_none());
        assert!(input.source_domain.is_none());
        assert!(input.bottleneck.is_none());
        assert!(input.profiler_recommendations.is_empty());
        assert!(!input.is_complete);
    }

    #[test]
    fn test_scorecard_input_clone_debug() {
        let input = sample_input();
        let cloned = input.clone();
        assert_eq!(cloned.task_id, input.task_id);
        let debug = format!("{:?}", input);
        assert!(debug.contains("ScorecardInput"));
    }

    // ==================== TaskScorecard serde ====================

    #[test]
    fn test_task_scorecard_serde_roundtrip() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();

        let json = serde_json::to_string(&card).unwrap();
        let deserialized: TaskScorecard = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, card.task_id);
        assert_eq!(deserialized.task_name, card.task_name);
        assert!((deserialized.composite_score - card.composite_score).abs() < 0.01);
        assert_eq!(deserialized.grade, card.grade);
        assert_eq!(deserialized.dimensions.len(), card.dimensions.len());
    }

    #[test]
    fn test_task_scorecard_serde_unicode_task_id() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "任务-中文-🎉".to_string(),
            task_name: "测试下载.zip".to_string(),
            ..sample_input()
        };
        let card = manager.generate_scorecard(&input).unwrap();

        let json = serde_json::to_string(&card).unwrap();
        let deserialized: TaskScorecard = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "任务-中文-🎉");
    }

    #[test]
    fn test_task_scorecard_clone() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();
        let cloned = card.clone();
        assert_eq!(cloned.task_id, card.task_id);
        assert_eq!(cloned.composite_score, card.composite_score);
    }

    // ==================== TaskScorecard fields ====================

    #[test]
    fn test_task_scorecard_fields_correct() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "field-test".to_string(),
            task_name: "field.zip".to_string(),
            protocol: "torrent".to_string(),
            source_domain: Some("tracker.example.com".to_string()),
            source_reliability_score: Some(0.75),
            bottleneck: Some("CPU".to_string()),
            is_complete: true,
            progress_pct: 100.0,
            ..sample_input()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert_eq!(card.protocol, "torrent");
        assert_eq!(card.source_domain.as_deref(), Some("tracker.example.com"));
        assert_eq!(card.source_reliability_tier.as_deref(), Some("Good"));
        assert_eq!(card.bottleneck.as_deref(), Some("CPU"));
        assert!(card.summary.contains("Complete"));
    }

    #[test]
    fn test_task_scorecard_summary_incomplete() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "incomplete".to_string(),
            task_name: "partial.zip".to_string(),
            is_complete: false,
            progress_pct: 42.5,
            ..sample_input()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.summary.contains("42.5% done"));
    }

    // ==================== ScorecardSummary serde ====================

    #[test]
    fn test_scorecard_summary_serde_roundtrip() {
        let summary = ScorecardSummary {
            total_scorecards: 10,
            avg_score: 75.5,
            best_task: Some(TaskBrief {
                task_id: "best".to_string(),
                task_name: "best.zip".to_string(),
                score: 95.0,
                grade: LetterGrade::A,
            }),
            worst_task: None,
            grade_distribution: HashMap::new(),
            avg_dimensions: HashMap::new(),
            top_recommendations: vec![("Rec1".to_string(), 5)],
            failing_count: 2,
            excellent_count: 3,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: ScorecardSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_scorecards, 10);
        assert!(deserialized.best_task.is_some());
        assert!(deserialized.worst_task.is_none());
    }

    #[test]
    fn test_scorecard_summary_serde_empty() {
        let summary = ScorecardSummary::default();
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: ScorecardSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_scorecards, 0);
    }

    #[test]
    fn test_scorecard_summary_clone_debug() {
        let summary = ScorecardSummary::default();
        let cloned = summary.clone();
        assert_eq!(cloned.total_scorecards, summary.total_scorecards);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("ScorecardSummary"));
    }

    // ==================== TaskBrief serde ====================

    #[test]
    fn test_task_brief_serde_roundtrip() {
        let brief = TaskBrief {
            task_id: "tb-1".to_string(),
            task_name: "brief.zip".to_string(),
            score: 88.5,
            grade: LetterGrade::BPlus,
        };
        let json = serde_json::to_string(&brief).unwrap();
        let deserialized: TaskBrief = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "tb-1");
        assert_eq!(deserialized.grade, LetterGrade::BPlus);
    }

    #[test]
    fn test_task_brief_clone_debug() {
        let brief = TaskBrief {
            task_id: "tb-2".to_string(),
            task_name: "test.zip".to_string(),
            score: 50.0,
            grade: LetterGrade::C,
        };
        let cloned = brief.clone();
        assert_eq!(cloned.task_id, brief.task_id);
        let debug = format!("{:?}", brief);
        assert!(debug.contains("TaskBrief"));
    }

    // ==================== TaskScorecardManager ====================

    #[test]
    fn test_manager_default_equals_new() {
        let a = TaskScorecardManager::new();
        let b = TaskScorecardManager::default();
        assert_eq!(a.scorecard_count(), b.scorecard_count());
        assert_eq!(a.config.enabled, b.config.enabled);
    }

    #[test]
    fn test_manager_with_config() {
        let config = ScorecardConfig {
            max_scorecards: 100,
            ..Default::default()
        };
        let manager = TaskScorecardManager::with_config(config);
        assert_eq!(manager.config.max_scorecards, 100);
    }

    #[test]
    fn test_manager_generate_overwrites_existing() {
        let mut manager = TaskScorecardManager::new();
        let input1 = ScorecardInput {
            task_id: "same-id".to_string(),
            task_name: "first.zip".to_string(),
            efficiency_score: 50.0,
            ..Default::default()
        };
        let input2 = ScorecardInput {
            task_id: "same-id".to_string(),
            task_name: "second.zip".to_string(),
            efficiency_score: 90.0,
            ..Default::default()
        };
        manager.generate_scorecard(&input1);
        manager.generate_scorecard(&input2);
        assert_eq!(manager.scorecard_count(), 1);
        let card = manager.get_scorecard("same-id").unwrap();
        assert_eq!(card.task_name, "second.zip");
    }

    #[test]
    fn test_manager_get_by_grade() {
        let mut manager = TaskScorecardManager::new();
        let excellent = ScorecardInput {
            task_id: "ex".to_string(),
            efficiency_score: 99.0,
            progress_pct: 100.0,
            avg_speed_bps: 5_000_000.0,
            peak_speed_bps: 5_000_000.0,
            source_reliability_score: Some(0.99),
            ..Default::default()
        };
        manager.generate_scorecard(&excellent);

        let poor = ScorecardInput {
            task_id: "poor".to_string(),
            efficiency_score: 5.0,
            progress_pct: 2.0,
            stall_count: 50,
            retry_count: 20,
            error_count: 10,
            source_reliability_score: Some(0.01),
            ..Default::default()
        };
        manager.generate_scorecard(&poor);

        let a_plus_cards = manager.get_by_grade(LetterGrade::APlus);
        assert!(a_plus_cards.iter().any(|c| c.task_id == "ex"));

        let f_cards = manager.get_by_grade(LetterGrade::F);
        assert!(f_cards.iter().any(|c| c.task_id == "poor"));
    }

    #[test]
    fn test_manager_get_by_grade_empty() {
        let manager = TaskScorecardManager::new();
        let cards = manager.get_by_grade(LetterGrade::APlus);
        assert!(cards.is_empty());
    }

    #[test]
    fn test_manager_get_worst_performers_empty() {
        let manager = TaskScorecardManager::new();
        let worst = manager.get_worst_performers(5);
        assert!(worst.is_empty());
    }

    #[test]
    fn test_manager_get_worst_performers_single() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);
        let worst = manager.get_worst_performers(3);
        assert_eq!(worst.len(), 1);
    }

    #[test]
    fn test_manager_remove_scorecard_idempotent() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);
        assert!(manager.remove_scorecard("task-1"));
        assert!(!manager.remove_scorecard("task-1")); // second remove returns false
    }

    #[test]
    fn test_manager_remove_nonexistent() {
        let mut manager = TaskScorecardManager::new();
        assert!(!manager.remove_scorecard("nonexistent"));
    }

    #[test]
    fn test_manager_clear_all_empty() {
        let mut manager = TaskScorecardManager::new();
        manager.clear_all(); // should not panic
        assert_eq!(manager.scorecard_count(), 0);
    }

    #[test]
    fn test_manager_max_scorecards_zero() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            max_scorecards: 0,
            ..Default::default()
        });
        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }
        assert_eq!(manager.scorecard_count(), 0);
    }

    #[test]
    fn test_manager_max_scorecards_one() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            max_scorecards: 1,
            ..Default::default()
        });
        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("task-{}", i),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }
        assert_eq!(manager.scorecard_count(), 1);
    }

    // ==================== Manager config accessors ====================

    #[test]
    fn test_manager_get_config() {
        let manager = TaskScorecardManager::new();
        let config = manager.get_config();
        assert!(config.enabled);
        assert_eq!(config.max_scorecards, 500);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = TaskScorecardManager::new();
        let new_config = ScorecardConfig {
            enabled: false,
            max_scorecards: 10,
            ..Default::default()
        };
        manager.set_config(new_config);
        assert!(!manager.config.enabled);
        assert_eq!(manager.config.max_scorecards, 10);
    }

    #[test]
    fn test_manager_scorecard_count() {
        let mut manager = TaskScorecardManager::new();
        assert_eq!(manager.scorecard_count(), 0);
        let input = sample_input();
        manager.generate_scorecard(&input);
        assert_eq!(manager.scorecard_count(), 1);
    }

    // ==================== Assessment functions boundaries ====================

    #[test]
    fn test_assess_efficiency_all_ranges() {
        assert!(TaskScorecardManager::assess_efficiency(95.0).contains("Excellent"));
        assert!(TaskScorecardManager::assess_efficiency(80.0).contains("Good"));
        assert!(TaskScorecardManager::assess_efficiency(65.0).contains("Moderate"));
        assert!(TaskScorecardManager::assess_efficiency(45.0).contains("Below average"));
        assert!(TaskScorecardManager::assess_efficiency(20.0).contains("Very poor"));
    }

    #[test]
    fn test_assess_efficiency_boundaries() {
        assert!(TaskScorecardManager::assess_efficiency(90.0).contains("Excellent"));
        assert!(TaskScorecardManager::assess_efficiency(89.99).contains("Good"));
        assert!(TaskScorecardManager::assess_efficiency(75.0).contains("Good"));
        assert!(TaskScorecardManager::assess_efficiency(74.99).contains("Moderate"));
        assert!(TaskScorecardManager::assess_efficiency(60.0).contains("Moderate"));
        assert!(TaskScorecardManager::assess_efficiency(59.99).contains("Below average"));
        assert!(TaskScorecardManager::assess_efficiency(40.0).contains("Below average"));
        assert!(TaskScorecardManager::assess_efficiency(39.99).contains("Very poor"));
    }

    #[test]
    fn test_assess_speed_all_ranges() {
        assert!(TaskScorecardManager::assess_speed(95.0).contains("Consistently fast"));
        assert!(TaskScorecardManager::assess_speed(80.0).contains("Good speed"));
        assert!(TaskScorecardManager::assess_speed(65.0).contains("Moderate"));
        assert!(TaskScorecardManager::assess_speed(45.0).contains("Significant"));
        assert!(TaskScorecardManager::assess_speed(20.0).contains("Highly inconsistent"));
    }

    #[test]
    fn test_assess_stability_all_ranges() {
        assert!(TaskScorecardManager::assess_stability(95.0).contains("Very stable"));
        assert!(TaskScorecardManager::assess_stability(80.0).contains("Mostly stable"));
        assert!(TaskScorecardManager::assess_stability(65.0).contains("Some stability"));
        assert!(TaskScorecardManager::assess_stability(45.0).contains("Frequent"));
        assert!(TaskScorecardManager::assess_stability(20.0).contains("Highly unstable"));
    }

    #[test]
    fn test_assess_reliability_all_ranges() {
        assert!(TaskScorecardManager::assess_reliability(95.0).contains("Highly reliable"));
        assert!(TaskScorecardManager::assess_reliability(80.0).contains("Reliable"));
        assert!(TaskScorecardManager::assess_reliability(65.0).contains("Moderately"));
        assert!(TaskScorecardManager::assess_reliability(45.0).contains("Unreliable"));
        assert!(TaskScorecardManager::assess_reliability(20.0).contains("Very unreliable"));
    }

    #[test]
    fn test_assess_progress_all_ranges() {
        assert!(TaskScorecardManager::assess_progress(100.0).contains("complete"));
        assert!(TaskScorecardManager::assess_progress(80.0).contains("Nearly complete"));
        assert!(TaskScorecardManager::assess_progress(55.0).contains("Good progress"));
        assert!(TaskScorecardManager::assess_progress(30.0).contains("Making progress"));
        assert!(TaskScorecardManager::assess_progress(10.0).contains("Early stage"));
    }

    #[test]
    fn test_assess_progress_boundaries() {
        assert!(TaskScorecardManager::assess_progress(100.0).contains("complete"));
        assert!(TaskScorecardManager::assess_progress(99.99).contains("Nearly complete"));
        assert!(TaskScorecardManager::assess_progress(75.0).contains("Nearly complete"));
        assert!(TaskScorecardManager::assess_progress(74.99).contains("Good progress"));
        assert!(TaskScorecardManager::assess_progress(50.0).contains("Good progress"));
        assert!(TaskScorecardManager::assess_progress(49.99).contains("Making progress"));
        assert!(TaskScorecardManager::assess_progress(25.0).contains("Making progress"));
        assert!(TaskScorecardManager::assess_progress(24.99).contains("Early stage"));
    }

    // ==================== Recommendation logic ====================

    #[test]
    fn test_recommendations_speed_low() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "speed-low".to_string(),
            task_name: "slow.zip".to_string(),
            avg_speed_bps: 1_000_000.0,
            peak_speed_bps: 5_000_000.0, // ratio 0.2 → speed score 20
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(
            card.recommendations
                .iter()
                .any(|r| r.contains("mirror sources"))
        );
    }

    #[test]
    fn test_recommendations_stability_low_with_stalls() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "unstable".to_string(),
            task_name: "unstable.zip".to_string(),
            stall_count: 10,
            retry_count: 5,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.recommendations.iter().any(|r| r.contains("stalled")));
        assert!(card.recommendations.iter().any(|r| r.contains("Retried")));
    }

    #[test]
    fn test_recommendations_reliability_low_with_domain() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "unrel".to_string(),
            task_name: "unrel.zip".to_string(),
            source_domain: Some("bad-host.com".to_string()),
            source_reliability_score: Some(0.1),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(
            card.recommendations
                .iter()
                .any(|r| r.contains("bad-host.com"))
        );
    }

    #[test]
    fn test_recommendations_reliability_low_without_domain() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "unrel-nodomain".to_string(),
            task_name: "unrel2.zip".to_string(),
            source_domain: None,
            source_reliability_score: Some(0.1),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(
            card.recommendations
                .iter()
                .any(|r| r.contains("trusted mirrors"))
        );
    }

    #[test]
    fn test_recommendations_efficiency_low() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "low-eff".to_string(),
            task_name: "inefficient.zip".to_string(),
            efficiency_score: 30.0,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(
            card.recommendations
                .iter()
                .any(|r| r.contains("efficiency"))
        );
    }

    // ==================== Persistence ====================

    #[test]
    fn test_persistence_config_missing_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = TaskScorecardManager::load_config(Path::new(
                "/tmp/nonexistent_scorecard_config.json",
            ))
            .await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_persistence_config_corrupt_json() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_corrupt_config.json");
            tokio::fs::write(path, "not valid json{{{").await.unwrap();
            let result = TaskScorecardManager::load_config(path).await;
            assert!(result.is_err());
            tokio::fs::remove_file(path).await.ok();
        });
    }

    #[test]
    fn test_persistence_config_overwrite() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_overwrite_config.json");
            let manager1 = TaskScorecardManager::new();
            manager1.save_config(path).await.unwrap();

            let manager2 = TaskScorecardManager::with_config(ScorecardConfig {
                max_scorecards: 42,
                ..Default::default()
            });
            manager2.save_config(path).await.unwrap();

            let loaded = TaskScorecardManager::load_config(path).await.unwrap();
            assert_eq!(loaded.max_scorecards, 42);

            tokio::fs::remove_file(path).await.ok();
        });
    }

    #[test]
    fn test_persistence_data_missing_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScorecardManager::new();
            let result = manager
                .load_data(Path::new("/tmp/nonexistent_scorecard_data.json"))
                .await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_persistence_data_corrupt_json() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_corrupt_data.json");
            tokio::fs::write(path, "corrupt data!!!").await.unwrap();
            let mut manager = TaskScorecardManager::new();
            let result = manager.load_data(path).await;
            assert!(result.is_err());
            tokio::fs::remove_file(path).await.ok();
        });
    }

    #[test]
    fn test_persistence_data_no_tmp_residue() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_no_tmp.json");
            let manager = TaskScorecardManager::new();
            manager.save_data(path).await.unwrap();

            // Check no .tmp file left behind
            let tmp_path = Path::new("/tmp/test_scorecard_no_tmp.json.tmp");
            assert!(!tmp_path.exists());

            tokio::fs::remove_file(path).await.ok();
        });
    }

    #[test]
    fn test_persistence_empty_data_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_empty_data.json");
            let manager = TaskScorecardManager::new();
            manager.save_data(path).await.unwrap();

            let mut loaded = TaskScorecardManager::new();
            loaded.load_data(path).await.unwrap();
            assert_eq!(loaded.scorecard_count(), 0);

            tokio::fs::remove_file(path).await.ok();
        });
    }

    // ==================== Unicode ====================

    #[test]
    fn test_unicode_task_id_emoji() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "🎉-emoji-task".to_string(),
            task_name: "🚀rocket.zip".to_string(),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert_eq!(card.task_id, "🎉-emoji-task");
        assert!(card.summary.contains("🚀rocket.zip"));
    }

    #[test]
    fn test_unicode_chinese_task_name() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "cn-task".to_string(),
            task_name: "中文下载文件.zip".to_string(),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert_eq!(card.task_name, "中文下载文件.zip");
    }

    #[test]
    fn test_unicode_japanese_task_name() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "jp-task".to_string(),
            task_name: "日本語ダウンロード.zip".to_string(),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert_eq!(card.task_name, "日本語ダウンロード.zip");
    }

    // ==================== Boundary conditions ====================

    #[test]
    fn test_zero_bytes_download() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "zero-bytes".to_string(),
            task_name: "empty.zip".to_string(),
            total_bytes: 0,
            downloaded_bytes: 0,
            progress_pct: 0.0,
            avg_speed_bps: 0.0,
            peak_speed_bps: 0.0,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.composite_score >= 0.0);
        assert!(card.composite_score <= 100.0);
    }

    #[test]
    fn test_complete_task() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "complete".to_string(),
            task_name: "done.zip".to_string(),
            is_complete: true,
            progress_pct: 100.0,
            efficiency_score: 95.0,
            avg_speed_bps: 10_000_000.0,
            peak_speed_bps: 10_000_000.0,
            source_reliability_score: Some(0.95),
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.summary.contains("Complete"));
        assert!(card.composite_score > 80.0);
    }

    #[test]
    fn test_negative_speed_values_clamped() {
        let manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            avg_speed_bps: -1000.0,
            peak_speed_bps: 5_000_000.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!(score >= 0.0);
    }

    #[test]
    fn test_efficiency_score_clamped() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "clamp-test".to_string(),
            efficiency_score: 150.0, // over 100
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        let eff_dim = card
            .dimensions
            .iter()
            .find(|d| d.name == "Efficiency")
            .unwrap();
        assert!(eff_dim.score <= 100.0);
    }

    #[test]
    fn test_efficiency_score_negative_clamped() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "neg-clamp".to_string(),
            efficiency_score: -50.0,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        let eff_dim = card
            .dimensions
            .iter()
            .find(|d| d.name == "Efficiency")
            .unwrap();
        assert!(eff_dim.score >= 0.0);
    }

    #[test]
    fn test_progress_pct_clamped() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "prog-clamp".to_string(),
            progress_pct: 150.0,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        let prog_dim = card
            .dimensions
            .iter()
            .find(|d| d.name == "Progress")
            .unwrap();
        assert!(prog_dim.score <= 100.0);
    }

    #[test]
    fn test_reliability_score_clamped() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "rel-clamp".to_string(),
            source_reliability_score: Some(1.5), // over 1.0
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        let rel_dim = card
            .dimensions
            .iter()
            .find(|d| d.name == "Reliability")
            .unwrap();
        assert!(rel_dim.score <= 100.0);
    }

    // ==================== Source reliability tier mapping ====================

    #[test]
    fn test_reliability_tier_all_boundaries() {
        let mut manager = TaskScorecardManager::new();

        let tiers = vec![
            (0.9, "Excellent"),
            (0.8, "Excellent"),
            (0.7, "Good"),
            (0.6, "Good"),
            (0.5, "Fair"),
            (0.4, "Fair"),
            (0.3, "Poor"),
            (0.2, "Poor"),
            (0.1, "Unreliable"),
            (0.0, "Unreliable"),
        ];
        for (score, expected_tier) in tiers {
            let input = ScorecardInput {
                task_id: format!("tier-{}", score),
                source_reliability_score: Some(score),
                ..Default::default()
            };
            let card = manager.generate_scorecard(&input).unwrap();
            assert_eq!(
                card.source_reliability_tier.as_deref(),
                Some(expected_tier),
                "score {} should map to tier {}",
                score,
                expected_tier
            );
        }
    }

    #[test]
    fn test_reliability_tier_none_when_no_data() {
        let mut manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            task_id: "no-rel".to_string(),
            source_reliability_score: None,
            ..Default::default()
        };
        let card = manager.generate_scorecard(&input).unwrap();
        assert!(card.source_reliability_tier.is_none());
    }

    // ==================== Summary format ====================

    #[test]
    fn test_format_summary_all_sections() {
        let mut manager = TaskScorecardManager::new();

        let excellent = ScorecardInput {
            task_id: "excellent".to_string(),
            task_name: "excellent.zip".to_string(),
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
            task_name: "poor.zip".to_string(),
            efficiency_score: 10.0,
            progress_pct: 5.0,
            stall_count: 50,
            retry_count: 20,
            error_count: 10,
            source_reliability_score: Some(0.05),
            avg_speed_bps: 100.0,
            peak_speed_bps: 5_000_000.0,
            ..Default::default()
        };
        manager.generate_scorecard(&poor);

        let summary = manager.get_summary();
        let formatted = TaskScorecardManager::format_summary(&summary);

        assert!(formatted.contains("Task Scorecard Summary"));
        assert!(formatted.contains("Average Score"));
        assert!(formatted.contains("Excellent"));
        assert!(formatted.contains("Failing"));
        assert!(formatted.contains("Best:"));
        assert!(formatted.contains("Worst:"));
        assert!(formatted.contains("Average Dimension Scores"));
    }

    #[test]
    fn test_format_summary_no_recommendations() {
        let mut manager = TaskScorecardManager::with_config(ScorecardConfig {
            include_recommendations: false,
            ..Default::default()
        });
        let input = sample_input();
        manager.generate_scorecard(&input);

        let summary = manager.get_summary();
        let formatted = TaskScorecardManager::format_summary(&summary);
        // Should not contain recommendations section when empty
        assert!(formatted.contains("Task Scorecard Summary"));
    }

    // ==================== Grade distribution ====================

    #[test]
    fn test_grade_distribution_counts_all() {
        let mut manager = TaskScorecardManager::new();

        // Create tasks with different grades
        let excellent = ScorecardInput {
            task_id: "a-plus".to_string(),
            efficiency_score: 99.0,
            progress_pct: 100.0,
            avg_speed_bps: 5_000_000.0,
            peak_speed_bps: 5_000_000.0,
            source_reliability_score: Some(0.99),
            ..Default::default()
        };
        manager.generate_scorecard(&excellent);

        let poor = ScorecardInput {
            task_id: "f-grade".to_string(),
            efficiency_score: 5.0,
            progress_pct: 2.0,
            stall_count: 50,
            retry_count: 20,
            error_count: 10,
            source_reliability_score: Some(0.01),
            ..Default::default()
        };
        manager.generate_scorecard(&poor);

        let summary = manager.get_summary();
        assert!(summary.grade_distribution.contains_key("APlus"));
        assert_eq!(*summary.grade_distribution.get("APlus").unwrap(), 1);
    }

    // ==================== Complex workflow ====================

    #[test]
    fn test_full_lifecycle() {
        let mut manager = TaskScorecardManager::new();

        // Generate scorecards
        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("lifecycle-{}", i),
                task_name: format!("file-{}.zip", i),
                efficiency_score: 40.0 + (i as f64) * 15.0,
                progress_pct: 20.0 + (i as f64) * 20.0,
                avg_speed_bps: 1_000_000.0 + (i as f64) * 1_000_000.0,
                peak_speed_bps: 5_000_000.0,
                source_reliability_score: Some(0.3 + (i as f64) * 0.15),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }
        assert_eq!(manager.scorecard_count(), 5);

        // Get summary
        let summary = manager.get_summary();
        assert_eq!(summary.total_scorecards, 5);
        assert!(summary.best_task.is_some());
        assert!(summary.worst_task.is_some());

        // Remove one
        manager.remove_scorecard("lifecycle-0");
        assert_eq!(manager.scorecard_count(), 4);

        // Clear all
        manager.clear_all();
        assert_eq!(manager.scorecard_count(), 0);

        // Summary after clear
        let summary = manager.get_summary();
        assert_eq!(summary.total_scorecards, 0);
    }

    #[test]
    fn test_multi_task_independent_scores() {
        let mut manager = TaskScorecardManager::new();

        let fast = ScorecardInput {
            task_id: "fast".to_string(),
            task_name: "fast.zip".to_string(),
            efficiency_score: 95.0,
            progress_pct: 100.0,
            avg_speed_bps: 10_000_000.0,
            peak_speed_bps: 10_000_000.0,
            source_reliability_score: Some(0.95),
            ..Default::default()
        };
        manager.generate_scorecard(&fast);

        let slow = ScorecardInput {
            task_id: "slow".to_string(),
            task_name: "slow.zip".to_string(),
            efficiency_score: 20.0,
            progress_pct: 10.0,
            stall_count: 20,
            retry_count: 10,
            error_count: 5,
            avg_speed_bps: 100.0,
            peak_speed_bps: 5_000_000.0,
            source_reliability_score: Some(0.1),
            ..Default::default()
        };
        manager.generate_scorecard(&slow);

        let fast_card = manager.get_scorecard("fast").unwrap();
        let slow_card = manager.get_scorecard("slow").unwrap();
        assert!(fast_card.composite_score > slow_card.composite_score);
    }

    // ==================== mini_bar ====================

    #[test]
    fn test_mini_bar_boundaries() {
        let bar_zero = TaskScorecardManager::mini_bar(0.0);
        assert!(bar_zero.contains("░"));
        assert!(!bar_zero.contains("█"));

        let bar_full = TaskScorecardManager::mini_bar(100.0);
        assert!(bar_full.contains("█"));
        assert!(!bar_full.contains("░"));

        let bar_half = TaskScorecardManager::mini_bar(50.0);
        assert!(bar_half.contains("█"));
        assert!(bar_half.contains("░"));
    }

    // ==================== dim_emoji ====================

    #[test]
    fn test_dim_emoji_all_known() {
        assert_eq!(TaskScorecardManager::dim_emoji("Efficiency"), "⚡");
        assert_eq!(TaskScorecardManager::dim_emoji("Speed"), "🚀");
        assert_eq!(TaskScorecardManager::dim_emoji("Stability"), "🛡️");
        assert_eq!(TaskScorecardManager::dim_emoji("Reliability"), "🔗");
        assert_eq!(TaskScorecardManager::dim_emoji("Progress"), "📊");
    }

    #[test]
    fn test_dim_emoji_unknown() {
        assert_eq!(TaskScorecardManager::dim_emoji("Unknown"), "📋");
        assert_eq!(TaskScorecardManager::dim_emoji(""), "📋");
    }

    // ==================== Stability score boundaries ====================

    #[test]
    fn test_stability_score_exact_penalties() {
        let manager = TaskScorecardManager::new();

        // 1 stall = -10
        let input = ScorecardInput {
            stall_count: 1,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 90.0).abs() < 0.01);

        // 1 retry = -8
        let input = ScorecardInput {
            retry_count: 1,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 92.0).abs() < 0.01);

        // 1 error = -15
        let input = ScorecardInput {
            error_count: 1,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 85.0).abs() < 0.01);
    }

    #[test]
    fn test_stability_score_max_penalty_caps() {
        let manager = TaskScorecardManager::new();

        // Stalls cap at -40 (4 stalls)
        let input = ScorecardInput {
            stall_count: 4,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 60.0).abs() < 0.01);

        // 5 stalls still -40 (capped)
        let input = ScorecardInput {
            stall_count: 5,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 60.0).abs() < 0.01);

        // Retries cap at -30 (4 * 8 = 32, capped at 30)
        let input = ScorecardInput {
            retry_count: 4,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 70.0).abs() < 0.01); // 100 - 30 (capped) = 70

        // Errors cap at -30 (2 errors)
        let input = ScorecardInput {
            error_count: 2,
            ..Default::default()
        };
        let score = manager.calculate_stability_score(&input);
        assert!((score - 70.0).abs() < 0.01); // 100 - 30 = 70
    }

    // ==================== Speed score boundaries ====================

    #[test]
    fn test_speed_score_negative_peak() {
        let manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            avg_speed_bps: 1000.0,
            peak_speed_bps: -1.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!((score - 50.0).abs() < 0.01); // neutral for non-positive peak
    }

    #[test]
    fn test_speed_score_avg_exceeds_peak() {
        let manager = TaskScorecardManager::new();
        let input = ScorecardInput {
            avg_speed_bps: 10_000_000.0,
            peak_speed_bps: 5_000_000.0,
            ..Default::default()
        };
        let score = manager.calculate_speed_score(&input);
        assert!(score >= 100.0); // ratio > 1.0, clamped to 100
    }

    // ==================== Top recommendations in summary ====================

    #[test]
    fn test_top_recommendations_sorted_by_count() {
        let mut manager = TaskScorecardManager::new();

        // Create multiple tasks with same recommendation
        for i in 0..5 {
            let input = ScorecardInput {
                task_id: format!("rec-task-{}", i),
                task_name: format!("rec-{}.zip", i),
                stall_count: 10,
                retry_count: 5,
                ..Default::default()
            };
            manager.generate_scorecard(&input);
        }

        let summary = manager.get_summary();
        // Top recommendations should be sorted by count descending
        if summary.top_recommendations.len() >= 2 {
            assert!(summary.top_recommendations[0].1 >= summary.top_recommendations[1].1);
        }
    }

    // ==================== Manager Clone ====================

    #[test]
    fn test_manager_clone() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);

        let cloned = manager.clone();
        assert_eq!(cloned.scorecard_count(), manager.scorecard_count());
        assert!(cloned.get_scorecard("task-1").is_some());
    }

    #[test]
    fn test_manager_clone_independence() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);

        let mut cloned = manager.clone();
        cloned.clear_all();
        assert_eq!(manager.scorecard_count(), 1); // original unaffected
        assert_eq!(cloned.scorecard_count(), 0);
    }

    // ==================== ScorecardError ====================

    #[test]
    fn test_scorecard_error_display() {
        let io_err = ScorecardError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        let display = format!("{}", io_err);
        assert!(display.contains("file not found") || display.contains("I/O error"));

        let json_err =
            ScorecardError::Json(serde_json::from_str::<ScorecardConfig>("invalid").unwrap_err());
        let display = format!("{}", json_err);
        assert!(display.contains("JSON") || display.contains("json"));
    }

    #[test]
    fn test_scorecard_error_debug() {
        let io_err = ScorecardError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        let debug = format!("{:?}", io_err);
        assert!(debug.contains("Io"));
    }

    // ==================== Persistence with Unicode ====================

    #[test]
    fn test_persistence_unicode_task_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = Path::new("/tmp/test_scorecard_unicode.json");
            let mut manager = TaskScorecardManager::new();
            let input = ScorecardInput {
                task_id: "unicode-中文-🎉".to_string(),
                task_name: "unicode-file.zip".to_string(),
                ..Default::default()
            };
            manager.generate_scorecard(&input);
            manager.save_data(path).await.unwrap();

            let mut loaded = TaskScorecardManager::new();
            loaded.load_data(path).await.unwrap();
            assert!(loaded.get_scorecard("unicode-中文-🎉").is_some());

            tokio::fs::remove_file(path).await.ok();
        });
    }

    // ==================== Summary avg_dimensions ====================

    #[test]
    fn test_avg_dimensions_correct() {
        let mut manager = TaskScorecardManager::new();

        let input1 = ScorecardInput {
            task_id: "dim-1".to_string(),
            efficiency_score: 80.0,
            progress_pct: 60.0,
            ..Default::default()
        };
        manager.generate_scorecard(&input1);

        let input2 = ScorecardInput {
            task_id: "dim-2".to_string(),
            efficiency_score: 60.0,
            progress_pct: 80.0,
            ..Default::default()
        };
        manager.generate_scorecard(&input2);

        let summary = manager.get_summary();
        // Efficiency avg should be ~70 (80+60)/2
        let eff_avg = summary.avg_dimensions.get("Efficiency").unwrap();
        assert!((eff_avg - 70.0).abs() < 1.0);

        // Progress avg should be ~70 (60+80)/2
        let prog_avg = summary.avg_dimensions.get("Progress").unwrap();
        assert!((prog_avg - 70.0).abs() < 1.0);
    }

    // ==================== get_top_performers limit ====================

    #[test]
    fn test_top_performers_more_than_available() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        manager.generate_scorecard(&input);
        let top = manager.get_top_performers(100);
        assert_eq!(top.len(), 1);
    }

    // ==================== Scorecard generated_at timestamp ====================

    #[test]
    fn test_scorecard_has_timestamp() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();
        // generated_at should be recent (within last minute)
        let now = chrono::Utc::now();
        let diff = (now - card.generated_at).num_seconds();
        assert!(diff < 60);
    }

    // ==================== Composite score calculation ====================

    #[test]
    fn test_composite_score_is_sum_of_weighted() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();

        let expected_composite: f64 = card.dimensions.iter().map(|d| d.weighted_score).sum();
        assert!(
            (card.composite_score - expected_composite).abs() < 0.01,
            "composite {} != sum of weighted {}",
            card.composite_score,
            expected_composite
        );
    }

    #[test]
    fn test_dimension_weighted_score_correct() {
        let mut manager = TaskScorecardManager::new();
        let input = sample_input();
        let card = manager.generate_scorecard(&input).unwrap();

        for dim in &card.dimensions {
            let expected = dim.score * dim.weight;
            assert!(
                (dim.weighted_score - expected).abs() < 0.01,
                "dimension {} weighted_score {} != score {} * weight {}",
                dim.name,
                dim.weighted_score,
                dim.score,
                dim.weight
            );
        }
    }
}
