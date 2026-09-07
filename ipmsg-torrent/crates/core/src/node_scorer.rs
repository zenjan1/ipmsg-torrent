//! Node scoring system for evaluating connection quality.
//!
//! Tracks per-node metrics (latency, reliability, availability, freshness) and
//! computes a weighted composite score used by `SmartBootstrap` and
//! `RelayDiscovery` to rank candidate nodes.

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Weight for latency component in composite score.
const WEIGHT_LATENCY: f64 = 0.4;
/// Weight for reliability component in composite score.
const WEIGHT_RELIABILITY: f64 = 0.3;
/// Weight for availability component in composite score.
const WEIGHT_AVAILABILITY: f64 = 0.2;
/// Weight for freshness component in composite score.
const WEIGHT_FRESHNESS: f64 = 0.1;

/// RTT below this value is considered perfect latency.
const LATENCY_BEST_MS: f64 = 50.0;
/// RTT above this value scores zero.
const LATENCY_WORST_MS: f64 = 5000.0;

/// How long before a node is considered stale (seconds).
const STALE_AGE_SECS: u64 = 3600;

/// Number of RTT samples kept for rolling average.
const MAX_RTT_SAMPLES: usize = 20;

// ---------------------------------------------------------------------------
// NodeScore
// ---------------------------------------------------------------------------

/// Composite score record for a single node.
#[derive(Debug, Clone)]
pub struct NodeScore {
    /// Peer / node identifier (multiaddr string or PeerId).
    pub node_id: String,
    /// Rolling average RTT in milliseconds.
    pub avg_rtt_ms: f64,
    /// Individual RTT samples (newest last).
    pub rtt_samples: Vec<f64>,
    /// Connection attempts.
    pub attempts: u64,
    /// Successful connections.
    pub successes: u64,
    /// Consecutive failures (reset on success).
    pub consecutive_failures: u32,
    /// Number of times this node was available (responded to ping/dial).
    pub availability_hits: u64,
    /// Number of availability probes.
    pub availability_probes: u64,
    /// Last time we successfully connected.
    pub last_success: Option<Instant>,
    /// Last time we attempted a connection.
    pub last_attempt: Option<Instant>,
    /// Optional geographic region hint (e.g. "cn-beijing").
    pub region: Option<String>,
    /// Computed composite score (0.0 – 1.0, higher is better).
    pub score: f64,
}

impl NodeScore {
    fn new(node_id: String) -> Self {
        Self {
            node_id,
            avg_rtt_ms: 0.0,
            rtt_samples: Vec::new(),
            attempts: 0,
            successes: 0,
            consecutive_failures: 0,
            availability_hits: 0,
            availability_probes: 0,
            last_success: None,
            last_attempt: None,
            region: None,
            score: 0.0,
        }
    }

    /// Connection success rate (0.0 – 1.0).
    pub fn success_rate(&self) -> f64 {
        if self.attempts == 0 {
            return 0.5; // neutral prior
        }
        self.successes as f64 / self.attempts as f64
    }

    /// Availability ratio (0.0 – 1.0).
    pub fn availability(&self) -> f64 {
        if self.availability_probes == 0 {
            return 0.5;
        }
        self.availability_hits as f64 / self.availability_probes as f64
    }

    /// How recently the node was successfully reached (0.0 – 1.0).
    fn freshness(&self) -> f64 {
        match self.last_success {
            Some(t) => {
                let age = t.elapsed().as_secs() as f64;
                if age >= STALE_AGE_SECS as f64 {
                    0.0
                } else {
                    1.0 - age / STALE_AGE_SECS as f64
                }
            }
            None => 0.0,
        }
    }

    /// Latency score (0.0 – 1.0, lower RTT → higher score).
    fn latency_score(&self) -> f64 {
        if self.avg_rtt_ms <= 0.0 {
            return 0.5; // unknown
        }
        if self.avg_rtt_ms <= LATENCY_BEST_MS {
            return 1.0;
        }
        if self.avg_rtt_ms >= LATENCY_WORST_MS {
            return 0.0;
        }
        1.0 - (self.avg_rtt_ms - LATENCY_BEST_MS) / (LATENCY_WORST_MS - LATENCY_BEST_MS)
    }

    /// Recompute the composite score.
    pub fn recompute_score(&mut self) {
        let latency = self.latency_score();
        let reliability = self.success_rate();
        let availability = self.availability();
        let freshness = self.freshness();

        self.score = WEIGHT_LATENCY * latency
            + WEIGHT_RELIABILITY * reliability
            + WEIGHT_AVAILABILITY * availability
            + WEIGHT_FRESHNESS * freshness;
    }
}

// ---------------------------------------------------------------------------
// NodeScorer
// ---------------------------------------------------------------------------

/// Manages scoring for all known nodes.
pub struct NodeScorer {
    scores: HashMap<String, NodeScore>,
}

impl NodeScorer {
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    /// Get or create a score record for `node_id`.
    pub fn get_or_create(&mut self, node_id: &str) -> &mut NodeScore {
        self.scores
            .entry(node_id.to_string())
            .or_insert_with(|| NodeScore::new(node_id.to_string()))
    }

    /// Record a connection attempt.
    pub fn record_attempt(&mut self, node_id: &str) {
        let entry = self.get_or_create(node_id);
        entry.attempts += 1;
        entry.last_attempt = Some(Instant::now());
        entry.recompute_score();
    }

    /// Record a successful connection with measured RTT.
    pub fn record_success(&mut self, node_id: &str, rtt: Duration) {
        let entry = self.get_or_create(node_id);
        entry.successes += 1;
        entry.consecutive_failures = 0;
        entry.last_success = Some(Instant::now());

        let rtt_ms = rtt.as_secs_f64() * 1000.0;
        entry.rtt_samples.push(rtt_ms);
        if entry.rtt_samples.len() > MAX_RTT_SAMPLES {
            entry.rtt_samples.remove(0);
        }
        // Rolling average
        let sum: f64 = entry.rtt_samples.iter().sum();
        entry.avg_rtt_ms = sum / entry.rtt_samples.len() as f64;

        entry.recompute_score();
    }

    /// Record a failed connection.
    pub fn record_failure(&mut self, node_id: &str) {
        let entry = self.get_or_create(node_id);
        entry.consecutive_failures += 1;
        entry.recompute_score();
    }

    /// Record an availability probe result.
    pub fn record_availability(&mut self, node_id: &str, available: bool) {
        let entry = self.get_or_create(node_id);
        entry.availability_probes += 1;
        if available {
            entry.availability_hits += 1;
        }
        entry.recompute_score();
    }

    /// Set optional geographic region for a node.
    pub fn set_region(&mut self, node_id: &str, region: String) {
        let entry = self.get_or_create(node_id);
        entry.region = Some(region);
    }

    /// Get a read-only reference to a node's score.
    pub fn get_score(&self, node_id: &str) -> Option<&NodeScore> {
        self.scores.get(node_id)
    }

    /// Return all node IDs sorted by score (descending).
    pub fn ranked_nodes(&self) -> Vec<(String, f64)> {
        let mut list: Vec<(String, f64)> = self
            .scores
            .iter()
            .map(|(id, s)| (id.clone(), s.score))
            .collect();
        list.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        list
    }

    /// Return top-N ranked node IDs.
    pub fn top_nodes(&self, n: usize) -> Vec<(String, f64)> {
        self.ranked_nodes().into_iter().take(n).collect()
    }

    /// Number of tracked nodes.
    pub fn node_count(&self) -> usize {
        self.scores.len()
    }

    /// Remove nodes that have not been seen for a long time.
    pub fn cleanup_stale(&mut self) {
        self.scores.retain(|_, s| {
            s.last_attempt
                .map(|t| t.elapsed().as_secs() < STALE_AGE_SECS)
                .unwrap_or(false)
        });
    }

    /// Get all scores as a serializable snapshot.
    pub fn snapshot(&self) -> Vec<NodeScoreSnapshot> {
        self.scores
            .values()
            .map(|s| NodeScoreSnapshot {
                node_id: s.node_id.clone(),
                avg_rtt_ms: s.avg_rtt_ms,
                attempts: s.attempts,
                successes: s.successes,
                score: s.score,
                region: s.region.clone(),
                last_success_age_secs: s.last_success.map(|t| t.elapsed().as_secs()),
            })
            .collect()
    }

    /// Restore scores from a snapshot (e.g. loaded from database).
    pub fn restore(&mut self, snapshots: Vec<NodeScoreSnapshot>) {
        for snap in snapshots {
            let entry = self.get_or_create(&snap.node_id);
            entry.avg_rtt_ms = snap.avg_rtt_ms;
            entry.attempts = snap.attempts;
            entry.successes = snap.successes;
            entry.score = snap.score;
            entry.region = snap.region;
        }
    }
}

impl Default for NodeScorer {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of a node score (for persistence).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeScoreSnapshot {
    pub node_id: String,
    pub avg_rtt_ms: f64,
    pub attempts: u64,
    pub successes: u64,
    pub score: f64,
    pub region: Option<String>,
    pub last_success_age_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_scorer_empty() {
        let scorer = NodeScorer::new();
        assert_eq!(scorer.node_count(), 0);
        assert!(scorer.ranked_nodes().is_empty());
    }

    #[test]
    fn test_record_attempt_creates_entry() {
        let mut scorer = NodeScorer::new();
        scorer.record_attempt("node-a");
        assert_eq!(scorer.node_count(), 1);
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.attempts, 1);
        assert_eq!(score.successes, 0);
    }

    #[test]
    fn test_record_success_updates_rtt() {
        let mut scorer = NodeScorer::new();
        scorer.record_success("node-a", Duration::from_millis(100));
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.successes, 1);
        assert!((score.avg_rtt_ms - 100.0).abs() < 0.1);
        assert!(score.score > 0.0);
    }

    #[test]
    fn test_record_failure_increments_consecutive() {
        let mut scorer = NodeScorer::new();
        scorer.record_failure("node-a");
        scorer.record_failure("node-a");
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.consecutive_failures, 2);
        assert_eq!(score.successes, 0);
    }

    #[test]
    fn test_success_resets_consecutive_failures() {
        let mut scorer = NodeScorer::new();
        scorer.record_failure("node-a");
        scorer.record_failure("node-a");
        scorer.record_success("node-a", Duration::from_millis(50));
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.consecutive_failures, 0);
    }

    #[test]
    fn test_ranked_nodes_sorted() {
        let mut scorer = NodeScorer::new();
        // node-a: good latency, 100% success
        scorer.record_success("node-a", Duration::from_millis(30));
        // node-b: bad latency, 50% success
        scorer.record_attempt("node-b");
        scorer.record_success("node-b", Duration::from_millis(3000));
        // node-c: no attempts → neutral
        scorer.get_or_create("node-c");

        let ranked = scorer.ranked_nodes();
        assert_eq!(ranked.len(), 3);
        // node-a should be first (best latency + 100% success)
        assert_eq!(ranked[0].0, "node-a");
    }

    #[test]
    fn test_top_nodes_limit() {
        let mut scorer = NodeScorer::new();
        for i in 0..10 {
            scorer.record_success(&format!("node-{}", i), Duration::from_millis(50 + i as u64 * 10));
        }
        assert_eq!(scorer.top_nodes(3).len(), 3);
    }

    #[test]
    fn test_availability_probe() {
        let mut scorer = NodeScorer::new();
        scorer.record_availability("node-a", true);
        scorer.record_availability("node-a", true);
        scorer.record_availability("node-a", false);
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.availability_probes, 3);
        assert_eq!(score.availability_hits, 2);
        assert!((score.availability() - 2.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_latency_score_bounds() {
        let mut scorer = NodeScorer::new();
        // Perfect latency
        scorer.record_success("fast", Duration::from_millis(10));
        let s = scorer.get_score("fast").unwrap();
        assert!((s.latency_score() - 1.0).abs() < 0.01);

        // Terrible latency
        scorer.record_success("slow", Duration::from_millis(6000));
        let s = scorer.get_score("slow").unwrap();
        assert!((s.latency_score() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_snapshot_restore() {
        let mut scorer = NodeScorer::new();
        scorer.record_success("node-a", Duration::from_millis(100));
        scorer.record_success("node-b", Duration::from_millis(200));
        let snap = scorer.snapshot();
        assert_eq!(snap.len(), 2);

        let mut scorer2 = NodeScorer::new();
        scorer2.restore(snap);
        assert_eq!(scorer2.node_count(), 2);
        assert!(scorer2.get_score("node-a").is_some());
    }

    #[test]
    fn test_set_region() {
        let mut scorer = NodeScorer::new();
        scorer.set_region("node-a", "cn-beijing".to_string());
        let score = scorer.get_score("node-a").unwrap();
        assert_eq!(score.region.as_deref(), Some("cn-beijing"));
    }

    #[test]
    fn test_empty_node_list_top_nodes() {
        let scorer = NodeScorer::new();
        assert!(scorer.top_nodes(5).is_empty());
    }

    #[test]
    fn test_score_composition() {
        let mut scorer = NodeScorer::new();
        // 100% success, good latency → high score
        for _ in 0..5 {
            scorer.record_success("good", Duration::from_millis(30));
        }
        let s = scorer.get_score("good").unwrap();
        assert!(s.score > 0.8, "expected high score, got {}", s.score);
    }
}
