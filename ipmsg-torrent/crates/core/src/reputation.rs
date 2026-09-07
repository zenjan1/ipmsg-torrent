//! Peer reputation system for node selection optimization.
//!
//! Tracks peer behavior across multiple dimensions (message quality, response latency,
//! file sharing, uptime, violations) and produces a normalized 0.0–1.0 score used to
//! prioritize peers for message routing and file downloads.

use chrono::{DateTime, Utc};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum score before a peer is automatically blocked.
pub const BLOCK_THRESHOLD: f64 = 0.2;

/// Weight constants for the weighted-average score calculation.
const WEIGHT_MESSAGE_QUALITY: f64 = 0.25;
const WEIGHT_RESPONSE_SPEED: f64 = 0.20;
const WEIGHT_FILE_SHARING: f64 = 0.20;
const WEIGHT_UPTIME: f64 = 0.15;
const WEIGHT_VIOLATIONS: f64 = 0.20;

/// Latency thresholds (ms) for response speed scoring.
const FAST_RESPONSE_MS: f64 = 200.0;
const SLOW_RESPONSE_MS: f64 = 5000.0;

/// Maximum violations before the violation component hits zero.
const MAX_VIOLATIONS: f64 = 10.0;

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Events that influence a peer's reputation.
#[derive(Debug, Clone)]
pub enum ReputationEvent {
    /// Peer sent a valid, non-duplicate message.
    ValidMessage,
    /// Peer sent a message we already have.
    DuplicateMessage,
    /// Peer responded quickly to a request.
    FastResponse { latency_ms: u64 },
    /// Peer responded slowly to a request.
    SlowResponse { latency_ms: u64 },
    /// Peer shared a file with the network.
    FileShared,
    /// Peer served a file download to us.
    FileDownloaded,
    /// Peer committed a protocol violation (spam, malformed payload, etc.).
    Violation { severity: u32 },
}

// ---------------------------------------------------------------------------
// PeerReputation
// ---------------------------------------------------------------------------

/// Per-peer reputation record.
#[derive(Debug, Clone)]
pub struct PeerReputation {
    pub peer_id: String,
    /// Composite score in [0.0, 1.0].
    pub score: f64,
    /// Message quality ratio (valid / total) in [0.0, 1.0].
    pub message_quality: f64,
    /// Exponential moving average of response latency in milliseconds.
    pub response_latency_ms: u64,
    /// Number of files this peer has shared.
    pub file_shares: u32,
    /// Number of times this peer served a file download.
    pub file_downloads: u32,
    /// Cumulative online hours (connection stability proxy).
    pub uptime_hours: f64,
    /// Number of recorded violations.
    pub violations: u32,
    pub last_updated: DateTime<Utc>,

    // Internal counters used to derive the public fields.
    total_messages: u64,
    valid_messages: u64,
    latency_samples: u64,
    connected_since: Option<DateTime<Utc>>,
}

impl PeerReputation {
    /// Create a fresh reputation record for a new peer.
    pub fn new(peer_id: String) -> Self {
        let now = Utc::now();
        Self {
            peer_id,
            score: 0.5, // start at neutral
            message_quality: 0.5,
            response_latency_ms: 0,
            file_shares: 0,
            file_downloads: 0,
            uptime_hours: 0.0,
            violations: 0,
            last_updated: now,
            total_messages: 0,
            valid_messages: 0,
            latency_samples: 0,
            connected_since: Some(now),
        }
    }

    /// Apply a single reputation event and recompute the composite score.
    pub fn apply_event(&mut self, event: &ReputationEvent) {
        self.last_updated = Utc::now();

        match event {
            ReputationEvent::ValidMessage => {
                self.total_messages += 1;
                self.valid_messages += 1;
            }
            ReputationEvent::DuplicateMessage => {
                self.total_messages += 1;
                // valid_messages unchanged → lowers ratio
            }
            ReputationEvent::FastResponse { latency_ms } => {
                self.update_latency(*latency_ms);
            }
            ReputationEvent::SlowResponse { latency_ms } => {
                self.update_latency(*latency_ms);
            }
            ReputationEvent::FileShared => {
                self.file_shares += 1;
            }
            ReputationEvent::FileDownloaded => {
                self.file_downloads += 1;
            }
            ReputationEvent::Violation { severity } => {
                self.violations += severity;
            }
        }

        self.recompute();
    }

    /// Record that the peer has been online for `hours` additional hours.
    pub fn add_uptime(&mut self, hours: f64) {
        self.uptime_hours += hours;
        self.recompute();
    }

    /// Mark the peer as currently connected (starts uptime tracking).
    pub fn mark_connected(&mut self) {
        if self.connected_since.is_none() {
            self.connected_since = Some(Utc::now());
        }
    }

    /// Mark the peer as disconnected; accumulate elapsed uptime.
    pub fn mark_disconnected(&mut self) {
        if let Some(since) = self.connected_since.take() {
            let elapsed = Utc::now()
                .signed_duration_since(since)
                .num_seconds()
                .max(0) as f64
                / 3600.0;
            self.uptime_hours += elapsed;
            self.recompute();
        }
    }

    /// Create a PeerReputation from database fields.
    pub fn from_db(
        peer_id: String,
        score: f64,
        message_quality: f64,
        response_latency_ms: u64,
        file_shares: u32,
        file_downloads: u32,
        uptime_hours: f64,
        violations: u32,
        total_messages: u64,
        valid_messages: u64,
        latency_samples: u64,
        connected_since: Option<DateTime<Utc>>,
        last_updated: DateTime<Utc>,
    ) -> Self {
        Self {
            peer_id,
            score,
            message_quality,
            response_latency_ms,
            file_shares,
            file_downloads,
            uptime_hours,
            violations,
            last_updated,
            total_messages,
            valid_messages,
            latency_samples,
            connected_since,
        }
    }

    /// Get total messages count.
    pub fn total_messages(&self) -> u64 {
        self.total_messages
    }

    /// Get valid messages count.
    pub fn valid_messages(&self) -> u64 {
        self.valid_messages
    }

    /// Get latency samples count.
    pub fn latency_samples(&self) -> u64 {
        self.latency_samples
    }

    /// Get connected since timestamp.
    pub fn connected_since(&self) -> Option<DateTime<Utc>> {
        self.connected_since
    }

    // -- internal helpers ---------------------------------------------------

    fn update_latency(&mut self, latency_ms: u64) {
        // Exponential moving average (α = 0.3 for new samples).
        let alpha = 0.3;
        if self.latency_samples == 0 {
            self.response_latency_ms = latency_ms;
        } else {
            let ema = (alpha * latency_ms as f64)
                + ((1.0 - alpha) * self.response_latency_ms as f64);
            self.response_latency_ms = ema.round() as u64;
        }
        self.latency_samples += 1;
    }

    fn recompute(&mut self) {
        // Update derived fields.
        if self.total_messages > 0 {
            self.message_quality = self.valid_messages as f64 / self.total_messages as f64;
        }

        // --- Component scores (each 0.0 – 1.0) ---

        // 1. Message quality (ratio + activity bonus).
        //    Peers with more valid messages get a bonus, using asymptotic curve.
        let quality_ratio = self.message_quality;
        // Asymptotic curve: 1 - 1/(n+1), approaches 1.0 as n increases
        let activity_bonus = 1.0 - 1.0 / (self.valid_messages as f64 + 1.0);
        // Combine: 70% ratio, 30% activity bonus
        let mq = 0.7 * quality_ratio + 0.3 * activity_bonus;

        // 2. Response speed: lower latency → higher score.
        let rs = if self.latency_samples == 0 {
            0.5 // neutral when no data
        } else {
            let lat = self.response_latency_ms as f64;
            if lat <= FAST_RESPONSE_MS {
                1.0
            } else if lat >= SLOW_RESPONSE_MS {
                0.0
            } else {
                1.0 - (lat - FAST_RESPONSE_MS) / (SLOW_RESPONSE_MS - FAST_RESPONSE_MS)
            }
        };

        // 3. File sharing contribution (log-scaled, capped at 1.0).
        let total_file_contrib = self.file_shares as f64 + self.file_downloads as f64;
        let fs = (total_file_contrib + 1.0).ln() / 5.0_f64.ln().max(1.0);
        let fs = fs.min(1.0);

        // 4. Uptime (log-scaled, capped at 1.0; 100 h → ~1.0).
        let ut = if self.uptime_hours <= 0.0 {
            0.3 // small baseline so new peers aren't penalised
        } else {
            ((self.uptime_hours + 1.0).ln() / 5.0_f64.ln()).min(1.0)
        };

        // 5. Violations (linear decay to 0).
        let vl = if self.violations == 0 {
            1.0
        } else {
            (1.0 - self.violations as f64 / MAX_VIOLATIONS).max(0.0)
        };

        // Weighted average.
        let weighted_score = WEIGHT_MESSAGE_QUALITY * mq
            + WEIGHT_RESPONSE_SPEED * rs
            + WEIGHT_FILE_SHARING * fs
            + WEIGHT_UPTIME * ut
            + WEIGHT_VIOLATIONS * vl;

        // If violations exceed threshold, override to zero (unrecoverable).
        self.score = if self.violations as f64 > MAX_VIOLATIONS {
            0.0
        } else {
            weighted_score
        };

        // Clamp to [0, 1].
        self.score = self.score.clamp(0.0, 1.0);
    }
}

// ---------------------------------------------------------------------------
// ReputationManager
// ---------------------------------------------------------------------------

/// Manages reputation records for all known peers.
pub struct ReputationManager {
    peers: HashMap<String, PeerReputation>,
}

impl ReputationManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    /// Apply a reputation event for a peer, creating the record if needed.
    pub fn update_score(&mut self, peer_id: &str, event: ReputationEvent) {
        let rep = self
            .peers
            .entry(peer_id.to_string())
            .or_insert_with(|| PeerReputation::new(peer_id.to_string()));
        rep.apply_event(&event);
    }

    /// Get the current composite score for a peer (0.0 – 1.0).
    pub fn get_score(&self, peer_id: &str) -> Option<f64> {
        self.peers.get(peer_id).map(|r| r.score)
    }

    /// Return the top `limit` peers sorted by descending score.
    pub fn get_top_peers(&self, limit: usize) -> Vec<(String, f64)> {
        let mut list: Vec<(String, f64)> = self
            .peers
            .iter()
            .map(|(id, r)| (id.clone(), r.score))
            .collect();
        list.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        list.truncate(limit);
        list
    }

    /// Whether a peer should be automatically blocked (score < BLOCK_THRESHOLD).
    pub fn should_block(&self, peer_id: &str) -> bool {
        self.peers
            .get(peer_id)
            .map(|r| r.score < BLOCK_THRESHOLD)
            .unwrap_or(false)
    }

    /// Get a read-only reference to a peer's full reputation record.
    pub fn get_reputation(&self, peer_id: &str) -> Option<&PeerReputation> {
        self.peers.get(peer_id)
    }

    /// Get a mutable reference to a peer's full reputation record.
    pub fn get_reputation_mut(&mut self, peer_id: &str) -> Option<&mut PeerReputation> {
        self.peers.get_mut(peer_id)
    }

    /// Return all peer records (for persistence / serialisation).
    pub fn all_peers(&self) -> &HashMap<String, PeerReputation> {
        &self.peers
    }

    /// Insert or replace a full record (used when loading from DB).
    pub fn insert(&mut self, rep: PeerReputation) {
        self.peers.insert(rep.peer_id.clone(), rep);
    }

    /// Number of tracked peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Remove peers whose score is below the block threshold.
    /// Returns the list of removed peer IDs.
    pub fn evict_blocked(&mut self) -> Vec<String> {
        let blocked: Vec<String> = self
            .peers
            .iter()
            .filter(|(_, r)| r.score < BLOCK_THRESHOLD)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &blocked {
            self.peers.remove(id);
        }
        blocked
    }
}

impl Default for ReputationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_peer_has_neutral_score() {
        let rep = PeerReputation::new("peer1".into());
        assert!((rep.score - 0.5).abs() < 1e-6);
        assert_eq!(rep.violations, 0);
        assert_eq!(rep.file_shares, 0);
    }

    #[test]
    fn test_valid_message_increases_score() {
        let mut mgr = ReputationManager::new();
        let initial = 0.5;
        mgr.update_score("p1", ReputationEvent::ValidMessage);
        let score = mgr.get_score("p1").unwrap();
        assert!(score > initial, "score should increase after valid message");
    }

    #[test]
    fn test_duplicate_message_decreases_quality() {
        let mut rep = PeerReputation::new("p1".into());
        rep.apply_event(&ReputationEvent::ValidMessage);
        let q_before = rep.message_quality;
        rep.apply_event(&ReputationEvent::DuplicateMessage);
        assert!(rep.message_quality < q_before);
    }

    #[test]
    fn test_fast_response_better_than_slow() {
        let mut rep_fast = PeerReputation::new("fast".into());
        rep_fast.apply_event(&ReputationEvent::FastResponse { latency_ms: 50 });

        let mut rep_slow = PeerReputation::new("slow".into());
        rep_slow.apply_event(&ReputationEvent::SlowResponse { latency_ms: 4000 });

        assert!(rep_fast.score > rep_slow.score);
    }

    #[test]
    fn test_file_sharing_increases_score() {
        let mut rep = PeerReputation::new("p1".into());
        let before = rep.score;
        rep.apply_event(&ReputationEvent::FileShared);
        rep.apply_event(&ReputationEvent::FileDownloaded);
        assert!(rep.score > before);
    }

    #[test]
    fn test_violation_decreases_score() {
        let mut rep = PeerReputation::new("p1".into());
        let before = rep.score;
        rep.apply_event(&ReputationEvent::Violation { severity: 3 });
        assert!(rep.score < before);
    }

    #[test]
    fn test_should_block_low_score() {
        let mut mgr = ReputationManager::new();
        // Drive score down with many violations.
        for _ in 0..20 {
            mgr.update_score("bad", ReputationEvent::Violation { severity: 5 });
        }
        assert!(mgr.should_block("bad"));
    }

    #[test]
    fn test_should_not_block_good_peer() {
        let mut mgr = ReputationManager::new();
        for _ in 0..50 {
            mgr.update_score("good", ReputationEvent::ValidMessage);
        }
        assert!(!mgr.should_block("good"));
    }

    #[test]
    fn test_get_top_peers_ordering() {
        let mut mgr = ReputationManager::new();
        for _ in 0..5 {
            mgr.update_score("a", ReputationEvent::ValidMessage);
        }
        for _ in 0..20 {
            mgr.update_score("b", ReputationEvent::ValidMessage);
        }
        for _ in 0..10 {
            mgr.update_score("c", ReputationEvent::ValidMessage);
        }

        let top = mgr.get_top_peers(2);
        assert_eq!(top.len(), 2);
        // b should be first (most valid messages).
        assert_eq!(top[0].0, "b");
    }

    #[test]
    fn test_uptime_increases_score() {
        let mut rep = PeerReputation::new("p1".into());
        let before = rep.score;
        rep.add_uptime(10.0);
        assert!(rep.score > before);
    }

    #[test]
    fn test_latency_ema_converges() {
        let mut rep = PeerReputation::new("p1".into());
        for _ in 0..20 {
            rep.apply_event(&ReputationEvent::FastResponse { latency_ms: 100 });
        }
        // After many 100ms samples the EMA should be close to 100.
        assert!((rep.response_latency_ms as i64 - 100).abs() < 20);
    }

    #[test]
    fn test_score_clamped_0_to_1() {
        let mut rep = PeerReputation::new("p1".into());
        // Extreme violations.
        for _ in 0..100 {
            rep.apply_event(&ReputationEvent::Violation { severity: 10 });
        }
        assert!(rep.score >= 0.0);
        assert!(rep.score <= 1.0);
    }

    #[test]
    fn test_manager_peer_count() {
        let mut mgr = ReputationManager::new();
        assert_eq!(mgr.peer_count(), 0);
        mgr.update_score("a", ReputationEvent::ValidMessage);
        mgr.update_score("b", ReputationEvent::ValidMessage);
        assert_eq!(mgr.peer_count(), 2);
    }

    #[test]
    fn test_evict_blocked() {
        let mut mgr = ReputationManager::new();
        for _ in 0..50 {
            mgr.update_score("bad", ReputationEvent::Violation { severity: 5 });
        }
        mgr.update_score("good", ReputationEvent::ValidMessage);
        let evicted = mgr.evict_blocked();
        assert!(evicted.contains(&"bad".to_string()));
        assert_eq!(mgr.peer_count(), 1);
    }

    #[test]
    fn test_get_score_unknown_peer_returns_none() {
        let mgr = ReputationManager::new();
        assert!(mgr.get_score("unknown").is_none());
    }
}
