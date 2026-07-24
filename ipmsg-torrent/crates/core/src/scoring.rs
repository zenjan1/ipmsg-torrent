//! Peer scoring system inspired by libp2p gossipsub's PeerScore
//! Tracks peer behavior to prioritize good peers and penalize bad ones

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Score thresholds
pub const SCORE_THRESHOLD_GRAFT: f64 = 0.0;
pub const SCORE_THRESHOLD_PRUNE: f64 = -10.0;
pub const SCORE_THRESHOLD_BLACKLIST: f64 = -50.0;
pub const SCORE_THRESHOLD_GRAYLIST: f64 = -10.0;

/// Decay interval for score normalization
pub const SCORE_DECAY_INTERVAL: Duration = Duration::from_secs(60);

/// Behavior types that affect peer score
#[derive(Debug, Clone, Copy)]
pub enum PeerBehavior {
    /// Sent a valid message
    ValidMessage,
    /// Sent a duplicate message we already have
    DuplicateMessage,
    /// Sent an invalid/malformed message
    InvalidMessage,
    /// Responded to a file transfer request
    ValidResponse,
    /// Failed to respond to a request
    FailedResponse,
    /// Peer disconnected unexpectedly
    UnexpectedDisconnect,
    /// Peer timed out on a request
    RequestTimeout,
}

impl PeerBehavior {
    /// Score delta for each behavior type
    pub fn score_delta(&self) -> f64 {
        match self {
            Self::ValidMessage => 1.0,
            Self::DuplicateMessage => -0.5,
            Self::InvalidMessage => -5.0,
            Self::ValidResponse => 2.0,
            Self::FailedResponse => -2.0,
            Self::UnexpectedDisconnect => -3.0,
            Self::RequestTimeout => -1.0,
        }
    }
}

/// Per-peer score tracking
#[derive(Debug, Clone)]
pub struct PeerScore {
    /// Current score (decays over time)
    pub score: f64,
    /// Total messages received from this peer
    pub messages_received: u64,
    /// Total valid messages
    pub valid_messages: u64,
    /// Total invalid messages
    pub invalid_messages: u64,
    /// Total duplicate messages
    pub duplicate_messages: u64,
    /// Total requests sent to this peer
    pub requests_sent: u64,
    /// Total successful responses
    pub responses_received: u64,
    /// Total failed responses
    pub failed_responses: u64,
    /// Total timeouts
    pub timeouts: u64,
    /// Number of disconnections
    pub disconnects: u64,
    /// When this peer was first seen
    pub first_seen: Instant,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Whether this peer is currently connected
    pub connected: bool,
}

impl PeerScore {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            score: 0.0,
            messages_received: 0,
            valid_messages: 0,
            invalid_messages: 0,
            duplicate_messages: 0,
            requests_sent: 0,
            responses_received: 0,
            failed_responses: 0,
            timeouts: 0,
            disconnects: 0,
            first_seen: now,
            last_activity: now,
            connected: true,
        }
    }

    /// Record a behavior event and update score
    pub fn record_behavior(&mut self, behavior: PeerBehavior) {
        self.score += behavior.score_delta();
        self.last_activity = Instant::now();

        match behavior {
            PeerBehavior::ValidMessage => {
                self.messages_received += 1;
                self.valid_messages += 1;
            }
            PeerBehavior::DuplicateMessage => {
                self.messages_received += 1;
                self.duplicate_messages += 1;
            }
            PeerBehavior::InvalidMessage => {
                self.messages_received += 1;
                self.invalid_messages += 1;
            }
            PeerBehavior::ValidResponse => {
                self.responses_received += 1;
            }
            PeerBehavior::FailedResponse => {
                self.failed_responses += 1;
            }
            PeerBehavior::UnexpectedDisconnect => {
                self.disconnects += 1;
            }
            PeerBehavior::RequestTimeout => {
                self.timeouts += 1;
            }
        }
    }

    /// Apply exponential decay to the score
    pub fn decay(&mut self, decay_factor: f64) {
        self.score *= decay_factor;
    }

    /// Calculate delivery reliability (0.0 to 1.0)
    pub fn delivery_reliability(&self) -> f64 {
        let total_requests = self.responses_received + self.failed_responses + self.timeouts;
        if total_requests == 0 {
            return 1.0; // No requests yet, assume reliable
        }
        self.responses_received as f64 / total_requests as f64
    }

    /// Calculate message validity ratio (0.0 to 1.0)
    pub fn message_validity_ratio(&self) -> f64 {
        if self.messages_received == 0 {
            return 1.0;
        }
        self.valid_messages as f64 / self.messages_received as f64
    }

    /// Check if peer should be grafted (accepted into mesh)
    pub fn should_graft(&self) -> bool {
        self.score >= SCORE_THRESHOLD_GRAFT
    }

    /// Check if peer should be pruned from mesh
    pub fn should_prune(&self) -> bool {
        self.score <= SCORE_THRESHOLD_PRUNE
    }

    /// Check if peer should be blacklisted
    pub fn should_blacklist(&self) -> bool {
        self.score <= SCORE_THRESHOLD_BLACKLIST
    }
}

impl Default for PeerScore {
    fn default() -> Self {
        Self::new()
    }
}

/// Manager for all peer scores
pub struct PeerScoreManager {
    scores: HashMap<String, PeerScore>,
    decay_factor: f64,
    last_decay: Instant,
}

impl PeerScoreManager {
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
            decay_factor: 0.95, // 5% decay per interval
            last_decay: Instant::now(),
        }
    }

    /// Get or create score for a peer
    pub fn get_or_create(&mut self, peer_id: &str) -> &mut PeerScore {
        self.scores.entry(peer_id.to_string()).or_default()
    }

    /// Get score for a peer (read-only)
    pub fn get(&self, peer_id: &str) -> Option<&PeerScore> {
        self.scores.get(peer_id)
    }

    /// Record a behavior for a peer
    pub fn record_behavior(&mut self, peer_id: &str, behavior: PeerBehavior) {
        let score = self.get_or_create(peer_id);
        score.record_behavior(behavior);
    }

    /// Mark a peer as disconnected
    pub fn mark_disconnected(&mut self, peer_id: &str) {
        if let Some(score) = self.scores.get_mut(peer_id) {
            score.connected = false;
            score.record_behavior(PeerBehavior::UnexpectedDisconnect);
        }
    }

    /// Mark a peer as connected
    pub fn mark_connected(&mut self, peer_id: &str) {
        if let Some(score) = self.scores.get_mut(peer_id) {
            score.connected = true;
        }
    }

    /// Apply periodic decay to all scores
    pub fn apply_decay(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_decay) >= SCORE_DECAY_INTERVAL {
            for score in self.scores.values_mut() {
                score.decay(self.decay_factor);
            }
            self.last_decay = now;
        }
    }

    /// Get list of peers that should be pruned
    pub fn peers_to_prune(&self) -> Vec<String> {
        self.scores
            .iter()
            .filter(|(_, score)| score.should_prune())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get list of peers that should be blacklisted
    pub fn peers_to_blacklist(&self) -> Vec<String> {
        self.scores
            .iter()
            .filter(|(_, score)| score.should_blacklist())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get top N peers by score (for prioritization)
    pub fn top_peers(&self, n: usize) -> Vec<(String, f64)> {
        let mut peers: Vec<(String, f64)> = self
            .scores
            .iter()
            .filter(|(_, score)| score.connected)
            .map(|(id, score)| (id.clone(), score.score))
            .collect();
        peers.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        peers.truncate(n);
        peers
    }

    /// Remove stale peers (disconnected for too long)
    pub fn cleanup_stale(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.scores.retain(|_, score| {
            score.connected || now.duration_since(score.last_activity) < max_age
        });
    }

    /// Get total number of tracked peers
    pub fn peer_count(&self) -> usize {
        self.scores.len()
    }

    /// Get number of connected peers
    pub fn connected_count(&self) -> usize {
        self.scores.values().filter(|s| s.connected).count()
    }
}

impl Default for PeerScoreManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_score_new() {
        let score = PeerScore::new();
        assert_eq!(score.score, 0.0);
        assert!(score.connected);
        assert_eq!(score.messages_received, 0);
    }

    #[test]
    fn test_record_valid_message() {
        let mut score = PeerScore::new();
        score.record_behavior(PeerBehavior::ValidMessage);
        assert_eq!(score.score, 1.0);
        assert_eq!(score.valid_messages, 1);
        assert_eq!(score.messages_received, 1);
    }

    #[test]
    fn test_record_invalid_message() {
        let mut score = PeerScore::new();
        score.record_behavior(PeerBehavior::InvalidMessage);
        assert_eq!(score.score, -5.0);
        assert_eq!(score.invalid_messages, 1);
    }

    #[test]
    fn test_score_decay() {
        let mut score = PeerScore::new();
        score.score = 100.0;
        score.decay(0.5);
        assert_eq!(score.score, 50.0);
    }

    #[test]
    fn test_delivery_reliability() {
        let mut score = PeerScore::new();
        score.record_behavior(PeerBehavior::ValidResponse);
        score.record_behavior(PeerBehavior::ValidResponse);
        score.record_behavior(PeerBehavior::RequestTimeout);
        // 2 success out of 3 total = 0.666...
        assert!((score.delivery_reliability() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_message_validity_ratio() {
        let mut score = PeerScore::new();
        score.record_behavior(PeerBehavior::ValidMessage);
        score.record_behavior(PeerBehavior::ValidMessage);
        score.record_behavior(PeerBehavior::DuplicateMessage);
        // 2 valid out of 3 total = 0.666...
        assert!((score.message_validity_ratio() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_should_graft() {
        let mut score = PeerScore::new();
        assert!(score.should_graft()); // 0.0 >= 0.0
        score.score = -5.0;
        assert!(!score.should_graft());
    }

    #[test]
    fn test_should_prune() {
        let mut score = PeerScore::new();
        assert!(!score.should_prune());
        score.score = -15.0;
        assert!(score.should_prune());
    }

    #[test]
    fn test_should_blacklist() {
        let mut score = PeerScore::new();
        assert!(!score.should_blacklist());
        score.score = -60.0;
        assert!(score.should_blacklist());
    }

    #[test]
    fn test_peer_score_manager() {
        let mut manager = PeerScoreManager::new();

        // Create scores for peers
        manager.record_behavior("peer1", PeerBehavior::ValidMessage);
        manager.record_behavior("peer2", PeerBehavior::ValidMessage);
        manager.record_behavior("peer2", PeerBehavior::ValidMessage);

        assert_eq!(manager.peer_count(), 2);
        assert_eq!(manager.connected_count(), 2);

        // peer2 should have higher score
        let top = manager.top_peers(1);
        assert_eq!(top[0].0, "peer2");
    }

    #[test]
    fn test_peers_to_prune() {
        let mut manager = PeerScoreManager::new();
        manager.get_or_create("good_peer").score = 10.0;
        manager.get_or_create("bad_peer").score = -15.0;

        let to_prune = manager.peers_to_prune();
        assert_eq!(to_prune.len(), 1);
        assert!(to_prune.contains(&"bad_peer".to_string()));
    }

    #[test]
    fn test_mark_disconnected() {
        let mut manager = PeerScoreManager::new();
        manager.get_or_create("peer1");
        assert_eq!(manager.connected_count(), 1);

        manager.mark_disconnected("peer1");
        assert_eq!(manager.connected_count(), 0);
    }
}
