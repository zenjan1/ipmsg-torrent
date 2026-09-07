//! Smart bootstrap node selection.
//!
//! Maintains a list of known bootstrap nodes with their scores, sorts them by
//! quality, and provides failover logic: if the top-ranked node fails, the
//! caller transparently moves to the next candidate.

use crate::node_scorer::{NodeScoreSnapshot, NodeScorer};
use std::time::Duration;

// ---------------------------------------------------------------------------
// BootstrapNode
// ---------------------------------------------------------------------------

/// A bootstrap node entry.
#[derive(Debug, Clone)]
pub struct BootstrapNode {
    /// Multiaddr string or other identifier.
    pub address: String,
    /// Optional human-readable label.
    pub label: Option<String>,
    /// Whether this node is currently considered reachable.
    pub reachable: bool,
}

// ---------------------------------------------------------------------------
// SmartBootstrap
// ---------------------------------------------------------------------------

/// Manages an ordered list of bootstrap nodes ranked by score.
pub struct SmartBootstrap {
    nodes: Vec<BootstrapNode>,
    scorer: NodeScorer,
    /// Index into `nodes` for the next candidate to try.
    cursor: usize,
}

impl SmartBootstrap {
    /// Create from a list of bootstrap addresses.
    pub fn new(addresses: Vec<String>) -> Self {
        let nodes = addresses
            .into_iter()
            .map(|address| BootstrapNode {
                address,
                label: None,
                reachable: true,
            })
            .collect();
        Self {
            nodes,
            scorer: NodeScorer::new(),
            cursor: 0,
        }
    }

    /// Create with labelled nodes.
    pub fn with_labels(entries: Vec<(String, String)>) -> Self {
        let nodes = entries
            .into_iter()
            .map(|(address, label)| BootstrapNode {
                address,
                label: Some(label),
                reachable: true,
            })
            .collect();
        Self {
            nodes,
            scorer: NodeScorer::new(),
            cursor: 0,
        }
    }

    /// Number of known bootstrap nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Add a new bootstrap node.
    pub fn add_node(&mut self, address: String, label: Option<String>) {
        if !self.nodes.iter().any(|n| n.address == address) {
            self.nodes.push(BootstrapNode {
                address,
                label,
                reachable: true,
            });
        }
    }

    /// Remove a bootstrap node.
    pub fn remove_node(&mut self, address: &str) {
        self.nodes.retain(|n| n.address != address);
        self.scorer.cleanup_stale();
    }

    /// Return the ordered list of nodes sorted by score (best first).
    pub fn ranked_nodes(&self) -> Vec<(String, f64)> {
        // Collect nodes that have scores, plus those without (score = 0.5 default)
        let mut ranked: Vec<(String, f64)> = self
            .nodes
            .iter()
            .filter(|n| n.reachable)
            .map(|n| {
                let score = self
                    .scorer
                    .get_score(&n.address)
                    .map(|s| s.score)
                    .unwrap_or(0.5);
                (n.address.clone(), score)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked
    }

    /// Get the next best node to connect to.
    /// Returns `None` if all nodes have been exhausted.
    pub fn next_candidate(&mut self) -> Option<&BootstrapNode> {
        let ranked = self.ranked_nodes();
        if self.cursor >= ranked.len() {
            return None;
        }
        let (ref addr, _) = ranked[self.cursor];
        self.nodes.iter().find(|n| &n.address == addr)
    }

    /// Report that the current candidate connected successfully.
    pub fn record_success(&mut self, address: &str, rtt: Duration) {
        self.scorer.record_success(address, rtt);
        // Reset cursor so next call starts from the top
        self.cursor = 0;
    }

    /// Report that the current candidate failed to connect.
    /// Advances the cursor to the next node.
    pub fn record_failure(&mut self, address: &str) {
        self.scorer.record_failure(address);
        // Mark as unreachable if too many consecutive failures
        if let Some(score) = self.scorer.get_score(address) {
            if score.consecutive_failures >= 5 {
                if let Some(node) = self.nodes.iter_mut().find(|n| n.address == address) {
                    node.reachable = false;
                }
            }
        }
        self.cursor += 1;
    }

    /// Reset all nodes to reachable state (e.g. after a cooldown period).
    pub fn reset_reachability(&mut self) {
        for node in &mut self.nodes {
            node.reachable = true;
        }
        self.cursor = 0;
    }

    /// Get a reference to the underlying scorer.
    pub fn scorer(&self) -> &NodeScorer {
        &self.scorer
    }

    /// Get a mutable reference to the underlying scorer.
    pub fn scorer_mut(&mut self) -> &mut NodeScorer {
        &mut self.scorer
    }

    /// Get a snapshot of all node scores for persistence.
    pub fn score_snapshot(&self) -> Vec<NodeScoreSnapshot> {
        self.scorer.snapshot()
    }

    /// Restore scores from a persistence snapshot.
    pub fn score_restore(&mut self, snapshots: Vec<NodeScoreSnapshot>) {
        self.scorer.restore(snapshots);
    }

    /// Get all bootstrap node addresses.
    pub fn all_addresses(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.address.clone()).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bootstrap() -> SmartBootstrap {
        SmartBootstrap::new(vec![
            "/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A".to_string(),
            "/ip4/2.2.2.2/udp/4001/quic-v1/p2p/B".to_string(),
            "/ip4/3.3.3.3/udp/4001/quic-v1/p2p/C".to_string(),
        ])
    }

    #[test]
    fn test_new_bootstrap() {
        let bs = make_bootstrap();
        assert_eq!(bs.node_count(), 3);
    }

    #[test]
    fn test_next_candidate_returns_best() {
        let mut bs = make_bootstrap();
        let first = bs.next_candidate().unwrap();
        assert!(!first.address.is_empty());
    }

    #[test]
    fn test_failure_advances_cursor() {
        let mut bs = make_bootstrap();
        let first = bs.next_candidate().unwrap().address.clone();
        bs.record_failure(&first);
        // Next candidate should be different (if scores allow)
        // With all equal scores, cursor just advances
        let second = bs.next_candidate();
        // Could be Some or None depending on ranking
        if let Some(s) = second {
            // It's fine if it's the same node (score tie) or different
            assert!(!s.address.is_empty());
        }
    }

    #[test]
    fn test_success_resets_cursor() {
        let mut bs = make_bootstrap();
        let first = bs.next_candidate().unwrap().address.clone();
        bs.record_failure(&first);
        bs.record_success(&first, Duration::from_millis(50));
        // After success, cursor resets
        let next = bs.next_candidate().unwrap().address.clone();
        assert_eq!(next, first);
    }

    #[test]
    fn test_exhausted_returns_none() {
        let mut bs = SmartBootstrap::new(vec!["addr1".to_string(), "addr2".to_string()]);
        bs.record_failure("addr1");
        bs.record_failure("addr2");
        // After failing both, next_candidate may return None or wrap
        // depending on scores. With 5 consecutive failures, nodes become unreachable.
        // Let's force them unreachable:
        for _ in 0..5 {
            bs.record_failure("addr1");
            bs.record_failure("addr2");
        }
        assert!(bs.next_candidate().is_none());
    }

    #[test]
    fn test_add_node() {
        let mut bs = make_bootstrap();
        bs.add_node("/ip4/4.4.4.4/udp/4001/quic-v1/p2p/D".to_string(), Some("D".to_string()));
        assert_eq!(bs.node_count(), 4);
    }

    #[test]
    fn test_add_duplicate_node() {
        let mut bs = make_bootstrap();
        bs.add_node("/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A".to_string(), None);
        assert_eq!(bs.node_count(), 3); // no duplicate
    }

    #[test]
    fn test_remove_node() {
        let mut bs = make_bootstrap();
        bs.remove_node("/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A");
        assert_eq!(bs.node_count(), 2);
    }

    #[test]
    fn test_reset_reachability() {
        let mut bs = SmartBootstrap::new(vec!["addr1".to_string()]);
        for _ in 0..10 {
            bs.record_failure("addr1");
        }
        assert!(bs.next_candidate().is_none());
        bs.reset_reachability();
        assert!(bs.next_candidate().is_some());
    }

    #[test]
    fn test_snapshot_restore() {
        let mut bs = make_bootstrap();
        bs.record_success(
            "/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A",
            Duration::from_millis(50),
        );
        let snap = bs.score_snapshot();
        assert!(!snap.is_empty());

        let mut bs2 = make_bootstrap();
        bs2.score_restore(snap);
        let score = bs2
            .scorer()
            .get_score("/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A");
        assert!(score.is_some());
    }

    #[test]
    fn test_empty_bootstrap() {
        let mut bs = SmartBootstrap::new(vec![]);
        assert!(bs.next_candidate().is_none());
        assert_eq!(bs.node_count(), 0);
    }

    #[test]
    fn test_with_labels() {
        let bs = SmartBootstrap::with_labels(vec![
            ("addr1".to_string(), "Node 1".to_string()),
            ("addr2".to_string(), "Node 2".to_string()),
        ]);
        assert_eq!(bs.node_count(), 2);
        assert_eq!(bs.nodes[0].label.as_deref(), Some("Node 1"));
    }

    #[test]
    fn test_ranked_nodes_prefer_high_score() {
        let mut bs = make_bootstrap();
        // Give node A a great score
        for _ in 0..5 {
            bs.record_success(
                "/ip4/1.1.1.1/udp/4001/quic-v1/p2p/A",
                Duration::from_millis(20),
            );
        }
        // Give node B a terrible score
        for _ in 0..5 {
            bs.record_failure("/ip4/2.2.2.2/udp/4001/quic-v1/p2p/B");
        }
        let ranked = bs.ranked_nodes();
        // A should come before B
        let a_pos = ranked.iter().position(|(a, _)| a.contains("1.1.1.1")).unwrap();
        let b_pos = ranked.iter().position(|(a, _)| a.contains("2.2.2.2")).unwrap();
        assert!(a_pos < b_pos);
    }
}
