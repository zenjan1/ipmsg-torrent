//! Relay node auto-discovery and selection.
//!
//! Scans connected peers to identify those capable of acting as libp2p circuit
//! relays, evaluates their quality, and maintains a pool of candidate relays
//! ranked by score.

use crate::node_scorer::NodeScorer;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum relay pool size.
pub const MAX_RELAY_POOL_SIZE: usize = 10;

/// Minimum score to be considered a good relay.
pub const MIN_RELAY_SCORE: f64 = 0.3;

/// How long before a relay is considered stale (seconds).
pub const RELAY_STALE_SECS: u64 = 600;

// ---------------------------------------------------------------------------
// RelayNode
// ---------------------------------------------------------------------------

/// Information about a discovered relay node.
#[derive(Debug, Clone)]
pub struct RelayNode {
    /// Peer ID or multiaddr of the relay.
    pub peer_id: String,
    /// Whether the relay supports circuit relay v2.
    pub supports_relay_v2: bool,
    /// Estimated bandwidth capacity (bytes/sec, 0 = unknown).
    pub bandwidth_bps: u64,
    /// Number of active circuits through this relay.
    pub active_circuits: u32,
    /// Maximum circuits the relay allows.
    pub max_circuits: u32,
    /// Whether we currently have a reservation with this relay.
    pub has_reservation: bool,
    /// When the reservation was made.
    pub reservation_time: Option<Instant>,
    /// When we last heard from this relay.
    pub last_seen: Instant,
}

impl RelayNode {
    /// Whether this relay has capacity for more circuits.
    pub fn has_capacity(&self) -> bool {
        self.max_circuits == 0 || self.active_circuits < self.max_circuits
    }

    /// Age of the reservation in seconds.
    pub fn reservation_age_secs(&self) -> Option<u64> {
        self.reservation_time.map(|t| t.elapsed().as_secs())
    }

    /// Whether this relay is stale (not seen for a long time).
    pub fn is_stale(&self) -> bool {
        self.last_seen.elapsed().as_secs() > RELAY_STALE_SECS
    }
}

// ---------------------------------------------------------------------------
// RelayDiscovery
// ---------------------------------------------------------------------------

/// Manages discovery and selection of relay nodes.
pub struct RelayDiscovery {
    /// Pool of known relay nodes.
    relays: HashMap<String, RelayNode>,
    /// Node scorer for ranking relays.
    scorer: NodeScorer,
    /// Peer IDs of peers we've asked about relay support.
    queried_peers: Vec<String>,
}

impl RelayDiscovery {
    pub fn new() -> Self {
        Self {
            relays: HashMap::new(),
            scorer: NodeScorer::new(),
            queried_peers: Vec::new(),
        }
    }

    /// Register a peer as a potential relay.
    pub fn add_potential_relay(&mut self, peer_id: &str, supports_relay_v2: bool) {
        if self.relays.contains_key(peer_id) {
            // Update existing
            if let Some(relay) = self.relays.get_mut(peer_id) {
                relay.supports_relay_v2 = supports_relay_v2;
                relay.last_seen = Instant::now();
            }
        } else if self.relays.len() < MAX_RELAY_POOL_SIZE {
            self.relays.insert(
                peer_id.to_string(),
                RelayNode {
                    peer_id: peer_id.to_string(),
                    supports_relay_v2,
                    bandwidth_bps: 0,
                    active_circuits: 0,
                    max_circuits: 0,
                    has_reservation: false,
                    reservation_time: None,
                    last_seen: Instant::now(),
                },
            );
        }
    }

    /// Record that a peer was queried about relay support.
    pub fn mark_queried(&mut self, peer_id: &str) {
        if !self.queried_peers.contains(&peer_id.to_string()) {
            self.queried_peers.push(peer_id.to_string());
        }
    }

    /// Check if a peer has been queried.
    pub fn was_queried(&self, peer_id: &str) -> bool {
        self.queried_peers.contains(&peer_id.to_string())
    }

    /// Update relay bandwidth estimate.
    pub fn update_bandwidth(&mut self, peer_id: &str, bandwidth_bps: u64) {
        if let Some(relay) = self.relays.get_mut(peer_id) {
            relay.bandwidth_bps = bandwidth_bps;
            relay.last_seen = Instant::now();
        }
    }

    /// Update relay circuit counts.
    pub fn update_circuits(&mut self, peer_id: &str, active: u32, max: u32) {
        if let Some(relay) = self.relays.get_mut(peer_id) {
            relay.active_circuits = active;
            relay.max_circuits = max;
            relay.last_seen = Instant::now();
        }
    }

    /// Record a successful reservation with a relay.
    pub fn record_reservation(&mut self, peer_id: &str) {
        if let Some(relay) = self.relays.get_mut(peer_id) {
            relay.has_reservation = true;
            relay.reservation_time = Some(Instant::now());
            relay.last_seen = Instant::now();
        }
    }

    /// Record a connection success through a relay.
    pub fn record_relay_success(&mut self, peer_id: &str, rtt: Duration) {
        self.scorer.record_success(peer_id, rtt);
        if let Some(relay) = self.relays.get_mut(peer_id) {
            relay.last_seen = Instant::now();
        }
    }

    /// Record a connection failure through a relay.
    pub fn record_relay_failure(&mut self, peer_id: &str) {
        self.scorer.record_failure(peer_id);
    }

    /// Get the best relay candidate (highest score, has capacity).
    pub fn best_relay(&self) -> Option<&RelayNode> {
        let ranked = self.scorer.ranked_nodes();
        for (peer_id, score) in &ranked {
            if *score < MIN_RELAY_SCORE {
                continue;
            }
            if let Some(relay) = self.relays.get(peer_id) {
                if relay.has_capacity() && !relay.is_stale() {
                    return Some(relay);
                }
            }
        }
        // Fallback: return any non-stale relay
        self.relays.values().find(|r| !r.is_stale())
    }

    /// Get all relay nodes sorted by score.
    pub fn ranked_relays(&self) -> Vec<(RelayNode, f64)> {
        let mut result: Vec<(RelayNode, f64)> = self
            .relays
            .values()
            .map(|r| {
                let score = self.scorer.get_score(&r.peer_id).map(|s| s.score).unwrap_or(0.5);
                (r.clone(), score)
            })
            .collect();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        result
    }

    /// Remove stale relays from the pool.
    pub fn cleanup_stale(&mut self) {
        let stale_ids: Vec<String> = self
            .relays
            .iter()
            .filter(|(_, r)| r.is_stale())
            .map(|(id, _)| id.clone())
            .collect();
        for id in &stale_ids {
            self.relays.remove(id);
        }
    }

    /// Number of known relays.
    pub fn relay_count(&self) -> usize {
        self.relays.len()
    }

    /// Number of relays with active reservations.
    pub fn reservation_count(&self) -> usize {
        self.relays.values().filter(|r| r.has_reservation).count()
    }

    /// Get a reference to the underlying scorer.
    pub fn scorer(&self) -> &NodeScorer {
        &self.scorer
    }

    /// Get all relay peer IDs.
    pub fn relay_peer_ids(&self) -> Vec<String> {
        self.relays.keys().cloned().collect()
    }

    /// Check if we have any relay with a reservation.
    pub fn has_active_reservation(&self) -> bool {
        self.relays.values().any(|r| r.has_reservation)
    }

    /// Remove a relay from the pool.
    pub fn remove_relay(&mut self, peer_id: &str) {
        self.relays.remove(peer_id);
    }
}

impl Default for RelayDiscovery {
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
    fn test_new_discovery_empty() {
        let rd = RelayDiscovery::new();
        assert_eq!(rd.relay_count(), 0);
        assert!(rd.best_relay().is_none());
    }

    #[test]
    fn test_add_potential_relay() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        assert_eq!(rd.relay_count(), 1);
    }

    #[test]
    fn test_max_pool_size() {
        let mut rd = RelayDiscovery::new();
        for i in 0..MAX_RELAY_POOL_SIZE + 5 {
            rd.add_potential_relay(&format!("peer-{}", i), true);
        }
        assert_eq!(rd.relay_count(), MAX_RELAY_POOL_SIZE);
    }

    #[test]
    fn test_best_relay_with_capacity() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        // Give it a good score
        for _ in 0..5 {
            rd.record_relay_success("peer-a", Duration::from_millis(50));
        }
        let best = rd.best_relay();
        assert!(best.is_some());
        assert_eq!(best.unwrap().peer_id, "peer-a");
    }

    #[test]
    fn test_reservation_tracking() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        assert!(!rd.has_active_reservation());
        rd.record_reservation("peer-a");
        assert!(rd.has_active_reservation());
        assert_eq!(rd.reservation_count(), 1);
    }

    #[test]
    fn test_update_bandwidth() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        rd.update_bandwidth("peer-a", 1_000_000);
        let relay = rd.relays.get("peer-a").unwrap();
        assert_eq!(relay.bandwidth_bps, 1_000_000);
    }

    #[test]
    fn test_update_circuits() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        rd.update_circuits("peer-a", 3, 10);
        let relay = rd.relays.get("peer-a").unwrap();
        assert_eq!(relay.active_circuits, 3);
        assert_eq!(relay.max_circuits, 10);
        assert!(relay.has_capacity());
    }

    #[test]
    fn test_no_capacity_when_full() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        rd.update_circuits("peer-a", 10, 10);
        let relay = rd.relays.get("peer-a").unwrap();
        assert!(!relay.has_capacity());
    }

    #[test]
    fn test_ranked_relays() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        rd.add_potential_relay("peer-b", true);
        rd.record_relay_success("peer-a", Duration::from_millis(30));
        rd.record_relay_failure("peer-b");
        let ranked = rd.ranked_relays();
        assert_eq!(ranked.len(), 2);
        // peer-a should have higher score
        assert_eq!(ranked[0].0.peer_id, "peer-a");
    }

    #[test]
    fn test_remove_relay() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        assert_eq!(rd.relay_count(), 1);
        rd.remove_relay("peer-a");
        assert_eq!(rd.relay_count(), 0);
    }

    #[test]
    fn test_queried_tracking() {
        let mut rd = RelayDiscovery::new();
        assert!(!rd.was_queried("peer-a"));
        rd.mark_queried("peer-a");
        assert!(rd.was_queried("peer-a"));
        // Duplicate mark is idempotent
        rd.mark_queried("peer-a");
        assert_eq!(rd.queried_peers.len(), 1);
    }

    #[test]
    fn test_relay_peer_ids() {
        let mut rd = RelayDiscovery::new();
        rd.add_potential_relay("peer-a", true);
        rd.add_potential_relay("peer-b", false);
        let ids = rd.relay_peer_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"peer-a".to_string()));
        assert!(ids.contains(&"peer-b".to_string()));
    }
}
