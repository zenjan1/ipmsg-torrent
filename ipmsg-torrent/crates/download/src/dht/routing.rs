//! DHT routing table implementation
//!
//! Implements Kademlia-style routing with k-buckets

use super::NodeId;
use std::collections::VecDeque;
use std::net::SocketAddr;

/// Maximum nodes per bucket (k value)
const K: usize = 8;

/// Number of buckets (160 for 160-bit node IDs)
const NUM_BUCKETS: usize = 160;

/// A node in the DHT network
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub last_seen: std::time::Instant,
}

/// A k-bucket containing up to K nodes
#[derive(Debug)]
pub struct KBucket {
    nodes: VecDeque<Node>,
}

impl KBucket {
    fn new() -> Self {
        Self {
            nodes: VecDeque::with_capacity(K),
        }
    }

    /// Add or update a node in the bucket
    pub fn add_or_update(&mut self, node: Node) -> bool {
        // Check if node already exists
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node.id) {
            // Move to end (most recently seen)
            self.nodes.remove(pos);
            self.nodes.push_back(node);
            return true;
        }

        // If bucket not full, add to end
        if self.nodes.len() < K {
            self.nodes.push_back(node);
            return true;
        }

        // Bucket full - could ping oldest node and replace if unresponsive
        // For now, just reject
        false
    }

    /// Remove a node by ID
    pub fn remove(&mut self, id: &NodeId) -> bool {
        if let Some(pos) = self.nodes.iter().position(|n| &n.id == id) {
            self.nodes.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get all nodes in the bucket
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// Check if bucket is full
    pub fn is_full(&self) -> bool {
        self.nodes.len() >= K
    }
}

/// Routing table with k-buckets
pub struct RoutingTable {
    our_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    /// Create a new routing table
    pub fn new(our_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(KBucket::new());
        }
        Self { our_id, buckets }
    }

    /// Calculate the bucket index for a given node ID
    fn bucket_index(&self, id: &NodeId) -> usize {
        let distance = xor_distance(&self.our_id, id);

        // Find the highest bit set
        for (i, byte) in distance.iter().enumerate() {
            if *byte != 0 {
                let bit_pos = 7 - byte.leading_zeros() as usize;
                return (i * 8) + bit_pos;
            }
        }

        // Same ID (shouldn't happen in practice)
        0
    }

    /// Add or update a node
    pub fn add_node(&mut self, node: Node) -> bool {
        if node.id == self.our_id {
            return false; // Don't add ourselves
        }

        let bucket_idx = self.bucket_index(&node.id);
        self.buckets[bucket_idx].add_or_update(node)
    }

    /// Remove a node
    pub fn remove_node(&mut self, id: &NodeId) -> bool {
        let bucket_idx = self.bucket_index(id);
        self.buckets[bucket_idx].remove(id)
    }

    /// Find the K closest nodes to a target ID
    pub fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<Node> {
        let mut all_nodes: Vec<Node> = self
            .buckets
            .iter()
            .flat_map(|b| b.nodes().cloned())
            .collect();

        // Sort by XOR distance to target
        all_nodes.sort_by(|a, b| {
            let dist_a = xor_distance(&a.id, target);
            let dist_b = xor_distance(&b.id, target);
            dist_a.cmp(&dist_b)
        });

        all_nodes.into_iter().take(count).collect()
    }

    /// Get total number of nodes in routing table
    pub fn node_count(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }
}

/// Calculate XOR distance between two node IDs
fn xor_distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = a[i] ^ b[i];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_distance() {
        let a = [0u8; 20];
        let b = [0xFFu8; 20];
        let dist = xor_distance(&a, &b);
        assert_eq!(dist, [0xFFu8; 20]);
    }

    #[test]
    fn test_routing_table_add_node() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        let mut node_id = [0u8; 20];
        node_id[0] = 0x80; // Different in first bit

        let node = Node {
            id: node_id,
            addr: "127.0.0.1:6881".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };

        assert!(table.add_node(node));
        assert_eq!(table.node_count(), 1);
    }

    #[test]
    fn test_closest_nodes() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add some nodes
        for i in 0..5 {
            let mut node_id = [0u8; 20];
            node_id[0] = i;

            let node = Node {
                id: node_id,
                addr: format!("127.0.0.1:{}", 6881u16 + i as u16).parse().unwrap(),
                last_seen: std::time::Instant::now(),
            };
            table.add_node(node);
        }

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 3);
        assert_eq!(closest.len(), 3);
    }
}
