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

    // Helper to create a node with a specific ID pattern
    fn make_node(id_byte: u8, port: u16) -> Node {
        let mut id = [0u8; 20];
        id[0] = id_byte;
        Node {
            id,
            addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            last_seen: std::time::Instant::now(),
        }
    }

    fn make_node_at_byte(byte_idx: usize, value: u8) -> Node {
        let mut id = [0u8; 20];
        id[byte_idx] = value;
        Node {
            id,
            addr: format!("127.0.0.1:{}", 6881 + byte_idx as u16)
                .parse()
                .unwrap(),
            last_seen: std::time::Instant::now(),
        }
    }

    // ===== Constants =====
    #[test]
    fn test_k_constant() {
        assert_eq!(K, 8);
    }

    #[test]
    fn test_num_buckets_constant() {
        assert_eq!(NUM_BUCKETS, 160);
    }

    // ===== xor_distance =====
    #[test]
    fn test_xor_distance_all_zeros_vs_all_ones() {
        let a = [0u8; 20];
        let b = [0xFFu8; 20];
        let dist = xor_distance(&a, &b);
        assert_eq!(dist, [0xFFu8; 20]);
    }

    #[test]
    fn test_xor_distance_same_id() {
        let a = [0x42u8; 20];
        let dist = xor_distance(&a, &a);
        assert_eq!(dist, [0u8; 20]);
    }

    #[test]
    fn test_xor_distance_symmetry() {
        let a = [0xAAu8; 20];
        let b = [0x55u8; 20];
        assert_eq!(xor_distance(&a, &b), xor_distance(&b, &a));
    }

    #[test]
    fn test_xor_distance_single_bit() {
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        b[0] = 0x01;
        let dist = xor_distance(&a, &b);
        assert_eq!(dist[0], 0x01);
        assert_eq!(dist[1], 0x00);
    }

    #[test]
    fn test_xor_distance_last_byte() {
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        b[19] = 0xFF;
        let dist = xor_distance(&a, &b);
        assert_eq!(dist[19], 0xFF);
        assert_eq!(dist[0], 0x00);
    }

    #[test]
    fn test_xor_distance_mixed_pattern() {
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        a[0] = 0xF0;
        b[0] = 0x0F;
        let dist = xor_distance(&a, &b);
        assert_eq!(dist[0], 0xFF);
    }

    // ===== KBucket =====
    #[test]
    fn test_kbucket_new_is_empty() {
        let bucket = KBucket::new();
        assert_eq!(bucket.nodes.len(), 0);
        assert!(!bucket.is_full());
    }

    #[test]
    fn test_kbucket_add_node() {
        let mut bucket = KBucket::new();
        let node = make_node(0x80, 6881);
        assert!(bucket.add_or_update(node));
        assert_eq!(bucket.nodes.len(), 1);
        assert!(!bucket.is_full());
    }

    #[test]
    fn test_kbucket_add_k_nodes() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            let node = make_node(i as u8, 6881 + i as u16);
            assert!(bucket.add_or_update(node));
        }
        assert_eq!(bucket.nodes.len(), K);
        assert!(bucket.is_full());
    }

    #[test]
    fn test_kbucket_reject_when_full() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            let node = make_node(i as u8, 6881 + i as u16);
            bucket.add_or_update(node);
        }
        // Try to add one more
        let extra = make_node(0xFF, 9999);
        assert!(!bucket.add_or_update(extra));
        assert_eq!(bucket.nodes.len(), K);
    }

    #[test]
    fn test_kbucket_update_existing_node() {
        let mut bucket = KBucket::new();
        let node1 = make_node(0x80, 6881);
        bucket.add_or_update(node1);

        // Update same node with different port
        let node1_updated = make_node(0x80, 7000);
        assert!(bucket.add_or_update(node1_updated));
        assert_eq!(bucket.nodes.len(), 1);

        // Verify the node was moved to the end (most recently seen)
        let stored = bucket.nodes.back().unwrap();
        assert_eq!(stored.addr.port(), 7000);
    }

    #[test]
    fn test_kbucket_update_moves_to_back() {
        let mut bucket = KBucket::new();
        for i in 0..4 {
            bucket.add_or_update(make_node(i, 6881 + i as u16));
        }
        // Update first node
        bucket.add_or_update(make_node(0, 9999));
        // Should be at the back now
        let last = bucket.nodes.back().unwrap();
        assert_eq!(last.id[0], 0);
        assert_eq!(last.addr.port(), 9999);
    }

    #[test]
    fn test_kbucket_remove_existing() {
        let mut bucket = KBucket::new();
        bucket.add_or_update(make_node(0x80, 6881));
        let mut id = [0u8; 20];
        id[0] = 0x80;
        assert!(bucket.remove(&id));
        assert_eq!(bucket.nodes.len(), 0);
    }

    #[test]
    fn test_kbucket_remove_nonexistent() {
        let mut bucket = KBucket::new();
        bucket.add_or_update(make_node(0x80, 6881));
        let id = [0xFFu8; 20];
        assert!(!bucket.remove(&id));
        assert_eq!(bucket.nodes.len(), 1);
    }

    #[test]
    fn test_kbucket_remove_from_empty() {
        let mut bucket = KBucket::new();
        let id = [0u8; 20];
        assert!(!bucket.remove(&id));
    }

    #[test]
    fn test_kbucket_nodes_iterator() {
        let mut bucket = KBucket::new();
        for i in 0..3 {
            bucket.add_or_update(make_node(i, 6881 + i as u16));
        }
        let nodes: Vec<_> = bucket.nodes().collect();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_kbucket_is_full_boundary() {
        let mut bucket = KBucket::new();
        for i in 0..K - 1 {
            bucket.add_or_update(make_node(i as u8, 6881 + i as u16));
            assert!(!bucket.is_full());
        }
        bucket.add_or_update(make_node((K - 1) as u8, 9999));
        assert!(bucket.is_full());
    }

    #[test]
    fn test_kbucket_add_after_remove() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            bucket.add_or_update(make_node(i as u8, 6881 + i as u16));
        }
        assert!(bucket.is_full());

        // Remove one
        let id = [0u8; 20];
        bucket.remove(&id);
        assert!(!bucket.is_full());

        // Now can add
        let new_node = make_node(0xAA, 8888);
        assert!(bucket.add_or_update(new_node));
    }

    // ===== RoutingTable =====
    #[test]
    fn test_routing_table_new() {
        let our_id = [0u8; 20];
        let table = RoutingTable::new(our_id);
        assert_eq!(table.buckets.len(), NUM_BUCKETS);
        assert_eq!(table.node_count(), 0);
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
    fn test_routing_table_reject_self() {
        let our_id = [0x42u8; 20];
        let mut table = RoutingTable::new(our_id);

        let node = Node {
            id: our_id,
            addr: "127.0.0.1:6881".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };

        assert!(!table.add_node(node));
        assert_eq!(table.node_count(), 0);
    }

    #[test]
    fn test_routing_table_add_multiple() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        for i in 1..10 {
            table.add_node(make_node(i, 6881 + i as u16));
        }
        assert_eq!(table.node_count(), 9);
    }

    #[test]
    fn test_routing_table_remove_node() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        table.add_node(make_node(0x80, 6881));
        assert_eq!(table.node_count(), 1);

        let id = {
            let mut id = [0u8; 20];
            id[0] = 0x80;
            id
        };
        assert!(table.remove_node(&id));
        assert_eq!(table.node_count(), 0);
    }

    #[test]
    fn test_routing_table_remove_nonexistent() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        table.add_node(make_node(0x80, 6881));
        let id = [0xFFu8; 20];
        assert!(!table.remove_node(&id));
        assert_eq!(table.node_count(), 1);
    }

    #[test]
    fn test_routing_table_closest_nodes() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add some nodes
        for i in 0..5 {
            table.add_node(make_node(i, 6881 + i as u16));
        }

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 3);
        assert_eq!(closest.len(), 3);
    }

    #[test]
    fn test_routing_table_closest_nodes_sorted_by_distance() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add nodes with different distances from target [0;20]
        table.add_node(make_node(1, 6881)); // distance = 1
        table.add_node(make_node(3, 6882)); // distance = 3
        table.add_node(make_node(2, 6883)); // distance = 2

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 3);
        assert_eq!(closest.len(), 3);
        // Verify sorted by XOR distance
        let d0 = xor_distance(&closest[0].id, &target);
        let d1 = xor_distance(&closest[1].id, &target);
        let d2 = xor_distance(&closest[2].id, &target);
        assert!(d0 <= d1);
        assert!(d1 <= d2);
    }

    #[test]
    fn test_routing_table_closest_nodes_more_than_available() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        table.add_node(make_node(1, 6881));
        table.add_node(make_node(2, 6882));

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 10);
        assert_eq!(closest.len(), 2);
    }

    #[test]
    fn test_routing_table_closest_nodes_empty() {
        let our_id = [0u8; 20];
        let table = RoutingTable::new(our_id);

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 5);
        assert_eq!(closest.len(), 0);
    }

    #[test]
    fn test_routing_table_closest_nodes_zero_count() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        table.add_node(make_node(1, 6881));
        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 0);
        assert_eq!(closest.len(), 0);
    }

    #[test]
    fn test_routing_table_bucket_index_first_byte() {
        let our_id = [0u8; 20];
        let table = RoutingTable::new(our_id);

        // Node with first bit different -> bucket 7 (highest bit of byte 0)
        let mut id = [0u8; 20];
        id[0] = 0x80; // 10000000
        let idx = table.bucket_index(&id);
        assert_eq!(idx, 7);
    }

    #[test]
    fn test_routing_table_bucket_index_second_byte() {
        let our_id = [0u8; 20];
        let table = RoutingTable::new(our_id);

        // First byte same, second byte differs
        let mut id = [0u8; 20];
        id[1] = 0x01; // lowest bit of byte 1
        let idx = table.bucket_index(&id);
        assert_eq!(idx, 8); // byte 1 * 8 + bit 0
    }

    #[test]
    fn test_routing_table_bucket_index_last_byte() {
        let our_id = [0u8; 20];
        let table = RoutingTable::new(our_id);

        let mut id = [0u8; 20];
        id[19] = 0x80;
        let idx = table.bucket_index(&id);
        assert_eq!(idx, 159); // byte 19 * 8 + bit 7
    }

    #[test]
    fn test_routing_table_bucket_index_same_id() {
        let our_id = [0x42u8; 20];
        let table = RoutingTable::new(our_id);

        let id = [0x42u8; 20];
        let idx = table.bucket_index(&id);
        assert_eq!(idx, 0); // Same ID returns bucket 0
    }

    #[test]
    fn test_routing_table_node_count_across_buckets() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add nodes that go to different buckets
        for i in 0..8 {
            let mut id = [0u8; 20];
            id[0] = 1 << i; // Different bit positions
            let node = Node {
                id,
                addr: format!("127.0.0.1:{}", 6881 + i).parse().unwrap(),
                last_seen: std::time::Instant::now(),
            };
            table.add_node(node);
        }
        assert_eq!(table.node_count(), 8);
    }

    #[test]
    fn test_routing_table_update_existing_node() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        let mut node_id = [0u8; 20];
        node_id[0] = 0x80;
        let node = Node {
            id: node_id,
            addr: "127.0.0.1:6881".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        table.add_node(node);

        // Update same node
        let node2 = Node {
            id: node_id,
            addr: "127.0.0.1:7000".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        table.add_node(node2);

        // Count should still be 1
        assert_eq!(table.node_count(), 1);
    }

    // ===== Node =====
    #[test]
    fn test_node_clone() {
        let node = make_node(0x42, 6881);
        let cloned = node.clone();
        assert_eq!(node.id, cloned.id);
        assert_eq!(node.addr, cloned.addr);
    }

    #[test]
    fn test_node_debug() {
        let node = make_node(0x42, 6881);
        let debug_str = format!("{:?}", node);
        assert!(debug_str.contains("Node"));
    }

    // ===== KBucket Debug =====
    #[test]
    fn test_kbucket_debug() {
        let mut bucket = KBucket::new();
        bucket.add_or_update(make_node(0x42, 6881));
        let debug_str = format!("{:?}", bucket);
        assert!(debug_str.contains("KBucket"));
    }

    // ===== Integration / complex workflows =====
    #[test]
    fn test_routing_table_add_remove_readd() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        let mut node_id = [0u8; 20];
        node_id[0] = 0x80;

        let node = Node {
            id: node_id,
            addr: "127.0.0.1:6881".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        table.add_node(node);
        assert_eq!(table.node_count(), 1);

        table.remove_node(&node_id);
        assert_eq!(table.node_count(), 0);

        let node2 = Node {
            id: node_id,
            addr: "127.0.0.1:7000".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        table.add_node(node2);
        assert_eq!(table.node_count(), 1);
    }

    #[test]
    fn test_routing_table_many_nodes_different_buckets() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add nodes spanning many buckets
        for byte_idx in 0..20 {
            for bit in 0..8 {
                let mut id = [0u8; 20];
                id[byte_idx] = 1 << bit;
                let node = Node {
                    id,
                    addr: format!("127.0.0.1:{}", 6881 + byte_idx * 8 + bit)
                        .parse()
                        .unwrap(),
                    last_seen: std::time::Instant::now(),
                };
                table.add_node(node);
            }
        }
        assert_eq!(table.node_count(), 160);
    }

    #[test]
    fn test_closest_nodes_with_specific_target() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Add nodes at various distances
        for i in 1..=5 {
            let mut id = [0u8; 20];
            id[0] = i;
            let node = Node {
                id,
                addr: format!("127.0.0.1:{}", 6880 + i as u16).parse().unwrap(),
                last_seen: std::time::Instant::now(),
            };
            table.add_node(node);
        }

        // Target is node with id[0]=3
        let mut target = [0u8; 20];
        target[0] = 3;
        let closest = table.closest_nodes(&target, 2);

        // Closest should be the node with id[0]=3 itself (distance 0)
        assert_eq!(closest[0].id[0], 3);
    }

    #[test]
    fn test_routing_table_bucket_full_rejects() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Fill a specific bucket (bucket 7 = first bit of byte 0 differs)
        for i in 0..K {
            let mut id = [0u8; 20];
            id[0] = 0x80 | (i as u8 & 0x7F); // All go to bucket 7
            // Make sure they're unique
            id[1] = i as u8;
            let node = Node {
                id,
                addr: format!("127.0.0.1:{}", 6881 + i as u16).parse().unwrap(),
                last_seen: std::time::Instant::now(),
            };
            table.add_node(node);
        }

        // Try to add one more to same bucket
        let mut overflow_id = [0u8; 20];
        overflow_id[0] = 0x80;
        overflow_id[1] = 0xFF;
        overflow_id[2] = 0xFF;
        let overflow_node = Node {
            id: overflow_id,
            addr: "127.0.0.1:9999".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        assert!(!table.add_node(overflow_node));
    }

    #[test]
    fn test_xor_distance_self_is_zero() {
        let id = [0xABu8; 20];
        let dist = xor_distance(&id, &id);
        assert_eq!(dist, [0u8; 20]);
    }

    #[test]
    fn test_routing_table_closest_nodes_unicode_addr() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        // Nodes with various addresses (not unicode in addr, but testing robustness)
        let node = Node {
            id: make_node(1, 6881).id,
            addr: "127.0.0.1:6881".parse().unwrap(),
            last_seen: std::time::Instant::now(),
        };
        table.add_node(node);

        let target = [0u8; 20];
        let closest = table.closest_nodes(&target, 1);
        assert_eq!(closest.len(), 1);
    }

    #[test]
    fn test_kbucket_remove_preserves_order() {
        let mut bucket = KBucket::new();
        for i in 0..5 {
            bucket.add_or_update(make_node(i, 6881 + i as u16));
        }

        // Remove middle node (id[0] = 2)
        let mut remove_id = [0u8; 20];
        remove_id[0] = 2;
        bucket.remove(&remove_id);

        assert_eq!(bucket.nodes.len(), 4);
        let ids: Vec<u8> = bucket.nodes.iter().map(|n| n.id[0]).collect();
        assert_eq!(ids, vec![0, 1, 3, 4]);
    }

    #[test]
    fn test_routing_table_independent_instances() {
        let id_a = [0u8; 20];
        let id_b = [0xFFu8; 20];

        let mut table_a = RoutingTable::new(id_a);
        let mut table_b = RoutingTable::new(id_b);

        let node = make_node(0x80, 6881);
        table_a.add_node(node.clone());
        table_b.add_node(node.clone());

        assert_eq!(table_a.node_count(), 1);
        assert_eq!(table_b.node_count(), 1);

        // Remove from A doesn't affect B
        let mut rm_id = [0u8; 20];
        rm_id[0] = 0x80;
        table_a.remove_node(&rm_id);
        assert_eq!(table_a.node_count(), 0);
        assert_eq!(table_b.node_count(), 1);
    }
}
