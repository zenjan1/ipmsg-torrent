//! DHT (Distributed Hash Table) implementation for BitTorrent
//!
//! Implements BEP 0005: DHT Protocol
//! Allows finding peers and fetching metadata without trackers

pub mod message;
pub mod node;
pub mod routing;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// DHT node ID (20 bytes)
pub type NodeId = [u8; 20];

/// Info hash (20 bytes)
pub type InfoHash = [u8; 20];

/// DHT manager
pub struct DhtManager {
    node_id: NodeId,
    routing_table: Arc<Mutex<routing::RoutingTable>>,
    #[allow(dead_code)]
    pending_queries: Arc<Mutex<HashMap<NodeId, QueryState>>>,
    peers: Arc<Mutex<HashMap<InfoHash, Vec<SocketAddr>>>>,
    tokens: Arc<Mutex<HashMap<SocketAddr, Vec<u8>>>>,
    socket: Option<Arc<UdpSocket>>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum QueryState {
    FindingPeers {
        info_hash: InfoHash,
        peers_found: Vec<SocketAddr>,
    },
    FetchingMetadata {
        info_hash: InfoHash,
        metadata: Option<Vec<u8>>,
    },
}

impl DhtManager {
    /// Create a new DHT manager with random node ID
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut node_id = [0u8; 20];
        for byte in node_id.iter_mut() {
            *byte = rand::random();
        }

        Self {
            node_id,
            routing_table: Arc::new(Mutex::new(routing::RoutingTable::new(node_id))),
            pending_queries: Arc::new(Mutex::new(HashMap::new())),
            peers: Arc::new(Mutex::new(HashMap::new())),
            tokens: Arc::new(Mutex::new(HashMap::new())),
            socket: None,
        }
    }

    /// Set the UDP socket for sending messages
    pub fn set_socket(&mut self, socket: Arc<UdpSocket>) {
        self.socket = Some(socket);
    }

    /// Add a node to the routing table
    pub async fn add_node(&self, node: routing::Node) {
        let mut table = self.routing_table.lock().await;
        table.add_node(node);
    }

    /// Get the K closest nodes to a target ID
    pub async fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<(NodeId, SocketAddr)> {
        let table = self.routing_table.lock().await;
        table
            .closest_nodes(target, count)
            .into_iter()
            .map(|n| (n.id, n.addr))
            .collect()
    }

    /// Get peers for an info hash
    pub async fn get_peers(&self, info_hash: &InfoHash) -> Vec<SocketAddr> {
        let peers = self.peers.lock().await;
        peers.get(info_hash).cloned().unwrap_or_default()
    }

    /// Add a peer for an info hash
    pub async fn add_peer(&self, info_hash: InfoHash, peer: SocketAddr) {
        let mut peers = self.peers.lock().await;
        peers.entry(info_hash).or_insert_with(Vec::new).push(peer);
    }

    /// Generate a token for a peer
    pub async fn generate_token(&self, addr: SocketAddr) -> Vec<u8> {
        let mut token = vec![0u8; 8];
        for byte in token.iter_mut() {
            *byte = rand::random();
        }

        let mut tokens = self.tokens.lock().await;
        tokens.insert(addr, token.clone());

        token
    }

    /// Verify a token from a peer
    pub async fn verify_token(&self, token: &[u8], addr: SocketAddr) -> bool {
        let tokens = self.tokens.lock().await;
        tokens.get(&addr).map(|t| t == token).unwrap_or(false)
    }

    /// Bootstrap the DHT by contacting known nodes
    pub async fn bootstrap(&self, bootstrap_nodes: &[SocketAddr]) -> Result<(), DhtError> {
        tracing::info!("Bootstrapping DHT with {} nodes", bootstrap_nodes.len());

        // Send find_node queries to bootstrap nodes
        for addr in bootstrap_nodes {
            if let Err(e) = self.send_find_node(*addr, self.node_id).await {
                tracing::warn!("Failed to contact bootstrap node {}: {}", addr, e);
            }
        }

        Ok(())
    }

    /// Find peers for a given info hash using iterative lookup
    pub async fn find_peers(&self, info_hash: InfoHash) -> Result<Vec<SocketAddr>, DhtError> {
        tracing::info!("Finding peers for info hash: {}", hex::encode(info_hash));

        // Get peers from local storage
        let peers = self.get_peers(&info_hash).await;
        if !peers.is_empty() {
            return Ok(peers);
        }

        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| DhtError::Network("UDP socket not set".to_string()))?;

        // Iterative lookup: query closest nodes and collect their responses
        let mut queried_nodes = std::collections::HashSet::new();
        let mut found_peers = Vec::new();
        let mut closest_nodes = self.closest_nodes(&info_hash, 8).await;

        // Iterative lookup loop (max 3 rounds)
        for _round in 0..3 {
            if closest_nodes.is_empty() {
                break;
            }

            let mut new_nodes_this_round = Vec::new();

            for (node_id, node_addr) in &closest_nodes {
                if queried_nodes.contains(node_id) {
                    continue;
                }
                queried_nodes.insert(*node_id);

                // Send get_peers query
                let msg = message::DhtMessage::Query {
                    transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
                    query: message::QueryType::GetPeers {
                        id: self.node_id,
                        info_hash,
                    },
                };

                if let Ok(data) = msg.encode() {
                    if let Err(e) = socket.send_to(&data, node_addr).await {
                        tracing::debug!("Failed to send get_peers to {}: {}", node_addr, e);
                        continue;
                    }

                    // Wait for response with timeout
                    let mut buf = vec![0u8; 65535];
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        socket.recv_from(&mut buf),
                    )
                    .await
                    {
                        Ok(Ok((len, _from_addr))) => {
                            if let Ok(message::DhtMessage::Response { response, .. }) =
                                message::DhtMessage::decode(&buf[..len])
                                && let message::ResponseType::Peers { values, nodes, .. } = response
                            {
                                // Store any peers we found
                                if let Some(peer_addrs) = values {
                                    for peer_addr in peer_addrs {
                                        self.add_peer(info_hash, peer_addr).await;
                                        found_peers.push(peer_addr);
                                    }
                                }
                                // Add closer nodes for next iteration
                                for (closer_id, closer_addr) in nodes {
                                    self.add_node(routing::Node {
                                        id: closer_id,
                                        addr: closer_addr,
                                        last_seen: std::time::Instant::now(),
                                    })
                                    .await;
                                    new_nodes_this_round.push((closer_id, closer_addr));
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::debug!("Failed to receive from {}: {}", node_addr, e);
                        }
                        Err(_) => {
                            tracing::debug!("Timeout waiting for response from {}", node_addr);
                        }
                    }
                }
            }

            // Get new closest nodes (excluding already queried)
            let all_closest = self.closest_nodes(&info_hash, 16).await;
            let new_nodes: Vec<_> = all_closest
                .into_iter()
                .filter(|(id, _)| !queried_nodes.contains(id))
                .take(8)
                .collect();

            if new_nodes.is_empty() {
                break;
            }
            closest_nodes = new_nodes;
        }

        // Return any peers we found
        if !found_peers.is_empty() {
            return Ok(found_peers);
        }

        Ok(Vec::new())
    }

    /// Fetch torrent metadata from DHT peers
    pub async fn fetch_metadata(&self, info_hash: InfoHash) -> Result<Vec<u8>, DhtError> {
        tracing::info!(
            "Fetching metadata for info hash: {}",
            hex::encode(info_hash)
        );

        // Get peers that might have the metadata
        let peers = self.find_peers(info_hash).await?;
        if peers.is_empty() {
            return Err(DhtError::NoPeers);
        }

        tracing::info!("Found {} peers, attempting metadata exchange", peers.len());

        // Try to fetch metadata from each peer
        let mut fetcher = crate::torrent::metadata::MetadataFetcher::new(info_hash);
        let mut last_error = None;

        for peer_addr in &peers {
            tracing::debug!("Trying peer {} for metadata", peer_addr);

            // Generate a random peer ID for this connection
            let mut peer_id = [0u8; 20];
            for byte in peer_id.iter_mut() {
                *byte = rand::random();
            }

            match fetcher.fetch_from_peer(*peer_addr, peer_id).await {
                Ok(metadata_bytes) => {
                    tracing::info!("Successfully fetched metadata from {}", peer_addr);
                    return Ok(metadata_bytes);
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch from {}: {}", peer_addr, e);
                    last_error = Some(e);
                    continue;
                }
            }
        }

        Err(DhtError::Protocol(format!(
            "Failed to fetch metadata from all {} peers: {:?}",
            peers.len(),
            last_error
        )))
    }

    async fn send_find_node(&self, addr: SocketAddr, target: NodeId) -> Result<(), DhtError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| DhtError::Network("UDP socket not set".to_string()))?;

        let msg = message::DhtMessage::Query {
            transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
            query: message::QueryType::FindNode {
                id: self.node_id,
                target,
            },
        };

        let data = msg
            .encode()
            .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;

        socket
            .send_to(&data, addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        tracing::debug!(
            "Sent find_node to {} for target {}",
            addr,
            hex::encode(target)
        );
        Ok(())
    }

    /// Get this node's ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DhtError {
    #[error("not implemented")]
    NotImplemented,
    #[error("timeout")]
    Timeout,
    #[error("network error: {0}")]
    Network(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("no peers found")]
    NoPeers,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::routing::Node;
    use std::time::{Duration, Instant};

    // ── Type aliases ──

    #[test]
    fn test_node_id_type_is_20_bytes() {
        let id: NodeId = [0u8; 20];
        assert_eq!(id.len(), 20);
        assert_eq!(std::mem::size_of::<NodeId>(), 20);
    }

    #[test]
    fn test_info_hash_type_is_20_bytes() {
        let hash: InfoHash = [0u8; 20];
        assert_eq!(hash.len(), 20);
        assert_eq!(std::mem::size_of::<InfoHash>(), 20);
    }

    #[test]
    fn test_node_id_equality() {
        let a: NodeId = [1u8; 20];
        let b: NodeId = [1u8; 20];
        let c: NodeId = [2u8; 20];
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_info_hash_equality() {
        let a: InfoHash = [0xAA; 20];
        let b: InfoHash = [0xAA; 20];
        let c: InfoHash = [0xBB; 20];
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_node_id_clone() {
        let a: NodeId = [42u8; 20];
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_info_hash_clone() {
        let a: InfoHash = [99u8; 20];
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_node_id_all_zeros() {
        let id: NodeId = [0u8; 20];
        assert!(id.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_node_id_all_ff() {
        let id: NodeId = [0xFF; 20];
        assert!(id.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_info_hash_as_slice() {
        let hash: InfoHash = [0x42; 20];
        let slice = &hash[..];
        assert_eq!(slice.len(), 20);
        assert!(slice.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_node_id_hash_trait() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let a: NodeId = [1u8; 20];
        let b: NodeId = [2u8; 20];
        set.insert(a);
        set.insert(b);
        set.insert(a); // duplicate
        assert_eq!(set.len(), 2);
    }

    // ── DhtManager::new() ──

    #[test]
    fn test_dht_manager_creation() {
        let manager = DhtManager::new();
        assert_eq!(manager.node_id().len(), 20);
    }

    #[test]
    fn test_new_generates_non_zero_node_id() {
        // Extremely unlikely to be all zeros with random generation
        let manager = DhtManager::new();
        let id = manager.node_id();
        assert!(
            id.iter().any(|&b| b != 0),
            "node_id should not be all zeros"
        );
    }

    #[test]
    fn test_new_unique_node_ids() {
        let m1 = DhtManager::new();
        let m2 = DhtManager::new();
        assert_ne!(
            m1.node_id(),
            m2.node_id(),
            "two managers should have different node IDs"
        );
    }

    #[test]
    fn test_new_empty_routing_table() {
        let manager = DhtManager::new();
        // closest_nodes should return empty when no nodes added
        let target: NodeId = [0u8; 20];
        let rt = tokio::runtime::Runtime::new().unwrap();
        let closest = rt.block_on(manager.closest_nodes(&target, 8));
        assert!(closest.is_empty());
    }

    #[test]
    fn test_new_empty_peers() {
        let manager = DhtManager::new();
        let hash: InfoHash = [0u8; 20];
        let rt = tokio::runtime::Runtime::new().unwrap();
        let peers = rt.block_on(manager.get_peers(&hash));
        assert!(peers.is_empty());
    }

    #[test]
    fn test_new_no_socket_set() {
        let manager = DhtManager::new();
        assert!(manager.socket.is_none());
    }

    #[test]
    fn test_new_empty_tokens() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = rt.block_on(manager.verify_token(b"token", addr));
        assert!(!valid, "no tokens should exist initially");
    }

    #[test]
    fn test_new_empty_pending_queries() {
        let manager = DhtManager::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pending = rt.block_on(manager.pending_queries.lock());
        assert!(pending.is_empty());
    }

    // ── DhtManager::node_id() ──

    #[test]
    fn test_node_id_returns_same_value() {
        let manager = DhtManager::new();
        let id1 = manager.node_id();
        let id2 = manager.node_id();
        assert_eq!(id1, id2, "node_id() should return the same value");
    }

    #[test]
    fn test_node_id_is_20_bytes() {
        let manager = DhtManager::new();
        assert_eq!(manager.node_id().len(), 20);
    }

    // ── DhtManager::set_socket() ──

    #[tokio::test]
    async fn test_set_socket() {
        let mut manager = DhtManager::new();
        assert!(manager.socket.is_none());
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        manager.set_socket(socket.clone());
        assert!(manager.socket.is_some());
    }

    #[tokio::test]
    async fn test_set_socket_replaces_existing() {
        let mut manager = DhtManager::new();
        let socket1 = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket2 = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr1 = socket1.local_addr().unwrap();
        let addr2 = socket2.local_addr().unwrap();
        manager.set_socket(socket1);
        manager.set_socket(socket2.clone());
        assert_eq!(
            manager.socket.as_ref().unwrap().local_addr().unwrap(),
            addr2,
            "socket should be replaced"
        );
        assert_ne!(
            manager.socket.as_ref().unwrap().local_addr().unwrap(),
            addr1
        );
    }

    // ── DhtManager::add_node() ──

    #[tokio::test]
    async fn test_add_node_basic() {
        let manager = DhtManager::new();
        let node_id: NodeId = [1u8; 20];
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let node = Node {
            id: node_id,
            addr,
            last_seen: Instant::now(),
        };
        manager.add_node(node).await;
        let closest = manager.closest_nodes(&node_id, 8).await;
        assert_eq!(closest.len(), 1);
        assert_eq!(closest[0], (node_id, addr));
    }

    #[tokio::test]
    async fn test_add_multiple_nodes() {
        let manager = DhtManager::new();
        for i in 0u16..5 {
            let node_id = [i as u8; 20];
            let addr: SocketAddr = format!("127.0.0.1:{}", 8080 + i).parse().unwrap();
            manager
                .add_node(Node {
                    id: node_id,
                    addr,
                    last_seen: Instant::now(),
                })
                .await;
        }
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 10).await;
        assert_eq!(closest.len(), 5);
    }

    #[tokio::test]
    async fn test_add_node_updates_existing() {
        let manager = DhtManager::new();
        let node_id: NodeId = [5u8; 20];
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9090".parse().unwrap();

        manager
            .add_node(Node {
                id: node_id,
                addr: addr1,
                last_seen: Instant::now(),
            })
            .await;
        manager
            .add_node(Node {
                id: node_id,
                addr: addr2,
                last_seen: Instant::now(),
            })
            .await;

        let closest = manager.closest_nodes(&node_id, 8).await;
        // Should still have only 1 node (updated, not duplicated)
        assert_eq!(closest.len(), 1);
        assert_eq!(closest[0].1, addr2, "addr should be updated");
    }

    // ── DhtManager::closest_nodes() ──

    #[tokio::test]
    async fn test_closest_nodes_empty_table() {
        let manager = DhtManager::new();
        let target: NodeId = [42u8; 20];
        let closest = manager.closest_nodes(&target, 8).await;
        assert!(closest.is_empty());
    }

    #[tokio::test]
    async fn test_closest_nodes_returns_correct_count() {
        let manager = DhtManager::new();
        for i in 0u16..10 {
            let node_id = [i as u8; 20];
            let addr: SocketAddr = format!("127.0.0.1:{}", 8080 + i).parse().unwrap();
            manager
                .add_node(Node {
                    id: node_id,
                    addr,
                    last_seen: Instant::now(),
                })
                .await;
        }
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 5).await;
        assert_eq!(closest.len(), 5);
    }

    #[tokio::test]
    async fn test_closest_nodes_requesting_more_than_available() {
        let manager = DhtManager::new();
        for i in 0u16..3 {
            let node_id = [i as u8; 20];
            let addr: SocketAddr = format!("127.0.0.1:{}", 8080 + i).parse().unwrap();
            manager
                .add_node(Node {
                    id: node_id,
                    addr,
                    last_seen: Instant::now(),
                })
                .await;
        }
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 100).await;
        assert_eq!(closest.len(), 3);
    }

    #[tokio::test]
    async fn test_closest_nodes_sorted_by_distance() {
        let manager = DhtManager::new();
        // Add nodes at various distances from target [0u8; 20]
        let mut node_id = [0u8; 20];
        node_id[19] = 1; // distance 1
        manager
            .add_node(Node {
                id: node_id,
                addr: "127.0.0.1:8081".parse().unwrap(),
                last_seen: Instant::now(),
            })
            .await;

        let mut node_id2 = [0u8; 20];
        node_id2[19] = 3; // distance 3
        manager
            .add_node(Node {
                id: node_id2,
                addr: "127.0.0.1:8083".parse().unwrap(),
                last_seen: Instant::now(),
            })
            .await;

        let mut node_id3 = [0u8; 20];
        node_id3[19] = 2; // distance 2
        manager
            .add_node(Node {
                id: node_id3,
                addr: "127.0.0.1:8082".parse().unwrap(),
                last_seen: Instant::now(),
            })
            .await;

        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 3).await;
        assert_eq!(closest.len(), 3);
        // First should be closest (distance 1)
        assert_eq!(closest[0].0[19], 1);
        assert_eq!(closest[1].0[19], 2);
        assert_eq!(closest[2].0[19], 3);
    }

    #[tokio::test]
    async fn test_closest_nodes_zero_count() {
        let manager = DhtManager::new();
        let node_id: NodeId = [1u8; 20];
        manager
            .add_node(Node {
                id: node_id,
                addr: "127.0.0.1:8080".parse().unwrap(),
                last_seen: Instant::now(),
            })
            .await;
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 0).await;
        assert!(closest.is_empty());
    }

    // ── DhtManager::add_peer() ──

    #[tokio::test]
    async fn test_add_peer_basic() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        manager.add_peer(hash, peer).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], peer);
    }

    #[tokio::test]
    async fn test_add_peer_multiple_same_hash() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let peer2: SocketAddr = "192.168.1.2:6882".parse().unwrap();
        let peer3: SocketAddr = "192.168.1.3:6883".parse().unwrap();
        manager.add_peer(hash, peer1).await;
        manager.add_peer(hash, peer2).await;
        manager.add_peer(hash, peer3).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 3);
        assert!(peers.contains(&peer1));
        assert!(peers.contains(&peer2));
        assert!(peers.contains(&peer3));
    }

    #[tokio::test]
    async fn test_add_peer_different_hashes() {
        let manager = DhtManager::new();
        let hash1: InfoHash = [1u8; 20];
        let hash2: InfoHash = [2u8; 20];
        let peer1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let peer2: SocketAddr = "192.168.1.2:6882".parse().unwrap();
        manager.add_peer(hash1, peer1).await;
        manager.add_peer(hash2, peer2).await;
        let peers1 = manager.get_peers(&hash1).await;
        let peers2 = manager.get_peers(&hash2).await;
        assert_eq!(peers1.len(), 1);
        assert_eq!(peers2.len(), 1);
        assert_eq!(peers1[0], peer1);
        assert_eq!(peers2[0], peer2);
    }

    #[tokio::test]
    async fn test_add_peer_duplicate_same_addr() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        manager.add_peer(hash, peer).await;
        manager.add_peer(hash, peer).await;
        let peers = manager.get_peers(&hash).await;
        // Duplicate allowed (Vec, not HashSet)
        assert_eq!(peers.len(), 2);
    }

    #[tokio::test]
    async fn test_add_peer_ipv6() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "[::1]:6881".parse().unwrap();
        manager.add_peer(hash, peer).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 1);
        assert!(peers[0].ip().is_loopback());
    }

    #[tokio::test]
    async fn test_add_peer_port_boundaries() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer_low: SocketAddr = "192.168.1.1:1".parse().unwrap();
        let peer_high: SocketAddr = "192.168.1.1:65535".parse().unwrap();
        manager.add_peer(hash, peer_low).await;
        manager.add_peer(hash, peer_high).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 2);
    }

    // ── DhtManager::get_peers() ──

    #[tokio::test]
    async fn test_get_peers_nonexistent_hash() {
        let manager = DhtManager::new();
        let hash: InfoHash = [99u8; 20];
        let peers = manager.get_peers(&hash).await;
        assert!(peers.is_empty());
    }

    #[tokio::test]
    async fn test_get_peers_returns_correct_peers() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer1: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.2:6882".parse().unwrap();
        manager.add_peer(hash, peer1).await;
        manager.add_peer(hash, peer2).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers, vec![peer1, peer2]);
    }

    #[tokio::test]
    async fn test_get_peers_preserves_order() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        for i in 0u16..5 {
            let peer: SocketAddr = format!("10.0.0.{}:{}", i, 6881 + i).parse().unwrap();
            manager.add_peer(hash, peer).await;
        }
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 5);
        // Order should be insertion order
        assert_eq!(peers[0].port(), 6881);
        assert_eq!(peers[4].port(), 6885);
    }

    #[tokio::test]
    async fn test_get_peers_returns_clone() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        manager.add_peer(hash, peer).await;
        let peers1 = manager.get_peers(&hash).await;
        let peers2 = manager.get_peers(&hash).await;
        assert_eq!(peers1, peers2);
        // Modifying returned vec should not affect internal state
        assert_eq!(peers1.len(), peers2.len());
    }

    // ── DhtManager::generate_token() ──

    #[tokio::test]
    async fn test_generate_token_basic() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let token = manager.generate_token(addr).await;
        assert_eq!(token.len(), 8);
    }

    #[tokio::test]
    async fn test_generate_token_non_empty() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let token = manager.generate_token(addr).await;
        // Extremely unlikely all 8 bytes are zero
        assert!(token.iter().any(|&b| b != 0));
    }

    #[tokio::test]
    async fn test_generate_token_unique_per_addr() {
        let manager = DhtManager::new();
        let addr1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:6882".parse().unwrap();
        let token1 = manager.generate_token(addr1).await;
        let token2 = manager.generate_token(addr2).await;
        assert_ne!(
            token1, token2,
            "different addrs should get different tokens"
        );
    }

    #[tokio::test]
    async fn test_generate_token_overwrites_previous() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let token1 = manager.generate_token(addr).await;
        let token2 = manager.generate_token(addr).await;
        assert_ne!(token1, token2, "regenerating should produce new token");
        // Old token should no longer verify
        let valid = manager.verify_token(&token1, addr).await;
        assert!(!valid, "old token should be invalid after regeneration");
    }

    #[tokio::test]
    async fn test_generate_token_stores_for_verification() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let token = manager.generate_token(addr).await;
        let valid = manager.verify_token(&token, addr).await;
        assert!(valid, "generated token should verify for same addr");
    }

    // ── DhtManager::verify_token() ──

    #[tokio::test]
    async fn test_verify_token_no_token_exists() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let valid = manager.verify_token(b"token123", addr).await;
        assert!(!valid);
    }

    #[tokio::test]
    async fn test_verify_token_wrong_addr() {
        let manager = DhtManager::new();
        let addr1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:6882".parse().unwrap();
        let token = manager.generate_token(addr1).await;
        let valid = manager.verify_token(&token, addr2).await;
        assert!(!valid, "token should not verify for different addr");
    }

    #[tokio::test]
    async fn test_verify_token_wrong_value() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let _token = manager.generate_token(addr).await;
        let valid = manager.verify_token(b"wrongtoken", addr).await;
        assert!(!valid, "wrong token value should not verify");
    }

    #[tokio::test]
    async fn test_verify_token_empty_token() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let _token = manager.generate_token(addr).await;
        let valid = manager.verify_token(b"", addr).await;
        assert!(!valid);
    }

    #[tokio::test]
    async fn test_verify_token_after_regeneration() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let token1 = manager.generate_token(addr).await;
        let token2 = manager.generate_token(addr).await;
        assert!(!manager.verify_token(&token1, addr).await);
        assert!(manager.verify_token(&token2, addr).await);
    }

    #[tokio::test]
    async fn test_verify_token_multiple_addrs_independent() {
        let manager = DhtManager::new();
        let addr1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:6882".parse().unwrap();
        let token1 = manager.generate_token(addr1).await;
        let token2 = manager.generate_token(addr2).await;
        assert!(manager.verify_token(&token1, addr1).await);
        assert!(manager.verify_token(&token2, addr2).await);
        assert!(!manager.verify_token(&token1, addr2).await);
        assert!(!manager.verify_token(&token2, addr1).await);
    }

    // ── DhtManager::bootstrap() ──

    #[tokio::test]
    async fn test_bootstrap_without_socket_returns_error() {
        let manager = DhtManager::new();
        let nodes: Vec<SocketAddr> = vec!["192.168.1.1:6881".parse().unwrap()];
        let result = manager.bootstrap(&nodes).await;
        assert!(result.is_err());
        match result {
            Err(DhtError::Network(msg)) => {
                assert!(msg.contains("UDP socket not set"));
            }
            _ => panic!("Expected Network error"),
        }
    }

    #[tokio::test]
    async fn test_bootstrap_empty_nodes_ok() {
        let manager = DhtManager::new();
        let result = manager.bootstrap(&[]).await;
        assert!(result.is_ok(), "bootstrap with empty list should succeed");
    }

    #[tokio::test]
    async fn test_bootstrap_with_socket_sends_queries() {
        let mut manager = DhtManager::new();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();
        manager.set_socket(socket.clone());

        // Create a receiver socket to capture the bootstrap query
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let result = manager.bootstrap(&[recv_addr]).await;
        assert!(result.is_ok());

        // Should receive a find_node query
        let mut buf = vec![0u8; 65535];
        let received =
            tokio::time::timeout(Duration::from_millis(500), recv_socket.recv_from(&mut buf)).await;
        assert!(received.is_ok(), "should receive bootstrap query");
    }

    #[tokio::test]
    async fn test_bootstrap_multiple_nodes() {
        let mut manager = DhtManager::new();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        manager.set_socket(socket);

        let recv1 = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv2 = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr1 = recv1.local_addr().unwrap();
        let addr2 = recv2.local_addr().unwrap();

        let result = manager.bootstrap(&[addr1, addr2]).await;
        assert!(result.is_ok());

        // Both receivers should get a query
        let mut buf = vec![0u8; 65535];
        let r1 = tokio::time::timeout(Duration::from_millis(500), recv1.recv_from(&mut buf)).await;
        let r2 = tokio::time::timeout(Duration::from_millis(500), recv2.recv_from(&mut buf)).await;
        assert!(r1.is_ok(), "recv1 should get query");
        assert!(r2.is_ok(), "recv2 should get query");
    }

    // ── DhtManager::find_peers() ──

    #[tokio::test]
    async fn test_find_peers_returns_local_peers() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        manager.add_peer(hash, peer).await;

        let result = manager.find_peers(hash).await;
        assert!(result.is_ok());
        let peers = result.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], peer);
    }

    #[tokio::test]
    async fn test_find_peers_no_socket_no_local_peers() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let result = manager.find_peers(hash).await;
        assert!(result.is_err());
        match result {
            Err(DhtError::Network(msg)) => {
                assert!(msg.contains("UDP socket not set"));
            }
            _ => panic!("Expected Network error"),
        }
    }

    #[tokio::test]
    async fn test_find_peers_with_socket_no_nodes() {
        let mut manager = DhtManager::new();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        manager.set_socket(socket);

        let hash: InfoHash = [1u8; 20];
        // No nodes in routing table, so iterative lookup finds nothing
        let result = manager.find_peers(hash).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_find_peers_local_peers_priority() {
        // If we already have local peers, should return them without network
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer1: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.2:6882".parse().unwrap();
        manager.add_peer(hash, peer1).await;
        manager.add_peer(hash, peer2).await;

        let result = manager.find_peers(hash).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    // ── DhtManager::fetch_metadata() ──

    #[tokio::test]
    async fn test_fetch_metadata_no_peers_error() {
        let mut manager = DhtManager::new();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        manager.set_socket(socket);

        let hash: InfoHash = [1u8; 20];
        let result = manager.fetch_metadata(hash).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fetch_metadata_no_socket_error() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let result = manager.fetch_metadata(hash).await;
        assert!(result.is_err());
    }

    // ── DhtManager::send_find_node() ──

    #[tokio::test]
    async fn test_send_find_node_without_socket() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let target: NodeId = [0u8; 20];
        let result = manager.send_find_node(addr, target).await;
        assert!(result.is_err());
        match result {
            Err(DhtError::Network(msg)) => {
                assert!(msg.contains("UDP socket not set"));
            }
            _ => panic!("Expected Network error"),
        }
    }

    #[tokio::test]
    async fn test_send_find_node_with_socket() {
        let mut manager = DhtManager::new();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        manager.set_socket(socket);

        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let target: NodeId = [42u8; 20];
        let result = manager.send_find_node(recv_addr, target).await;
        assert!(result.is_ok());

        // Verify message received
        let mut buf = vec![0u8; 65535];
        let received =
            tokio::time::timeout(Duration::from_millis(500), recv_socket.recv_from(&mut buf)).await;
        assert!(received.is_ok());
    }

    // ── DhtError tests ──

    #[test]
    fn test_error_display_not_implemented() {
        let err = DhtError::NotImplemented;
        assert_eq!(format!("{}", err), "not implemented");
    }

    #[test]
    fn test_error_display_timeout() {
        let err = DhtError::Timeout;
        assert_eq!(format!("{}", err), "timeout");
    }

    #[test]
    fn test_error_display_network() {
        let err = DhtError::Network("connection refused".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("network error"));
        assert!(msg.contains("connection refused"));
    }

    #[test]
    fn test_error_display_protocol() {
        let err = DhtError::Protocol("bad message".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("protocol error"));
        assert!(msg.contains("bad message"));
    }

    #[test]
    fn test_error_display_no_peers() {
        let err = DhtError::NoPeers;
        assert_eq!(format!("{}", err), "no peers found");
    }

    #[test]
    fn test_error_debug_all_variants() {
        let variants = vec![
            DhtError::NotImplemented,
            DhtError::Timeout,
            DhtError::Network("net".to_string()),
            DhtError::Protocol("proto".to_string()),
            DhtError::NoPeers,
        ];
        for v in &variants {
            let debug = format!("{:?}", v);
            assert!(!debug.is_empty());
        }
    }

    #[test]
    fn test_error_network_unicode() {
        let err = DhtError::Network("连接被拒绝".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("连接被拒绝"));
    }

    #[test]
    fn test_error_protocol_unicode() {
        let err = DhtError::Protocol("メッセージエラー".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("メッセージエラー"));
    }

    #[test]
    fn test_error_network_empty_string() {
        let err = DhtError::Network(String::new());
        let msg = format!("{}", err);
        assert!(msg.contains("network error"));
    }

    #[test]
    fn test_error_protocol_empty_string() {
        let err = DhtError::Protocol(String::new());
        let msg = format!("{}", err);
        assert!(msg.contains("protocol error"));
    }

    // ── QueryState tests ──

    #[test]
    fn test_query_state_finding_peers_debug() {
        let state = QueryState::FindingPeers {
            info_hash: [1u8; 20],
            peers_found: vec!["127.0.0.1:6881".parse().unwrap()],
        };
        let debug = format!("{:?}", state);
        assert!(debug.contains("FindingPeers"));
    }

    #[test]
    fn test_query_state_fetching_metadata_debug() {
        let state = QueryState::FetchingMetadata {
            info_hash: [2u8; 20],
            metadata: Some(vec![0u8; 100]),
        };
        let debug = format!("{:?}", state);
        assert!(debug.contains("FetchingMetadata"));
    }

    #[test]
    fn test_query_state_fetching_metadata_none() {
        let state = QueryState::FetchingMetadata {
            info_hash: [3u8; 20],
            metadata: None,
        };
        let debug = format!("{:?}", state);
        assert!(debug.contains("FetchingMetadata"));
        assert!(debug.contains("None"));
    }

    // ── Integration / lifecycle tests ──

    #[tokio::test]
    async fn test_manager_lifecycle_add_nodes_find_closest() {
        let manager = DhtManager::new();
        // Add 10 nodes
        for i in 0u16..10 {
            let mut node_id = [0u8; 20];
            node_id[0] = i as u8;
            let addr: SocketAddr = format!("10.0.0.{}:{}", i, 6881 + i).parse().unwrap();
            manager
                .add_node(Node {
                    id: node_id,
                    addr,
                    last_seen: Instant::now(),
                })
                .await;
        }

        // Find closest to [0u8; 20]
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 5).await;
        assert_eq!(closest.len(), 5);
        // Closest should be node_id[0] = 0 (exact match)
        assert_eq!(closest[0].0, [0u8; 20]);
    }

    #[tokio::test]
    async fn test_manager_lifecycle_peers_and_tokens() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let addr: SocketAddr = "10.0.0.1:6881".parse().unwrap();

        // Generate token
        let token = manager.generate_token(addr).await;
        assert!(manager.verify_token(&token, addr).await);

        // Add peer
        manager.add_peer(hash, addr).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], addr);
    }

    #[tokio::test]
    async fn test_manager_multiple_hash_independent_peers() {
        let manager = DhtManager::new();
        let hash1: InfoHash = [1u8; 20];
        let hash2: InfoHash = [2u8; 20];
        let peer1: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.2:6882".parse().unwrap();

        manager.add_peer(hash1, peer1).await;
        manager.add_peer(hash2, peer2).await;

        let p1 = manager.get_peers(&hash1).await;
        let p2 = manager.get_peers(&hash2).await;
        assert_eq!(p1, vec![peer1]);
        assert_eq!(p2, vec![peer2]);
    }

    #[tokio::test]
    async fn test_manager_many_peers_same_hash() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        for i in 0u16..50 {
            let peer: SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, 6881 + i)
                .parse()
                .unwrap();
            manager.add_peer(hash, peer).await;
        }
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 50);
    }

    #[tokio::test]
    async fn test_manager_token_per_addr_isolation() {
        let manager = DhtManager::new();
        let addrs: Vec<SocketAddr> = (0u16..10)
            .map(|i| format!("10.0.0.{}:{}", i, 6881 + i).parse().unwrap())
            .collect();

        let mut tokens = Vec::new();
        for addr in &addrs {
            tokens.push(manager.generate_token(*addr).await);
        }

        // Each token should verify for its own addr only
        for (i, addr) in addrs.iter().enumerate() {
            assert!(
                manager.verify_token(&tokens[i], *addr).await,
                "token {} should verify for addr {}",
                i,
                addr
            );
            if i > 0 {
                assert!(
                    !manager.verify_token(&tokens[i], addrs[0]).await,
                    "token {} should not verify for different addr",
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_manager_node_id_stable_across_operations() {
        let manager = DhtManager::new();
        let id_before = manager.node_id();

        // Perform various operations
        let hash: InfoHash = [1u8; 20];
        manager
            .add_peer(hash, "10.0.0.1:6881".parse().unwrap())
            .await;
        manager
            .generate_token("10.0.0.1:6881".parse().unwrap())
            .await;
        manager
            .add_node(Node {
                id: [2u8; 20],
                addr: "10.0.0.2:6882".parse().unwrap(),
                last_seen: Instant::now(),
            })
            .await;

        let id_after = manager.node_id();
        assert_eq!(id_before, id_after, "node_id should not change");
    }

    // ── Edge cases ──

    #[tokio::test]
    async fn test_closest_nodes_with_identical_ids() {
        let manager = DhtManager::new();
        // Add same node twice (should update, not duplicate)
        let node_id: NodeId = [5u8; 20];
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        manager
            .add_node(Node {
                id: node_id,
                addr,
                last_seen: Instant::now(),
            })
            .await;
        manager
            .add_node(Node {
                id: node_id,
                addr,
                last_seen: Instant::now(),
            })
            .await;
        let target: NodeId = [0u8; 20];
        let closest = manager.closest_nodes(&target, 8).await;
        assert_eq!(closest.len(), 1);
    }

    #[tokio::test]
    async fn test_get_peers_zero_port() {
        let manager = DhtManager::new();
        let hash: InfoHash = [1u8; 20];
        let peer: SocketAddr = "127.0.0.1:0".parse().unwrap();
        manager.add_peer(hash, peer).await;
        let peers = manager.get_peers(&hash).await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].port(), 0);
    }

    #[tokio::test]
    async fn test_verify_token_after_clear_tokens() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let token = manager.generate_token(addr).await;
        assert!(manager.verify_token(&token, addr).await);

        // Clear tokens map directly
        {
            let mut tokens = manager.tokens.lock().await;
            tokens.clear();
        }
        assert!(!manager.verify_token(&token, addr).await);
    }

    #[tokio::test]
    async fn test_generate_token_ipv6_addr() {
        let manager = DhtManager::new();
        let addr: SocketAddr = "[::1]:6881".parse().unwrap();
        let token = manager.generate_token(addr).await;
        assert_eq!(token.len(), 8);
        assert!(manager.verify_token(&token, addr).await);
    }

    #[tokio::test]
    async fn test_add_node_with_zero_node_id() {
        let manager = DhtManager::new();
        let node_id: NodeId = [0u8; 20];
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        manager
            .add_node(Node {
                id: node_id,
                addr,
                last_seen: Instant::now(),
            })
            .await;
        let closest = manager.closest_nodes(&node_id, 8).await;
        assert_eq!(closest.len(), 1);
    }

    #[tokio::test]
    async fn test_add_node_with_max_node_id() {
        let manager = DhtManager::new();
        let node_id: NodeId = [0xFF; 20];
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        manager
            .add_node(Node {
                id: node_id,
                addr,
                last_seen: Instant::now(),
            })
            .await;
        let closest = manager.closest_nodes(&node_id, 8).await;
        assert_eq!(closest.len(), 1);
    }

    #[tokio::test]
    async fn test_find_peers_empty_hash() {
        let manager = DhtManager::new();
        let hash: InfoHash = [0u8; 20];
        let peers = manager.get_peers(&hash).await;
        assert!(peers.is_empty());
    }

    #[tokio::test]
    async fn test_find_peers_max_hash() {
        let manager = DhtManager::new();
        let hash: InfoHash = [0xFF; 20];
        let peers = manager.get_peers(&hash).await;
        assert!(peers.is_empty());
    }
}
