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

    #[test]
    fn test_dht_manager_creation() {
        let manager = DhtManager::new();
        assert_eq!(manager.node_id().len(), 20);
    }
}
