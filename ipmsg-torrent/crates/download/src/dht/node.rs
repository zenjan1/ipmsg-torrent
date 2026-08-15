//! DHT node implementation
//!
//! Handles UDP communication and message processing

use super::message::{DhtMessage, QueryType, ResponseType};
use super::routing::Node;
use super::{DhtError, DhtManager, InfoHash, NodeId};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// UDP buffer size
const UDP_BUFFER_SIZE: usize = 65535;

/// DHT node that handles UDP communication
pub struct DhtNode {
    socket: Arc<UdpSocket>,
    manager: Arc<DhtManager>,
    running: Arc<Mutex<bool>>,
}

impl DhtNode {
    /// Create a new DHT node bound to the given address
    pub async fn bind(addr: SocketAddr, manager: Arc<DhtManager>) -> Result<Self, DhtError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        tracing::info!("DHT node bound to {}", socket.local_addr().unwrap());

        Ok(Self {
            socket: Arc::new(socket),
            manager,
            running: Arc::new(Mutex::new(false)),
        })
    }

    /// Start the DHT node message loop
    pub async fn start(&self) -> Result<(), DhtError> {
        let mut running = self.running.lock().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        let socket = self.socket.clone();
        let manager = self.manager.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; UDP_BUFFER_SIZE];

            loop {
                let should_stop = {
                    let r = running.lock().await;
                    !*r
                };

                if should_stop {
                    break;
                }

                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        if let Err(e) =
                            Self::handle_message(&manager, &socket, &buf[..len], addr).await
                        {
                            tracing::debug!("Failed to handle message from {}: {}", addr, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("UDP receive error: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the DHT node
    pub async fn stop(&self) {
        let mut running = self.running.lock().await;
        *running = false;
    }

    /// Handle an incoming DHT message
    async fn handle_message(
        manager: &DhtManager,
        socket: &UdpSocket,
        data: &[u8],
        from: SocketAddr,
    ) -> Result<(), DhtError> {
        let msg = DhtMessage::decode(data)
            .map_err(|e| DhtError::Protocol(format!("Decode error: {:?}", e)))?;

        match msg {
            DhtMessage::Query {
                transaction_id,
                query,
            } => {
                Self::handle_query(manager, socket, &transaction_id, query, from).await?;
            }
            DhtMessage::Response {
                transaction_id,
                response,
            } => {
                Self::handle_response(manager, &transaction_id, response, from).await?;
            }
            DhtMessage::Error {
                transaction_id: _,
                code,
                message,
            } => {
                tracing::warn!("DHT error from {}: {} (code {})", from, message, code);
            }
        }

        Ok(())
    }

    /// Handle a query message
    async fn handle_query(
        manager: &DhtManager,
        socket: &UdpSocket,
        transaction_id: &[u8],
        query: QueryType,
        from: SocketAddr,
    ) -> Result<(), DhtError> {
        match query {
            QueryType::Ping { id } => {
                // Respond with pong
                let response = DhtMessage::Response {
                    transaction_id: transaction_id.to_vec(),
                    response: ResponseType::Pong {
                        id: manager.node_id(),
                    },
                };
                let data = response
                    .encode()
                    .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;
                socket
                    .send_to(&data, from)
                    .await
                    .map_err(|e| DhtError::Network(e.to_string()))?;

                // Add sender to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
            QueryType::FindNode { id, target } => {
                // Find closest nodes to target
                let closest = manager.closest_nodes(&target, 8).await;

                let response = DhtMessage::Response {
                    transaction_id: transaction_id.to_vec(),
                    response: ResponseType::Nodes {
                        id: manager.node_id(),
                        nodes: closest,
                    },
                };
                let data = response
                    .encode()
                    .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;
                socket
                    .send_to(&data, from)
                    .await
                    .map_err(|e| DhtError::Network(e.to_string()))?;

                // Add sender to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
            QueryType::GetPeers { id, info_hash } => {
                // Check if we have peers for this info_hash
                let peers = manager.get_peers(&info_hash).await;
                let token = manager.generate_token(from).await;

                let response = if peers.is_empty() {
                    // Return closest nodes instead
                    let closest = manager.closest_nodes(&info_hash, 8).await;
                    DhtMessage::Response {
                        transaction_id: transaction_id.to_vec(),
                        response: ResponseType::Peers {
                            id: manager.node_id(),
                            token,
                            values: None,
                            nodes: closest,
                        },
                    }
                } else {
                    DhtMessage::Response {
                        transaction_id: transaction_id.to_vec(),
                        response: ResponseType::Peers {
                            id: manager.node_id(),
                            token,
                            values: Some(peers),
                            nodes: Vec::new(),
                        },
                    }
                };

                let data = response
                    .encode()
                    .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;
                socket
                    .send_to(&data, from)
                    .await
                    .map_err(|e| DhtError::Network(e.to_string()))?;

                // Add sender to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
            QueryType::AnnouncePeer {
                id,
                info_hash,
                port,
                token,
                implied_port,
            } => {
                // Verify token
                if !manager.verify_token(&token, from).await {
                    let error = DhtMessage::Error {
                        transaction_id: transaction_id.to_vec(),
                        code: 203,
                        message: "Invalid token".to_string(),
                    };
                    let data = error
                        .encode()
                        .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;
                    socket
                        .send_to(&data, from)
                        .await
                        .map_err(|e| DhtError::Network(e.to_string()))?;
                    return Ok(());
                }

                let peer_addr = if implied_port {
                    from
                } else {
                    SocketAddr::new(from.ip(), port)
                };

                // Store peer
                manager.add_peer(info_hash, peer_addr).await;

                // Send success response
                let response = DhtMessage::Response {
                    transaction_id: transaction_id.to_vec(),
                    response: ResponseType::AnnounceSuccess {
                        id: manager.node_id(),
                    },
                };
                let data = response
                    .encode()
                    .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;
                socket
                    .send_to(&data, from)
                    .await
                    .map_err(|e| DhtError::Network(e.to_string()))?;

                // Add sender to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
        }

        Ok(())
    }

    /// Handle a response message
    async fn handle_response(
        manager: &DhtManager,
        _transaction_id: &[u8],
        response: ResponseType,
        from: SocketAddr,
    ) -> Result<(), DhtError> {
        match response {
            ResponseType::Pong { id } => {
                // Add node to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
            ResponseType::Nodes { id, nodes } => {
                // Add all nodes to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;

                for (node_id, node_addr) in nodes {
                    manager
                        .add_node(Node {
                            id: node_id,
                            addr: node_addr,
                            last_seen: std::time::Instant::now(),
                        })
                        .await;
                }
            }
            ResponseType::Peers {
                id,
                token: _,
                values,
                nodes,
            } => {
                // Add node to routing table
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;

                // Add closer nodes
                for (node_id, node_addr) in nodes {
                    manager
                        .add_node(Node {
                            id: node_id,
                            addr: node_addr,
                            last_seen: std::time::Instant::now(),
                        })
                        .await;
                }

                // Store peers if we got any
                if let Some(peers) = values {
                    // TODO: Match peers to pending queries
                    tracing::info!("Received {} peers from DHT", peers.len());
                }
            }
            ResponseType::AnnounceSuccess { id } => {
                manager
                    .add_node(Node {
                        id,
                        addr: from,
                        last_seen: std::time::Instant::now(),
                    })
                    .await;
            }
        }

        Ok(())
    }

    /// Send a ping message to a node
    pub async fn ping(&self, addr: SocketAddr) -> Result<(), DhtError> {
        let msg = DhtMessage::Query {
            transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
            query: QueryType::Ping {
                id: self.manager.node_id(),
            },
        };

        let data = msg
            .encode()
            .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;

        self.socket
            .send_to(&data, addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        Ok(())
    }

    /// Send a find_node query
    pub async fn find_node(&self, addr: SocketAddr, target: NodeId) -> Result<(), DhtError> {
        let msg = DhtMessage::Query {
            transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
            query: QueryType::FindNode {
                id: self.manager.node_id(),
                target,
            },
        };

        let data = msg
            .encode()
            .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;

        self.socket
            .send_to(&data, addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        Ok(())
    }

    /// Send a get_peers query
    pub async fn get_peers(&self, addr: SocketAddr, info_hash: InfoHash) -> Result<(), DhtError> {
        let msg = DhtMessage::Query {
            transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
            query: QueryType::GetPeers {
                id: self.manager.node_id(),
                info_hash,
            },
        };

        let data = msg
            .encode()
            .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;

        self.socket
            .send_to(&data, addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        Ok(())
    }

    /// Send an announce_peer query
    pub async fn announce_peer(
        &self,
        addr: SocketAddr,
        info_hash: InfoHash,
        port: u16,
        token: Vec<u8>,
    ) -> Result<(), DhtError> {
        let msg = DhtMessage::Query {
            transaction_id: rand::random::<u16>().to_be_bytes().to_vec(),
            query: QueryType::AnnouncePeer {
                id: self.manager.node_id(),
                info_hash,
                port,
                token,
                implied_port: false,
            },
        };

        let data = msg
            .encode()
            .map_err(|e| DhtError::Protocol(format!("Encode error: {:?}", e)))?;

        self.socket
            .send_to(&data, addr)
            .await
            .map_err(|e| DhtError::Network(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::message::{DhtMessage, QueryType, ResponseType};
    use std::time::Duration;

    /// Helper: create a DhtNode on a random loopback port
    async fn make_node() -> DhtNode {
        let manager = Arc::new(DhtManager::new());
        DhtNode::bind("127.0.0.1:0".parse().unwrap(), manager)
            .await
            .expect("bind should succeed")
    }

    /// Helper: create a DhtNode and return (node, local_addr)
    async fn make_node_with_addr() -> (DhtNode, SocketAddr) {
        let manager = Arc::new(DhtManager::new());
        let node = DhtNode::bind("127.0.0.1:0".parse().unwrap(), manager)
            .await
            .expect("bind should succeed");
        let addr = node.socket.local_addr().unwrap();
        (node, addr)
    }

    /// Helper: receive a UDP message with a short timeout
    async fn recv_with_timeout(socket: &UdpSocket) -> Option<(Vec<u8>, SocketAddr)> {
        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, addr)) => Some((buf[..len].to_vec(), addr)),
                    Err(_) => None,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                None
            }
        }
    }

    // ── bind tests ──

    #[tokio::test]
    async fn test_dht_node_bind() {
        let manager = Arc::new(DhtManager::new());
        let node = DhtNode::bind("127.0.0.1:0".parse().unwrap(), manager).await;
        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn test_bind_returns_valid_socket_addr() {
        let node = make_node().await;
        let addr = node.socket.local_addr().unwrap();
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0);
    }

    #[tokio::test]
    async fn test_bind_initial_running_false() {
        let node = make_node().await;
        let running = node.running.lock().await;
        assert!(!*running, "node should not be running after bind");
    }

    // ── start / stop tests ──

    #[tokio::test]
    async fn test_start_sets_running() {
        let (node, _addr) = make_node_with_addr().await;
        node.start().await.expect("start should succeed");
        let running = node.running.lock().await;
        assert!(*running, "node should be running after start");
        node.stop().await;
    }

    #[tokio::test]
    async fn test_start_idempotent() {
        let (node, _addr) = make_node_with_addr().await;
        node.start().await.expect("first start");
        node.start().await.expect("second start should be ok");
        let running = node.running.lock().await;
        assert!(*running);
        node.stop().await;
    }

    #[tokio::test]
    async fn test_stop_clears_running() {
        let (node, _addr) = make_node_with_addr().await;
        node.start().await.unwrap();
        node.stop().await;
        let running = node.running.lock().await;
        assert!(!*running, "node should not be running after stop");
    }

    #[tokio::test]
    async fn test_stop_without_start() {
        let (node, _addr) = make_node_with_addr().await;
        // stopping a node that was never started should not panic
        node.stop().await;
        let running = node.running.lock().await;
        assert!(!*running);
    }

    #[tokio::test]
    async fn test_stop_idempotent() {
        let (node, _addr) = make_node_with_addr().await;
        node.start().await.unwrap();
        node.stop().await;
        node.stop().await; // should not panic
        let running = node.running.lock().await;
        assert!(!*running);
    }

    // ── ping query/response tests ──

    #[tokio::test]
    async fn test_ping_sends_query() {
        let (node_a, _addr_a) = make_node_with_addr().await;
        let (node_b, addr_b) = make_node_with_addr().await;

        // node_a sends ping to node_b
        node_a.ping(addr_b).await.expect("ping should send");

        // node_b should receive the ping query
        let data = recv_with_timeout(&node_b.socket).await;
        assert!(data.is_some(), "node_b should receive ping");
        let (buf, from) = data.unwrap();
        assert_eq!(from, _addr_a);

        let msg = DhtMessage::decode(&buf).expect("should decode");
        match msg {
            DhtMessage::Query {
                query: QueryType::Ping { .. },
                ..
            } => {}
            _ => panic!("expected Ping query, got {:?}", msg),
        }
    }

    #[tokio::test]
    async fn test_ping_response_flow() {
        // Create node_a (sender) and node_b (receiver)
        let socket_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr_a = socket_a.local_addr().unwrap();
        let mut mgr_a_with_socket = DhtManager::new();
        mgr_a_with_socket.set_socket(socket_a.clone());
        let manager_a = Arc::new(mgr_a_with_socket);
        let node_a = DhtNode {
            socket: socket_a.clone(),
            manager: manager_a.clone(),
            running: Arc::new(Mutex::new(false)),
        };

        let manager_b = Arc::new(DhtManager::new());
        let socket_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr_b = socket_b.local_addr().unwrap();
        let _node_b = DhtNode {
            socket: socket_b.clone(),
            manager: manager_b.clone(),
            running: Arc::new(Mutex::new(false)),
        };

        // node_a sends ping to node_b
        node_a.ping(addr_b).await.unwrap();

        // node_b receives and handles the message
        let data = recv_with_timeout(&socket_b).await;
        assert!(data.is_some());
        let (buf, from) = data.unwrap();
        DhtNode::handle_message(&manager_b, &socket_b, &buf, from)
            .await
            .expect("handle_message should succeed");

        // node_a should receive a Pong response
        let resp = recv_with_timeout(&socket_a).await;
        assert!(resp.is_some(), "node_a should receive pong");
        let (buf, _from_b) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).expect("should decode pong");
        match msg {
            DhtMessage::Response {
                response: ResponseType::Pong { id },
                ..
            } => {
                assert_eq!(id, manager_b.node_id());
            }
            _ => panic!("expected Pong response, got {:?}", msg),
        }

        // node_b should have added node_a to its routing table
        // (the id field in the Ping query was manager_a's node_id)
        let node_a_id = manager_a.node_id();
        let closest = manager_b.closest_nodes(&node_a_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == node_a_id && *addr == addr_a),
            "node_b should have added node_a to routing table"
        );
    }

    // ── find_node tests ──

    #[tokio::test]
    async fn test_find_node_sends_query() {
        let (node_a, _addr_a) = make_node_with_addr().await;
        let (node_b, addr_b) = make_node_with_addr().await;

        let target: NodeId = [42u8; 20];
        node_a.find_node(addr_b, target).await.expect("find_node");

        let data = recv_with_timeout(&node_b.socket).await;
        assert!(data.is_some(), "node_b should receive find_node");
        let (buf, _from) = data.unwrap();
        let msg = DhtMessage::decode(&buf).expect("decode");
        match msg {
            DhtMessage::Query {
                query: QueryType::FindNode { target: t, .. },
                ..
            } => {
                assert_eq!(t, target);
            }
            _ => panic!("expected FindNode query"),
        }
    }

    // ── get_peers tests ──

    #[tokio::test]
    async fn test_get_peers_sends_query() {
        let (node_a, _addr_a) = make_node_with_addr().await;
        let (node_b, addr_b) = make_node_with_addr().await;

        let info_hash: InfoHash = [7u8; 20];
        node_a
            .get_peers(addr_b, info_hash)
            .await
            .expect("get_peers");

        let data = recv_with_timeout(&node_b.socket).await;
        assert!(data.is_some(), "node_b should receive get_peers");
        let (buf, _from) = data.unwrap();
        let msg = DhtMessage::decode(&buf).expect("decode");
        match msg {
            DhtMessage::Query {
                query: QueryType::GetPeers { info_hash: ih, .. },
                ..
            } => {
                assert_eq!(ih, info_hash);
            }
            _ => panic!("expected GetPeers query"),
        }
    }

    // ── announce_peer tests ──

    #[tokio::test]
    async fn test_announce_peer_sends_query() {
        let (node_a, _addr_a) = make_node_with_addr().await;
        let (node_b, addr_b) = make_node_with_addr().await;

        let info_hash: InfoHash = [99u8; 20];
        let token = vec![1, 2, 3, 4];
        node_a
            .announce_peer(addr_b, info_hash, 6881, token.clone())
            .await
            .expect("announce_peer");

        let data = recv_with_timeout(&node_b.socket).await;
        assert!(data.is_some(), "node_b should receive announce_peer");
        let (buf, _from) = data.unwrap();
        let msg = DhtMessage::decode(&buf).expect("decode");
        match msg {
            DhtMessage::Query {
                query:
                    QueryType::AnnouncePeer {
                        info_hash: ih,
                        port,
                        token: t,
                        implied_port,
                        ..
                    },
                ..
            } => {
                assert_eq!(ih, info_hash);
                assert_eq!(port, 6881);
                assert_eq!(t, token);
                assert!(!implied_port);
            }
            _ => panic!("expected AnnouncePeer query"),
        }
    }

    // ── handle_message error cases ──

    #[tokio::test]
    async fn test_handle_message_invalid_data() {
        let manager = Arc::new(DhtManager::new());
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let from: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        // garbage data should fail decode
        let result = DhtNode::handle_message(&manager, &socket, &[0xFF, 0xFE], from).await;
        assert!(result.is_err(), "invalid data should produce error");
    }

    #[tokio::test]
    async fn test_handle_message_empty_data() {
        let manager = Arc::new(DhtManager::new());
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let from: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let result = DhtNode::handle_message(&manager, &socket, &[], from).await;
        assert!(result.is_err(), "empty data should produce error");
    }

    // ── handle_response tests ──

    #[tokio::test]
    async fn test_handle_response_pong_adds_node() {
        let manager = Arc::new(DhtManager::new());
        let node_id: NodeId = [11u8; 20];
        let from: SocketAddr = "127.0.0.1:8888".parse().unwrap();

        let response = ResponseType::Pong { id: node_id };
        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        // The node should now be in the routing table
        let closest = manager.closest_nodes(&node_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == node_id && *addr == from),
            "pong should add node to routing table"
        );
    }

    #[tokio::test]
    async fn test_handle_response_nodes_adds_all() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [1u8; 20];
        let from: SocketAddr = "127.0.0.1:7777".parse().unwrap();

        let node1_id: NodeId = [2u8; 20];
        let node1_addr: SocketAddr = "127.0.0.1:7778".parse().unwrap();
        let node2_id: NodeId = [3u8; 20];
        let node2_addr: SocketAddr = "127.0.0.1:7779".parse().unwrap();

        let response = ResponseType::Nodes {
            id: sender_id,
            nodes: vec![(node1_id, node1_addr), (node2_id, node2_addr)],
        };

        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        // All 3 nodes should be in routing table (sender + 2 from response)
        let closest = manager.closest_nodes(&sender_id, 10).await;
        assert!(
            closest.iter().any(|(id, _)| *id == sender_id),
            "sender should be in routing table"
        );
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == node1_id && *addr == node1_addr),
            "node1 should be in routing table"
        );
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == node2_id && *addr == node2_addr),
            "node2 should be in routing table"
        );
    }

    #[tokio::test]
    async fn test_handle_response_nodes_empty_list() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [5u8; 20];
        let from: SocketAddr = "127.0.0.1:6666".parse().unwrap();

        let response = ResponseType::Nodes {
            id: sender_id,
            nodes: vec![],
        };

        // Should not panic with empty node list
        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest.iter().any(|(id, _)| *id == sender_id),
            "sender should still be added"
        );
    }

    #[tokio::test]
    async fn test_handle_response_peers_with_values() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [10u8; 20];
        let from: SocketAddr = "127.0.0.1:5555".parse().unwrap();

        let peer1: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let peer2: SocketAddr = "192.168.1.2:6882".parse().unwrap();

        let response = ResponseType::Peers {
            id: sender_id,
            token: vec![1, 2, 3],
            values: Some(vec![peer1, peer2]),
            nodes: vec![],
        };

        // Should not panic
        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest.iter().any(|(id, _)| *id == sender_id),
            "sender should be added"
        );
    }

    #[tokio::test]
    async fn test_handle_response_peers_with_nodes() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [15u8; 20];
        let from: SocketAddr = "127.0.0.1:4444".parse().unwrap();

        let closer_id: NodeId = [16u8; 20];
        let closer_addr: SocketAddr = "127.0.0.1:4445".parse().unwrap();

        let response = ResponseType::Peers {
            id: sender_id,
            token: vec![4, 5, 6],
            values: None,
            nodes: vec![(closer_id, closer_addr)],
        };

        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        let closest = manager.closest_nodes(&closer_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == closer_id && *addr == closer_addr),
            "closer node should be added"
        );
    }

    #[tokio::test]
    async fn test_handle_response_peers_no_values_no_nodes() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [20u8; 20];
        let from: SocketAddr = "127.0.0.1:3333".parse().unwrap();

        let response = ResponseType::Peers {
            id: sender_id,
            token: vec![],
            values: None,
            nodes: vec![],
        };

        // Should not panic
        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_handle_response_announce_success() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [25u8; 20];
        let from: SocketAddr = "127.0.0.1:2222".parse().unwrap();

        let response = ResponseType::AnnounceSuccess { id: sender_id };

        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest.iter().any(|(id, _)| *id == sender_id),
            "announce success should add node"
        );
    }

    // ── handle_query integration via handle_message ──

    #[tokio::test]
    async fn test_handle_query_ping_via_handle_message() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        // Construct a Ping query
        let query = DhtMessage::Query {
            transaction_id: vec![0xAA, 0xBB],
            query: QueryType::Ping { id: [30u8; 20] },
        };
        let data = query.encode().unwrap();

        // Use a separate socket as the "sender"
        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        // Handle the message
        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        // Should have sent a Pong response back to sender
        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some(), "should receive pong response");
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::Pong { .. },
            } => {
                assert_eq!(transaction_id, vec![0xAA, 0xBB]);
            }
            _ => panic!("expected Pong response"),
        }
    }

    #[tokio::test]
    async fn test_handle_query_find_node_via_handle_message() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let target: NodeId = [50u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0xCC],
            query: QueryType::FindNode {
                id: [31u8; 20],
                target,
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some(), "should receive Nodes response");
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::Nodes { .. },
            } => {
                assert_eq!(transaction_id, vec![0xCC]);
            }
            _ => panic!("expected Nodes response"),
        }
    }

    #[tokio::test]
    async fn test_handle_query_get_peers_no_peers_returns_nodes() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let info_hash: InfoHash = [60u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0xDD],
            query: QueryType::GetPeers {
                id: [32u8; 20],
                info_hash,
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                response: ResponseType::Peers { values, .. },
                ..
            } => {
                // No peers stored, so values should be None
                assert!(values.is_none());
            }
            _ => panic!("expected Peers response"),
        }
    }

    #[tokio::test]
    async fn test_handle_query_get_peers_with_peers_returns_values() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let info_hash: InfoHash = [70u8; 20];
        let peer_addr: SocketAddr = "192.168.1.100:6881".parse().unwrap();

        // Pre-populate peers
        manager.add_peer(info_hash, peer_addr).await;

        let query = DhtMessage::Query {
            transaction_id: vec![0xEE],
            query: QueryType::GetPeers {
                id: [33u8; 20],
                info_hash,
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                response: ResponseType::Peers { values, .. },
                ..
            } => {
                assert!(values.is_some(), "should return stored peers");
                let peers = values.unwrap();
                assert_eq!(peers.len(), 1);
                assert_eq!(peers[0], peer_addr);
            }
            _ => panic!("expected Peers response"),
        }
    }

    #[tokio::test]
    async fn test_handle_query_announce_peer_invalid_token() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let info_hash: InfoHash = [80u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0xFF],
            query: QueryType::AnnouncePeer {
                id: [34u8; 20],
                info_hash,
                port: 6881,
                token: vec![0xDE, 0xAD], // invalid token
                implied_port: false,
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        // Should receive an error response (code 203)
        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Error { code, message, .. } => {
                assert_eq!(code, 203);
                assert!(message.contains("Invalid token"));
            }
            _ => panic!("expected Error response for invalid token, got {:?}", msg),
        }
    }

    #[tokio::test]
    async fn test_handle_query_announce_peer_valid_token() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();

        // First, generate a valid token for the sender
        let token = manager.generate_token(sender_addr).await;

        let info_hash: InfoHash = [90u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x11],
            query: QueryType::AnnouncePeer {
                id: [35u8; 20],
                info_hash,
                port: 6881,
                token: token.clone(),
                implied_port: false,
            },
        };
        let data = query.encode().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        // Should receive AnnounceSuccess
        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::AnnounceSuccess { .. },
            } => {
                assert_eq!(transaction_id, vec![0x11]);
            }
            _ => panic!("expected AnnounceSuccess, got {:?}", msg),
        }

        // Peer should be stored (with explicit port, not implied)
        let stored = manager.get_peers(&info_hash).await;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].port(), 6881);
        assert_eq!(stored[0].ip(), sender_addr.ip());
    }

    #[tokio::test]
    async fn test_handle_query_announce_peer_implied_port() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();

        let token = manager.generate_token(sender_addr).await;

        let info_hash: InfoHash = [100u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x22],
            query: QueryType::AnnouncePeer {
                id: [36u8; 20],
                info_hash,
                port: 9999, // ignored when implied_port is true
                token,
                implied_port: true,
            },
        };
        let data = query.encode().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        // Should receive AnnounceSuccess
        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());

        // Peer should be stored with sender's actual address (implied port)
        let stored = manager.get_peers(&info_hash).await;
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored[0], sender_addr,
            "implied_port should use sender's address"
        );
    }

    // ── DHT message round-trip via handle_message ──

    #[tokio::test]
    async fn test_handle_message_error_message_logged() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        // Construct a DHT error message
        let error_msg = DhtMessage::Error {
            transaction_id: vec![0x33],
            code: 201,
            message: "A Generic Error".to_string(),
        };
        let data = error_msg.encode().unwrap();

        let from: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        // Should succeed (just logs a warning)
        DhtNode::handle_message(&manager, &socket, &data, from)
            .await
            .unwrap();
    }

    // ── UDP_BUFFER_SIZE constant ──

    #[test]
    fn test_udp_buffer_size() {
        assert_eq!(UDP_BUFFER_SIZE, 65535);
    }

    // ── multiple nodes in routing table ──

    #[tokio::test]
    async fn test_handle_response_multiple_pongs() {
        let manager = Arc::new(DhtManager::new());

        // Add multiple nodes via pong responses
        for i in 0u16..5 {
            let node_id: NodeId = [(i + 40) as u8; 20];
            let from: SocketAddr = format!("127.0.0.1:{}", 10000 + i).parse().unwrap();
            let response = ResponseType::Pong { id: node_id };
            DhtNode::handle_response(&manager, &[i as u8], response, from)
                .await
                .unwrap();
        }

        // All nodes should be in the routing table
        let target: NodeId = [42u8; 20];
        let closest = manager.closest_nodes(&target, 10).await;
        assert_eq!(closest.len(), 5, "all 5 nodes should be in routing table");
    }

    #[tokio::test]
    async fn test_handle_response_nodes_many() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [1u8; 20];
        let from: SocketAddr = "127.0.0.1:11111".parse().unwrap();

        // Create a list of 8 nodes
        let nodes: Vec<(NodeId, SocketAddr)> = (0u16..8)
            .map(|i| {
                let id = [(i + 50) as u8; 20];
                let addr: SocketAddr = format!("127.0.0.1:{}", 20000 + i).parse().unwrap();
                (id, addr)
            })
            .collect();

        let response = ResponseType::Nodes {
            id: sender_id,
            nodes: nodes.clone(),
        };

        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        let target: NodeId = [55u8; 20];
        let closest = manager.closest_nodes(&target, 20).await;
        // Should have sender + up to 8 nodes
        assert!(closest.len() >= 8, "should have at least 8 nodes");
    }

    // ── transaction_id preservation ──

    #[tokio::test]
    async fn test_transaction_id_preserved_in_ping_response() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        // Use a multi-byte transaction ID
        let txn_id = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let query = DhtMessage::Query {
            transaction_id: txn_id.clone(),
            query: QueryType::Ping { id: [77u8; 20] },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response { transaction_id, .. } => {
                assert_eq!(transaction_id, txn_id, "transaction ID must be preserved");
            }
            _ => panic!("expected Response"),
        }
    }

    #[tokio::test]
    async fn test_transaction_id_preserved_in_error_response() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let txn_id = vec![0xCA, 0xFE];
        let query = DhtMessage::Query {
            transaction_id: txn_id.clone(),
            query: QueryType::AnnouncePeer {
                id: [78u8; 20],
                info_hash: [0u8; 20],
                port: 6881,
                token: vec![0xFF], // invalid
                implied_port: false,
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Error { transaction_id, .. } => {
                assert_eq!(transaction_id, txn_id, "error must preserve transaction ID");
            }
            _ => panic!("expected Error response"),
        }
    }

    // ── announce_peer stores multiple peers ──

    #[tokio::test]
    async fn test_announce_peer_stores_multiple_peers() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let info_hash: InfoHash = [110u8; 20];

        // Announce from two different senders
        for i in 0u8..2 {
            let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let sender_addr = sender_socket.local_addr().unwrap();
            let token = manager.generate_token(sender_addr).await;

            let query = DhtMessage::Query {
                transaction_id: vec![i],
                query: QueryType::AnnouncePeer {
                    id: [120 + i; 20],
                    info_hash,
                    port: 6881 + i as u16,
                    token,
                    implied_port: true, // use implied port for simplicity
                },
            };
            let data = query.encode().unwrap();
            sender_socket.send_to(&data, local_addr).await.unwrap();

            let mut buf = vec![0u8; UDP_BUFFER_SIZE];
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            DhtNode::handle_message(&manager, &socket, &buf[..len], from)
                .await
                .unwrap();

            // Consume the response
            let _ = recv_with_timeout(&sender_socket).await;
        }

        let stored = manager.get_peers(&info_hash).await;
        assert_eq!(
            stored.len(),
            2,
            "should store peers from both announcements"
        );
    }

    // ── handle_query adds sender to routing table ──

    #[tokio::test]
    async fn test_ping_adds_sender_to_routing_table() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_id: NodeId = [200u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x01],
            query: QueryType::Ping { id: sender_id },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        // Consume response
        let _ = recv_with_timeout(&sender_socket).await;

        // Verify sender was added to routing table
        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == sender_id && *addr == sender_addr),
            "ping sender should be added to routing table"
        );
    }

    #[tokio::test]
    async fn test_find_node_adds_sender_to_routing_table() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_id: NodeId = [201u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x02],
            query: QueryType::FindNode {
                id: sender_id,
                target: [0u8; 20],
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let _ = recv_with_timeout(&sender_socket).await;

        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == sender_id && *addr == sender_addr),
            "find_node sender should be added to routing table"
        );
    }

    #[tokio::test]
    async fn test_get_peers_adds_sender_to_routing_table() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_id: NodeId = [202u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x03],
            query: QueryType::GetPeers {
                id: sender_id,
                info_hash: [0u8; 20],
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let _ = recv_with_timeout(&sender_socket).await;

        let closest = manager.closest_nodes(&sender_id, 8).await;
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == sender_id && *addr == sender_addr),
            "get_peers sender should be added to routing table"
        );
    }

    // ── edge cases ──

    #[tokio::test]
    async fn test_announce_peer_generates_token_in_response() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        let token = manager.generate_token(sender_addr).await;

        let info_hash: InfoHash = [130u8; 20];
        let query = DhtMessage::Query {
            transaction_id: vec![0x44],
            query: QueryType::AnnouncePeer {
                id: [140u8; 20],
                info_hash,
                port: 6881,
                token,
                implied_port: false,
            },
        };
        let data = query.encode().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                response: ResponseType::AnnounceSuccess { id },
                ..
            } => {
                assert_eq!(id, manager.node_id());
            }
            _ => panic!("expected AnnounceSuccess"),
        }
    }

    #[tokio::test]
    async fn test_get_peers_response_includes_token() {
        let manager = Arc::new(DhtManager::new());
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();

        let query = DhtMessage::Query {
            transaction_id: vec![0x55],
            query: QueryType::GetPeers {
                id: [150u8; 20],
                info_hash: [0u8; 20],
            },
        };
        let data = query.encode().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();
        sender_socket.send_to(&data, local_addr).await.unwrap();

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        DhtNode::handle_message(&manager, &socket, &buf[..len], from)
            .await
            .unwrap();

        let resp = recv_with_timeout(&sender_socket).await;
        assert!(resp.is_some());
        let (buf, _) = resp.unwrap();
        let msg = DhtMessage::decode(&buf).unwrap();
        match msg {
            DhtMessage::Response {
                response: ResponseType::Peers { token, .. },
                ..
            } => {
                assert!(!token.is_empty(), "token should be non-empty");
            }
            _ => panic!("expected Peers response"),
        }
    }

    #[tokio::test]
    async fn test_handle_response_peers_with_values_and_nodes() {
        let manager = Arc::new(DhtManager::new());
        let sender_id: NodeId = [170u8; 20];
        let from: SocketAddr = "127.0.0.1:15555".parse().unwrap();

        let peer: SocketAddr = "192.168.1.50:6881".parse().unwrap();
        let closer_id: NodeId = [171u8; 20];
        let closer_addr: SocketAddr = "127.0.0.1:15556".parse().unwrap();

        let response = ResponseType::Peers {
            id: sender_id,
            token: vec![7, 8, 9],
            values: Some(vec![peer]),
            nodes: vec![(closer_id, closer_addr)],
        };

        DhtNode::handle_response(&manager, &[0], response, from)
            .await
            .unwrap();

        // Both sender and closer node should be in routing table
        let closest = manager.closest_nodes(&sender_id, 10).await;
        assert!(closest.iter().any(|(id, _)| *id == sender_id));
        assert!(
            closest
                .iter()
                .any(|(id, addr)| *id == closer_id && *addr == closer_addr)
        );
    }
}
