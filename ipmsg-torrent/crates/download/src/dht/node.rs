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

    #[tokio::test]
    async fn test_dht_node_bind() {
        let manager = Arc::new(DhtManager::new());
        let node = DhtNode::bind("127.0.0.1:0".parse().unwrap(), manager).await;
        assert!(node.is_ok());
    }
}
