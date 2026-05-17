pub mod identity;
pub mod transport;
pub mod discovery;
pub mod messaging;
pub mod store;

pub use identity::Identity;
pub use store::{MessageStore, PeerInfo};

use futures::StreamExt;
use ipmsg_protocol::message::{ChatMessage, ChannelId};
use libp2p::PeerId;
use std::path::PathBuf;
use thiserror::Error;

/// Error types for the P2P engine
#[derive(Debug, Error)]
pub enum P2PError {
    #[error("identity error: {0}")]
    Identity(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("peer not found: {0}")]
    PeerNotFound(String),
    #[error("network error: {0}")]
    Network(String),
}

/// Events emitted by the P2P engine
#[derive(Debug, Clone)]
pub enum P2PEvent {
    PeerJoined {
        peer_id: String,
        username: String,
        platforms: Vec<String>,
    },
    PeerLeft { peer_id: String },
    MessageReceived(ChatMessage),
    MessageSent(ChatMessage),
    FileOffer {
        from: String,
        file_ref: ipmsg_protocol::message::FileRef,
    },
    Typing { from: String },
    Status(String),
}

/// Peer info returned by list_peers
#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    pub peer_id: String,
    pub username: String,
    pub platforms: Vec<String>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

/// The main P2P engine that manages all networking
pub struct P2PEngine {
    identity: Identity,
    store: MessageStore,
    username: String,
    platforms: Vec<String>,
    event_tx: tokio::sync::mpsc::UnboundedSender<P2PEvent>,
    event_rx: Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>>,
    swarm: Option<transport::P2PSwarm>,
    /// Next sequence number for outgoing messages
    next_seq: u64,
    /// Channels we've joined
    joined_channels: Vec<ChannelId>,
}

impl P2PEngine {
    pub fn new(data_dir: PathBuf) -> Result<Self, P2PError> {
        let identity = Identity::load_or_create(&data_dir.join("identity.key"))
            .map_err(|e| P2PError::Identity(e.to_string()))?;

        let store = MessageStore::new(&data_dir.join("messages.db"))
            .map_err(|e| P2PError::Store(e.to_string()))?;

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            identity,
            store,
            username: String::new(),
            platforms: vec!["rust".to_string()],
            event_tx,
            event_rx: Some(event_rx),
            swarm: None,
            next_seq: 0,
            joined_channels: Vec::new(),
        })
    }

    pub async fn start(
        &mut self,
        username: String,
        bootstrap_nodes: Vec<String>,
    ) -> Result<String, P2PError> {
        self.username = username.clone();
        self.platforms = detect_platforms();

        let swarm = transport::P2PSwarm::new(
            &self.identity,
            &self.username,
            &self.platforms,
            &self.event_tx,
            bootstrap_nodes,
        )
        .await?;

        self.swarm = Some(swarm);

        // Broadcast presence
        let presence = ChatMessage::new_presence(
            self.peer_id_str(),
            self.username.clone(),
            self.platforms.clone(),
        );
        if let Some(swarm) = &mut self.swarm {
            let _ = swarm.broadcast_presence(&presence);
        }

        tracing::info!(
            peer_id = %self.peer_id_str(),
            username = %self.username,
            "P2P engine started"
        );

        Ok(self.peer_id_str())
    }

    pub async fn run_event_loop(&mut self) {
        if let Some(swarm) = &mut self.swarm {
            loop {
                match swarm.next().await {
                    Some(events) => {
                        for evt in events {
                            let _ = self.event_tx.send(evt);
                        }
                    }
                    None => break,
                }
            }
        }
    }

    pub async fn next_event(&mut self) -> Option<P2PEvent> {
        if let Some(rx) = &mut self.event_rx {
            rx.recv().await
        } else {
            None
        }
    }

    /// Take the event receiver for external processing
    pub fn take_receiver(&mut self) -> Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>> {
        self.event_rx.take()
    }

    /// Send a text message to a peer with auto-incrementing sequence
    pub async fn send_text(&mut self, to: &str, content: &str) -> Result<(), P2PError> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let msg = ChatMessage::new_text(self.peer_id_str(), Some(to.to_string()), content.to_string())
            .with_sequence(seq);

        if let Some(swarm) = &mut self.swarm {
            swarm.send_message(to, &msg).await?;
        }

        self.store.save_message(&msg).map_err(|e| P2PError::Store(e.to_string()))?;
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg.clone()));

        Ok(())
    }

    /// Send a message to a channel
    pub async fn send_to_channel(&mut self, channel: &ChannelId, content: &str) -> Result<(), P2PError> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let msg = ChatMessage::for_channel(self.peer_id_str(), channel.clone(), content.to_string())
            .with_sequence(seq);

        if let Some(swarm) = &mut self.swarm {
            // For channels, broadcast to the chat topic
            swarm.broadcast_message(&msg).await?;
        }

        self.store.save_message(&msg).map_err(|e| P2PError::Store(e.to_string()))?;
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg.clone()));

        Ok(())
    }

    /// Broadcast a message to all peers
    pub async fn broadcast(&mut self, content: String) -> Result<(), P2PError> {
        let msg = ChatMessage::new_text(self.peer_id_str(), None, content);
        if let Some(swarm) = &mut self.swarm {
            swarm.broadcast_message(&msg).await?;
        }
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg));
        Ok(())
    }

    pub fn list_peers(&self) -> Vec<ConnectedPeer> {
        if let Some(swarm) = &self.swarm {
            swarm.get_peers()
        } else {
            Vec::new()
        }
    }

    pub fn get_history(&self, peer_id: &str, limit: u32) -> Vec<ChatMessage> {
        self.store.get_messages(peer_id, limit)
    }

    pub fn peer_id_str(&self) -> String {
        self.identity.peer_id_str()
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn joined_channels(&self) -> &[ChannelId] {
        &self.joined_channels
    }

    pub fn add_channel(&mut self, channel: ChannelId) {
        if !self.joined_channels.contains(&channel) {
            self.joined_channels.push(channel.clone());
        }
    }

    pub fn remove_channel(&mut self, channel: &ChannelId) {
        self.joined_channels.retain(|c| c != channel);
    }
}

fn detect_platforms() -> Vec<String> {
    let mut platforms = vec!["rust".to_string()];
    if cfg!(target_os = "android") {
        platforms.push("android".to_string());
    }
    if cfg!(target_os = "linux") {
        platforms.push("linux".to_string());
    }
    if cfg!(target_os = "windows") {
        platforms.push("windows".to_string());
    }
    if cfg!(target_os = "macos") {
        platforms.push("macos".to_string());
    }
    platforms
}
