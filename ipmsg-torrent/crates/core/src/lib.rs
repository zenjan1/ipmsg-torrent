pub mod identity;
pub mod transport;
pub mod discovery;
pub mod messaging;
pub mod store;

pub use identity::Identity;
pub use store::{MessageStore, PeerInfo};

use futures::StreamExt;
use ipmsg_protocol::message::{ChatMessage, ChannelId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Maximum tracked message IDs for dedup
const MAX_DEDUP_CACHE: usize = 4096;
/// ACK timeout in seconds
const ACK_TIMEOUT_SECS: u64 = 30;
/// Max retries before giving up
const MAX_RETRIES: u32 = 3;

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
    MessageDelivered(String), // message_id ACK received
    FileOffer {
        from: String,
        file_ref: ipmsg_protocol::message::FileRef,
    },
    Typing { from: String },
    Status(String),
}

/// Peer info returned by list_peers
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectedPeer {
    pub peer_id: String,
    pub username: String,
    pub platforms: Vec<String>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

/// Commands the CLI can send to the P2P engine
#[derive(Debug, Clone)]
pub enum SendCommand {
    SendText { to: String, content: String },
    SendToChannel { channel: ChannelId, content: String },
    Broadcast { content: String },
    AddChannel { channel: ChannelId },
    RemoveChannel { channel: ChannelId },
    SendAck { message_ids: Vec<String> },
    SendTyping { to: String },
    UpdateProfile { username: String, bio: Option<String> },
}

/// Tracks a pending message awaiting ACK
struct PendingAck {
    #[allow(dead_code)]
    message_id: String,
    retries: u32,
    content: Vec<u8>, // Raw CBOR for resend
}

/// The main P2P engine that manages all networking
pub struct P2PEngine {
    identity: Identity,
    store: MessageStore,
    username: String,
    platforms: Vec<String>,
    event_tx: tokio::sync::mpsc::UnboundedSender<P2PEvent>,
    event_rx: Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>>,
    command_rx: Option<tokio::sync::mpsc::UnboundedReceiver<SendCommand>>,
    command_tx: Option<tokio::sync::mpsc::UnboundedSender<SendCommand>>,
    swarm: Option<transport::P2PSwarm>,
    /// Next sequence number for outgoing messages
    next_seq: u64,
    /// Channels we've joined
    joined_channels: Vec<ChannelId>,
    /// Message IDs we've seen (dedup cache, LRU)
    seen_messages: VecDeque<String>,
    seen_set: HashSet<String>,
    /// Messages awaiting ACK
    pending_acks: HashMap<String, PendingAck>,
}

impl P2PEngine {
    pub fn new(data_dir: PathBuf) -> Result<Self, P2PError> {
        let identity = Identity::load_or_create(&data_dir.join("identity.key"))
            .map_err(|e| P2PError::Identity(e.to_string()))?;

        let store = MessageStore::new(&data_dir.join("messages.db"))
            .map_err(|e| P2PError::Store(e.to_string()))?;

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            identity,
            store,
            username: String::new(),
            platforms: vec!["rust".to_string()],
            event_tx,
            event_rx: Some(event_rx),
            command_rx: Some(command_rx),
            command_tx: Some(command_tx),
            swarm: None,
            next_seq: 0,
            joined_channels: Vec::new(),
            seen_messages: VecDeque::with_capacity(MAX_DEDUP_CACHE + 100),
            seen_set: HashSet::with_capacity(MAX_DEDUP_CACHE + 100),
            pending_acks: HashMap::new(),
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
        if let Some(mut swarm) = self.swarm.take() {
            loop {
                tokio::select! {
                    events = swarm.next() => {
                        match events {
                            Some(evts) => {
                                for evt in evts {
                                    // Deduplicate received messages
                                    let evt = match &evt {
                                        P2PEvent::MessageReceived(msg) => {
                                            if self.is_duplicate(&msg.id) {
                                                continue;
                                            }
                                            self.mark_seen(&msg.id);
                                            // Auto-ACK
                                            let ack_msg = ChatMessage::new_ack(
                                                self.peer_id_str(),
                                                msg.from.clone(),
                                                vec![msg.id.clone()],
                                            );
                                            let ack_bytes = ipmsg_protocol::codec::encode_message(&ack_msg);
                                            if let Some(s) = self.swarm.as_mut() {
                                                let _ = s.publish_to_topic(
                                                    crate::messaging::CHAT_TOPIC,
                                                    ack_bytes,
                                                );
                                            }
                                            evt
                                        }
                                        _ => evt,
                                    };
                                    let _ = self.event_tx.send(evt);
                                }
                            }
                            None => break,
                        }
                    }
                    cmd = async {
                        if let Some(rx) = &mut self.command_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        match cmd {
                            Some(SendCommand::SendText { to, content }) => {
                                let _ = self.send_text(&to, &content).await;
                            }
                            Some(SendCommand::SendToChannel { channel, content }) => {
                                let _ = self.send_to_channel(&channel, &content).await;
                            }
                            Some(SendCommand::Broadcast { content }) => {
                                let _ = self.broadcast(content).await;
                            }
                            Some(SendCommand::AddChannel { channel }) => {
                                self.add_channel(channel);
                            }
                            Some(SendCommand::RemoveChannel { channel }) => {
                                self.remove_channel(&channel);
                            }
                            Some(SendCommand::SendAck { message_ids }) => {
                                let _ = self.send_ack(&message_ids).await;
                            }
                            Some(SendCommand::SendTyping { to }) => {
                                let _ = self.send_typing(&to).await;
                            }
                            Some(SendCommand::UpdateProfile { username, bio }) => {
                                let _ = self.update_profile(&username, bio.as_deref());
                            }
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(ACK_TIMEOUT_SECS)) => {
                        // Check for timed-out ACKs and retry
                        self.check_pending_acks().await;
                    }
                }
            }
        }
    }

    /// Check if a message ID has been seen (dedup)
    fn is_duplicate(&mut self, id: &str) -> bool {
        if self.seen_set.contains(id) {
            true
        } else {
            if self.seen_messages.len() >= MAX_DEDUP_CACHE {
                if let Some(old) = self.seen_messages.pop_front() {
                    self.seen_set.remove(&old);
                }
            }
            false
        }
    }

    /// Mark a message ID as seen
    fn mark_seen(&mut self, id: &str) {
        if self.seen_messages.len() >= MAX_DEDUP_CACHE {
            if let Some(old) = self.seen_messages.pop_front() {
                self.seen_set.remove(&old);
            }
        }
        self.seen_set.insert(id.to_string());
        self.seen_messages.push_back(id.to_string());
    }

    /// Check pending ACKs and retry timed-out messages
    async fn check_pending_acks(&mut self) {
        let timed_out: Vec<String> = self.pending_acks.iter()
            .filter(|(_, p)| p.retries < MAX_RETRIES)
            .map(|(id, _)| id.clone())
            .collect();

        for msg_id in timed_out {
            if let Some(pending) = self.pending_acks.get_mut(&msg_id) {
                pending.retries += 1;
                tracing::warn!(message_id = %msg_id, retry = pending.retries, "Retrying message (ACK timeout)");
                let bytes = pending.content.clone();
                if let Some(s) = self.swarm.as_mut() {
                    let _ = s.publish_to_topic(crate::messaging::CHAT_TOPIC, bytes);
                }
            }
        }

        self.pending_acks.retain(|id, p| {
            if p.retries >= MAX_RETRIES {
                tracing::error!(message_id = %id, "Message delivery failed after max retries");
                false
            } else {
                true
            }
        });
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

    /// Take the command sender for CLI-to-engine communication
    pub fn take_command_sender(&mut self) -> Option<tokio::sync::mpsc::UnboundedSender<SendCommand>> {
        self.command_tx.take()
    }

    /// Send a text message to a peer with auto-incrementing sequence and signing
    pub async fn send_text(&mut self, to: &str, content: &str) -> Result<(), P2PError> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut msg = ChatMessage::new_text(self.peer_id_str(), Some(to.to_string()), content.to_string())
            .with_sequence(seq);

        // Sign the message
        self.sign_message(&mut msg);

        let bytes = ipmsg_protocol::codec::encode_message(&msg);

        if let Some(swarm) = &mut self.swarm {
            swarm.send_message(to, &msg).await?;
        }

        // Track for ACK
        self.pending_acks.insert(msg.id.clone(), PendingAck {
            message_id: msg.id.clone(),
            retries: 0,
            content: bytes.clone(),
        });

        self.store.save_message(&msg).map_err(|e| P2PError::Store(e.to_string()))?;
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg.clone()));

        Ok(())
    }

    /// Send a message to a channel
    pub async fn send_to_channel(&mut self, channel: &ChannelId, content: &str) -> Result<(), P2PError> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut msg = ChatMessage::for_channel(self.peer_id_str(), channel.clone(), content.to_string())
            .with_sequence(seq);

        self.sign_message(&mut msg);

        if let Some(swarm) = &mut self.swarm {
            swarm.broadcast_message(&msg).await?;
        }

        self.store.save_message(&msg).map_err(|e| P2PError::Store(e.to_string()))?;
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg.clone()));

        Ok(())
    }

    /// Broadcast a message to all peers
    pub async fn broadcast(&mut self, content: String) -> Result<(), P2PError> {
        let mut msg = ChatMessage::new_text(self.peer_id_str(), None, content);
        self.sign_message(&mut msg);

        if let Some(swarm) = &mut self.swarm {
            swarm.broadcast_message(&msg).await?;
        }
        let _ = self.event_tx.send(P2PEvent::MessageSent(msg));
        Ok(())
    }

    /// Send ACK for received messages
    async fn send_ack(&mut self, message_ids: &[String]) -> Result<(), P2PError> {
        let msg = ChatMessage::new_ack(self.peer_id_str(), String::new(), message_ids.to_vec());
        let bytes = ipmsg_protocol::codec::encode_message(&msg);
        if let Some(swarm) = &mut self.swarm {
            swarm.publish_to_topic(crate::messaging::CHAT_TOPIC, bytes)
                .map_err(|e| P2PError::Network(e.to_string()))?;
        }
        Ok(())
    }

    /// Send typing indicator
    async fn send_typing(&mut self, to: &str) -> Result<(), P2PError> {
        let msg = ChatMessage::new_typing(self.peer_id_str(), to.to_string());
        let bytes = ipmsg_protocol::codec::encode_message(&msg);
        if let Some(swarm) = &mut self.swarm {
            swarm.publish_to_topic(crate::messaging::CHAT_TOPIC, bytes)
                .map_err(|e| P2PError::Network(e.to_string()))?;
        }
        Ok(())
    }

    /// Update and broadcast profile
    fn update_profile(&mut self, username: &str, bio: Option<&str>) -> Result<(), P2PError> {
        self.username = username.to_string();
        let msg = ChatMessage::new_profile(self.peer_id_str(), username.to_string(), bio.map(|s| s.to_string()), None);
        let bytes = ipmsg_protocol::codec::encode_message(&msg);
        if let Some(swarm) = &mut self.swarm {
            swarm.publish_to_topic(crate::messaging::PRESENCE_TOPIC, bytes)
                .map_err(|e| P2PError::Network(e.to_string()))?;
        }
        Ok(())
    }

    /// Sign a message with our identity
    fn sign_message(&self, msg: &mut ChatMessage) {
        let digest = msg.signing_bytes();
        msg.signature = self.identity.sign(&digest);
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

    pub fn get_channel_history(&self, channel: &str, limit: u32) -> Vec<ChatMessage> {
        self.store.get_channel_messages(channel, limit)
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
            // Subscribe to channel topic
            let topic_name = crate::messaging::channel_topic(&format!("{:?}", channel));
            if let Some(swarm) = &mut self.swarm {
                let _ = swarm.subscribe_topic(&topic_name);
            }
        }
    }

    pub fn remove_channel(&mut self, channel: &ChannelId) {
        self.joined_channels.retain(|c| c != channel);
        let topic_name = crate::messaging::channel_topic(&format!("{:?}", channel));
        if let Some(swarm) = &mut self.swarm {
            let _ = swarm.unsubscribe_topic(&topic_name);
        }
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
