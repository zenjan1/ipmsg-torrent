pub mod identity;
pub mod transport;
pub mod discovery;
pub mod messaging;
pub mod store;
pub mod noise;
pub mod bloom;
pub mod fragment;
pub mod ratelimit;

pub use identity::Identity;
pub use store::{MessageStore, PeerInfo};
pub use bloom::DedupCache;
pub use fragment::{FragmentManager, FragmentMsg};
pub use noise::NoiseSessionManager;
pub use ratelimit::{RateLimiter, CoverTraffic, RateLimitConfig};

use ipmsg_protocol::message::{ChatMessage, ChannelId};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

const MAX_DEDUP_CACHE: usize = 4096;
const ACK_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;
const NOISE_REKEY_THRESHOLD: u64 = 100;

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

#[derive(Debug, Clone)]
pub enum P2PEvent {
    PeerJoined { peer_id: String, username: String, platforms: Vec<String> },
    PeerLeft { peer_id: String },
    MessageReceived(ChatMessage),
    MessageSent(ChatMessage),
    MessageDelivered(String),
    FileOffer { from: String, file_ref: ipmsg_protocol::message::FileRef },
    Typing { from: String },
    Status(String),
    PeerBlocked { peer_id: String },
    PeerVerified { peer_id: String },
    FragmentComplete { message_id: String, data: Vec<u8> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectedPeer {
    pub peer_id: String,
    pub username: String,
    pub platforms: Vec<String>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

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

struct PendingAck {
    #[allow(dead_code)]
    message_id: String,
    retries: u32,
    content: Vec<u8>,
}

pub struct P2PEngine {
    identity: Identity,
    store: MessageStore,
    data_dir: PathBuf,
    username: String,
    platforms: Vec<String>,
    event_tx: tokio::sync::mpsc::UnboundedSender<P2PEvent>,
    command_rx: Option<tokio::sync::mpsc::UnboundedReceiver<SendCommand>>,
    command_tx: Option<tokio::sync::mpsc::UnboundedSender<SendCommand>>,
    swarm: Option<transport::P2PSwarm>,
    next_seq: u64,
    joined_channels: Vec<ChannelId>,
    dedup: DedupCache,
    pending_acks: HashMap<String, PendingAck>,
    noise_sessions: NoiseSessionManager,
    #[allow(dead_code)]
    fragment_manager: FragmentManager,
    blocked_peers: HashSet<String>,
    favorite_peers: HashSet<String>,
}

impl P2PEngine {
    pub fn new(data_dir: PathBuf) -> Result<Self, P2PError> {
        let identity = Identity::load_or_create(&data_dir.join("identity.key"))
            .map_err(|e| P2PError::Identity(e.to_string()))?;

        let store = MessageStore::new(&data_dir.join("messages.db"))
            .map_err(|e| P2PError::Store(e.to_string()))?;

        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            identity,
            store,
            data_dir: data_dir.clone(),
            username: String::new(),
            platforms: vec!["rust".to_string()],
            event_tx,
            command_rx: Some(command_rx),
            command_tx: Some(command_tx),
            swarm: None,
            next_seq: 0,
            joined_channels: Vec::new(),
            dedup: DedupCache::new(MAX_DEDUP_CACHE),
            pending_acks: HashMap::new(),
            noise_sessions: NoiseSessionManager::new(NOISE_REKEY_THRESHOLD),
            fragment_manager: FragmentManager::new(),
            blocked_peers: HashSet::new(),
            favorite_peers: HashSet::new(),
        })
    }

    pub async fn start(&mut self, username: String, bootstrap_nodes: Vec<String>) -> Result<String, P2PError> {
        self.username = username.clone();
        self.platforms = detect_platforms();

        let swarm = transport::create_swarm(
            &self.identity,
            &self.username,
            &self.platforms,
            &self.event_tx,
            bootstrap_nodes,
            &self.data_dir,
        ).await?;

        self.swarm = Some(swarm);

        let presence = ChatMessage::new_presence(
            self.peer_id_str(),
            self.username.clone(),
            self.platforms.clone(),
        );
        if let Some(swarm) = &mut self.swarm {
            let _ = transport::publish_message(swarm, crate::messaging::PRESENCE_TOPIC, ipmsg_protocol::codec::encode_message(&presence));
        }

        tracing::info!(peer_id = %self.peer_id_str(), username = %self.username, "P2P engine started");
        Ok(self.peer_id_str())
    }

    pub fn list_peers(&self) -> Vec<ConnectedPeer> {
        if let Some(swarm) = &self.swarm {
            let count = transport::get_peer_count(swarm);
            vec![ConnectedPeer {
                peer_id: self.peer_id_str(),
                username: self.username.clone(),
                platforms: self.platforms.clone(),
                last_seen: chrono::Utc::now(),
            }]
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

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
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

    pub fn block_peer(&mut self, peer_id: &str) {
        self.blocked_peers.insert(peer_id.to_string());
        self.favorite_peers.remove(peer_id);
        self.noise_sessions.remove(peer_id);
        tracing::info!(peer_id = %peer_id, "Peer blocked");
    }

    pub fn unblock_peer(&mut self, peer_id: &str) {
        self.blocked_peers.remove(peer_id);
        tracing::info!(peer_id = %peer_id, "Peer unblocked");
    }

    pub fn is_blocked(&self, peer_id: &str) -> bool {
        self.blocked_peers.contains(peer_id)
    }

    pub fn mark_favorite(&mut self, peer_id: &str) {
        self.favorite_peers.insert(peer_id.to_string());
        tracing::info!(peer_id = %peer_id, "Peer marked as favorite");
    }

    pub fn remove_favorite(&mut self, peer_id: &str) {
        self.favorite_peers.remove(peer_id);
    }

    pub fn is_favorite(&self, peer_id: &str) -> bool {
        self.favorite_peers.contains(peer_id)
    }

    pub fn blocked_peers(&self) -> Vec<&str> {
        self.blocked_peers.iter().map(|s| s.as_str()).collect()
    }

    pub fn favorite_peers(&self) -> Vec<&str> {
        self.favorite_peers.iter().map(|s| s.as_str()).collect()
    }

    pub fn verify_peer_fingerprint(&self, peer_id: &str, expected_fingerprint: &str) -> bool {
        if let Some(_) = &self.swarm {
            use sha2::{Digest, Sha256};
            let fp = format!("{:x}", Sha256::digest(peer_id.as_bytes()));
            return fp == expected_fingerprint;
        }
        false
    }

    pub fn my_fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let pub_key = self.identity.verifying_key().to_bytes();
        format!("{:x}", Sha256::digest(&pub_key))
    }

    pub fn check_rate_limit(&mut self, peer_id: &str) -> bool {
        let _peer_id = peer_id;
        true
    }

    pub fn take_receiver(&mut self) -> Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let event_tx = self.event_tx.clone();
        self.event_tx = tx;
        Some(rx)
    }

    pub fn take_command_sender(&mut self) -> Option<tokio::sync::mpsc::UnboundedSender<SendCommand>> {
        self.command_tx.take()
    }

    pub async fn run_event_loop(&mut self) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn send_text(&mut self, to: &str, content: &str) -> Result<(), P2PError> {
        let msg = ChatMessage::new_text(
            self.peer_id_str(),
            Some(to.to_string()),
            content.to_string(),
        );
        if let Some(swarm) = &mut self.swarm {
            let data = ipmsg_protocol::codec::encode_message(&msg);
            transport::publish_message(swarm, crate::messaging::CHAT_TOPIC, data)?;
        }
        Ok(())
    }

    pub async fn send_to_channel(&mut self, channel: &ChannelId, content: &str) -> Result<(), P2PError> {
        let msg = ChatMessage::for_channel(
            self.peer_id_str(),
            channel.clone(),
            content.to_string(),
        );
        if let Some(swarm) = &mut self.swarm {
            let data = ipmsg_protocol::codec::encode_message(&msg);
            transport::publish_message(swarm, crate::messaging::CHAT_TOPIC, data)?;
        }
        Ok(())
    }

    pub async fn broadcast(&mut self, content: String) -> Result<(), P2PError> {
        self.send_to_channel(&ChannelId::Group("main".to_string()), &content).await
    }

    pub async fn next_event(&mut self) -> Option<P2PEvent> {
        None
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
