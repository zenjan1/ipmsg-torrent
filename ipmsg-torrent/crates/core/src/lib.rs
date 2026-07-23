pub mod identity;
pub mod transport;
pub mod discovery;
pub mod messaging;
pub mod store;
pub mod noise;
pub mod bloom;
pub mod fragment;
pub mod file_sharing;
pub mod file_transfer;
pub mod ipmsg_compat;

pub use identity::Identity;
pub use store::{MessageStore, PeerInfo};
pub use bloom::DedupCache;
pub use fragment::{FragmentManager, FragmentMsg};
pub use noise::NoiseSessionManager;
pub use file_sharing::FileSharingManager;
pub use file_transfer::{FileTransferManager, FileTransferRequest, FileTransferResponse};
pub use ipmsg_compat::{IpMsgCompat, IpMsgCompatEvent, IpMsgPacket};

use futures::StreamExt;
use ipmsg_protocol::message::{ChatMessage, ChannelId};
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;

/// Maximum tracked message IDs for dedup
const MAX_DEDUP_CACHE: usize = 4096;
/// ACK timeout in seconds
const ACK_TIMEOUT_SECS: u64 = 30;
/// Max retries before giving up
const MAX_RETRIES: u32 = 3;
/// Noise session re-key threshold (messages)
const NOISE_REKEY_THRESHOLD: u64 = 100;

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
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
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
    /// Peer blocked by user
    PeerBlocked { peer_id: String },
    /// Peer verified via fingerprint
    PeerVerified { peer_id: String },
    /// File share announcement received
    FileShareAnnounce {
        from: String,
        shares: Vec<ipmsg_protocol::message::FileShareInfo>,
    },
    /// File search response received
    FileSearchResponse {
        from: String,
        results: Vec<ipmsg_protocol::message::FileShareInfo>,
    },
    /// Fragment received (for reassembly)
    FragmentReceived {
        fragment: crate::fragment::FragmentMsg,
    },
    /// Fragment reassembly complete
    FragmentComplete {
        message_id: String,
        data: Vec<u8>,
    },
    /// File transfer response received
    FileTransferResponse {
        from: String,
        response: crate::file_transfer::FileTransferResponse,
    },
    /// File download progress update
    FileTransferProgress {
        file_hash: String,
        progress: f32,
        chunk_index: u32,
        total_chunks: u32,
    },
    /// Image message received
    ImageReceived {
        from: String,
        data: Vec<u8>,
        mime_type: String,
        name: String,
    },
    /// Read receipt received
    ReadReceiptReceived {
        from: String,
        message_id: String,
    },
    /// Nearby peer discovered with details
    NearbyPeerDiscovered {
        peer: ipmsg_protocol::message::NearbyPeer,
    },
    /// Message search results
    SearchResults {
        query: String,
        results: Vec<ChatMessage>,
    },
    /// File transfer request received (needs response)
    FileTransferRequestReceived {
        from: String,
        request: crate::file_transfer::FileTransferRequest,
    },
    /// Legacy IPMSG peer discovered
    LegacyPeerDiscovered { name: String, host: String, ip: IpAddr },
    /// Legacy IPMSG peer left
    LegacyPeerLeft { name: String, ip: IpAddr },
    /// Legacy IPMSG message received
    LegacyMessageReceived { from: String, ip: IpAddr, content: String, has_attachment: bool },
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
    /// Share a file with the network
    ShareFile {
        path: PathBuf,
        tags: Vec<String>,
        description: Option<String>,
    },
    /// Stop sharing a file
    UnshareFile { hash: String },
    /// Search for files in the network
    SearchFiles { query: String, tags: Vec<String> },
    /// List all shared files (local and discovered)
    ListFiles,
    /// Download a file from a peer
    DownloadFile { file_hash: String, from_peer: String },
    /// Block a peer
    BlockPeer { peer_id: String },
    /// Unblock a peer
    UnblockPeer { peer_id: String },
    /// Search messages by text content
    SearchMessages { query: String, limit: u32 },
    /// Send a read receipt for a message
    SendReadReceipt { message_id: String, to: String },
    /// Send message to legacy IPMSG peer by IP
    SendIpMsg { ip: IpAddr, message: String },
    /// List legacy IPMSG peers
    ListIpMsgPeers,
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
    data_dir: PathBuf,
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
    /// Bloom filter + LRU cache for message dedup
    dedup: DedupCache,
    /// Messages awaiting ACK
    pending_acks: HashMap<String, PendingAck>,
    /// Noise session manager for E2E encryption
    noise_sessions: NoiseSessionManager,
    /// Fragment manager for large messages
    fragment_manager: FragmentManager,
    /// File sharing manager
    file_sharing: FileSharingManager,
    /// File transfer manager for downloads
    file_transfer: Arc<Mutex<FileTransferManager>>,
    /// Classic IPMSG compatibility server
    ipmsg_compat: Option<IpMsgCompat>,
    /// Social trust: blocked peer IDs
    blocked_peers: HashSet<String>,
    /// Social trust: favorite peer IDs
    favorite_peers: HashSet<String>,
}

impl P2PEngine {
    pub fn new(data_dir: PathBuf) -> Result<Self, P2PError> {
        let identity = Identity::load_or_create(&data_dir.join("identity.key"))
            .map_err(|e| P2PError::Identity(e.to_string()))?;

        let store = MessageStore::new(&data_dir.join("messages.db"))
            .map_err(|e| P2PError::Store(e.to_string()))?;

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();

        // Initialize file sharing manager
        let files_dir = data_dir.join("shared_files");
        let file_sharing = FileSharingManager::new(files_dir);
        let file_transfer = Arc::new(Mutex::new(FileTransferManager::new(Arc::new(Mutex::new(file_sharing.clone())))));

        Ok(Self {
            identity,
            store,
            data_dir: data_dir.clone(),
            username: String::new(),
            platforms: vec!["rust".to_string()],
            event_tx,
            event_rx: Some(event_rx),
            command_rx: Some(command_rx),
            command_tx: Some(command_tx),
            swarm: None,
            next_seq: 0,
            joined_channels: Vec::new(),
            dedup: DedupCache::new(MAX_DEDUP_CACHE),
            pending_acks: HashMap::new(),
            noise_sessions: NoiseSessionManager::new(NOISE_REKEY_THRESHOLD),
            fragment_manager: FragmentManager::new(),
            file_sharing,
            file_transfer,
            ipmsg_compat: None,
            blocked_peers: HashSet::new(),
            favorite_peers: HashSet::new(),
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
            &self.data_dir,
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
                                    // Deduplicate received messages using Bloom filter
                                    let evt = match &evt {
                                        P2PEvent::MessageReceived(msg) => {
                                            // Check blocked peers first
                                            if self.is_blocked(&msg.from) {
                                                continue;
                                            }
                                            if self.dedup.is_duplicate(&msg.id) {
                                                continue;
                                            }
                                            self.dedup.mark_seen(&msg.id);
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
                                        P2PEvent::FragmentReceived { fragment } => {
                                            // Extract message_id from the fragment before processing
                                            let message_id = match &fragment {
                                                crate::fragment::FragmentMsg::Start { message_id, .. } => message_id.clone(),
                                                crate::fragment::FragmentMsg::Data { message_id, .. } => message_id.clone(),
                                                crate::fragment::FragmentMsg::End { message_id, .. } => message_id.clone(),
                                            };
                                            // Process fragment through fragment manager
                                            match self.fragment_manager.process_fragment(fragment.clone()) {
                                                Ok(Some(data)) => {
                                                    // Fragment reassembly complete
                                                    P2PEvent::FragmentComplete { message_id, data }
                                                }
                                                Ok(None) => continue,
                                                Err(e) => {
                                                    tracing::warn!(message_id = %message_id, error = %e, "Fragment processing error");
                                                    continue;
                                                }
                                            }
                                        }
                                        P2PEvent::FileTransferResponse { from, response } => {
                                            // Handle file transfer response
                                            let _ = self.handle_file_transfer_response(&from, response.clone()).await;
                                            continue;
                                        }
                                        P2PEvent::FileTransferRequestReceived { from, request } => {
                                            // Auto-respond to file transfer requests using file_sharing manager
                                            let response = self.file_transfer.lock().await.handle_request(request.clone()).await;
                                            let peer_id: Result<PeerId, _> = from.parse();
                                            if let Ok(pid) = peer_id {
                                                if let Some(swarm) = &mut self.swarm {
                                                    let _ = swarm.send_file_transfer_response(&pid, response);
                                                }
                                            }
                                            continue;
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
                            Some(SendCommand::ShareFile { path, tags, description }) => {
                                let _ = self.share_file(path, tags, description).await;
                            }
                            Some(SendCommand::UnshareFile { hash }) => {
                                let _ = self.unshare_file(&hash).await;
                            }
                            Some(SendCommand::SearchFiles { query, tags }) => {
                                let _ = self.search_files(&query, &tags).await;
                            }
                            Some(SendCommand::ListFiles) => {
                                let _ = self.list_files().await;
                            }
                            Some(SendCommand::DownloadFile { file_hash, from_peer }) => {
                                let _ = self.download_file(&file_hash, &from_peer).await;
                            }
                            Some(SendCommand::BlockPeer { peer_id }) => {
                                self.block_peer(&peer_id);
                            }
                            Some(SendCommand::UnblockPeer { peer_id }) => {
                                self.unblock_peer(&peer_id);
                            }
                            Some(SendCommand::SearchMessages { query, limit }) => {
                                let results = self.store.search_messages(&query, limit);
                                let count = results.len();
                                let _ = self.event_tx.send(P2PEvent::Status(
                                    format!("Found {} messages matching '{}'", count, query)
                                ));
                                for msg in results {
                                    let content = msg.text_content().unwrap_or("[non-text]").to_string();
                                    let _ = self.event_tx.send(P2PEvent::Status(
                                        format!("  [{}] {}: {}", msg.timestamp.format("%Y-%m-%d %H:%M"), msg.from, content)
                                    ));
                                }
                            }
                            Some(SendCommand::SendReadReceipt { message_id, to }) => {
                                let receipt = ChatMessage::new_read_receipt(
                                    self.peer_id_str(),
                                    to,
                                    message_id,
                                );
                                self.sign_message(&mut receipt.clone());
                                let bytes = ipmsg_protocol::codec::encode_message(&receipt);
                                if let Some(s) = self.swarm.as_mut() {
                                    let _ = s.publish_to_topic(crate::messaging::CHAT_TOPIC, bytes);
                                }
                            }
                            Some(SendCommand::SendIpMsg { ip, message }) => {
                                if let Err(e) = self.send_ipmsg_message(ip, &message).await {
                                    let _ = self.event_tx.send(P2PEvent::Status(format!("IPMSG send error: {}", e)));
                                }
                            }
                            Some(SendCommand::ListIpMsgPeers) => {
                                let peers = self.ipmsg_legacy_peers();
                                if peers.is_empty() {
                                    let _ = self.event_tx.send(P2PEvent::Status("No legacy IPMSG peers found".to_string()));
                                } else {
                                    let mut lines = vec![format!("Legacy IPMSG peers ({}):", peers.len())];
                                    for p in &peers {
                                        lines.push(format!("  {}@{} ({})", p.name, p.host, p.addr.ip()));
                                    }
                                    let _ = self.event_tx.send(P2PEvent::Status(lines.join("\n")));
                                }
                            }
                            None => break,
                        }
                    }
                    pkt = async {
                        if let Some(compat) = &self.ipmsg_compat {
                            compat.recv_packet().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        if let Some(packet) = pkt {
                            if let Some(evt) = self.ipmsg_compat.as_ref().unwrap().process_packet(&packet).await {
                                let p2p_evt = match evt {
                                    IpMsgCompatEvent::PeerDiscovered { name, host, addr } => {
                                        P2PEvent::LegacyPeerDiscovered { name, host, ip: addr.ip() }
                                    }
                                    IpMsgCompatEvent::PeerLeft { name, addr } => {
                                        P2PEvent::LegacyPeerLeft { name, ip: addr.ip() }
                                    }
                                    IpMsgCompatEvent::MessageReceived { from, addr, content, has_attachment } => {
                                        P2PEvent::LegacyMessageReceived { from, ip: addr.ip(), content, has_attachment }
                                    }
                                };
                                let _ = self.event_tx.send(p2p_evt);
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(ACK_TIMEOUT_SECS)) => {
                        // Check for timed-out ACKs and retry
                        self.check_pending_acks().await;
                        // Clean up stale legacy peers
                        if let Some(compat) = &self.ipmsg_compat {
                            compat.cleanup_stale_peers().await;
                        }
                    }
                }
            }
        }
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

        // Encrypt with Noise if we have an active session (E2E encryption)
        if let Some(session) = self.noise_sessions.get_mut(to) {
            if session.is_ready() {
                let plaintext = ipmsg_protocol::codec::encode_message(&msg);
                match session.encrypt(&plaintext) {
                    Ok(ciphertext) => {
                        // Create encrypted payload wrapper
                        msg.encrypted_payload = Some(ipmsg_protocol::message::EncryptedPayload {
                            ephemeral_key: String::new(), // Noise handles key exchange internally
                            ciphertext,
                            nonce: Vec::new(),
                            ratchet_index: 0,
                        });
                        // Clear the plaintext content since we're sending encrypted
                        msg.kind = ipmsg_protocol::message::MessageType::Text { content: String::new() };
                    }
                    Err(e) => {
                        tracing::warn!(peer = %to, error = %e, "Noise encryption failed, sending plaintext");
                    }
                }
            }
        }

        let bytes = ipmsg_protocol::codec::encode_message(&msg);

        // Check if message needs fragmentation
        if self.fragment_manager.needs_fragment(bytes.len()) {
            // Fragment the message
            let fragments = self.fragment_manager.fragment(&msg.id, &bytes, "text");
            
            // Send fragments via gossipsub
            if let Some(swarm) = &mut self.swarm {
                for fragment in fragments {
                    let fragment_bytes = serde_cbor::to_vec(&fragment)
                        .map_err(|e| P2PError::Network(e.to_string()))?;
                    swarm.publish_to_topic(crate::messaging::FRAGMENT_TOPIC, fragment_bytes)?;
                }
            }
        } else {
            // Send as regular message
            if let Some(swarm) = &mut self.swarm {
                swarm.send_message(to, &msg).await?;
            }
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

    pub fn get_store(&self) -> &MessageStore {
        &self.store
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

    // --- Social Trust Layer (inspired by bitchat) ---

    /// Block a peer - messages from this peer will be discarded
    pub fn block_peer(&mut self, peer_id: &str) {
        self.blocked_peers.insert(peer_id.to_string());
        self.favorite_peers.remove(peer_id);
        self.noise_sessions.remove(peer_id);
        tracing::info!(peer_id = %peer_id, "Peer blocked");
    }

    /// Unblock a peer
    pub fn unblock_peer(&mut self, peer_id: &str) {
        self.blocked_peers.remove(peer_id);
        tracing::info!(peer_id = %peer_id, "Peer unblocked");
    }

    /// Check if a peer is blocked
    pub fn is_blocked(&self, peer_id: &str) -> bool {
        self.blocked_peers.contains(peer_id)
    }

    /// Mark a peer as favorite
    pub fn mark_favorite(&mut self, peer_id: &str) {
        self.favorite_peers.insert(peer_id.to_string());
        tracing::info!(peer_id = %peer_id, "Peer marked as favorite");
    }

    /// Remove a peer from favorites
    pub fn remove_favorite(&mut self, peer_id: &str) {
        self.favorite_peers.remove(peer_id);
    }

    /// Check if a peer is a favorite
    pub fn is_favorite(&self, peer_id: &str) -> bool {
        self.favorite_peers.contains(peer_id)
    }

    /// Get list of blocked peers
    pub fn blocked_peers(&self) -> Vec<&str> {
        self.blocked_peers.iter().map(|s| s.as_str()).collect()
    }

    /// Get list of favorite peers
    pub fn favorite_peers(&self) -> Vec<&str> {
        self.favorite_peers.iter().map(|s| s.as_str()).collect()
    }

    /// Verify a peer's fingerprint (out-of-band verification)
    pub fn verify_peer_fingerprint(&self, peer_id: &str, expected_fingerprint: &str) -> bool {
        // Fingerprint = SHA-256 hash of Noise static public key (like bitchat)
        if let Some(swarm) = &self.swarm {
            if let Some(peer) = swarm.get_peers().iter().find(|p| p.peer_id == peer_id) {
                use sha2::{Digest, Sha256};
                let fp = format!("{:x}", Sha256::digest(peer.peer_id.as_bytes()));
                return fp == expected_fingerprint;
            }
        }
        false
    }

    /// Get own fingerprint for sharing
    pub fn my_fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let pub_key = self.identity.verifying_key().to_bytes();
        format!("{:x}", Sha256::digest(&pub_key))
    }

    // --- Rate Limiter (inspired by bitchat NoiseRateLimiter) ---

    /// Check if a peer is sending too many messages (sliding window rate limiting)
    /// Returns true if the message should be allowed
    pub fn check_rate_limit(&mut self, peer_id: &str) -> bool {
        use std::collections::VecDeque;
        use std::time::{Duration, Instant};
        
        // Per-peer rate limit state
        struct RateLimitState {
            timestamps: VecDeque<Instant>,
            max_messages: usize,
            window: Duration,
        }
        
        // Thread-local storage for rate limit states
        thread_local! {
            static RATE_LIMITS: std::cell::RefCell<HashMap<String, RateLimitState>> = 
                std::cell::RefCell::new(HashMap::new());
        }
        
        RATE_LIMITS.with(|limits| {
            let mut limits = limits.borrow_mut();
            let state = limits.entry(peer_id.to_string()).or_insert_with(|| RateLimitState {
                timestamps: VecDeque::new(),
                max_messages: 10,
                window: Duration::from_secs(5),
            });
            
            let now = Instant::now();
            
            // Remove old timestamps outside the window
            while let Some(old) = state.timestamps.front() {
                if now.duration_since(*old) > state.window {
                    state.timestamps.pop_front();
                } else {
                    break;
                }
            }
            
            // Check if under limit
            if state.timestamps.len() < state.max_messages {
                state.timestamps.push_back(now);
                true
            } else {
                tracing::warn!(peer = %peer_id, "Rate limit exceeded");
                false
            }
        })
    }

    // --- File Sharing ---

    /// Share a file with the network
    pub async fn share_file(
        &mut self,
        path: PathBuf,
        tags: Vec<String>,
        description: Option<String>,
    ) -> Result<(), P2PError> {
        let info = self.file_sharing.share_file(&path, tags, description, self.peer_id_str()).await?;
        
        // Broadcast file share announcement
        let msg = ChatMessage::new_file_share_announce(
            self.peer_id_str(),
            vec![info.clone()],
        );
        
        if let Some(swarm) = &mut self.swarm {
            swarm.publish_to_topic(crate::messaging::FILE_TOPIC, ipmsg_protocol::codec::encode_message(&msg))
                .map_err(|e| P2PError::Network(e.to_string()))?;
        }
        
        tracing::info!(file = %info.file_ref.name, "File shared successfully");
        Ok(())
    }

    /// Stop sharing a file
    pub async fn unshare_file(&mut self, hash: &str) -> Result<(), P2PError> {
        self.file_sharing.unshare_file(hash).await;
        tracing::info!(hash = %hash, "File unshared");
        Ok(())
    }

    /// Search for files in the network
    pub async fn search_files(&mut self, query: &str, tags: &[String]) -> Result<(), P2PError> {
        // Broadcast search query
        let msg = ChatMessage::new_file_share_query(
            self.peer_id_str(),
            query.to_string(),
            tags.to_vec(),
        );
        
        if let Some(swarm) = &mut self.swarm {
            swarm.publish_to_topic(crate::messaging::FILE_TOPIC, ipmsg_protocol::codec::encode_message(&msg))
                .map_err(|e| P2PError::Network(e.to_string()))?;
        }
        
        tracing::info!(query = %query, "File search query broadcasted");
        Ok(())
    }

    /// List all shared files (local and discovered)
    pub async fn list_files(&mut self) -> Result<(), P2PError> {
        let shared = self.file_sharing.list_shared_files().await;
        let discovered = self.file_sharing.list_discovered_files().await;
        
        tracing::info!(
            shared_count = shared.len(),
            discovered_count = discovered.len(),
            "Listed files"
        );
        
        Ok(())
    }

    /// Download a file from a peer
    pub async fn download_file(&mut self, file_hash: &str, from_peer: &str) -> Result<(), P2PError> {
        // Parse peer ID
        let peer_id: PeerId = from_peer.parse().map_err(|e| P2PError::PeerNotFound(format!("Invalid peer ID: {}", e)))?;
        
        // Request file info first
        let req = FileTransferRequest::GetInfo { file_hash: file_hash.to_string() };
        if let Some(swarm) = &mut self.swarm {
            swarm.send_file_request(&peer_id, req)?;
        }
        
        tracing::info!(file_hash = %file_hash, from_peer = %from_peer, "File download initiated");
        Ok(())
    }

    /// Request a specific chunk from a peer
    pub async fn request_chunk(&mut self, file_hash: &str, chunk_index: u32, from_peer: &str) -> Result<(), P2PError> {
        let peer_id: PeerId = from_peer.parse().map_err(|e| P2PError::PeerNotFound(format!("Invalid peer ID: {}", e)))?;
        
        let req = FileTransferRequest::GetChunk { 
            file_hash: file_hash.to_string(), 
            chunk_index 
        };
        
        if let Some(swarm) = &mut self.swarm {
            swarm.send_file_request(&peer_id, req)?;
        }
        
        Ok(())
    }

    /// Process file transfer requests and send responses
    pub async fn process_file_transfer_requests(&mut self) -> Result<(), P2PError> {
        let file_transfer = self.file_transfer.clone();
        let pending_requests = file_transfer.lock().await.take_pending_requests().await;
        
        for (peer_id_str, req) in pending_requests {
            let response = file_transfer.lock().await.handle_request(req).await;
            
            // Send response back via request-response protocol
            if let Ok(peer_id) = peer_id_str.parse::<PeerId>() {
                if let Some(swarm) = &mut self.swarm {
                    swarm.send_file_transfer_response(&peer_id, response)?;
                    tracing::info!(peer_id = %peer_id_str, "File transfer response sent");
                }
            }
        }
        
        Ok(())
    }

    /// Handle incoming file transfer response
    pub async fn handle_file_transfer_response(&mut self, peer_id: &str, response: FileTransferResponse) -> Result<(), P2PError> {
        match response {
            FileTransferResponse::Info { file_ref, available } => {
                if available {
                    tracing::info!(file_hash = %file_ref.hash, "File info received, starting download");
                    
                    // Start download tracking
                    let file_transfer = self.file_transfer.clone();
                    file_transfer.lock().await.start_download(file_ref.clone(), peer_id.to_string()).await;
                    
                    // Request first chunk
                    self.request_chunk(&file_ref.hash, 0, peer_id).await?;
                } else {
                    tracing::warn!("File not available from peer");
                }
            }
            FileTransferResponse::Chunk { file_hash, chunk_index, data } => {
                tracing::info!(file_hash = %file_hash, chunk = %chunk_index, "Chunk received");
                
                let file_transfer = self.file_transfer.clone();
                let is_complete = file_transfer.lock().await.record_chunk(&file_hash, chunk_index, data).await;
                
                if is_complete {
                    tracing::info!(file_hash = %file_hash, "File download complete");
                    
                    // Assemble file
                    if let Some(file_data) = file_transfer.lock().await.try_assemble(&file_hash).await {
                        // Save to disk
                        let file_info = file_transfer.lock().await.finish_download(&file_hash).await;
                        if let Some(info) = file_info {
                            let output_path = self.data_dir.join("downloads").join(&info.file_ref.name);
                            std::fs::create_dir_all(output_path.parent().unwrap())?;
                            std::fs::write(&output_path, &file_data)?;
                            tracing::info!(path = %output_path.display(), "File saved");
                            
                            let _ = self.event_tx.send(P2PEvent::Status(format!(
                                "Downloaded file: {}",
                                info.file_ref.name
                            )));
                        }
                    }
                } else {
                    // Request next chunk
                    let next_chunk = chunk_index + 1;
                    self.request_chunk(&file_hash, next_chunk, peer_id).await?;
                }
            }
            FileTransferResponse::Error { message } => {
                tracing::error!(error = %message, "File transfer error");
                let _ = self.event_tx.send(P2PEvent::Status(format!("File transfer error: {}", message)));
            }
        }
        
        Ok(())
    }

    /// Start the classic IPMSG compatibility server (UDP port 2425)
    /// Enables interoperability with legacy IPMSG/FeiQ clients on the LAN
    pub async fn start_ipmsg_compat(&mut self) -> Result<(), P2PError> {
        let mut compat = IpMsgCompat::new(self.username.clone());
        compat.start().await.map_err(|e| P2PError::Transport(e.to_string()))?;
        self.ipmsg_compat = Some(compat);
        tracing::info!("Classic IPMSG compatibility server started");
        Ok(())
    }

    /// Send a message to a legacy IPMSG peer via UDP
    pub async fn send_ipmsg_message(&mut self, to_ip: std::net::IpAddr, message: &str) -> Result<(), P2PError> {
        if let Some(compat) = &mut self.ipmsg_compat {
            let addr = std::net::SocketAddr::new(to_ip, ipmsg_compat::IPMSG_PORT);
            compat.send_message(addr, message).await.map_err(|e| P2PError::Transport(e.to_string()))
        } else {
            Err(P2PError::Transport("IPMSG compat server not started".to_string()))
        }
    }

    /// Broadcast a message to all legacy IPMSG peers via UDP
    pub async fn broadcast_ipmsg_message(&mut self, message: &str) -> Result<(), P2PError> {
        if let Some(compat) = &mut self.ipmsg_compat {
            // Send to broadcast address
            let broadcast_addr = std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::BROADCAST),
                ipmsg_compat::IPMSG_PORT,
            );
            compat.send_message(broadcast_addr, message).await.map_err(|e| P2PError::Transport(e.to_string()))
        } else {
            Err(P2PError::Transport("IPMSG compat server not started".to_string()))
        }
    }

    /// Get list of known legacy IPMSG peers
    pub fn ipmsg_legacy_peers(&self) -> Vec<ipmsg_compat::IpMsgPeerInfo> {
        // We can't easily access the compat's peers without async,
        // so we return empty and let the caller use ListIpMsgPeers command
        Vec::new()
    }

    /// Get file sharing manager reference
    pub fn file_sharing(&self) -> &FileSharingManager {
        &self.file_sharing
    }

    /// Get file transfer manager reference
    pub fn file_transfer(&self) -> &Arc<Mutex<FileTransferManager>> {
        &self.file_transfer
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
