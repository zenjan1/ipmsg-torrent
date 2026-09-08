use crate::file_transfer::{
    FileTransferCodec, FileTransferRequest, FileTransferResponse, create_file_transfer_behaviour,
};
use crate::identity::Identity;
use crate::messaging::{
    CHAT_TOPIC, FILE_TOPIC, FRAGMENT_TOPIC, PRESENCE_TOPIC, parse_agent_version,
};
use crate::store::PeerInfo as StoredPeerInfo;
use crate::{ConnectedPeer, MessageStore, P2PError, P2PEvent};
use futures::stream::StreamExt;
use ipmsg_protocol::codec::decode_message;
use ipmsg_protocol::message::ChatMessage;
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::gossipsub::{self, IdentTopic, MessageAuthenticity};
use libp2p::identify::{self, Event as IdentifyEvent};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{Behaviour as Kademlia, Config as KademliaConfig};
use libp2p::mdns::{Behaviour as Mdns, Event as MdnsEvent};
use libp2p::relay;
use libp2p::request_response::{self, Event as RequestResponseEvent};
use libp2p::swarm::NetworkBehaviour;
use libp2p::swarm::SwarmEvent;
use libp2p::swarm::ToSwarm;
use libp2p::{Multiaddr, PeerId, StreamProtocol, Swarm};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

/// Maximum number of consecutive failures before circuit breaker opens.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 3;
/// Duration the circuit breaker stays open before allowing a retry.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(30);
/// Maximum number of dial retries attempts with exponential backoff.
const MAX_DIAL_RETRIES: u32 = 5;
/// Base delay for exponential backoff on dial retries.
const DIAL_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Check if a multiaddr contains a private/internal IP address.
/// Uses early-exit pattern matching on the string representation for speed.
fn is_private_addr(addr: &Multiaddr) -> bool {
    let addr_str = addr.to_string();
    // Fast path: most addresses are public, check common prefixes first
    if !addr_str.contains("/ip4/") {
        return false; // IPv6 or other — treat as public for now
    }
    // Check loopback
    if addr_str.contains("/ip4/127.") {
        return true;
    }
    // Check RFC1918 private ranges
    if addr_str.contains("/ip4/10.") || addr_str.contains("/ip4/192.168.") {
        return true;
    }
    // Check 172.16.0.0/12 (172.16.x - 172.31.x)
    if let Some(start) = addr_str.find("/ip4/172.") {
        let after = &addr_str[start + 9..]; // skip "/ip4/172."
        if let Some(dot_pos) = after.find('.') {
            if let Ok(second_octet) = after[..dot_pos].parse::<u8>() {
                if (16..=31).contains(&second_octet) {
                    return true;
                }
            }
        }
    }
    false
}

/// Parse a multiaddr string with retry and logging.
/// Returns `None` if the address cannot be parsed after `max_retries` attempts.
fn parse_addr_with_retry(addr_str: &str, context: &str, max_retries: u32) -> Option<Multiaddr> {
    for attempt in 1..=max_retries {
        match addr_str.parse::<Multiaddr>() {
            Ok(addr) => return Some(addr),
            Err(e) => {
                if attempt < max_retries {
                    tracing::warn!(
                        address = addr_str,
                        context = context,
                        attempt = attempt,
                        max_retries = max_retries,
                        error = %e,
                        "Failed to parse multiaddr, retrying"
                    );
                } else {
                    tracing::error!(
                        address = addr_str,
                        context = context,
                        attempts = max_retries,
                        error = %e,
                        "Failed to parse multiaddr after all retries, skipping"
                    );
                }
            }
        }
    }
    None
}

/// Per-peer circuit breaker state.
#[derive(Debug, Clone)]
struct CircuitBreakerEntry {
    /// Number of consecutive failures.
    consecutive_failures: u32,
    /// When the circuit was opened (None if closed).
    opened_at: Option<Instant>,
}

impl CircuitBreakerEntry {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            opened_at: None,
        }
    }

    /// Record a failure. Returns `true` if the circuit just opened.
    fn record_failure(&mut self) -> bool {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD && self.opened_at.is_none() {
            self.opened_at = Some(Instant::now());
            tracing::warn!(
                failures = self.consecutive_failures,
                cooldown_secs = CIRCUIT_BREAKER_COOLDOWN.as_secs(),
                "Circuit breaker opened for peer"
            );
            true
        } else {
            false
        }
    }

    /// Record a success, resetting the failure counter.
    fn record_success(&mut self) {
        if self.consecutive_failures > 0 {
            tracing::info!(
                previous_failures = self.consecutive_failures,
                "Circuit breaker reset for peer after successful connection"
            );
        }
        self.consecutive_failures = 0;
        self.opened_at = None;
    }

    /// Whether the circuit is currently open (tripped).
    fn is_open(&self) -> bool {
        if let Some(opened_at) = self.opened_at {
            if opened_at.elapsed() < CIRCUIT_BREAKER_COOLDOWN {
                return true;
            }
            // Cooldown elapsed — allow a half-open retry
            false
        } else {
            false
        }
    }

    /// Reset the circuit breaker manually.
    #[allow(dead_code)]
    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.opened_at = None;
    }
}

/// Combined LibP2P behaviour — all sub-protocols
#[derive(NetworkBehaviour)]
pub struct IpMsgNetBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: Kademlia<MemoryStore>,
    pub mdns: Mdns<libp2p::mdns::tokio::Tokio>,
    pub file_transfer: request_response::Behaviour<FileTransferCodec>,
    pub relay: relay::client::Behaviour,
    pub relay_server: relay::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::Behaviour,
}

impl IpMsgNetBehaviour {
    pub fn new(
        local_key: &libp2p::identity::Keypair,
        username: &str,
        platforms: &[String],
        relay_client: relay::client::Behaviour,
    ) -> Self {
        let local_peer_id = PeerId::from(local_key.public());

        // Kademlia
        let mut kad_config = KademliaConfig::new(StreamProtocol::new("/ipmsg/kad/1.0.0"));
        kad_config.set_query_timeout(Duration::from_secs(60));
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = Kademlia::new(local_peer_id, kad_store);
        kademlia.set_mode(Some(libp2p::kad::Mode::Server));

        // Gossipsub
        let gs_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_millis(1000))
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .build()
            .expect("valid gossipsub config");
        let gossipsub =
            gossipsub::Behaviour::new(MessageAuthenticity::Signed(local_key.clone()), gs_config)
                .expect("valid gossipsub");

        // Identify
        let identify_config = identify::Config::new("ipmsg/1.0.0".to_string(), local_key.public())
            .with_agent_version(format!(
                "ipmsg/2.1.0 ({}, {})",
                username,
                platforms.join(", ")
            ))
            .with_interval(Duration::from_secs(300));
        let identify = identify::Behaviour::new(identify_config);

        // mDNS
        let mdns = Mdns::new(Default::default(), local_peer_id).expect("mDNS creation failed");

        // File transfer request-response
        let file_transfer = create_file_transfer_behaviour();

        // DCUtR (Direct Connection Upgrade through Relay)
        let dcutr = dcutr::Behaviour::new(local_peer_id);

        // AutoNAT (NAT detection)
        let autonat = autonat::Behaviour::new(local_peer_id, autonat::Config::default());

        // Relay server — allow other peers to use this node as a relay
        let relay_server = relay::Behaviour::new(local_peer_id, relay::Config::default());

        Self {
            gossipsub,
            identify,
            kademlia,
            mdns,
            file_transfer,
            relay: relay_client,
            relay_server,
            dcutr,
            autonat,
        }
    }

    pub fn add_kademlia_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.kademlia.add_address(&peer_id, addr);
    }

    pub fn subscribe_topic(&mut self, name: &str) -> Result<(), P2PError> {
        let topic = IdentTopic::new(name);
        self.gossipsub
            .subscribe(&topic)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        tracing::info!(topic = name, "Subscribed to topic");
        Ok(())
    }

    pub fn unsubscribe_topic(&mut self, name: &str) -> Result<(), P2PError> {
        let topic = IdentTopic::new(name);
        self.gossipsub.unsubscribe(&topic);
        Ok(())
    }

    pub fn publish_to_topic(&mut self, topic_name: &str, data: Vec<u8>) -> Result<(), P2PError> {
        let topic = IdentTopic::new(topic_name);
        self.gossipsub
            .publish(topic, data)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        Ok(())
    }
}

/// The libp2p Swarm wrapper for IPMsg
pub struct P2PSwarm {
    swarm: Swarm<IpMsgNetBehaviour>,
    peers: HashMap<String, ConnectedPeer>,
    subscribed_topics: Vec<String>,
    store: MessageStore,
    connected_peers: HashSet<PeerId>,
    /// Pending response channels for file transfer requests
    pending_response_channels:
        HashMap<String, request_response::ResponseChannel<FileTransferResponse>>,
    /// Store relay node addresses for later relay address construction
    relay_node_addrs: HashMap<PeerId, Vec<Multiaddr>>,
    /// Circuit breaker state per peer for connection failure tracking
    circuit_breakers: HashMap<PeerId, CircuitBreakerEntry>,
}

pub struct SwarmConfig {
    pub bootstrap_nodes: Vec<String>,
    pub known_addrs: Vec<(String, Vec<String>)>,
    pub listen_port: u16,
}

impl P2PSwarm {
    pub async fn new(
        identity: &Identity,
        username: &str,
        platforms: &[String],
        _event_tx: &UnboundedSender<P2PEvent>,
        config: SwarmConfig,
        data_dir: &Path,
    ) -> Result<Self, P2PError> {
        let keypair = identity.to_keypair();

        let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .map_err(|e| P2PError::Transport(format!("tcp setup: {:?}", e)))?
            .with_quic()
            .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)
            .map_err(|e| P2PError::Transport(format!("relay_client: {:?}", e)))?
            .with_behaviour(|key, relay_client| {
                Ok(IpMsgNetBehaviour::new(
                    key,
                    username,
                    platforms,
                    relay_client,
                ))
            })
            .map_err(|e| P2PError::Transport(format!("behaviour: {:?}", e)))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(600)))
            .build();

        std::fs::create_dir_all(data_dir).ok();
        let db_path = data_dir.join("swarm_cache.db");
        let store = MessageStore::new(&db_path).map_err(|e| {
            P2PError::Transport(format!(
                "Failed to create swarm message store at {:?}: {}",
                db_path, e
            ))
        })?;

        let mut swarm_obj = Self {
            swarm,
            peers: HashMap::new(),
            subscribed_topics: Vec::new(),
            store,
            connected_peers: HashSet::new(),
            pending_response_channels: HashMap::new(),
            relay_node_addrs: HashMap::new(),
            circuit_breakers: HashMap::new(),
        };

        // Listen on TCP FIRST (must listen before dialing)
        let tcp_addr = if config.listen_port > 0 {
            format!("/ip4/0.0.0.0/tcp/{}", config.listen_port)
        } else {
            "/ip4/0.0.0.0/tcp/0".to_string()
        };
        let tcp_addr: Multiaddr = tcp_addr.parse().map_err(|e| {
            P2PError::Transport(format!("Invalid TCP listen address: {}", e))
        })?;
        swarm_obj
            .swarm
            .listen_on(tcp_addr)
            .map_err(|e| P2PError::Transport(format!("listen_on tcp: {:?}", e)))?;

        // Listen on TCP IPv6
        let tcp6_addr = if config.listen_port > 0 {
            format!("/ip6/::/tcp/{}", config.listen_port)
        } else {
            "/ip6/::/tcp/0".to_string()
        };
        match tcp6_addr.parse::<Multiaddr>() {
            Ok(addr) => {
                if let Err(e) = swarm_obj.swarm.listen_on(addr) {
                    tracing::warn!(error = ?e, "Failed to listen on IPv6 TCP (non-fatal)");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, address = tcp6_addr, "Invalid IPv6 TCP address, skipping");
            }
        }

        // Listen on QUIC
        let quic_addr = if config.listen_port > 0 {
            format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port)
        } else {
            "/ip4/0.0.0.0/udp/0/quic-v1".to_string()
        };
        let quic_addr: Multiaddr = quic_addr.parse().map_err(|e| {
            P2PError::Transport(format!("Invalid QUIC listen address: {}", e))
        })?;
        swarm_obj
            .swarm
            .listen_on(quic_addr)
            .map_err(|e| P2PError::Transport(format!("listen_on quic: {:?}", e)))?;

        // Listen on QUIC IPv6
        let quic6_addr = if config.listen_port > 0 {
            format!("/ip6/::/udp/{}/quic-v1", config.listen_port)
        } else {
            "/ip6/::/udp/0/quic-v1".to_string()
        };
        match quic6_addr.parse::<Multiaddr>() {
            Ok(addr) => {
                if let Err(e) = swarm_obj.swarm.listen_on(addr) {
                    tracing::warn!(error = ?e, "Failed to listen on IPv6 QUIC (non-fatal)");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, address = quic6_addr, "Invalid IPv6 QUIC address, skipping");
            }
        }
        // Dial bootstrap nodes with tolerance and statistics
        let mut bootstrap_success = 0;
        let mut bootstrap_failed = 0;
        for addr_str in &config.bootstrap_nodes {
            // Parse with retry and logging
            let addr = match parse_addr_with_retry(addr_str, "bootstrap node", 3) {
                Some(a) => a,
                None => {
                    bootstrap_failed += 1;
                    continue;
                }
            };

            // Extract peer ID from address
            let peer_id = match addr.iter().find_map(|p| match p {
                libp2p::multiaddr::Protocol::P2p(pid) => Some(pid),
                _ => None,
            }) {
                Some(pid) => pid,
                None => {
                    tracing::warn!(
                        address = %addr,
                        "Bootstrap node address missing peer ID, skipping"
                    );
                    bootstrap_failed += 1;
                    continue;
                }
            };

            // Dial with exponential backoff
            if let Err(e) = swarm_obj.dial_with_backoff(peer_id, addr.clone()) {
                tracing::warn!(
                    %peer_id,
                    %addr,
                    error = %e,
                    "Failed to dial bootstrap node after retries"
                );
                bootstrap_failed += 1;
                continue;
            }

            swarm_obj
                .swarm
                .behaviour_mut()
                .add_kademlia_peer(peer_id, addr.clone());
            swarm_obj.connected_peers.insert(peer_id);
            bootstrap_success += 1;
            tracing::info!(%peer_id, %addr, "Added bootstrap node");
        }

        if bootstrap_success > 0 || bootstrap_failed > 0 {
            tracing::info!(
                success = bootstrap_success,
                failed = bootstrap_failed,
                total = config.bootstrap_nodes.len(),
                "Bootstrap node connection summary"
            );
        }

        // Dial known peers from previous sessions (bootstrap from persistence)
        let mut known_success = 0;
        let mut known_failed = 0;
        for (peer_id_str, addrs) in &config.known_addrs {
            let peer_id = match peer_id_str.parse::<PeerId>() {
                Ok(pid) => pid,
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id_str,
                        error = %e,
                        "Invalid peer ID in known_addrs, skipping"
                    );
                    continue;
                }
            };

            let mut public_addrs = Vec::new();
            for addr_str in addrs {
                // Parse with retry
                let addr = match parse_addr_with_retry(addr_str, "known peer", 3) {
                    Some(a) => a,
                    None => {
                        known_failed += 1;
                        continue;
                    }
                };

                // Skip private/internal addresses
                if is_private_addr(&addr) {
                    continue;
                }

                // Dial with backoff
                if let Err(e) = swarm_obj.dial_with_backoff(peer_id, addr.clone()) {
                    tracing::warn!(
                        %peer_id,
                        %addr,
                        error = %e,
                        "Failed to dial known peer after retries"
                    );
                    known_failed += 1;
                    continue;
                }

                swarm_obj
                    .swarm
                    .behaviour_mut()
                    .add_kademlia_peer(peer_id, addr.clone());
                public_addrs.push(addr);
                known_success += 1;
            }
            if !public_addrs.is_empty() {
                tracing::info!(%peer_id, addrs = public_addrs.len(), "Added known peer from store");
            }
        }

        if known_success > 0 || known_failed > 0 {
            tracing::info!(
                success = known_success,
                failed = known_failed,
                "Known peer connection summary"
            );
        }

        // Subscribe to topics
        swarm_obj.subscribe_topic(PRESENCE_TOPIC)?;
        swarm_obj.subscribe_topic(CHAT_TOPIC)?;
        swarm_obj.subscribe_topic(FILE_TOPIC)?;
        swarm_obj.subscribe_topic(FRAGMENT_TOPIC)?;

        Ok(swarm_obj)
    }

    pub async fn send_message(&mut self, _to: &str, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = ipmsg_protocol::codec::encode_message(msg);
        self.publish_to_topic(CHAT_TOPIC, bytes)
    }

    pub async fn broadcast_message(&mut self, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = ipmsg_protocol::codec::encode_message(msg);
        self.publish_to_topic(CHAT_TOPIC, bytes)
    }

    pub fn broadcast_presence(&mut self, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = ipmsg_protocol::codec::encode_message(msg);
        self.publish_to_topic(PRESENCE_TOPIC, bytes)
    }

    pub fn subscribe_topic(&mut self, name: &str) -> Result<(), P2PError> {
        let topic = IdentTopic::new(name);
        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        self.subscribed_topics.push(name.to_string());
        tracing::info!(topic = name, "Subscribed to topic");
        Ok(())
    }

    pub fn unsubscribe_topic(&mut self, name: &str) -> Result<(), P2PError> {
        let topic = IdentTopic::new(name);
        self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
        self.subscribed_topics.retain(|t| t != name);
        Ok(())
    }

    pub fn publish_to_topic(&mut self, topic_name: &str, data: Vec<u8>) -> Result<(), P2PError> {
        let topic = IdentTopic::new(topic_name);
        let mesh_peers = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .mesh_peers(&topic.hash());
        let mesh_count = mesh_peers.count();
        tracing::debug!(
            topic = topic_name,
            mesh_peers = mesh_count,
            "Publishing to topic"
        );
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, data)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        Ok(())
    }

    /// Send a file transfer request to a peer
    pub fn send_file_request(
        &mut self,
        peer_id: &PeerId,
        req: FileTransferRequest,
    ) -> Result<(), P2PError> {
        self.swarm
            .behaviour_mut()
            .file_transfer
            .send_request(peer_id, req);
        Ok(())
    }

    /// Send a file transfer response to a peer using the saved response channel
    pub fn send_file_transfer_response(
        &mut self,
        peer_id: &PeerId,
        resp: FileTransferResponse,
    ) -> Result<(), P2PError> {
        let pid_str = peer_id.to_base58();
        if let Some(channel) = self.pending_response_channels.remove(&pid_str) {
            self.swarm
                .behaviour_mut()
                .file_transfer
                .send_response(channel, resp)
                .map_err(|e| P2PError::Network(format!("Failed to send response: {:?}", e)))?;
            Ok(())
        } else {
            Err(P2PError::PeerNotFound(format!(
                "No pending response channel for peer {}",
                pid_str
            )))
        }
    }

    pub fn get_peers(&self) -> Vec<ConnectedPeer> {
        self.peers.values().cloned().collect()
    }

    /// Trigger a Kademlia bootstrap to refresh the DHT routing table
    pub fn bootstrap_kademlia(&mut self) {
        match self.swarm.behaviour_mut().kademlia.bootstrap() {
            Ok(query_id) => tracing::debug!(?query_id, "Kademlia bootstrap started"),
            Err(e) => tracing::warn!(%e, "Kademlia bootstrap failed"),
        }
    }

    /// Dial a peer with exponential backoff and circuit breaker protection
    pub fn dial_with_backoff(&mut self, peer_id: PeerId, addr: Multiaddr) -> Result<(), P2PError> {
        // Evict stale circuit breaker entries if over capacity
        if self.circuit_breakers.len() > crate::MAX_CIRCUIT_BREAKERS {
            self.circuit_breakers.retain(|_, entry| !entry.is_open() || entry.opened_at.map_or(true, |t| t.elapsed() < Duration::from_secs(300)));
            // If still over capacity after cleanup, remove oldest entries
            if self.circuit_breakers.len() > crate::MAX_CIRCUIT_BREAKERS {
                let excess = self.circuit_breakers.len() - crate::MAX_CIRCUIT_BREAKERS;
                let keys: Vec<PeerId> = self.circuit_breakers.keys().take(excess).copied().collect();
                for key in keys {
                    self.circuit_breakers.remove(&key);
                }
                tracing::warn!(evicted = excess, "Circuit breaker capacity reached, evicted stale entries");
            }
        }

        // Check circuit breaker state
        let breaker = self.circuit_breakers.entry(peer_id).or_insert_with(CircuitBreakerEntry::new);
        
        if breaker.is_open() {
            tracing::warn!(
                %peer_id,
                %addr,
                "Circuit breaker is open, refusing to dial"
            );
            return Err(P2PError::Transport(format!(
                "Circuit breaker open for peer {}",
                peer_id
            )));
        }

        // Attempt dial with exponential backoff
        let mut last_error = None;
        for attempt in 0..MAX_DIAL_RETRIES {
            let delay = DIAL_BACKOFF_BASE * 2u32.pow(attempt);
            
            tracing::debug!(
                %peer_id,
                %addr,
                attempt = attempt + 1,
                max_attempts = MAX_DIAL_RETRIES,
                delay_secs = delay.as_secs(),
                "Attempting to dial peer"
            );

            match self.swarm.dial(addr.clone()) {
                Ok(_) => {
                    // Record success in circuit breaker
                    if let Some(breaker) = self.circuit_breakers.get_mut(&peer_id) {
                        breaker.record_success();
                    }
                    tracing::info!(%peer_id, %addr, "Successfully dialed peer");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        %peer_id,
                        %addr,
                        attempt = attempt + 1,
                        error = %e,
                        "Dial attempt failed"
                    );
                    last_error = Some(e);
                    
                    // Don't sleep on the last attempt
                    if attempt < MAX_DIAL_RETRIES - 1 {
                        std::thread::sleep(delay);
                    }
                }
            }
        }

        // All attempts failed, record failure in circuit breaker
        if let Some(breaker) = self.circuit_breakers.get_mut(&peer_id) {
            breaker.record_failure();
        }

        let error_msg = last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "Unknown dial error".to_string());
        
        tracing::error!(
            %peer_id,
            %addr,
            attempts = MAX_DIAL_RETRIES,
            "All dial attempts failed"
        );
        
        Err(P2PError::Transport(format!(
            "Failed to dial peer {} after {} attempts: {}",
            peer_id, MAX_DIAL_RETRIES, error_msg
        )))
    }

    fn on_gossipsub_message(&mut self, msg: &gossipsub::Message) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        let topic = msg.topic.as_str();
        tracing::debug!(
            topic = topic,
            data_len = msg.data.len(),
            "Received gossipsub message"
        );

        if topic != CHAT_TOPIC
            && topic != PRESENCE_TOPIC
            && topic != FRAGMENT_TOPIC
            && topic != FILE_TOPIC
        {
            return events;
        }

        match decode_message(&msg.data) {
            Ok(chat_msg) => {
                if topic == PRESENCE_TOPIC {
                    if let ipmsg_protocol::message::MessageType::Presence {
                        username,
                        platforms,
                        ..
                    } = &chat_msg.kind
                    {
                        let peer = ConnectedPeer {
                            peer_id: chat_msg.from.clone(),
                            username: username.clone(),
                            platforms: platforms.clone(),
                            last_seen: chrono::Utc::now(),
                        };
                        self.peers.insert(chat_msg.from.clone(), peer);
                        events.push(P2PEvent::PeerJoined {
                            peer_id: chat_msg.from.clone(),
                            username: username.clone(),
                            platforms: platforms.clone(),
                        });
                    }
                } else if topic == FRAGMENT_TOPIC {
                    // Handle fragment messages - decode as FragmentMsg
                    if let Ok(fragment) =
                        serde_cbor::from_slice::<crate::fragment::FragmentMsg>(&msg.data)
                    {
                        // Store fragment for later processing by fragment manager
                        events.push(P2PEvent::FragmentReceived { fragment });
                    }
                } else if topic == FILE_TOPIC {
                    // Handle file sharing messages
                    match &chat_msg.kind {
                        ipmsg_protocol::message::MessageType::FileShareAnnounce { shares } => {
                            events.push(P2PEvent::FileShareAnnounce {
                                from: chat_msg.from.clone(),
                                shares: shares.clone(),
                            });
                        }
                        ipmsg_protocol::message::MessageType::FileShareQuery { query, tags } => {
                            events.push(P2PEvent::FileSearchQuery {
                                from: chat_msg.from.clone(),
                                query: query.clone(),
                                tags: tags.clone(),
                            });
                        }
                        ipmsg_protocol::message::MessageType::FileShareResponse {
                            results, ..
                        } => {
                            events.push(P2PEvent::FileSearchResponse {
                                from: chat_msg.from.clone(),
                                results: results.clone(),
                            });
                        }
                        _ => {
                            // Other message types on FILE_TOPIC are ignored
                        }
                    }
                } else {
                    let _ = self.store.save_message(&chat_msg);
                    events.push(P2PEvent::MessageReceived(chat_msg));
                }
            }
            Err(e) => {
                tracing::warn!(%e, "Failed to decode gossipsub message");
            }
        }
        events
    }

    fn on_identify_received(&mut self, info: &identify::Info) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        let pid_str = info.public_key.to_peer_id().to_base58();
        let (username, platforms) =
            parse_agent_version(&info.agent_version).unwrap_or_else(|| (String::new(), Vec::new()));

        // Log supported protocols for debugging relay support
        tracing::info!(
            peer = %pid_str,
            protocols = ?info.protocols,
            "Identify received"
        );

        // Check if peer supports relay SERVER protocol (not just client)
        // Relay server protocol: /libp2p/circuit/relay/0.2.0/stop
        // Relay client protocol: /libp2p/circuit/relay/0.2.0/hop
        // We need the server protocol to request reservation
        let supports_relay_server = info.protocols.iter().any(|p| {
            let proto = p.to_string();
            // Only check for "stop" which indicates relay server capability
            proto.contains("circuit/relay") && proto.contains("stop")
        });

        // Log all protocols for debugging
        let relay_protocols: Vec<_> = info
            .protocols
            .iter()
            .filter(|p| p.to_string().contains("circuit") || p.to_string().contains("relay"))
            .map(|p| p.to_string())
            .collect();

        tracing::info!(
            peer = %pid_str,
            supports_relay_server,
            relay_protocols = ?relay_protocols,
            all_protocols_count = info.protocols.len(),
            "🔍 Peer relay protocol check"
        );

        // If peer supports relay server, trigger reservation using the stored connection address
        if supports_relay_server {
            let peer_id = info.public_key.to_peer_id();

            tracing::info!(
                peer = %pid_str,
                stored_addrs_count = self.relay_node_addrs.get(&peer_id).map(|v| v.len()).unwrap_or(0),
                "🎯 Peer supports relay server, attempting reservation"
            );

            // Use the stored relay node address from ConnectionEstablished
            if let Some(relay_addrs) = self.relay_node_addrs.get(&peer_id) {
                if let Some(relay_addr_base) = relay_addrs.first() {
                    // Build proper relay address: /relay_node_addr/p2p/relay_peer_id/p2p-circuit
                    let mut relay_addr = relay_addr_base.clone();

                    // Check if address already contains /p2p/<peer_id>
                    let has_p2p = relay_addr
                        .iter()
                        .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_)));

                    if !has_p2p {
                        relay_addr.push(libp2p::multiaddr::Protocol::P2p(peer_id));
                    }

                    // Add p2p-circuit protocol to request reservation
                    relay_addr.push(libp2p::multiaddr::Protocol::P2pCircuit);

                    tracing::info!(
                        peer = %pid_str,
                        relay_addr = %relay_addr,
                        "🔗 Attempting relay reservation via stored relay node address"
                    );

                    match self.swarm.listen_on(relay_addr.clone()) {
                        Ok(id) => {
                            tracing::info!(
                                peer = %pid_str,
                                listener_id = ?id,
                                "✅ Relay listen initiated, reservation will be requested"
                            );
                            events.push(P2PEvent::Status(format!(
                                "Relay reservation requested for {}",
                                &pid_str[..8]
                            )));
                        }
                        Err(e) => {
                            tracing::error!(
                                peer = %pid_str,
                                error = ?e,
                                "❌ Failed to initiate relay listen"
                            );
                        }
                    }
                } else {
                    tracing::warn!(
                        peer = %pid_str,
                        "⚠️ Peer supports relay but stored address list is empty"
                    );
                }
            } else {
                tracing::warn!(
                    peer = %pid_str,
                    "⚠️ Peer supports relay but no stored address (not connected via relay?)"
                );
            }
        } else {
            tracing::debug!(
                peer = %pid_str,
                "Peer does not support relay server protocol"
            );
        }

        let peer = ConnectedPeer {
            peer_id: pid_str.clone(),
            username: username.clone(),
            platforms: platforms.clone(),
            last_seen: chrono::Utc::now(),
        };

        self.save_peer(&peer, info.public_key.encode_protobuf());

        let is_new = !self.peers.contains_key(&pid_str);
        self.peers.insert(pid_str.clone(), peer);
        // connected_peers is already set in ConnectionEstablished to prevent dial loops

        if is_new {
            events.push(P2PEvent::PeerJoined {
                peer_id: pid_str.clone(),
                username,
                platforms,
            });
        }

        // Collect peer's listen addresses for bootstrap persistence
        let addrs: Vec<String> = info.listen_addrs.iter().map(|a| a.to_string()).collect();
        if !addrs.is_empty() {
            // Add only externally reachable addresses to Kademlia routing table
            // Filter out private/internal addresses using the existing helper
            for addr in &info.listen_addrs {
                if is_private_addr(addr) {
                    continue;
                }
                self.swarm
                    .behaviour_mut()
                    .add_kademlia_peer(info.public_key.to_peer_id(), addr.clone());
            }
            events.push(P2PEvent::PeerAddressesDiscovered {
                peer_id: pid_str,
                addrs,
            });
        }

        events
    }

    fn on_mdns_discovered(&mut self, peer_id: &PeerId, addr: &Multiaddr) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        if self.connected_peers.contains(peer_id) {
            return events;
        }

        self.connected_peers.insert(*peer_id);
        let pid_str = peer_id.to_base58();
        tracing::info!(%peer_id, "mDNS discovered peer");

        self.swarm
            .behaviour_mut()
            .add_kademlia_peer(*peer_id, addr.clone());
        let _ = self.swarm.dial(*peer_id);

        events.push(P2PEvent::PeerJoined {
            peer_id: pid_str,
            username: String::new(),
            platforms: Vec::new(),
        });
        events
    }

    fn on_mdns_expired(&mut self, peer_id: &PeerId) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        self.connected_peers.remove(peer_id);
        let pid_str = peer_id.to_base58();
        if self.peers.remove(&pid_str).is_some() {
            events.push(P2PEvent::PeerLeft { peer_id: pid_str });
        }
        events
    }

    fn on_file_transfer_request(
        &mut self,
        peer_id: &PeerId,
        request: FileTransferRequest,
        channel: request_response::ResponseChannel<FileTransferResponse>,
    ) -> Vec<P2PEvent> {
        tracing::info!(peer = %peer_id, ?request, "Received file transfer request");

        // Save the response channel keyed by peer_id
        self.pending_response_channels
            .insert(peer_id.to_base58(), channel);

        // Emit event for engine to handle
        let event = P2PEvent::FileTransferRequestReceived {
            from: peer_id.to_base58(),
            request,
        };

        vec![event]
    }

    fn on_file_transfer_response(
        &mut self,
        peer_id: &PeerId,
        response: FileTransferResponse,
    ) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        let pid_str = peer_id.to_base58();
        events.push(P2PEvent::FileTransferResponse {
            from: pid_str,
            response,
        });
        events
    }

    fn save_peer(&self, peer: &ConnectedPeer, public_key: Vec<u8>) {
        let platforms_json = serde_json::to_string(&peer.platforms).unwrap_or_default();
        let _ = self.store.upsert_peer(&StoredPeerInfo {
            peer_id: peer.peer_id.clone(),
            username: peer.username.clone(),
            public_key,
            platforms: platforms_json,
            last_seen: peer.last_seen,
            first_seen: peer.last_seen,
        });
    }

    /// Drain gossipsub/identify/mdns events from sub-behaviours after swarm polling
    fn drain_behaviour_events(&mut self) -> Vec<P2PEvent> {
        let mut events = Vec::new();

        // Poll gossipsub for messages
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            if let gossipsub::Event::Message { message, .. } = evt {
                let new = self.on_gossipsub_message(&message);
                events.extend(new);
            }
        }

        // Poll identify
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .identify
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            if let IdentifyEvent::Received { info, .. } = evt {
                let new = self.on_identify_received(&info);
                events.extend(new);
            }
        }

        // Poll mdns
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .mdns
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            match evt {
                MdnsEvent::Discovered(peers) => {
                    for (peer_id, addr) in peers {
                        let new = self.on_mdns_discovered(&peer_id, &addr);
                        events.extend(new);
                    }
                }
                MdnsEvent::Expired(peers) => {
                    for (peer_id, _addr) in peers {
                        let new = self.on_mdns_expired(&peer_id);
                        events.extend(new);
                    }
                }
            }
        }

        // Poll file_transfer request-response
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .file_transfer
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            if let RequestResponseEvent::Message { peer, message, .. } = evt {
                match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => {
                        // Handle incoming file transfer request
                        let new = self.on_file_transfer_request(&peer, request, channel);
                        events.extend(new);
                    }
                    request_response::Message::Response { response, .. } => {
                        // Handle incoming file transfer response
                        let new = self.on_file_transfer_response(&peer, response);
                        events.extend(new);
                    }
                }
            }
        }

        // Relay events are handled in SwarmEvent::Behaviour via IpMsgNetBehaviourEvent::Relay

        // Poll relay server events
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .relay_server
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            match evt {
                relay::Event::ReservationReqAccepted {
                    src_peer_id,
                    renewed,
                } => {
                    tracing::info!(%src_peer_id, renewed, "Relay server: reservation accepted");
                    events.push(P2PEvent::Status(format!(
                        "Relay server: reservation accepted for {}",
                        &src_peer_id.to_base58()[..8]
                    )));
                }
                relay::Event::ReservationReqDenied { src_peer_id } => {
                    tracing::info!(%src_peer_id, "Relay server: reservation denied");
                }
                relay::Event::ReservationTimedOut { src_peer_id } => {
                    tracing::info!(%src_peer_id, "Relay server: reservation timed out");
                }
                relay::Event::CircuitReqAccepted {
                    src_peer_id,
                    dst_peer_id,
                    ..
                } => {
                    tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit accepted");
                    events.push(P2PEvent::Status(format!(
                        "Relay server: circuit {} -> {}",
                        &src_peer_id.to_base58()[..8],
                        &dst_peer_id.to_base58()[..8]
                    )));
                }
                relay::Event::CircuitReqDenied {
                    src_peer_id,
                    dst_peer_id,
                    ..
                } => {
                    tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit denied");
                }
                relay::Event::CircuitClosed {
                    src_peer_id,
                    dst_peer_id,
                    ..
                } => {
                    tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit closed");
                }
                _ => {}
            }
        }

        // Poll dcutr events (hole punching)
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .dcutr
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            let dcutr::Event {
                remote_peer_id,
                result,
            } = evt;
            match result {
                Ok(connection_id) => {
                    tracing::info!(%remote_peer_id, ?connection_id, "DCUtR: Direct connection upgrade succeeded");
                    events.push(P2PEvent::Status(format!(
                        "Hole punch success with {}",
                        &remote_peer_id.to_base58()[..8]
                    )));
                }
                Err(error) => {
                    tracing::warn!(%remote_peer_id, %error, "DCUtR: Direct connection upgrade failed");
                    events.push(P2PEvent::Status(format!(
                        "Hole punch failed with {}: {}",
                        &remote_peer_id.to_base58()[..8],
                        error
                    )));
                }
            }
        }

        // Poll autonat events (NAT detection)
        while let std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) = self
            .swarm
            .behaviour_mut()
            .autonat
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ))
        {
            match evt {
                autonat::Event::StatusChanged { old, new } => {
                    tracing::info!(?old, ?new, "NAT status changed");
                    events.push(P2PEvent::Status(format!("NAT status: {:?}", new)));
                }
                autonat::Event::InboundProbe(event) => {
                    tracing::debug!(?event, "AutoNAT inbound probe");
                }
                autonat::Event::OutboundProbe(event) => {
                    tracing::debug!(?event, "AutoNAT outbound probe");
                }
            }
        }

        events
    }
}

impl futures::Stream for P2PSwarm {
    type Item = Vec<P2PEvent>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut events = Vec::new();

        loop {
            match self.swarm.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(swarm_event)) => {
                    match &swarm_event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            tracing::info!("Listening on {}", address);
                            events.push(P2PEvent::Status(format!("listening on {}", address)));
                        }
                        SwarmEvent::NewExternalAddrCandidate { address } => {
                            tracing::info!("External address candidate: {}", address);
                            // Confirm the external address to enable hole punching
                            self.swarm.add_external_address(address.clone());
                            events.push(P2PEvent::ExternalAddress(address.to_string()));
                        }
                        SwarmEvent::ExternalAddrConfirmed { address } => {
                            tracing::info!("External address confirmed: {}", address);
                            events.push(P2PEvent::ExternalAddress(address.to_string()));
                        }
                        SwarmEvent::ConnectionEstablished {
                            peer_id, endpoint, ..
                        } => {
                            tracing::info!("Connected to {}", peer_id);
                            // Mark as connected immediately to prevent RoutingUpdated dial loops
                            self.connected_peers.insert(*peer_id);

                            // Store relay node addresses for later relay address construction
                            if let libp2p::core::ConnectedPoint::Dialer { address, .. } = endpoint {
                                // Ensure address includes peer_id for proper relay address construction
                                let full_addr = if address.iter().any(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_))) {
                                    address.clone()
                                } else {
                                    address.clone().with(libp2p::multiaddr::Protocol::P2p(*peer_id))
                                };
                                
                                tracing::info!(
                                    peer = %peer_id,
                                    address = %full_addr,
                                    "📝 Storing relay node address for potential reservation"
                                );
                                self.relay_node_addrs
                                    .entry(*peer_id)
                                    .or_default()
                                    .push(full_addr);
                            } else if let libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } = endpoint {
                                // For incoming connections, use the send_back_addr which contains the peer's address
                                let full_addr = if send_back_addr.iter().any(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_))) {
                                    send_back_addr.clone()
                                } else {
                                    send_back_addr.clone().with(libp2p::multiaddr::Protocol::P2p(*peer_id))
                                };
                                
                                tracing::info!(
                                    peer = %peer_id,
                                    address = %full_addr,
                                    "📝 Storing incoming connection address for potential relay reservation"
                                );
                                self.relay_node_addrs
                                    .entry(*peer_id)
                                    .or_default()
                                    .push(full_addr);
                            }

                            // Don't add addresses to Kademlia here — wait for Identify
                            // to get the peer's actual listen addresses (which are filtered
                            // for private IPs in on_identify_received).
                            // Trigger Kademlia bootstrap to refresh routing table
                            let _ = self.swarm.behaviour_mut().kademlia.bootstrap();

                            // Note: Relay reservation is triggered in on_identify_received
                            // after confirming the peer supports relay server protocol
                        }
                        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                            tracing::info!("Disconnected from {}: {:?}", peer_id, cause);
                            let pid_str = peer_id.to_base58();
                            self.peers.remove(&pid_str);
                            events.push(P2PEvent::PeerLeft { peer_id: pid_str });
                        }
                        SwarmEvent::Behaviour(behaviour_evt) => {
                            // Handle Identify events directly from the swarm event
                            if let IpMsgNetBehaviourEvent::Identify(identify::Event::Received {
                                info,
                                ..
                            }) = behaviour_evt
                            {
                                let new = self.on_identify_received(info);
                                events.extend(new);
                            }
                            // Handle mDNS events directly
                            if let IpMsgNetBehaviourEvent::Mdns(libp2p::mdns::Event::Discovered(
                                peers,
                            )) = behaviour_evt
                            {
                                for (peer_id, addr) in peers {
                                    let new = self.on_mdns_discovered(peer_id, addr);
                                    events.extend(new);
                                }
                            }
                            if let IpMsgNetBehaviourEvent::Mdns(libp2p::mdns::Event::Expired(
                                peers,
                            )) = behaviour_evt
                            {
                                for (peer_id, _addr) in peers {
                                    let new = self.on_mdns_expired(peer_id);
                                    events.extend(new);
                                }
                            }
                            // Handle Kademlia events - discover and dial new peers
                            if let IpMsgNetBehaviourEvent::Kademlia(kad_evt) = behaviour_evt {
                                match kad_evt {
                                    libp2p::kad::Event::RoutingUpdated {
                                        peer, addresses, ..
                                    } if !self.connected_peers.contains(peer) => {
                                        let public_addrs: Vec<_> = addresses
                                            .iter()
                                            .filter(|a| !is_private_addr(a))
                                            .collect();
                                        if !public_addrs.is_empty() {
                                            tracing::info!(%peer, addrs = public_addrs.len(), "Kademlia discovered new peer, dialing");
                                            for addr in public_addrs {
                                                let _ = self.swarm.dial(addr.clone());
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            // Handle relay client events (reservation accepted, circuits)
                            // Note: libp2p automatically handles relay listening after reservation,
                            // we only need to log the events
                            if let IpMsgNetBehaviourEvent::Relay(relay_evt) = behaviour_evt {
                                match relay_evt {
                                    relay::client::Event::ReservationReqAccepted {
                                        relay_peer_id,
                                        renewal,
                                        ..
                                    } => {
                                        tracing::info!(%relay_peer_id, renewal, "Relay reservation accepted");
                                        events.push(P2PEvent::Status(format!(
                                            "Relay reservation accepted by {}",
                                            &relay_peer_id.to_base58()[..8]
                                        )));
                                    }
                                    relay::client::Event::OutboundCircuitEstablished {
                                        relay_peer_id,
                                        ..
                                    } => {
                                        tracing::info!(%relay_peer_id, "Outbound circuit established");
                                        events.push(P2PEvent::Status(format!(
                                            "Relay circuit established with {}",
                                            &relay_peer_id.to_base58()[..8]
                                        )));
                                    }
                                    relay::client::Event::InboundCircuitEstablished {
                                        src_peer_id,
                                        ..
                                    } => {
                                        tracing::info!(%src_peer_id, "Inbound circuit established");
                                        events.push(P2PEvent::Status(format!(
                                            "Incoming relay circuit from {}",
                                            &src_peer_id.to_base58()[..8]
                                        )));
                                    }
                                }
                            }
                            // Handle relay server events
                            if let IpMsgNetBehaviourEvent::RelayServer(relay_evt) = behaviour_evt {
                                match relay_evt {
                                    relay::Event::ReservationReqAccepted {
                                        src_peer_id,
                                        renewed,
                                    } => {
                                        tracing::info!(%src_peer_id, renewed, "Relay server: reservation accepted");
                                    }
                                    relay::Event::ReservationReqDenied { src_peer_id } => {
                                        tracing::info!(%src_peer_id, "Relay server: reservation denied");
                                    }
                                    relay::Event::ReservationTimedOut { src_peer_id } => {
                                        tracing::info!(%src_peer_id, "Relay server: reservation timed out");
                                    }
                                    relay::Event::CircuitReqAccepted {
                                        src_peer_id,
                                        dst_peer_id,
                                        ..
                                    } => {
                                        tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit accepted");
                                    }
                                    relay::Event::CircuitReqDenied {
                                        src_peer_id,
                                        dst_peer_id,
                                        ..
                                    } => {
                                        tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit denied");
                                    }
                                    relay::Event::CircuitClosed {
                                        src_peer_id,
                                        dst_peer_id,
                                        ..
                                    } => {
                                        tracing::info!(%src_peer_id, %dst_peer_id, "Relay server: circuit closed");
                                    }
                                    _ => {}
                                }
                            }
                            // Handle DCUtR events (hole punching)
                            if let IpMsgNetBehaviourEvent::Dcutr(dcutr_evt) = behaviour_evt {
                                let dcutr::Event {
                                    remote_peer_id,
                                    result,
                                } = dcutr_evt;
                                match result {
                                    Ok(connection_id) => {
                                        tracing::info!(%remote_peer_id, ?connection_id, "DCUtR: Direct connection upgrade succeeded");
                                    }
                                    Err(error) => {
                                        tracing::warn!(%remote_peer_id, %error, "DCUtR: Direct connection upgrade failed");
                                    }
                                }
                            }
                            // Handle AutoNAT events
                            if let IpMsgNetBehaviourEvent::Autonat(autonat_evt) = behaviour_evt {
                                match autonat_evt {
                                    autonat::Event::StatusChanged { old, new } => {
                                        tracing::info!(?old, ?new, "AutoNAT status changed");
                                    }
                                    autonat::Event::InboundProbe(event) => {
                                        tracing::debug!(?event, "AutoNAT inbound probe");
                                    }
                                    autonat::Event::OutboundProbe(event) => {
                                        tracing::debug!(?event, "AutoNAT outbound probe");
                                    }
                                }
                            }
                            // Handle Gossipsub events (message delivery)
                            if let IpMsgNetBehaviourEvent::Gossipsub(gossipsub_evt) = behaviour_evt
                                && let gossipsub::Event::Message { message, .. } = gossipsub_evt
                            {
                                let new = self.on_gossipsub_message(message);
                                events.extend(new);
                            }
                            // Drain remaining behaviour events (gossipsub, file_transfer, etc.)
                            let new = self.drain_behaviour_events();
                            events.extend(new);
                        }
                        _ => {}
                    }
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => {
                    let new = self.drain_behaviour_events();
                    events.extend(new);
                    return if events.is_empty() {
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(Some(events))
                    };
                }
            }
        }
    }
}
