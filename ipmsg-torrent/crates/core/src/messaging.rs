use libp2p::gossipsub::{self, IdentTopic, MessageAuthenticity};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{Behaviour as Kademlia, Config as KademliaConfig};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{identify, PeerId, StreamProtocol};
use std::time::Duration;

/// Gossipsub topic for presence announcements
pub const PRESENCE_TOPIC: &str = "ipmsg-presence-v1";
/// Gossipsub topic for chat messages
pub const CHAT_TOPIC: &str = "ipmsg-chat-v1";
/// Gossipsub topic for file transfers
pub const FILE_TOPIC: &str = "ipmsg-files-v1";

/// Channel topic builder
pub fn channel_topic(channel: &str) -> IdentTopic {
    IdentTopic::new(format!("ipmsg-chan-{}", channel))
}

/// Peer tracking info
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub username: String,
    pub platforms: Vec<String>,
    pub bio: Option<String>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
    pub msg_seq: u64, // Last seen message sequence from this peer
}

/// The main libp2p NetworkBehaviour combining all sub-protocols
#[derive(NetworkBehaviour)]
pub struct IpMsgBehaviour {
    pub kademlia: Kademlia<MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
}

impl IpMsgBehaviour {
    pub fn new(
        local_key: &libp2p::identity::Keypair,
        username: &str,
        platforms: &[String],
    ) -> Self {
        let local_peer_id = PeerId::from(local_key.public());

        // Kademlia setup
        let mut kademlia_config = KademliaConfig::new(
            StreamProtocol::new("/ipmsg/kad/1.0.0"),
        );
        kademlia_config.set_query_timeout(Duration::from_secs(60));
        let kademlia_store = MemoryStore::new(local_peer_id);
        let mut kademlia = Kademlia::new(local_peer_id, kademlia_store);
        kademlia.set_mode(Some(libp2p::kad::Mode::Client));

        // Gossipsub setup
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .build()
            .expect("valid gossipsub config");
        let gossipsub = gossipsub::Behaviour::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
        .expect("valid gossipsub");

        // Identify setup
        let identify_config = identify::Config::new(
            "ipmsg/1.0.0".to_string(),
            local_key.public(),
        )
        .with_agent_version(format!(
            "ipmsg/2.0.0 ({}, {})",
            username,
            platforms.join(", ")
        ));
        let identify = identify::Behaviour::new(identify_config);

        Self {
            kademlia,
            gossipsub,
            identify,
        }
    }

    /// Add a peer to Kademlia routing table
    pub fn add_kademlia_peer(&mut self, peer_id: PeerId, addr: libp2p::Multiaddr) {
        self.kademlia.add_address(&peer_id, addr);
    }

    /// Publish a message to a gossipsub topic
    pub fn publish_message(
        &mut self,
        topic: IdentTopic,
        data: Vec<u8>,
    ) -> Result<gossipsub::MessageId, gossipsub::PublishError> {
        self.gossipsub.publish(topic, data)
    }

    /// Subscribe to a gossipsub topic
    pub fn subscribe(&mut self, topic: &IdentTopic) -> Result<bool, gossipsub::SubscriptionError> {
        self.gossipsub.subscribe(topic)
    }

    /// Unsubscribe from a gossipsub topic
    pub fn unsubscribe(&mut self, topic: &IdentTopic) -> bool {
        self.gossipsub.unsubscribe(topic)
    }
}

/// Parse identify events to extract peer metadata
pub fn parse_agent_version(agent_version: &str) -> Option<(String, Vec<String>)> {
    // Format: "ipmsg/2.0.0 (username, platform1, platform2)"
    let content = agent_version.strip_prefix("ipmsg/2.0.0 (")?;
    let content = content.strip_suffix(')')?;
    let parts: Vec<&str> = content.splitn(2, ", ").collect();
    if parts.len() == 2 {
        let username = parts[0].to_string();
        let platforms: Vec<String> = parts[1].split(", ").map(|s| s.to_string()).collect();
        Some((username, platforms))
    } else {
        None
    }
}
