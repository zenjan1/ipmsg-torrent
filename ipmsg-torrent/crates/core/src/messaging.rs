use ipmsg_protocol::message::ChannelId;
use libp2p::PeerId;

/// Gossipsub topic for presence announcements
pub const PRESENCE_TOPIC: &str = "ipmsg-presence-v1";
/// Gossipsub topic for chat messages
pub const CHAT_TOPIC: &str = "ipmsg-chat-v1";
/// Gossipsub topic for file transfers
pub const FILE_TOPIC: &str = "ipmsg-files-v1";

/// Channel topic builder
pub fn channel_topic(channel: &str) -> String {
    format!("ipmsg-chan-{}", channel)
}

/// Peer tracking info
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub username: String,
    pub platforms: Vec<String>,
    pub bio: Option<String>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
    pub msg_seq: u64,
}

/// Parse identify agent version to extract username and platforms
pub fn parse_agent_version(agent_version: &str) -> Option<(String, Vec<String>)> {
    // Format: "ipmsg/2.1.0 (username, platform1, platform2)"
    // Try v2.1.0 format
    for prefix in &["ipmsg/2.1.0 (", "ipmsg/2.0.0 (", "ipmsg/1.0.0 ("] {
        if let Some(content) = agent_version.strip_prefix(prefix) {
            if let Some(content) = content.strip_suffix(')') {
                let parts: Vec<&str> = content.splitn(2, ", ").collect();
                if parts.len() == 2 {
                    let username = parts[0].to_string();
                    let platforms: Vec<String> = parts[1].split(", ").map(|s| s.to_string()).collect();
                    return Some((username, platforms));
                }
            }
        }
    }
    None
}
