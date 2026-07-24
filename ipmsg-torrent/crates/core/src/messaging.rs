use libp2p::PeerId;

/// Gossipsub topic for presence announcements
pub const PRESENCE_TOPIC: &str = "ipmsg-presence-v1";
/// Gossipsub topic for chat messages
pub const CHAT_TOPIC: &str = "ipmsg-chat-v1";
/// Gossipsub topic for file transfers
pub const FILE_TOPIC: &str = "ipmsg-files-v1";

/// Gossipsub topic for message fragments
pub const FRAGMENT_TOPIC: &str = "ipmsg-fragments-v1";

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
        if let Some(content) = agent_version.strip_prefix(prefix)
            && let Some(content) = content.strip_suffix(')')
        {
            let parts: Vec<&str> = content.splitn(2, ", ").collect();
            if parts.len() == 2 {
                let username = parts[0].to_string();
                let platforms: Vec<String> = parts[1].split(", ").map(|s| s.to_string()).collect();
                return Some((username, platforms));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_agent_version_v210() {
        let agent = "ipmsg/2.1.0 (alice, rust, linux)";
        let (username, platforms) = parse_agent_version(agent).unwrap();
        assert_eq!(username, "alice");
        assert_eq!(platforms, vec!["rust".to_string(), "linux".to_string()]);
    }

    #[test]
    fn test_parse_agent_version_v200() {
        let agent = "ipmsg/2.0.0 (bob, rust)";
        let (username, platforms) = parse_agent_version(agent).unwrap();
        assert_eq!(username, "bob");
        assert_eq!(platforms, vec!["rust".to_string()]);
    }

    #[test]
    fn test_parse_agent_version_v100() {
        let agent = "ipmsg/1.0.0 (charlie, rust, windows, macos)";
        let (username, platforms) = parse_agent_version(agent).unwrap();
        assert_eq!(username, "charlie");
        assert_eq!(platforms.len(), 3);
    }

    #[test]
    fn test_parse_agent_version_invalid_format() {
        assert!(parse_agent_version("random string").is_none());
        assert!(parse_agent_version("ipmsg/3.0.0 (alice, rust)").is_none());
        assert!(parse_agent_version("ipmsg/2.1.0 alice, rust)").is_none());
        assert!(parse_agent_version("ipmsg/2.1.0 (alice, rust").is_none());
    }

    #[test]
    fn test_parse_agent_version_no_platform() {
        // If there's no ", " separator, should return None
        let agent = "ipmsg/2.1.0 (alice)";
        assert!(parse_agent_version(agent).is_none());
    }

    #[test]
    fn test_channel_topic() {
        let topic = channel_topic("devs");
        assert_eq!(topic, "ipmsg-chan-devs");
    }

    #[test]
    fn test_topic_constants() {
        assert_eq!(PRESENCE_TOPIC, "ipmsg-presence-v1");
        assert_eq!(CHAT_TOPIC, "ipmsg-chat-v1");
        assert_eq!(FILE_TOPIC, "ipmsg-files-v1");
        assert_eq!(FRAGMENT_TOPIC, "ipmsg-fragments-v1");
    }
}
