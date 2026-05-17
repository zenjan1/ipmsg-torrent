use libp2p::PeerId;

/// Default bootstrap nodes for the IPMsg-Torrent network
/// These are well-known peers that new nodes connect to initially
pub const DEFAULT_BOOTSTRAP_NODES: &[&str] = &[
    // Format: /ip4/{addr}/udp/{port}/quic-v1/p2p/{peer_id}
    // TODO: Replace with actual bootstrap node addresses
    // /ip4/1.2.3.4/udp/4001/quic-v1/p2p/12D3KooW...,
    // /ip4/5.6.7.8/udp/4001/quic-v1/p2p/12D3KooW...,
];

/// LAN multicast address for mDNS discovery
pub const MDNS_SERVICE_NAME: &str = "_ipmsg._udp.local.";

/// Gossipsub topic for presence announcements
pub const PRESENCE_TOPIC: &str = "ipmsg-presence-v1";

/// Gossipsub topic for chat messages
pub const CHAT_TOPIC: &str = "ipmsg-chat-v1";

/// Gossipsub topic for file transfer metadata
pub const FILE_TOPIC: &str = "ipmsg-files-v1";

/// Kademlia protocol name
pub const KADEMLIA_PROTOCOL: &str = "/ipmsg/kad/1.0.0";

/// Parse a bootstrap node address string
pub fn parse_bootstrap_addr(addr: &str) -> Option<(PeerId, libp2p::Multiaddr)> {
    let multiaddr: libp2p::Multiaddr = addr.parse().ok()?;

    // Extract peer ID from the multiaddr
    let peer_id = multiaddr.iter().find_map(|protocol| {
        if let libp2p::multiaddr::Protocol::P2p(pid) = protocol {
            Some(pid)
        } else {
            None
        }
    })?;

    Some((peer_id, multiaddr))
}

/// Create the list of bootstrap addresses from strings
pub fn bootstrap_addresses(addrs: &[String]) -> Vec<(PeerId, libp2p::Multiaddr)> {
    addrs
        .iter()
        .filter_map(|addr| parse_bootstrap_addr(addr))
        .collect()
}

/// Default list of bootstrap addresses
pub fn default_bootstrap_addrs() -> Vec<String> {
    DEFAULT_BOOTSTRAP_NODES.iter().map(|s| s.to_string()).collect()
}
