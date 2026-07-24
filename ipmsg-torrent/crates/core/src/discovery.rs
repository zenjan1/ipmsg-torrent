use libp2p::PeerId;

/// Default bootstrap nodes for the IPMsg-Torrent network
/// These are well-known peers that new nodes connect to initially
///
/// On a LAN, mDNS discovery (see `MDNS_SERVICE_NAME`) handles peer discovery automatically.
/// For WAN/remote peers, supply your own relay node via `--bootstrap` flag, e.g.:
///   ipmsg --bootstrap "/ip4/1.2.3.4/udp/4001/quic-v1/p2p/12D3KooW..."
pub const DEFAULT_BOOTSTRAP_NODES: &[&str] = &[
    // Format: /ip4/{addr}/udp/{port}/quic-v1/p2p/{peer_id}
    // TODO: Deploy a public bootstrap relay node and add its address here
    // /ip4/1.2.3.4/udp/4001/quic-v1/p2p/12D3KooW...,
    // /ip4/5.6.7.8/udp/4001/quic-v1/p2p/12D3KooW...,
];

/// Interval between periodic Kademlia bootstrap calls (seconds)
pub const BOOTSTRAP_INTERVAL_SECS: u64 = 300;

/// Maximum number of known peer addresses to persist
pub const MAX_KNOWN_ADDRS: usize = 200;

/// How long to keep known peer addresses (days)
pub const KNOWN_ADDR_MAX_AGE_DAYS: i64 = 7;

/// LAN multicast address for mDNS discovery
pub const MDNS_SERVICE_NAME: &str = "_ipmsg._udp.local.";

/// Kademlia protocol name
pub const KADEMLIA_PROTOCOL: &str = "/ipmsg/kad/1.0.0";

// Topic constants are defined in crate::messaging

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
    DEFAULT_BOOTSTRAP_NODES
        .iter()
        .map(|s| s.to_string())
        .collect()
}
