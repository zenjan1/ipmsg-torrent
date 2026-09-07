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

#[cfg(test)]
mod tests {
    use super::*;

    // A valid multiaddr with a peer ID for testing
    fn valid_bootstrap_addr() -> String {
        // Use a known valid PeerId format (identity hash)
        let peer_id = PeerId::random();
        format!("/ip4/127.0.0.1/udp/4001/quic-v1/p2p/{}", peer_id)
    }

    #[test]
    fn test_parse_bootstrap_addr_valid() {
        let addr = valid_bootstrap_addr();
        let result = parse_bootstrap_addr(&addr);
        assert!(result.is_some());
        let (peer_id, multiaddr) = result.unwrap();
        assert!(multiaddr.to_string().contains("127.0.0.1"));
        assert!(!peer_id.to_string().is_empty());
    }

    #[test]
    fn test_parse_bootstrap_addr_invalid_string() {
        let result = parse_bootstrap_addr("not a valid address");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_bootstrap_addr_no_peer_id() {
        let result = parse_bootstrap_addr("/ip4/127.0.0.1/udp/4001/quic-v1");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_bootstrap_addr_empty() {
        let result = parse_bootstrap_addr("");
        assert!(result.is_none());
    }

    #[test]
    fn test_bootstrap_addresses_valid_list() {
        let addr1 = valid_bootstrap_addr();
        let addr2 = valid_bootstrap_addr();
        let addrs = vec![addr1, addr2];
        let result = bootstrap_addresses(&addrs);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_bootstrap_addresses_mixed_valid_invalid() {
        let valid = valid_bootstrap_addr();
        let addrs = vec![valid, "invalid".to_string()];
        let result = bootstrap_addresses(&addrs);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_bootstrap_addresses_empty() {
        let addrs: Vec<String> = vec![];
        let result = bootstrap_addresses(&addrs);
        assert!(result.is_empty());
    }

    #[test]
    fn test_default_bootstrap_addrs() {
        let addrs = default_bootstrap_addrs();
        // Currently empty (TODO in source)
        assert!(addrs.is_empty() || addrs.len() == DEFAULT_BOOTSTRAP_NODES.len());
    }

    #[test]
    fn test_constants_values() {
        assert_eq!(BOOTSTRAP_INTERVAL_SECS, 300);
        assert_eq!(MAX_KNOWN_ADDRS, 200);
        assert_eq!(KNOWN_ADDR_MAX_AGE_DAYS, 7);
        assert_eq!(MDNS_SERVICE_NAME, "_ipmsg._udp.local.");
        assert_eq!(KADEMLIA_PROTOCOL, "/ipmsg/kad/1.0.0");
    }

    #[test]
    fn test_parse_bootstrap_addr_tcp() {
        let peer_id = PeerId::random();
        let addr = format!("/ip4/192.168.1.1/tcp/8080/p2p/{}", peer_id);
        let result = parse_bootstrap_addr(&addr);
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_bootstrap_addr_ipv6() {
        let peer_id = PeerId::random();
        let addr = format!("/ip6/::1/udp/4001/quic-v1/p2p/{}", peer_id);
        let result = parse_bootstrap_addr(&addr);
        assert!(result.is_some());
    }

    #[test]
    fn test_bootstrap_addresses_dedup() {
        let addr = valid_bootstrap_addr();
        let addrs = vec![addr.clone(), addr];
        let result = bootstrap_addresses(&addrs);
        // Both parse successfully (no dedup in bootstrap_addresses)
        assert_eq!(result.len(), 2);
    }
}
