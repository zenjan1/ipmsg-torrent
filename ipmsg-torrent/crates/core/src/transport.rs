use crate::identity::Identity;
use crate::messaging::{CHAT_TOPIC, FILE_TOPIC, PRESENCE_TOPIC};
use crate::{P2PError, P2PEvent};
use libp2p::gossipsub::{self, IdentTopic, MessageAuthenticity};
use libp2p::swarm::Swarm;
use libp2p::{Multiaddr, PeerId, Transport};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

pub type P2PSwarm = Swarm<gossipsub::Behaviour>;

pub async fn create_swarm(
    identity: &Identity,
    username: &str,
    platforms: &[String],
    _event_tx: &UnboundedSender<P2PEvent>,
    bootstrap_nodes: Vec<String>,
    _data_dir: &Path,
) -> Result<P2PSwarm, P2PError> {
    let keypair = identity.to_keypair();

    let _agent_version = format!(
        "ipmsg/2.0.0 ({}, {})",
        username,
        platforms.join(", ")
    );

    let gs_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(30))
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .history_length(50)
        .history_gossip(25)
        .build()
        .expect("valid gossipsub config");

    let gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gs_config,
    )
    .expect("valid gossipsub");

    let peer_id = PeerId::from(keypair.public());

    let transport = libp2p::tcp::tokio::Transport::new(
        libp2p::tcp::Config::default()
    )
    .upgrade(libp2p::core::upgrade::Version::V1)
    .authenticate(libp2p::noise::Config::new(&keypair).unwrap())
    .multiplex(libp2p::yamux::Config::default())
    .boxed();

    let mut swarm = libp2p::Swarm::new(transport, gossipsub, peer_id, libp2p::swarm::Config::with_tokio_executor());

    for addr_str in &bootstrap_nodes {
        if let Ok(addr) = addr_str.parse::<Multiaddr>() {
            if let Some(peer_id) = addr.iter().find_map(|p| match p {
                libp2p::multiaddr::Protocol::P2p(pid) => Some(pid),
                _ => None,
            }) {
                let _ = swarm.dial(addr.clone());
                tracing::info!(%peer_id, %addr, "Added bootstrap node");
            }
        }
    }

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();

    let topic_presence = IdentTopic::new(PRESENCE_TOPIC);
    let topic_chat = IdentTopic::new(CHAT_TOPIC);
    let topic_file = IdentTopic::new(FILE_TOPIC);

    swarm.behaviour_mut().subscribe(&topic_presence).unwrap();
    swarm.behaviour_mut().subscribe(&topic_chat).unwrap();
    swarm.behaviour_mut().subscribe(&topic_file).unwrap();

    Ok(swarm)
}

pub fn publish_message(swarm: &mut Swarm<gossipsub::Behaviour>, topic_name: &str, data: Vec<u8>) -> Result<(), P2PError> {
    let topic = IdentTopic::new(topic_name);
    swarm.behaviour_mut().publish(topic, data).map_err(|e| P2PError::Network(e.to_string()))?;
    Ok(())
}

pub fn get_peer_count(_swarm: &Swarm<gossipsub::Behaviour>) -> usize {
    0
}
