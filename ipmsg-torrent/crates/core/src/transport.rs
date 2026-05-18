use crate::identity::Identity;
use crate::messaging::{parse_agent_version, CHAT_TOPIC, FILE_TOPIC, PRESENCE_TOPIC};
use crate::store::PeerInfo as StoredPeerInfo;
use crate::{ConnectedPeer, MessageStore, P2PError, P2PEvent};
use futures::stream::StreamExt;
use ipmsg_protocol::codec::decode_message;
use ipmsg_protocol::message::ChatMessage;
use libp2p::gossipsub::{self, IdentTopic, MessageAuthenticity};
use libp2p::identify::{self, Event as IdentifyEvent};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{Behaviour as Kademlia, Config as KademliaConfig};
use libp2p::mdns::{Behaviour as Mdns, Event as MdnsEvent};
use libp2p::swarm::NetworkBehaviour;
use libp2p::swarm::SwarmEvent;
use libp2p::swarm::ToSwarm;
use libp2p::{Multiaddr, PeerId, StreamProtocol, Swarm};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// Combined LibP2P behaviour — all sub-protocols
#[derive(NetworkBehaviour)]
pub struct IpMsgNetBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: Kademlia<MemoryStore>,
    pub mdns: Mdns<libp2p::mdns::tokio::Tokio>,
}

impl IpMsgNetBehaviour {
    pub fn new(
        local_key: &libp2p::identity::Keypair,
        username: &str,
        platforms: &[String],
    ) -> Self {
        let local_peer_id = PeerId::from(local_key.public());

        // Kademlia
        let mut kad_config = KademliaConfig::new(StreamProtocol::new("/ipmsg/kad/1.0.0"));
        kad_config.set_query_timeout(Duration::from_secs(60));
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = Kademlia::new(local_peer_id, kad_store);
        kademlia.set_mode(Some(libp2p::kad::Mode::Client));

        // Gossipsub
        let gs_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .build()
            .expect("valid gossipsub config");
        let gossipsub = gossipsub::Behaviour::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gs_config,
        )
        .expect("valid gossipsub");

        // Identify
        let identify_config = identify::Config::new(
            "ipmsg/1.0.0".to_string(),
            local_key.public(),
        )
        .with_agent_version(format!(
            "ipmsg/2.1.0 ({}, {})",
            username,
            platforms.join(", ")
        ))
        .with_interval(Duration::from_secs(300));
        let identify = identify::Behaviour::new(identify_config);

        // mDNS
        let mdns = Mdns::new(Default::default(), local_peer_id)
            .expect("mDNS creation failed");

        Self {
            gossipsub,
            identify,
            kademlia,
            mdns,
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
}

impl P2PSwarm {
    pub async fn new(
        identity: &Identity,
        username: &str,
        platforms: &[String],
        _event_tx: &UnboundedSender<P2PEvent>,
        bootstrap_nodes: Vec<String>,
    ) -> Result<Self, P2PError> {
        let keypair = identity.to_keypair();
        let behaviour = IpMsgNetBehaviour::new(&keypair, username, platforms);

        let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_quic()
            .with_behaviour(|_| behaviour)
            .map_err(|e| P2PError::Transport(e.to_string()))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(600)))
            .build();

        let store = MessageStore::new(std::path::Path::new("/tmp/ipmsg_msg.db"))
            .unwrap_or_else(|_| panic!("Failed to create message store"));

        let mut swarm_obj = Self {
            swarm,
            peers: HashMap::new(),
            subscribed_topics: Vec::new(),
            store,
            connected_peers: HashSet::new(),
        };

        // Dial bootstrap nodes
        for addr_str in &bootstrap_nodes {
            if let Ok(addr) = addr_str.parse::<Multiaddr>() {
                if let Some(peer_id) = addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(pid) => Some(pid),
                    _ => None,
                }) {
                    let _ = swarm_obj.swarm.dial(addr.clone());
                    swarm_obj.swarm.behaviour_mut().add_kademlia_peer(peer_id, addr.clone());
                    swarm_obj.connected_peers.insert(peer_id);
                    tracing::info!(%peer_id, %addr, "Added bootstrap node");
                }
            }
        }

        // Listen on QUIC
        swarm_obj
            .swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
            .map_err(|e| P2PError::Transport(e.to_string()))?;

        // Subscribe to topics
        swarm_obj.subscribe_topic(PRESENCE_TOPIC)?;
        swarm_obj.subscribe_topic(CHAT_TOPIC)?;
        swarm_obj.subscribe_topic(FILE_TOPIC)?;

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
        self.swarm.behaviour_mut().gossipsub
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
        self.swarm.behaviour_mut().gossipsub
            .publish(topic, data)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        Ok(())
    }

    pub fn get_peers(&self) -> Vec<ConnectedPeer> {
        self.peers.values().cloned().collect()
    }

    fn on_gossipsub_message(&mut self, msg: &gossipsub::Message) -> Vec<P2PEvent> {
        let mut events = Vec::new();
        let topic = msg.topic.as_str();

        if topic != CHAT_TOPIC && topic != PRESENCE_TOPIC {
            return events;
        }

        match decode_message(&msg.data) {
            Ok(chat_msg) => {
                if topic == PRESENCE_TOPIC {
                    if let ipmsg_protocol::message::MessageType::Presence { username, platforms, .. } = &chat_msg.kind {
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
        let (username, platforms) = parse_agent_version(&info.agent_version)
            .unwrap_or_else(|| (String::new(), Vec::new()));

        let peer = ConnectedPeer {
            peer_id: pid_str.clone(),
            username: username.clone(),
            platforms: platforms.clone(),
            last_seen: chrono::Utc::now(),
        };

        self.save_peer(&peer, info.public_key.encode_protobuf());

        let is_new = !self.peers.contains_key(&pid_str);
        self.peers.insert(pid_str.clone(), peer);
        self.connected_peers.insert(info.public_key.to_peer_id());

        if is_new {
            events.push(P2PEvent::PeerJoined {
                peer_id: pid_str,
                username,
                platforms,
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

        self.swarm.behaviour_mut().add_kademlia_peer(*peer_id, addr.clone());
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
        loop {
            match self.swarm.behaviour_mut().gossipsub.poll(
                &mut std::task::Context::from_waker(futures::task::noop_waker_ref()),
            ) {
                std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) => {
                    match evt {
                        gossipsub::Event::Message { message, .. } => {
                            let new = self.on_gossipsub_message(&message);
                            events.extend(new);
                        }
                        _ => {}
                    }
                }
                _ => break,
            }
        }

        // Poll identify
        loop {
            match self.swarm.behaviour_mut().identify.poll(
                &mut std::task::Context::from_waker(futures::task::noop_waker_ref()),
            ) {
                std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) => {
                    match evt {
                        IdentifyEvent::Received { info, .. } => {
                            let new = self.on_identify_received(&info);
                            events.extend(new);
                        }
                        _ => {}
                    }
                }
                _ => break,
            }
        }

        // Poll mdns
        loop {
            match self.swarm.behaviour_mut().mdns.poll(
                &mut std::task::Context::from_waker(futures::task::noop_waker_ref()),
            ) {
                std::task::Poll::Ready(ToSwarm::GenerateEvent(evt)) => {
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
                _ => break,
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
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            tracing::info!("Connected to {}", peer_id);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            tracing::info!("Disconnected from {}", peer_id);
                            let pid_str = peer_id.to_base58();
                            self.peers.remove(&pid_str);
                            events.push(P2PEvent::PeerLeft { peer_id: pid_str });
                        }
                        SwarmEvent::Behaviour(_) => {
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
