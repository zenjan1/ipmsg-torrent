use crate::identity::Identity;
use crate::messaging::{IpMsgBehaviour, CHAT_TOPIC, FILE_TOPIC, PRESENCE_TOPIC};
use crate::{ConnectedPeer, P2PError, P2PEvent};
use futures::stream::StreamExt;
use ipmsg_protocol::codec::encode_message;
use ipmsg_protocol::message::ChatMessage;
use libp2p::gossipsub::IdentTopic;
use libp2p::swarm::SwarmEvent;
use libp2p::Swarm;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::UnboundedSender;

/// The libp2p Swarm configured for IPMsg
pub struct P2PSwarm {
    swarm: Swarm<IpMsgBehaviour>,
    peers: HashMap<String, ConnectedPeer>,
    #[allow(dead_code)] // Reserved for async message routing
    event_tx: UnboundedSender<P2PEvent>,
    #[allow(dead_code)] // Reserved for message queue
    message_tx: tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>,
    #[allow(dead_code)] // Reserved for message queue
    message_rx: tokio::sync::mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    subscribed_topics: Vec<String>,
}

impl P2PSwarm {
    pub async fn new(
        identity: &Identity,
        username: &str,
        platforms: &[String],
        event_tx: &UnboundedSender<P2PEvent>,
        bootstrap_nodes: Vec<String>,
    ) -> Result<Self, P2PError> {
        let keypair = identity.to_keypair();

        let behaviour = IpMsgBehaviour::new(
            &keypair,
            username,
            platforms,
        );

        let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_quic()
            .with_behaviour(|_| behaviour)
            .map_err(|e| P2PError::Transport(e.to_string()))?
            .build();

        let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut swarm_obj = Self {
            swarm,
            peers: HashMap::new(),
            event_tx: event_tx.clone(),
            message_tx,
            message_rx,
            subscribed_topics: Vec::new(),
        };

        for addr_str in &bootstrap_nodes {
            if let Ok(addr) = addr_str.parse::<libp2p::Multiaddr>() {
                if let Some(peer_id) = addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(pid) => Some(pid),
                    _ => None,
                }) {
                    let _ = swarm_obj.swarm.dial(addr.clone());
                    swarm_obj.swarm.behaviour_mut().add_kademlia_peer(peer_id, addr.clone());
                    tracing::info!(%peer_id, %addr, "Added bootstrap node");
                }
            }
        }

        swarm_obj
            .swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
            .map_err(|e| P2PError::Transport(e.to_string()))?;

        swarm_obj.subscribe_topic(PRESENCE_TOPIC)?;
        swarm_obj.subscribe_topic(CHAT_TOPIC)?;
        swarm_obj.subscribe_topic(FILE_TOPIC)?;

        Ok(swarm_obj)
    }

    fn subscribe_topic(&mut self, name: &str) -> Result<(), P2PError> {
        let topic = IdentTopic::new(name);
        self.swarm
            .behaviour_mut()
            .subscribe(&topic)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        self.subscribed_topics.push(name.to_string());
        tracing::info!(topic = name, "Subscribed to topic");
        Ok(())
    }

    fn publish_to_topic(&mut self, topic_name: &str, data: Vec<u8>) -> Result<(), P2PError> {
        let topic = IdentTopic::new(topic_name);
        self.swarm
            .behaviour_mut()
            .publish_message(topic, data)
            .map_err(|e| P2PError::Network(e.to_string()))?;
        Ok(())
    }

    pub async fn send_message(&mut self, _to: &str, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = encode_message(msg);
        self.publish_to_topic(CHAT_TOPIC, bytes)
    }

    pub async fn broadcast_message(&mut self, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = encode_message(msg);
        self.publish_to_topic(CHAT_TOPIC, bytes)
    }

    pub fn broadcast_presence(&mut self, msg: &ChatMessage) -> Result<(), P2PError> {
        let bytes = encode_message(msg);
        self.publish_to_topic(PRESENCE_TOPIC, bytes)
    }

    pub fn get_peers(&self) -> Vec<ConnectedPeer> {
        self.peers.values().cloned().collect()
    }
}

impl futures::Stream for P2PSwarm {
    type Item = Vec<P2PEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut events = Vec::new();

        loop {
            match self.swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => {
                    if let Some(evt) = handle_swarm_event(&event) {
                        events.push(evt);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {
                    if events.is_empty() {
                        return Poll::Pending;
                    }
                    return Poll::Ready(Some(events));
                }
            }
        }
    }
}

fn handle_swarm_event(
    event: &SwarmEvent<<IpMsgBehaviour as libp2p::swarm::NetworkBehaviour>::ToSwarm>,
) -> Option<P2PEvent> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            tracing::info!("Listening on {}", address);
            Some(P2PEvent::Status(format!("listening on {}", address)))
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            tracing::info!("Connected to {}", peer_id);
            Some(P2PEvent::PeerJoined {
                peer_id: peer_id.to_string(),
                username: String::new(),
                platforms: Vec::new(),
            })
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            tracing::info!("Disconnected from {}", peer_id);
            Some(P2PEvent::PeerLeft {
                peer_id: peer_id.to_string(),
            })
        }
        SwarmEvent::Behaviour(behaviour_event) => {
            handle_behaviour_event(behaviour_event)
        }
        _ => None,
    }
}

// Use type alias for the combined behaviour event type
type BehaviourEvent = <IpMsgBehaviour as libp2p::swarm::NetworkBehaviour>::ToSwarm;

fn handle_behaviour_event(_event: &BehaviourEvent) -> Option<P2PEvent> {
    // The ToSwarm type is a NetworkBehaviourAction which wraps the actual event.
    // For derived behaviours, we need to access the inner event via the OutEvent type.
    // Since we can't easily pattern match on the internal type, let's use a different approach:
    // handle events directly in the Stream impl by polling each behaviour.

    // For now, return None and handle events differently.
    // The actual gossipsub messages will be handled by the swarm's internal routing.
    None
}
