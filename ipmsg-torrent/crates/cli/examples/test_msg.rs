// Test message sender - connects to same bootstrap, discovers peers, sends a broadcast
use ipmsg_core::P2PEngine;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("ipmsg_core=info")
        .init();

    let data_dir = "/tmp/ipmsg-test-sender";
    let bootstrap = vec![
        "/ip4/140.83.57.37/tcp/43363/p2p/12D3KooWSa5Rn51bVUTSGE8HanAaiQcLBeFQkkGhJXoF5eFNdBfg"
            .to_string(),
    ];

    eprintln!("Test message sender starting...");

    let mut engine = P2PEngine::new(data_dir.into())?;
    let peer_id = engine
        .start("test-sender".to_string(), bootstrap, 41227)
        .await?;
    eprintln!("My peer ID: {}", &peer_id[..16]);

    let mut event_rx = engine.take_receiver().expect("receiver already taken");
    let cmd_tx = engine
        .take_command_sender()
        .expect("command sender already taken");

    // Spawn swarm loop
    tokio::spawn(async move {
        engine.run_event_loop().await;
    });

    // Wait for peers
    eprintln!("Waiting for peers...");
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        tokio::select! {
            Some(evt) = event_rx.recv() => {
                match evt {
                    ipmsg_core::P2PEvent::PeerJoined { username, peer_id, .. } => {
                        eprintln!("Peer joined: {} ({})", username, &peer_id[..12]);
                    }
                    ipmsg_core::P2PEvent::MessageReceived(msg) => {
                        eprintln!("Message from {}: {:?}", msg.from, msg.kind);
                    }
                    ipmsg_core::P2PEvent::Status(s) => {
                        eprintln!("Status: {}", s);
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    // Send broadcast
    eprintln!("Sending broadcast message...");
    match cmd_tx.send(ipmsg_core::SendCommand::Broadcast {
        content: "Hello from test sender!".to_string(),
    }) {
        Ok(_) => eprintln!("Command sent successfully"),
        Err(e) => eprintln!("Failed to send command: {}", e),
    }

    // Wait longer for message propagation
    eprintln!("Waiting for message propagation...");
    let start2 = std::time::Instant::now();
    while start2.elapsed() < Duration::from_secs(10) {
        tokio::select! {
            Some(evt) = event_rx.recv() => {
                match evt {
                    ipmsg_core::P2PEvent::MessageReceived(msg) => {
                        eprintln!("Received message from {}: {:?}", msg.from, msg.kind);
                    }
                    ipmsg_core::P2PEvent::MessageSent(msg) => {
                        eprintln!("Message sent confirmed: {:?}", msg.kind);
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    eprintln!("Done!");
    Ok(())
}
