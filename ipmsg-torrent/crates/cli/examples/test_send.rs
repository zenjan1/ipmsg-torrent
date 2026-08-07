// Test message sender - sends a message to test1
use ipmsg_core::P2PEngine;
use ipmsg_protocol::message::MessageType;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("Test message sender starting...");

    // Create engine for test2
    let mut engine = P2PEngine::new(
        "test-sender".to_string(),
        "/tmp/ipmsg-test-sender".to_string(),
        None,
    )
    .await
    .expect("Failed to create engine");

    // Connect to bootstrap
    let bootstrap =
        "/ip4/140.83.57.37/tcp/43363/p2p/12D3KooWSa5Rn51bVUTSGE8HanAaiQcLBeFQkkGhJXoF5eFNdBfg";
    println!("Connecting to bootstrap...");
    engine.add_bootstrap(bootstrap.to_string());

    // Start engine
    let (cmd_tx, event_rx) = engine.start().await.expect("Failed to start engine");

    // Wait for connection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get peer list and send message
    println!("Sending broadcast message...");
    let _ = cmd_tx.send(ipmsg_core::SendCommand::Broadcast {
        content: "Hello from test sender!".to_string(),
    });

    // Wait for events
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Done!");
}
