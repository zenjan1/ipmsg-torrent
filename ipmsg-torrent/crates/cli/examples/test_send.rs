// Test message sender - sends a broadcast message
use ipmsg_core::P2PEngine;
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Test message sender starting...");

    // Create engine
    let mut engine = P2PEngine::new(PathBuf::from("/tmp/ipmsg-test-sender"))?;

    // Bootstrap node
    let bootstrap = vec![
        "/ip4/140.83.57.37/tcp/43363/p2p/12D3KooWSa5Rn51bVUTSGE8HanAaiQcLBeFQkkGhJXoF5eFNdBfg"
            .to_string(),
    ];

    // Start engine with username, bootstrap nodes, and listen port
    let peer_id = engine
        .start("test-sender".to_string(), bootstrap, 0)
        .await?;
    println!("Started with peer_id: {}", peer_id);

    // Wait for connection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get command sender
    let cmd_tx = engine.take_command_sender().expect("command sender");

    // Send broadcast message
    println!("Sending broadcast message...");
    cmd_tx.send(ipmsg_core::SendCommand::Broadcast {
        content: "Hello from test sender!".to_string(),
    })?;

    // Wait for events
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Done!");
    Ok(())
}
