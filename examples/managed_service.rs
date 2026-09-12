//! Starts a supervised one-shot process and prints its live status and logs.

use std::time::Duration;

use temps_agent_runtime::{
    ManagedProcessEvent, ManagedProcessSpec, ManagedProcessStatus, ManagedProcessSupervisor,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ManagedProcessSupervisor::new();
    let directory = std::env::current_dir()?;

    // Replace this with `bun run dev`, a worker binary, or any other direct
    // executable + argv pair. The command is owned by `supervisor`, not by an
    // agent turn.
    let mut process = supervisor
        .start(
            ManagedProcessSpec::background("example worker", "/bin/sh", directory)
                .args(["-c", "echo ready; sleep 2; echo finished"]),
        )
        .await?;

    while let Ok(event) = process.recv().await {
        match event {
            ManagedProcessEvent::Log { line, .. } => {
                println!("{}: {}", line.stream, line.text);
            }
            ManagedProcessEvent::StatusChanged { snapshot } => {
                println!("{:?}: {}", snapshot.status, snapshot.detail);
                if matches!(
                    snapshot.status,
                    ManagedProcessStatus::Succeeded
                        | ManagedProcessStatus::Failed
                        | ManagedProcessStatus::Cancelled
                ) {
                    break;
                }
            }
            ManagedProcessEvent::RestartScheduled {
                attempt, delay_ms, ..
            } => {
                println!(
                    "restart {attempt} in {}",
                    Duration::from_millis(delay_ms).as_secs_f32()
                );
            }
            _ => {}
        }
    }

    Ok(())
}
