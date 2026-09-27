//! Verify with a real Codex app server that a background terminal outlives
//! its turn and the idle timeout while the retained process owns it.
//!
//! Usage: `cargo run --example background_terminal_retention_smoke --features codex -- <model> [port]`

#[cfg(unix)]
mod unix {
    use std::net::TcpStream;
    use std::time::Duration;

    use temps_agent_runtime::lifecycle::{InvocationId, RuntimeId};
    use temps_agent_runtime::providers::Codex;
    use temps_agent_runtime::retained::{
        InProcessRuntimeClient, RuntimeClient, RuntimeSpec, TurnInput,
    };
    use temps_agent_runtime::{AgentRuntime, CodexProcessRetention, PermissionMode, Provider};

    fn listening(port: u16) -> bool {
        TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(1)).is_ok()
    }

    pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut arguments = std::env::args().skip(1);
        let model = arguments.next().ok_or("model is required")?;
        let port: u16 = arguments.next().map_or(Ok(5393), |port| port.parse())?;
        if listening(port) {
            return Err(format!("port {port} is already in use").into());
        }

        let idle_timeout = Duration::from_secs(5);
        let mut builder = AgentRuntime::builder().codex_process_retention(CodexProcessRetention {
            max_processes: 1,
            idle_timeout,
        });
        builder.register(Codex::app_server());
        let client = InProcessRuntimeClient::new(builder.build()?);
        let directory = tempfile::tempdir()?;
        let mut spec = RuntimeSpec::new(
            RuntimeId::new("background-terminal-smoke")?,
            Provider::Codex,
            directory.path(),
        );
        spec.model = Some(model);
        spec.reasoning = Some("low".into());
        spec.permission_mode = PermissionMode::FullAccess;
        spec.turn_timeout = Duration::from_secs(240);
        let handle = client.acquire(spec).await?;

        let prompt = format!(
            "Start `python3 -m http.server {port}` as a long-running background terminal \
             session. Do not detach it with nohup, setsid, disown or `&`. Confirm it listens, \
             then reply with exactly DONE and leave it running."
        );
        let turn = handle
            .start_turn(TurnInput::new(InvocationId::new("start")?, prompt))
            .await?;
        turn.wait().await?;
        println!("after turn: listening={}", listening(port));

        tokio::time::sleep(idle_timeout * 4).await;
        let after_idle = listening(port);
        println!("after 4 idle timeouts: listening={after_idle}");

        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("follow-up")?,
                "Reply with exactly OK. Do not run any commands.",
            ))
            .await?;
        turn.wait().await?;
        let after_follow_up = listening(port);
        println!("after follow-up turn: listening={after_follow_up}");

        client.dispose(handle.runtime_id()).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_dispose = listening(port);
        println!("after dispose: listening={after_dispose}");

        if after_idle && after_follow_up && !after_dispose {
            println!("PASS");
            Ok(())
        } else {
            Err("background terminal lifetime did not match its retained process".into())
        }
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unix::run().await
}

#[cfg(not(unix))]
fn main() {
    eprintln!("background_terminal_retention_smoke requires Unix");
}
