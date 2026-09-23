//! Opt-in live baseline: two turns with provider-native session continuity.
//! Uses the selected CLI's normal authentication and can incur model usage.

use std::sync::Arc;

use temps_agent_runtime::{
    AgentRuntime, NoopEventSink, Provider, StartupObserver, StartupTiming, TurnRequest,
};

struct PrintTimings;

impl StartupObserver for PrintTimings {
    fn observe(&self, sample: StartupTiming) {
        println!(
            "run={} provider={:?} stage={:?} elapsed_ms={} event_delivery_ms={}",
            sample.observation_id,
            sample.provider,
            sample.stage,
            sample.elapsed.as_millis(),
            sample.event_delivery_elapsed.as_millis(),
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = match std::env::args().nth(1).as_deref() {
        Some("claude") => Provider::Claude,
        Some("codex") => Provider::Codex,
        _ => return Err("usage: cargo run --example startup_timings -- <claude|codex>".into()),
    };
    let workspace = tempfile::tempdir()?;
    let mut builder = AgentRuntime::builder();
    builder.register(temps_agent_runtime::providers::Codex::app_server());
    let runtime = builder.startup_observer(Arc::new(PrintTimings)).build()?;
    let mut session = None;
    for _ in 0..2 {
        let mut request = TurnRequest::new(
            provider,
            workspace.path(),
            "Reply with exactly READY. Do not use tools or modify files.",
        );
        request.session_id = session;
        request.timeout = std::time::Duration::from_secs(60);
        let result = runtime.run(request, &NoopEventSink, None).await?;
        session = result.session_id;
        if session.is_none() {
            return Err(
                "provider did not report a resumable session; cannot measure continuation".into(),
            );
        }
    }
    Ok(())
}
