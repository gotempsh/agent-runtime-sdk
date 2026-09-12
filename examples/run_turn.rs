//! Run one Claude Code turn and print normalized events.

use async_trait::async_trait;
use temps_agent_runtime::{AgentRuntime, EventSink, Provider, Result, TurnEvent, TurnRequest};

struct PrintEvents;

#[async_trait]
impl EventSink for PrintEvents {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        println!("{event:?}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = AgentRuntime::builder().build()?;
    let request = TurnRequest::new(
        Provider::Claude,
        std::env::current_dir()?,
        "Summarize this repository in three sentences without changing files.",
    );
    let result = runtime.run(request, &PrintEvents, None).await?;
    println!("\n{}", result.text);
    Ok(())
}
