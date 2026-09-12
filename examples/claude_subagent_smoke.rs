//! Runs one minimal Claude native subagent and reports normalized task events.
//!
//! Usage: `cargo run --example claude_subagent_smoke -- <model>`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use temps_agent_runtime::{
    AgentRuntime, EventSink, PermissionMode, Provider, Result, TurnEvent, TurnRequest,
};

#[derive(Default)]
struct Events {
    task_snapshots: AtomicUsize,
    task_activity: AtomicUsize,
    nested_tools: AtomicUsize,
}

#[async_trait]
impl EventSink for Events {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        match event {
            TurnEvent::TasksChanged { tasks } => {
                self.task_snapshots.fetch_add(1, Ordering::Relaxed);
                println!("tasks={tasks:?}");
            }
            TurnEvent::TaskActivity { activity } => {
                self.task_activity.fetch_add(1, Ordering::Relaxed);
                println!("activity={activity:?}");
            }
            TurnEvent::ToolCall {
                name,
                task_id: Some(task_id),
                ..
            } => {
                self.nested_tools.fetch_add(1, Ordering::Relaxed);
                println!("nested_tool={name} task_id={task_id}");
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let model = std::env::args().nth(1).ok_or("model is required")?;
    let runtime = AgentRuntime::builder().concurrency_limit(1).build()?;
    let mut request = TurnRequest::new(
        Provider::Claude,
        std::env::current_dir()?,
        "Use the Agent tool exactly once with subagent_type Explore. Ask it to reply exactly SUBAGENT_OK without reading files or using tools. Wait for it, then reply exactly PARENT_OK.",
    );
    request.model = Some(model);
    request.reasoning = Some("low".to_string());
    request.permission_mode = PermissionMode::AcceptEdits;
    request.max_turns = Some(4);
    request.timeout = Duration::from_secs(120);

    let events = Events::default();
    let result = runtime.run(request, &events, None).await?;
    println!(
        "model={}",
        result.model.as_deref().unwrap_or("not-reported")
    );
    println!("status={:?}", result.status);
    println!("answer={}", result.text.trim());
    println!(
        "task_snapshots={}",
        events.task_snapshots.load(Ordering::Relaxed)
    );
    println!(
        "task_activity={}",
        events.task_activity.load(Ordering::Relaxed)
    );
    println!(
        "nested_tools={}",
        events.nested_tools.load(Ordering::Relaxed)
    );
    if events.task_activity.load(Ordering::Relaxed) == 0 {
        return Err("Claude emitted no native task activity".into());
    }
    Ok(())
}
