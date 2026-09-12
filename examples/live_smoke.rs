//! Run one minimal, read-only live turn through an optional Nono backend.
//!
//! Usage: `cargo run --example live_smoke -- <provider> <model> [nono-profile] [run|wrap]`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use temps_agent_runtime::nono::{NonoExecution, NonoMode};
use temps_agent_runtime::{
    AgentRuntime, EventSink, PermissionMode, Provider, Result, SandboxCapabilities, SandboxRequest,
    TurnEvent, TurnRequest,
};

#[derive(Default)]
struct EventCounts {
    text: AtomicUsize,
    reasoning: AtomicUsize,
    tools: AtomicUsize,
    warnings: AtomicUsize,
}

#[async_trait]
impl EventSink for EventCounts {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        match event {
            TurnEvent::TextDelta { .. } => self.text.fetch_add(1, Ordering::Relaxed),
            TurnEvent::ReasoningDelta { .. } => self.reasoning.fetch_add(1, Ordering::Relaxed),
            TurnEvent::ToolCall { .. } => self.tools.fetch_add(1, Ordering::Relaxed),
            TurnEvent::Warning { .. } => self.warnings.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        Ok(())
    }
}

fn provider(value: &str) -> std::result::Result<Provider, String> {
    match value {
        "claude" => Ok(Provider::Claude),
        "codex" => Ok(Provider::Codex),
        "opencode" => Ok(Provider::OpenCode),
        _ => Err(format!("unsupported provider `{value}`")),
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let provider = provider(&arguments.next().ok_or("provider is required")?)?;
    let model = arguments.next().ok_or("model is required")?;
    let nono_profile = arguments.next();
    let nono_mode = match arguments.next().as_deref() {
        None | Some("run") => NonoMode::Run,
        Some("wrap") => NonoMode::Wrap,
        Some(value) => return Err(format!("unsupported Nono mode `{value}`").into()),
    };
    if arguments.next().is_some() {
        return Err("expected: <provider> <model> [nono-profile] [run|wrap]".into());
    }

    let runtime = AgentRuntime::builder().concurrency_limit(1).build()?;
    let readiness = runtime.readiness(provider).await?;
    if !readiness.installed {
        return Err(readiness.detail.into());
    }

    let mut request = TurnRequest::new(
        provider,
        std::env::current_dir()?,
        "Reply with exactly OK. Do not inspect files, call tools, or explain.",
    );
    request.model = Some(model);
    if matches!(provider, Provider::Claude | Provider::Codex) {
        request.reasoning = Some("low".into());
    }
    request.permission_mode = PermissionMode::Plan;
    request.max_turns = Some(1);
    request.timeout = Duration::from_secs(90);
    request.interaction_timeout = Duration::from_secs(10);
    if let Some(profile) = nono_profile {
        let mut nono = NonoExecution::discover(profile)?;
        nono.mode = nono_mode;
        request.sandbox = Some(SandboxRequest::new(nono).requiring(SandboxCapabilities {
            filesystem: true,
            network_allowlist: nono_mode == NonoMode::Run,
            process_isolation: true,
            ..SandboxCapabilities::NONE
        }));
    }

    let events = EventCounts::default();
    let result = runtime.run(request, &events, None).await?;
    println!("provider={provider}");
    println!(
        "model={}",
        result.model.as_deref().unwrap_or("not-reported")
    );
    println!("status={:?}", result.status);
    println!("answer={}", result.text.trim());
    println!("text_events={}", events.text.load(Ordering::Relaxed));
    println!(
        "reasoning_events={}",
        events.reasoning.load(Ordering::Relaxed)
    );
    println!("tool_events={}", events.tools.load(Ordering::Relaxed));
    println!("warnings={}", events.warnings.load(Ordering::Relaxed));
    println!("input_tokens={:?}", result.usage.input_tokens);
    println!("output_tokens={:?}", result.usage.output_tokens);
    Ok(())
}
