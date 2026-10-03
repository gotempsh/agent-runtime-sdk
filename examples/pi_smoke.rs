//! Drive an installed `pi` through every behavior the adapter relies on.
//!
//! Usage: `cargo run --example pi_smoke --features pi -- [provider/model]`
//!
//! The scenarios run in a temporary workspace with real model calls, so pi
//! needs a usable credential: a stored `/login`, or a provider key named in
//! `PI_SMOKE_ENVIRONMENT` (comma-separated variable names, for example
//! `ANTHROPIC_API_KEY`), which is forwarded explicitly because the runtime
//! never inherits the host environment. Set `PI_CODING_AGENT_DIR` to run
//! against an isolated pi configuration.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use temps_agent_runtime::{
    AgentRuntime, EventSink, PermissionMode, Provider, ProviderProbeContext, Result, RuntimeError,
    SecretString, ToolCallStatus, TurnEvent, TurnRequest,
};

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<TurnEvent>>,
    first_text: Mutex<Option<tokio_util::sync::CancellationToken>>,
}

#[async_trait]
impl EventSink for Recorder {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        if matches!(event, TurnEvent::TextDelta { .. }) {
            if let Some(token) = self.first_text.lock().unwrap().take() {
                token.cancel();
            }
        }
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

impl Recorder {
    fn cancelling_on_first_text(token: tokio_util::sync::CancellationToken) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            first_text: Mutex::new(Some(token)),
        }
    }

    fn tool_calls(&self) -> Vec<(String, ToolCallStatus, Option<String>)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                TurnEvent::ToolCall {
                    name,
                    status,
                    output,
                    error,
                    ..
                } => Some((
                    name.clone(),
                    *status,
                    output.clone().or_else(|| error.clone()),
                )),
                _ => None,
            })
            .collect()
    }

    fn count(&self, matches: impl Fn(&TurnEvent) -> bool) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches(event))
            .count()
    }
}

struct Smoke {
    workspace: tempfile::TempDir,
    model: Option<String>,
    environment: BTreeMap<String, SecretString>,
}

impl Smoke {
    fn request(&self, prompt: &str, mode: PermissionMode) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::Pi, self.workspace.path(), prompt);
        request.model.clone_from(&self.model);
        request.permission_mode = mode;
        request.timeout = Duration::from_secs(180);
        request.environment.clone_from(&self.environment);
        request
    }
}

fn check(condition: bool, message: &str) -> std::result::Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let smoke = Smoke {
        workspace: tempfile::tempdir()?,
        model: std::env::args().nth(1),
        environment: std::env::var("PI_SMOKE_ENVIRONMENT")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| (name.to_string(), SecretString::new(value)))
            })
            .collect(),
    };
    let runtime = AgentRuntime::builder().build()?;

    let readiness = runtime.readiness(Provider::Pi).await?;
    println!(
        "readiness: installed={} version={:?}",
        readiness.installed, readiness.version
    );
    check(readiness.installed, "pi is not installed")?;

    // Probes see only the environment they are given, like turns.
    let mut context = ProviderProbeContext::new(smoke.workspace.path());
    for (name, value) in &smoke.environment {
        context = context.with_environment(name.clone(), value.clone());
    }
    let inventory = runtime.discover_harnesses_with(context).await?;
    let pi = inventory
        .harnesses
        .iter()
        .find(|harness| harness.provider == Provider::Pi)
        .ok_or("pi was not discovered")?;
    println!(
        "discovery: status={:?} authentication={:?} catalog={:?} models={}",
        pi.status,
        pi.authentication.status,
        pi.models.status,
        pi.models.models.len()
    );
    if let Some(default) = pi.models.models.iter().find(|model| model.is_default) {
        let efforts = default
            .reasoning_efforts
            .iter()
            .map(|effort| effort.id.as_str())
            .collect::<Vec<_>>();
        println!(
            "discovery: default model {} thinking {efforts:?}",
            default.id
        );
    }

    // 1. A tool-using turn.
    let started = Instant::now();
    let events = Recorder::default();
    let first = runtime
        .run(
            smoke.request(
                "Use the bash tool to run `echo pi-smoke-TOOL` exactly once, then reply with the command output only.",
                PermissionMode::FullAccess,
            ),
            &events,
            None,
        )
        .await?;
    let tools = events.tool_calls();
    println!(
        "tool turn: {:?} session={:?} model={:?} text={:?} tools={tools:?} usage={:?}",
        started.elapsed(),
        first.session_id,
        first.model,
        first.text.trim(),
        first.usage
    );
    let warnings = events
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            TurnEvent::Warning { message } => Some(message.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    println!("tool turn warnings: {warnings:?}");
    check(first.session_id.is_some(), "no session id")?;
    check(
        tools
            .iter()
            .any(|(name, status, _)| name == "bash" && *status == ToolCallStatus::Succeeded),
        "bash did not complete",
    )?;
    check(
        events.count(|event| matches!(event, TurnEvent::SessionStarted { .. })) == 1,
        "SessionStarted was not reported exactly once",
    )?;

    // 2. Resume the same session.
    let mut resume = smoke.request(
        "Reply with the exact command you asked bash to run in this conversation.",
        PermissionMode::FullAccess,
    );
    resume.session_id.clone_from(&first.session_id);
    let events = Recorder::default();
    let second = runtime.run(resume, &events, None).await?;
    println!(
        "resumed turn: session={:?} text={:?}",
        second.session_id,
        second.text.trim()
    );
    check(
        second.session_id == first.session_id,
        "resume changed the session",
    )?;
    check(
        events.count(|event| matches!(event, TurnEvent::SessionStarted { .. })) == 0,
        "a resumed session was announced again",
    )?;

    // 3. A resume of a session that does not exist fails before the prompt.
    let mut missing = smoke.request("hello", PermissionMode::FullAccess);
    missing.session_id = Some("pi-smoke-missing-session".into());
    match runtime.run(missing, &Recorder::default(), None).await {
        Err(RuntimeError::ProcessFailed { provider_code, .. }) => {
            println!("missing session: {provider_code:?}");
            check(
                provider_code.as_deref() == Some("pi::session_not_found"),
                "wrong code for a missing session",
            )?;
        }
        other => return Err(format!("missing session was not rejected: {other:?}").into()),
    }

    // 4. Plan mode removes bash.
    let events = Recorder::default();
    let plan = runtime
        .run(
            smoke.request(
                "Use the bash tool to run `echo pi-smoke-TOOL`. If you cannot, say so.",
                PermissionMode::Plan,
            ),
            &events,
            None,
        )
        .await?;
    let tools = events.tool_calls();
    println!("plan turn: text={:?} tools={tools:?}", plan.text.trim());
    check(
        !tools
            .iter()
            .any(|(name, status, _)| name == "bash" && *status == ToolCallStatus::Succeeded),
        "bash ran in plan mode",
    )?;

    // 5. Run summaries larger than the event limit do not fail the turn.
    let small = {
        let mut builder = AgentRuntime::builder().max_event_line_bytes(4 * 1024);
        builder.register(temps_agent_runtime::providers::Pi::default());
        builder.build()?
    };
    let oversized = small
        .run(
            smoke.request("Reply with the word ok.", PermissionMode::FullAccess),
            &Recorder::default(),
            None,
        )
        .await?;
    println!("small-limit turn: text={:?}", oversized.text.trim());

    // 6. A failing tool whose output is larger than the event limit is still
    // reported as failed.
    let events = Recorder::default();
    small
        .run(
            smoke.request(
                "BIGFAIL: Use the bash tool to run `seq 1 3000; exit 3` exactly once, then reply with the word done.",
                PermissionMode::FullAccess,
            ),
            &events,
            None,
        )
        .await?;
    let tools = events.tool_calls();
    println!(
        "oversized failing tool: {:?}",
        tools
            .iter()
            .map(|(name, status, text)| {
                let text = text.as_deref().unwrap_or_default();
                (name, status, &text[..text.len().min(80)])
            })
            .collect::<Vec<_>>()
    );
    check(
        tools
            .iter()
            .any(|(name, status, _)| name == "bash" && *status == ToolCallStatus::Failed),
        "the oversized failing bash call was not reported as failed",
    )?;
    check(
        !tools
            .iter()
            .any(|(name, status, _)| name == "bash" && *status == ToolCallStatus::Succeeded),
        "the oversized failing bash call was reported as succeeded",
    )?;

    // 7. Cancellation interrupts a running turn.
    let token = tokio_util::sync::CancellationToken::new();
    let mut slow = smoke.request(
        "SLOW: count from 1 to 400, one number per line.",
        PermissionMode::FullAccess,
    );
    slow.cancellation = token.clone();
    let started = Instant::now();
    let cancelled = runtime
        .run(slow, &Recorder::cancelling_on_first_text(token), None)
        .await;
    println!(
        "cancelled turn: {:?} after {:?}",
        cancelled.as_ref().err(),
        started.elapsed()
    );
    check(
        matches!(cancelled, Err(RuntimeError::Cancelled { .. })),
        "the turn was not cancelled",
    )?;

    println!("pi smoke passed");
    Ok(())
}
