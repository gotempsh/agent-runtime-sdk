//! Run one minimal Codex turn inside an existing Temps sandbox.

use std::path::PathBuf;
use std::time::Duration;

use temps_agent_runtime::providers::Codex;
use temps_agent_runtime::{
    AgentRuntime, NoopEventSink, PermissionMode, Provider, SecretString, TempsSandboxAuth,
    TempsSandboxTransport, TurnRequest,
};

fn required(name: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let base_url = required("TEMPS_TEST_BASE_URL")?;
    let sandbox_id = required("TEMPS_TEST_SANDBOX_ID")?;
    let auth = if let Ok(token) = std::env::var("TEMPS_TEST_API_KEY") {
        TempsSandboxAuth::Bearer(SecretString::new(token))
    } else {
        TempsSandboxAuth::SessionCookie(SecretString::new(required("TEMPS_TEST_SESSION_COOKIE")?))
    };
    let executable = std::env::var("TEMPS_TEST_CODEX")
        .unwrap_or_else(|_| "/home/temps/.bun/bin/codex".to_string());
    let workspace = std::env::var("TEMPS_TEST_WORKSPACE")
        .unwrap_or_else(|_| "/home/temps/workspace".to_string());
    let transport = TempsSandboxTransport::builder(base_url, &sandbox_id, auth)
        .poll_interval(Duration::from_millis(75))
        .build()?;
    let mut builder = AgentRuntime::builder()
        .transport(transport)
        .concurrency_limit(1);
    builder.register(Codex::with_executable(&executable));
    let runtime = builder.build()?;

    let readiness = runtime.readiness(Provider::Codex).await?;
    println!("transport=temps_sandbox");
    println!("installed={}", readiness.installed);
    println!(
        "version={}",
        readiness.version.as_deref().unwrap_or("unknown")
    );
    if !readiness.installed {
        return Err(readiness.detail.into());
    }

    let mut request = TurnRequest::new(
        Provider::Codex,
        PathBuf::from(workspace),
        "Reply with exactly OK. Do not inspect files, call tools, or explain.",
    );
    request.model = Some("gpt-5.6-sol".to_string());
    request.reasoning = Some("low".to_string());
    request.permission_mode = PermissionMode::Plan;
    request.timeout = Duration::from_secs(120);
    request.max_turns = Some(1);

    let result = runtime.run(request, &NoopEventSink, None).await?;
    println!("status={:?}", result.status);
    println!("answer={}", result.text.trim());
    println!(
        "session_id={}",
        result.session_id.as_deref().unwrap_or("none")
    );
    Ok(())
}
