//! Run one minimal Codex turn inside an SSH destination.

use std::path::PathBuf;
use std::time::Duration;

use temps_agent_runtime::providers::Codex;
use temps_agent_runtime::{
    AgentRuntime, NoopEventSink, PermissionMode, Provider, SshHostKeyPolicy, SshTransport,
    TurnRequest,
};

fn required(name: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let host = required("SSH_TEST_HOST")?;
    let port = required("SSH_TEST_PORT")?.parse::<u16>()?;
    let user = required("SSH_TEST_USER")?;
    let identity = required("SSH_TEST_IDENTITY")?;
    let known_hosts = required("SSH_TEST_KNOWN_HOSTS")?;
    let executable =
        std::env::var("SSH_TEST_CODEX").unwrap_or_else(|_| "/usr/local/bin/codex".to_string());
    let workspace = std::env::var("SSH_TEST_WORKSPACE").unwrap_or_else(|_| "/workspace".into());

    let transport = SshTransport::builder(host)
        .port(port)
        .user(user)
        .identity_file(identity)
        .known_hosts_file(known_hosts)
        .host_key_policy(SshHostKeyPolicy::Strict)
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let mut builder = AgentRuntime::builder()
        .transport(transport)
        .concurrency_limit(1);
    builder.register(Codex::with_executable(&executable));
    let runtime = builder.build()?;

    let readiness = runtime.readiness(Provider::Codex).await?;
    println!("transport=ssh");
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
