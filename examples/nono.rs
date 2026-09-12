//! Create a managed Nono profile and use it for one Claude Code turn.

use async_trait::async_trait;
use temps_agent_runtime::nono::{
    ManagedProfile, NetworkPolicy, NonoExecution, NonoManager, WorkdirAccess,
};
use temps_agent_runtime::{
    AgentRuntime, EventSink, Provider, Result, SandboxCapabilities, SandboxRequest, TurnEvent,
    TurnRequest,
};

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
    let workspace = std::env::current_dir()?;
    let manager = NonoManager::discover(workspace.join(".agent-runtime/nono"))?;
    let mut profile = ManagedProfile::new("read-only-example", "claude-code");
    profile.workdir = WorkdirAccess::Read;
    profile.network = NetworkPolicy::Block;
    profile.denied_environment = vec!["AWS_*".into(), "GITHUB_TOKEN".into()];
    let profile_path = manager.save(profile).await?;

    let runtime = AgentRuntime::builder().build()?;
    let mut request = TurnRequest::new(
        Provider::Claude,
        &workspace,
        "Inspect this repository and identify one maintainability improvement.",
    );
    request.sandbox = Some(
        SandboxRequest::new(NonoExecution::discover(profile_path.to_string_lossy())?).requiring(
            SandboxCapabilities {
                filesystem: true,
                process_isolation: true,
                ..SandboxCapabilities::NONE
            },
        ),
    );
    let result = runtime.run(request, &PrintEvents, None).await?;
    println!("\n{}", result.text);
    Ok(())
}
