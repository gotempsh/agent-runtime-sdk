//! Fetch account quota for the provider authenticated on this execution host.
//!
//! Usage: `cargo run --example account_usage -- claude|codex`

use temps_agent_runtime::{AgentRuntime, Provider};

fn provider(value: &str) -> std::result::Result<Provider, String> {
    match value {
        "claude" => Ok(Provider::Claude),
        "codex" => Ok(Provider::Codex),
        _ => Err(format!(
            "unsupported provider `{value}`; expected claude or codex"
        )),
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let provider = provider(&arguments.next().ok_or("provider is required")?)?;
    if arguments.next().is_some() {
        return Err("expected exactly one provider argument".into());
    }

    let runtime = AgentRuntime::builder().concurrency_limit(1).build()?;
    let report = runtime.fetch_account_usage(provider).await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
