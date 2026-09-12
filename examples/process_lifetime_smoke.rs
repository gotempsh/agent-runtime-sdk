//! Verify with a real provider that a detached tool process survives a completed turn.
//!
//! Usage: `cargo run --example process_lifetime_smoke -- <provider> <model>`

#[cfg(unix)]
mod unix {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use temps_agent_runtime::{
        AgentRuntime, EventSink, PermissionMode, Provider, Result, ToolProcessPolicy, TurnEvent,
        TurnRequest,
    };

    #[derive(Default)]
    struct Events {
        tools: AtomicUsize,
        text: AtomicUsize,
    }

    #[async_trait]
    impl EventSink for Events {
        async fn emit(&self, event: TurnEvent) -> Result<()> {
            match event {
                TurnEvent::ToolCall { .. } => {
                    self.tools.fetch_add(1, Ordering::Relaxed);
                }
                TurnEvent::TextDelta { .. } => {
                    self.text.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
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

    pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut arguments = std::env::args().skip(1);
        let provider = provider(&arguments.next().ok_or("provider is required")?)?;
        let model = arguments.next().ok_or("model is required")?;
        if arguments.next().is_some() {
            return Err("expected: <provider> <model>".into());
        }

        let directory = tempfile::tempdir()?;
        let pid_path = directory.path().join("detached.pid");
        let log_path = directory.path().join("detached.log");
        let prompt = format!(
            "Use the shell exactly once to run this exact command, then reply with exactly DONE: \
             nohup sleep 60 </dev/null >'{}' 2>&1 & echo $! >'{}'",
            log_path.display(),
            pid_path.display(),
        );

        let runtime = AgentRuntime::builder().concurrency_limit(1).build()?;
        let mut request = TurnRequest::new(provider, directory.path(), prompt);
        request.model = Some(model);
        request.reasoning = Some("low".into());
        request.permission_mode = PermissionMode::FullAccess;
        request.tool_process_policy = ToolProcessPolicy::PreserveOnCompletion;
        request.timeout = Duration::from_secs(120);
        request.interaction_timeout = Duration::from_secs(10);

        let events = Events::default();
        let result = runtime.run(request, &events, None).await?;
        drop(runtime);

        let pid = std::fs::read_to_string(&pid_path)?.trim().parse::<i32>()?;
        let alive_after_turn =
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok();
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGKILL,
        );

        println!("provider={provider}");
        println!(
            "model={}",
            result.model.as_deref().unwrap_or("not-reported")
        );
        println!("status={:?}", result.status);
        println!("answer={}", result.text.trim());
        println!("tool_events={}", events.tools.load(Ordering::Relaxed));
        println!("text_events={}", events.text.load(Ordering::Relaxed));
        println!("runtime_dropped=true");
        println!("detached_pid={pid}");
        println!("alive_after_turn={alive_after_turn}");
        println!("cleanup=SIGKILL");

        if !alive_after_turn {
            return Err("detached tool process did not survive turn completion".into());
        }
        Ok(())
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unix::run().await
}

#[cfg(not(unix))]
fn main() {
    eprintln!("process_lifetime_smoke currently supports Unix hosts only");
}
