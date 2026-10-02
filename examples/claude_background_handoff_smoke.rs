//! Verify with a real Claude CLI that a background subagent keeps running
//! when a new prompt is sent to its retained runtime.
//!
//! The first turn launches a background subagent that waits, then writes a
//! marker file. While it waits, a second prompt is submitted. The second turn
//! must be admitted without interrupting the first, inherit the running task,
//! and receive its completion; the marker proves the subagent was not killed.
//!
//! Usage: `cargo run --example claude_background_handoff_smoke -- [model]`

#[cfg(unix)]
mod unix {
    use std::time::Duration;

    use temps_agent_runtime::lifecycle::{InvocationId, RuntimeFailureKind, RuntimeId};
    use temps_agent_runtime::providers::Claude;
    use temps_agent_runtime::retained::{
        InProcessRuntimeClient, RuntimeClient, RuntimeEvent, RuntimeSpec, TurnHandle, TurnInput,
    };
    use temps_agent_runtime::{
        AgentRuntime, AgentTaskActivityKind, PermissionMode, Provider, ProviderProcessRetention,
        TurnEvent,
    };
    use tokio::sync::mpsc;

    const SUBAGENT_WAIT_SECS: u64 = 25;

    /// Forward a turn's provider events, tagged with the turn, to one channel.
    fn forward(
        turn: TurnHandle,
        label: &'static str,
        sender: mpsc::UnboundedSender<(&'static str, TurnEvent)>,
    ) -> tokio::task::JoinHandle<
        temps_agent_runtime::retained::RetainedRuntimeResult<temps_agent_runtime::TurnResult>,
    > {
        let (mut stream, completion) = turn.into_parts();
        tokio::spawn(async move {
            while let Some(envelope) = stream.next().await {
                if let RuntimeEvent::ProviderEvent { event } = envelope.event {
                    let _ = sender.send((label, event));
                }
            }
        });
        tokio::spawn(completion.wait())
    }

    pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let model = std::env::args().nth(1).unwrap_or_else(|| "haiku".into());
        let mut builder =
            AgentRuntime::builder().provider_process_retention(ProviderProcessRetention {
                max_processes: 1,
                idle_timeout: Duration::from_secs(60),
                initialization_timeout: Duration::from_secs(60),
                active_inactivity_timeout: Some(Duration::from_secs(180)),
            });
        builder.register(Claude::default());
        let client = InProcessRuntimeClient::new(builder.build()?);
        let directory = tempfile::tempdir()?;
        let marker = directory.path().join("subagent-finished.txt");
        let mut spec = RuntimeSpec::new(
            RuntimeId::new("claude-background-handoff-smoke")?,
            Provider::Claude,
            directory.path(),
        );
        spec.model = Some(model);
        spec.permission_mode = PermissionMode::FullAccess;
        spec.turn_timeout = Duration::from_secs(300);
        let handle = client.acquire(spec).await?;

        let prompt = format!(
            "Use the Agent tool with run_in_background set to true and subagent_type \
             general-purpose. Give the subagent exactly this task: 'Run this command with the \
             Bash tool and wait for it to finish: python3 -c \"import time; \
             time.sleep({SUBAGENT_WAIT_SECS}); open(\\\"{}\\\", \\\"w\\\").write(\\\"done\\\")\" \
             Then reply with exactly BGDONE.' Do not wait for the subagent. After launching it, \
             reply with exactly LAUNCHED and end your turn.",
            marker.display()
        );
        let (sender, mut events) = mpsc::unbounded_channel();
        let first = forward(
            handle
                .start_turn(TurnInput::new(InvocationId::new("launch")?, prompt))
                .await?,
            "first",
            sender.clone(),
        );

        let mut background_task = None;
        while background_task.is_none() {
            let (_, event) = tokio::time::timeout(Duration::from_secs(120), events.recv())
                .await?
                .ok_or("first turn ended before launching a subagent")?;
            if let TurnEvent::TaskActivity { activity } = event {
                if activity.kind == AgentTaskActivityKind::Started {
                    println!("first turn started task {}", activity.task_id);
                    background_task = Some(activity.task_id);
                }
            }
        }
        let background_task = background_task.unwrap_or_default();

        // Retry while the first turn is still composing its answer, as an
        // application following the busy failure's retry advice does.
        let mut second = None;
        for _ in 0..600 {
            match handle
                .start_turn(TurnInput::new(
                    InvocationId::new("follow-up")?,
                    "What is 2+2? Reply with just the number.",
                ))
                .await
            {
                Ok(turn) => {
                    second = Some(turn);
                    break;
                }
                Err(failure) if failure.kind == RuntimeFailureKind::RuntimeBusy => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(failure) => return Err(format!("follow-up rejected: {failure:?}").into()),
            }
        }
        let second = forward(
            second.ok_or("runtime stayed busy; the first turn never handed off")?,
            "second",
            sender,
        );
        let first = tokio::time::timeout(Duration::from_secs(30), first).await???;
        println!("first turn completed: {:?}", first.text);
        let handed_off_before_subagent = !marker.exists();
        println!("follow-up admitted while the subagent was running: {handed_off_before_subagent}");

        let second = tokio::time::timeout(Duration::from_secs(300), second).await???;
        println!("second turn completed: {:?}", second.text);

        let mut completed_in_second = false;
        while let Ok((label, event)) = events.try_recv() {
            if let TurnEvent::TaskActivity { activity } = event {
                println!("{label}: {} {:?}", activity.task_id, activity.kind);
                if label == "second"
                    && activity.task_id == background_task
                    && activity.kind == AgentTaskActivityKind::Completed
                {
                    completed_in_second = true;
                }
            }
        }
        let subagent_finished = marker.exists();
        println!("subagent wrote its marker: {subagent_finished}");
        println!("second turn received the inherited task's completion: {completed_in_second}");
        client.dispose(handle.runtime_id()).await?;

        if handed_off_before_subagent
            && subagent_finished
            && completed_in_second
            && second.text.contains('4')
        {
            println!("PASS");
            Ok(())
        } else {
            Err("the background subagent did not survive the follow-up turn".into())
        }
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unix::run().await
}

#[cfg(not(unix))]
fn main() {
    eprintln!("claude_background_handoff_smoke requires Unix");
}
