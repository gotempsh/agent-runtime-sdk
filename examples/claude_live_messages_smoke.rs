//! Verify with a real Claude CLI that a retained turn accepts messages while
//! it runs, and that interrupting it stops only its foreground work.
//!
//! 1. A turn runs a slow foreground command. A message sent meanwhile must be
//!    answered by that same turn.
//! 2. A turn starts a background shell, then a long foreground command, and is
//!    interrupted. The interruption must end the turn without killing the
//!    process: the next turn is admitted at once, and a later turn receives
//!    the background shell's completion, which only that process can report.
//!
//! Usage: `cargo run --example claude_live_messages_smoke -- [model]`

#[cfg(unix)]
mod unix {
    use std::path::Path;
    use std::time::Duration;

    use temps_agent_runtime::lifecycle::{
        InterruptOutcome, InvocationId, RuntimeFailureKind, RuntimeId,
    };
    use temps_agent_runtime::providers::Claude;
    use temps_agent_runtime::retained::{
        InProcessRuntimeClient, RetainedRuntimeResult, RuntimeClient, RuntimeEvent, RuntimeHandle,
        RuntimeSpec, TurnHandle, TurnInput,
    };
    use temps_agent_runtime::{
        AgentRuntime, AgentTaskActivityKind, PermissionMode, Provider, ProviderProcessRetention,
        ToolCallStatus, TurnEvent, TurnResult,
    };
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    type Events = mpsc::UnboundedReceiver<(&'static str, TurnEvent)>;
    type Error = Box<dyn std::error::Error>;

    const BACKGROUND_SECS: u64 = 20;

    /// Forward a turn's provider events, tagged with the turn, to one channel.
    fn forward(
        turn: TurnHandle,
        label: &'static str,
        sender: mpsc::UnboundedSender<(&'static str, TurnEvent)>,
    ) -> JoinHandle<RetainedRuntimeResult<TurnResult>> {
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

    /// Wait for an event matching `wanted`, failing after `secs`.
    async fn wait_for(
        events: &mut Events,
        secs: u64,
        what: &str,
        mut wanted: impl FnMut(&str, &TurnEvent) -> bool,
    ) -> Result<TurnEvent, Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let (label, event) = tokio::time::timeout_at(deadline, events.recv())
                .await
                .map_err(|_| format!("timed out waiting for {what}"))?
                .ok_or_else(|| format!("events ended before {what}"))?;
            if wanted(label, &event) {
                return Ok(event);
            }
        }
    }

    fn is_foreground_bash(event: &TurnEvent, command: &str) -> bool {
        matches!(
            event,
            TurnEvent::ToolCall { name, status: ToolCallStatus::Started, input: Some(input), .. }
                if name == "Bash"
                    && input["command"].as_str().is_some_and(|c| c.contains(command))
                    && input["run_in_background"].as_bool() != Some(true)
        )
    }

    async fn acquire(
        client: &InProcessRuntimeClient,
        id: &str,
        model: &str,
        directory: &Path,
    ) -> Result<RuntimeHandle, Error> {
        let mut spec = RuntimeSpec::new(RuntimeId::new(id)?, Provider::Claude, directory);
        spec.model = Some(model.to_owned());
        spec.permission_mode = PermissionMode::FullAccess;
        spec.turn_timeout = Duration::from_secs(300);
        Ok(client.acquire(spec).await?)
    }

    async fn message_into_a_running_turn(
        client: &InProcessRuntimeClient,
        model: &str,
    ) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let handle = acquire(client, "claude-live-message-smoke", model, directory.path()).await?;
        let (sender, mut events) = mpsc::unbounded_channel();
        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("fold")?,
                "Use the Bash tool to run exactly this command in the foreground: \
                 sleep 12 && echo FIRSTDONE. Then reply with its output.",
            ))
            .await?;
        let messages = turn.message_handle();
        let completion = forward(turn, "fold", sender);

        wait_for(&mut events, 120, "the foreground command", |_, event| {
            is_foreground_bash(event, "sleep 12")
        })
        .await?;
        messages
            .send("Also include the word BANANA in your final reply.")
            .await?;
        println!("message delivered while the command ran");

        let result = tokio::time::timeout(Duration::from_secs(240), completion).await???;
        println!("turn answered: {:?}", result.text);
        let late = messages.send("too late").await;
        println!(
            "a message after the turn ended: {:?}",
            late.as_ref().map_err(|failure| failure.kind)
        );
        client.dispose(handle.runtime_id()).await?;
        Ok(result.text.contains("BANANA")
            && matches!(late, Err(ref failure) if failure.kind == RuntimeFailureKind::InvalidRequest))
    }

    async fn interrupt_keeps_background_work(
        client: &InProcessRuntimeClient,
        model: &str,
    ) -> Result<bool, Error> {
        let directory = tempfile::tempdir()?;
        let marker = directory.path().join("background-finished.txt");
        let handle = acquire(
            client,
            "claude-soft-interrupt-smoke",
            model,
            directory.path(),
        )
        .await?;
        let (sender, mut events) = mpsc::unbounded_channel();
        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("work")?,
                format!(
                    "Do these two steps in order. Step 1: use the Bash tool with \
                     run_in_background set to true to run: sleep {BACKGROUND_SECS} && echo \
                     done > {}. Step 2: use the Bash tool in the foreground (not in the \
                     background) to run: sleep 299. Then reply DONE.",
                    marker.display()
                ),
            ))
            .await?;
        let interrupt = turn.interrupt_handle();
        let first = forward(turn, "work", sender.clone());

        let started = wait_for(&mut events, 120, "the background shell", |_, event| {
            matches!(event, TurnEvent::TaskActivity { activity }
                if activity.kind == AgentTaskActivityKind::Started)
        })
        .await?;
        let TurnEvent::TaskActivity { activity } = started else {
            unreachable!("matched a task activity");
        };
        let background = activity.task_id;
        println!("background shell running as task {background}");
        wait_for(&mut events, 120, "the foreground command", |_, event| {
            is_foreground_bash(event, "sleep 299")
        })
        .await?;
        let outcome = interrupt.interrupt().await;
        println!("interrupt: {outcome:?}");
        let first = tokio::time::timeout(Duration::from_secs(30), first).await??;
        let cancelled =
            matches!(first, Err(ref failure) if failure.kind == RuntimeFailureKind::Cancelled);
        println!("interrupted turn ended as cancelled: {cancelled}");

        let next = handle
            .start_turn(TurnInput::new(
                InvocationId::new("next")?,
                "What is 2+2? Reply with just the number.",
            ))
            .await?;
        let next = tokio::time::timeout(
            Duration::from_secs(240),
            forward(next, "next", sender.clone()),
        )
        .await???;
        println!("next turn answered: {:?}", next.text);

        for _ in 0..(BACKGROUND_SECS * 4) {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("background shell wrote its marker: {}", marker.exists());
        // Give Claude a moment to report the finished task to its parked
        // process, then let a turn collect what it reported.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let last = handle
            .start_turn(TurnInput::new(
                InvocationId::new("last")?,
                "Reply with exactly OK.",
            ))
            .await?;
        let last = tokio::time::timeout(Duration::from_secs(240), forward(last, "last", sender))
            .await???;
        println!("last turn answered: {:?}", last.text);

        let mut completed_later = false;
        while let Ok((label, event)) = events.try_recv() {
            if let TurnEvent::TaskActivity { activity } = event {
                println!("{label}: {} {:?}", activity.task_id, activity.kind);
                if label != "work"
                    && activity.task_id == background
                    && activity.kind == AgentTaskActivityKind::Completed
                {
                    completed_later = true;
                }
            }
        }
        println!("a later turn received the background completion: {completed_later}");
        client.dispose(handle.runtime_id()).await?;
        Ok(outcome == InterruptOutcome::Interrupted
            && cancelled
            && next.text.contains('4')
            && marker.exists()
            && completed_later)
    }

    pub async fn run() -> Result<(), Error> {
        let model = std::env::args().nth(1).unwrap_or_else(|| "haiku".into());
        let mut builder =
            AgentRuntime::builder().provider_process_retention(ProviderProcessRetention {
                max_processes: 1,
                idle_timeout: Duration::from_secs(120),
                initialization_timeout: Duration::from_secs(60),
                active_inactivity_timeout: Some(Duration::from_secs(180)),
            });
        builder.register(Claude::default());
        let client = InProcessRuntimeClient::new(builder.build()?);

        let folded = message_into_a_running_turn(&client, &model).await?;
        println!("message answered by the running turn: {folded}");
        let kept = interrupt_keeps_background_work(&client, &model).await?;
        println!("interrupt kept the background work: {kept}");
        if folded && kept {
            println!("PASS");
            Ok(())
        } else {
            Err("live messages or the soft interrupt did not behave as expected".into())
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
    eprintln!("claude_live_messages_smoke requires Unix");
}
