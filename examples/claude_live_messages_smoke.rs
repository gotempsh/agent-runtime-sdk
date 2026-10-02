//! Verify with a real Claude CLI that a retained turn accepts messages while
//! it runs, and that interrupting it stops only its foreground work.
//!
//! Scenarios (all run by default; name some to run only those):
//!
//! - `fold`: a message sent during a foreground command is answered by the
//!   same turn; a message after the turn ends is rejected as not sent.
//! - `multi`: two messages sent during one command are both answered.
//! - `queued`: a message still queued when the turn is interrupted is
//!   stopped with it and never runs.
//! - `idle`: interrupting a turn with no background work keeps the process.
//! - `bg-shell`: interrupting a long foreground command keeps a background
//!   shell running in the same process, and a later turn receives its
//!   completion.
//! - `bg-agent`: the same with a background subagent. Claude's own stop ends
//!   background subagents along with the foreground work; the process is
//!   still kept and the stop is reported as the task's `Stopped` activity.
//! - `bg-agent-answered`: interrupting after Claude has answered, while only
//!   a background subagent runs, sends Claude nothing: the subagent keeps
//!   running in the same process and a later turn receives its completion.
//! - `approval`: a background subagent asks for an approval while no turn is
//!   running; the request waits and the next turn's handler answers it.
//!
//! "The same process" is checked by having Claude print the parent PID of
//! its Bash tool on each side of the interruption.
//!
//! Usage: `cargo run --example claude_live_messages_smoke -- [model] [scenario...]`

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use temps_agent_runtime::lifecycle::{
        InterruptOutcome, InvocationId, RuntimeFailureKind, RuntimeId,
    };
    use temps_agent_runtime::providers::Claude;
    use temps_agent_runtime::retained::{
        InProcessRuntimeClient, RetainedRuntimeResult, RuntimeClient, RuntimeEvent, RuntimeHandle,
        RuntimeSpec, TurnHandle, TurnInput, TurnInterruptHandle, TurnMessageHandle,
    };
    use temps_agent_runtime::{
        AgentRuntime, AgentTaskActivityKind, ApprovalDecision, ApprovalRequest, InteractionHandler,
        PermissionMode, Provider, ProviderProcessRetention, QuestionAnswer, QuestionRequest,
        ToolCallStatus, TurnEvent, TurnResult,
    };
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    type Error = Box<dyn std::error::Error>;
    type Labeled = (String, TurnEvent);

    const SCENARIOS: &[&str] = &[
        "fold",
        "multi",
        "queued",
        "idle",
        "bg-shell",
        "bg-agent",
        "bg-agent-answered",
        "approval",
    ];
    const PID_COMMAND: &str = "echo CLAUDEPID=$PPID";

    /// Allows everything and records what it was asked, per turn.
    struct Approver {
        turn: &'static str,
        asked: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait]
    impl InteractionHandler for Approver {
        async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
            let summary = format!("{} {}", request.tool_name, request.input);
            println!("  [{}] approval requested: {summary}", self.turn);
            self.asked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((self.turn.to_owned(), summary));
            ApprovalDecision::Allow
        }

        async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
            None
        }
    }

    struct Running {
        messages: TurnMessageHandle,
        interrupt: TurnInterruptHandle,
        completion: JoinHandle<RetainedRuntimeResult<TurnResult>>,
    }

    impl Running {
        async fn finish(self, secs: u64) -> Result<RetainedRuntimeResult<TurnResult>, Error> {
            Ok(tokio::time::timeout(Duration::from_secs(secs), self.completion).await??)
        }
    }

    /// One retained runtime and every provider event its turns produced.
    struct Session<'a> {
        client: &'a InProcessRuntimeClient,
        handle: RuntimeHandle,
        sender: mpsc::UnboundedSender<Labeled>,
        receiver: mpsc::UnboundedReceiver<Labeled>,
        seen: Vec<Labeled>,
        directory: tempfile::TempDir,
    }

    impl<'a> Session<'a> {
        async fn new(
            client: &'a InProcessRuntimeClient,
            id: &str,
            model: &str,
            permission_mode: PermissionMode,
        ) -> Result<Session<'a>, Error> {
            let directory = tempfile::tempdir()?;
            let mut spec =
                RuntimeSpec::new(RuntimeId::new(id)?, Provider::Claude, directory.path());
            spec.model = Some(model.to_owned());
            spec.permission_mode = permission_mode;
            spec.turn_timeout = Duration::from_secs(400);
            let handle = client.acquire(spec).await?;
            let (sender, receiver) = mpsc::unbounded_channel();
            Ok(Session {
                client,
                handle,
                sender,
                receiver,
                seen: Vec::new(),
                directory,
            })
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        async fn start(&self, label: &str, prompt: String) -> Result<Running, Error> {
            let input = TurnInput::new(InvocationId::new(label)?, prompt);
            Ok(self.forward(self.handle.start_turn(input).await?, label))
        }

        async fn start_with(
            &self,
            label: &str,
            prompt: String,
            handler: Arc<dyn InteractionHandler>,
        ) -> Result<Running, Error> {
            let input = TurnInput::new(InvocationId::new(label)?, prompt);
            let turn = self
                .handle
                .start_turn_with_interactions(input, handler)
                .await?;
            Ok(self.forward(turn, label))
        }

        fn forward(&self, turn: TurnHandle, label: &str) -> Running {
            let messages = turn.message_handle();
            let interrupt = turn.interrupt_handle();
            let (mut stream, completion) = turn.into_parts();
            let sender = self.sender.clone();
            let label = label.to_owned();
            tokio::spawn(async move {
                while let Some(envelope) = stream.next().await {
                    if let RuntimeEvent::ProviderEvent { event } = envelope.event {
                        if let TurnEvent::ProviderProcessStatus { status, message } = &event {
                            println!("  [{label}] process {status:?}: {message}");
                        }
                        let _ = sender.send((label.clone(), event));
                    }
                }
            });
            Running {
                messages,
                interrupt,
                completion: tokio::spawn(completion.wait()),
            }
        }

        /// Wait for an event matching `wanted`, keeping everything seen.
        async fn wait_for(
            &mut self,
            secs: u64,
            what: &str,
            wanted: impl Fn(&str, &TurnEvent) -> bool,
        ) -> Result<TurnEvent, Error> {
            if let Some((_, event)) = self.seen.iter().find(|(l, e)| wanted(l, e)) {
                return Ok(event.clone());
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
            loop {
                let (label, event) = tokio::time::timeout_at(deadline, self.receiver.recv())
                    .await
                    .map_err(|_| format!("timed out waiting for {what}"))?
                    .ok_or_else(|| format!("events ended before {what}"))?;
                let matched = wanted(&label, &event);
                self.seen.push((label, event.clone()));
                if matched {
                    return Ok(event);
                }
            }
        }

        fn drain(&mut self) {
            while let Ok(item) = self.receiver.try_recv() {
                self.seen.push(item);
            }
        }

        /// Parent PIDs Claude's Bash tool reported in turn `label`.
        fn claude_pids(&mut self, label: &str) -> Vec<u32> {
            self.drain();
            self.seen
                .iter()
                .filter(|(l, _)| l == label)
                .filter_map(|(_, event)| match event {
                    TurnEvent::ToolCall {
                        output: Some(output),
                        ..
                    } => output.split("CLAUDEPID=").nth(1).and_then(|rest| {
                        rest.chars()
                            .take_while(char::is_ascii_digit)
                            .collect::<String>()
                            .parse()
                            .ok()
                    }),
                    _ => None,
                })
                .collect()
        }

        fn task_activity(&mut self, label: &str, task: &str, kind: AgentTaskActivityKind) -> bool {
            self.drain();
            self.seen.iter().any(|(l, event)| {
                l == label
                    && matches!(event, TurnEvent::TaskActivity { activity }
                        if activity.task_id == task && activity.kind == kind)
            })
        }

        async fn background_task_started(&mut self) -> Result<String, Error> {
            let event = self
                .wait_for(180, "a background task", |_, event| {
                    matches!(event, TurnEvent::TaskActivity { activity }
                        if activity.kind == AgentTaskActivityKind::Started)
                })
                .await?;
            let TurnEvent::TaskActivity { activity } = event else {
                unreachable!("matched a task activity");
            };
            Ok(activity.task_id)
        }

        /// Wait until a foreground Bash call running `command` has started
        /// and is still running a moment later (Claude's tool guard can
        /// reject a command, which also reports a start).
        async fn foreground_bash(&mut self, command: &str) -> Result<(), Error> {
            let started = self
                .wait_for(180, &format!("foreground `{command}`"), |_, event| {
                    matches!(
                        event,
                        TurnEvent::ToolCall { name, status: ToolCallStatus::Started, input: Some(input), .. }
                            if name == "Bash"
                                && input["command"].as_str().is_some_and(|c| c.contains(command))
                                && input["run_in_background"].as_bool() != Some(true)
                    )
                })
                .await?;
            let TurnEvent::ToolCall { id: Some(id), .. } = started else {
                return Err("a Bash call without an id".into());
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            self.drain();
            let ended = self.seen.iter().find_map(|(_, event)| match event {
                TurnEvent::ToolCall {
                    id: Some(other),
                    status,
                    output,
                    error,
                    ..
                } if *other == id && *status != ToolCallStatus::Started => {
                    Some(format!("{status:?}: {output:?} {error:?}"))
                }
                _ => None,
            });
            match ended {
                Some(ended) => Err(format!("`{command}` ended at once: {ended}").into()),
                None => Ok(()),
            }
        }

        async fn dispose(self) -> Result<(), Error> {
            self.client.dispose(self.handle.runtime_id()).await?;
            Ok(())
        }
    }

    fn check(results: &mut Vec<String>, name: &str, ok: bool) {
        println!("  {} {name}", if ok { "ok  " } else { "FAIL" });
        if !ok {
            results.push(name.to_owned());
        }
    }

    fn cancelled(result: &RetainedRuntimeResult<TurnResult>) -> bool {
        matches!(result, Err(failure) if failure.kind == RuntimeFailureKind::Cancelled)
    }

    async fn wait_for_file(path: &Path, secs: u64) -> bool {
        for _ in 0..secs * 2 {
            if path.exists() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        path.exists()
    }

    async fn fold(client: &InProcessRuntimeClient, model: &str) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let mut s = Session::new(client, "e2e-fold", model, PermissionMode::FullAccess).await?;
        let turn = s
            .start(
                "fold",
                "Use the Bash tool to run exactly this command in the foreground: \
                 python3 -c 'import time; time.sleep(12)' && echo FIRSTDONE. Then reply with its output."
                    .into(),
            )
            .await?;
        s.foreground_bash("time.sleep(12)").await?;
        let sent = turn
            .messages
            .send("Also include the word BANANA in your final reply.")
            .await;
        check(&mut failed, "message accepted mid-turn", sent.is_ok());
        let messages = turn.messages.clone();
        let result = turn.finish(240).await??;
        println!("  reply: {:?}", result.text);
        check(
            &mut failed,
            "same turn answered the message",
            result.text.contains("BANANA"),
        );
        check(
            &mut failed,
            "turn still did its own work",
            result.text.contains("FIRSTDONE"),
        );
        let late = messages.send("too late").await;
        check(
            &mut failed,
            "message after the turn is rejected as not sent",
            matches!(&late, Err(f) if f.kind == RuntimeFailureKind::InvalidRequest
                && f.delivery == temps_agent_runtime::lifecycle::DeliveryState::NotSent),
        );
        s.dispose().await?;
        Ok(failed)
    }

    async fn multi(client: &InProcessRuntimeClient, model: &str) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let mut s = Session::new(client, "e2e-multi", model, PermissionMode::FullAccess).await?;
        let turn = s
            .start(
                "multi",
                "Use the Bash tool to run exactly this command in the foreground: python3 -c 'import time; time.sleep(15)'. \
                 Then reply DONE."
                    .into(),
            )
            .await?;
        s.foreground_bash("time.sleep(15)").await?;
        let first = turn
            .messages
            .send("Include the word BANANA in your final reply.")
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let second = turn
            .messages
            .send("Include the word CHERRY in your final reply too.")
            .await;
        check(
            &mut failed,
            "both messages accepted",
            first.is_ok() && second.is_ok(),
        );
        let result = turn.finish(240).await??;
        s.drain();
        let all_text: String = s
            .seen
            .iter()
            .filter_map(|(_, e)| match e {
                TurnEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        println!("  reply: {:?}", result.text);
        check(
            &mut failed,
            "one turn answered both messages",
            all_text.contains("BANANA") && all_text.contains("CHERRY"),
        );
        s.dispose().await?;
        Ok(failed)
    }

    async fn queued(client: &InProcessRuntimeClient, model: &str) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let mut s = Session::new(client, "e2e-queued", model, PermissionMode::FullAccess).await?;
        let marker = s.path("queued-ran.txt");
        let turn = s
            .start(
                "work",
                format!(
                    "Do these steps in order. Step 1: use the Bash tool to run: {PID_COMMAND}. \
                     Step 2: use the Bash tool in the foreground to run: python3 -c 'import time; time.sleep(298)'. Then \
                     reply DONE."
                ),
            )
            .await?;
        s.foreground_bash("time.sleep(298)").await?;
        let sent = turn
            .messages
            .send(format!(
                "After that, use the Bash tool to run: echo ran > {}",
                marker.display()
            ))
            .await;
        check(
            &mut failed,
            "message queued behind the command",
            sent.is_ok(),
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        let outcome = turn.interrupt.interrupt().await;
        check(
            &mut failed,
            "interrupt confirmed",
            outcome == InterruptOutcome::Interrupted,
        );
        let result = turn.finish(30).await?;
        check(&mut failed, "turn ended as cancelled", cancelled(&result));

        let next = s
            .start(
                "next",
                format!("Use the Bash tool to run: {PID_COMMAND}. Then reply with its output."),
            )
            .await?;
        let next = next.finish(240).await??;
        println!("  next reply: {:?}", next.text);
        tokio::time::sleep(Duration::from_secs(8)).await;
        check(&mut failed, "queued message never ran", !marker.exists());
        let (before, after) = (s.claude_pids("work"), s.claude_pids("next"));
        println!("  claude pid before {before:?}, after {after:?}");
        check(
            &mut failed,
            "same Claude process after the interrupt",
            !before.is_empty() && before == after,
        );
        s.dispose().await?;
        Ok(failed)
    }

    async fn idle(client: &InProcessRuntimeClient, model: &str) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let mut s = Session::new(client, "e2e-idle", model, PermissionMode::FullAccess).await?;
        let turn = s
            .start(
                "work",
                format!(
                    "Do these steps in order. Step 1: use the Bash tool to run: {PID_COMMAND}. \
                     Step 2: use the Bash tool in the foreground to run: python3 -c 'import time; time.sleep(297)'. Then \
                     reply DONE."
                ),
            )
            .await?;
        s.foreground_bash("time.sleep(297)").await?;
        let outcome = turn.interrupt.interrupt().await;
        check(
            &mut failed,
            "interrupt confirmed",
            outcome == InterruptOutcome::Interrupted,
        );
        check(
            &mut failed,
            "turn ended as cancelled",
            cancelled(&turn.finish(30).await?),
        );
        let started = std::time::Instant::now();
        let next = s
            .start(
                "next",
                format!("Use the Bash tool to run: {PID_COMMAND}. Then reply with its output."),
            )
            .await?;
        let next = next.finish(240).await??;
        println!(
            "  next reply after {:?}: {:?}",
            started.elapsed(),
            next.text
        );
        let (before, after) = (s.claude_pids("work"), s.claude_pids("next"));
        println!("  claude pid before {before:?}, after {after:?}");
        check(
            &mut failed,
            "same Claude process after the interrupt",
            !before.is_empty() && before == after,
        );
        let leftover = std::process::Command::new("pgrep")
            .args(["-f", "time.sleep\\(297\\)"])
            .output()?;
        check(
            &mut failed,
            "interrupted foreground command was stopped",
            leftover.stdout.is_empty(),
        );
        s.dispose().await?;
        Ok(failed)
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Background {
        /// A background shell; the turn is stopped during foreground work.
        Shell,
        /// A background subagent; the turn is stopped during foreground work.
        AgentDuringForeground,
        /// A background subagent; the turn is stopped after Claude answered.
        AgentAfterAnswer,
    }

    async fn background(
        client: &InProcessRuntimeClient,
        model: &str,
        kind: Background,
    ) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let id = match kind {
            Background::Shell => "e2e-bg-shell",
            Background::AgentDuringForeground => "e2e-bg-agent",
            Background::AgentAfterAnswer => "e2e-bg-agent-answered",
        };
        let mut s = Session::new(client, id, model, PermissionMode::FullAccess).await?;
        let marker = s.path("background-finished.txt");
        let write = format!(
            "{} && echo done > {}",
            "python3 -c 'import time; time.sleep(25)'",
            marker.display()
        );
        let background_step = if kind == Background::Shell {
            format!("use the Bash tool with run_in_background set to true to run: {write}.")
        } else {
            format!(
                "use the Agent tool with run_in_background set to true and subagent_type \
                 general-purpose, giving it exactly this task: 'Run this with the Bash tool and \
                 wait for it: {write}. Then reply BGDONE.' Do not wait for it."
            )
        };
        let last_step = if kind == Background::AgentAfterAnswer {
            "Then reply with exactly LAUNCHED and end your turn.".to_owned()
        } else {
            "Step 3: use the Bash tool in the foreground (not in the background) to run: \
             python3 -c 'import time; time.sleep(296)'. Then reply DONE."
                .to_owned()
        };
        let turn = s
            .start(
                "work",
                format!(
                    "Do these steps in order. Step 1: use the Bash tool to run: {PID_COMMAND}. \
                     Step 2: {background_step} {last_step}"
                ),
            )
            .await?;
        let task = s.background_task_started().await?;
        println!("  background task {task}");
        if kind == Background::AgentAfterAnswer {
            s.wait_for(180, "the answer", |_, event| {
                matches!(event, TurnEvent::TextDelta { text } if text.contains("LAUNCHED"))
            })
            .await?;
            tokio::time::sleep(Duration::from_secs(3)).await;
        } else {
            s.foreground_bash("time.sleep(296)").await?;
        }
        let outcome = turn.interrupt.interrupt().await;
        check(
            &mut failed,
            "interrupt confirmed",
            outcome == InterruptOutcome::Interrupted,
        );
        check(
            &mut failed,
            "turn ended as cancelled",
            cancelled(&turn.finish(30).await?),
        );

        let next = s
            .start(
                "next",
                format!(
                    "Use the Bash tool to run: {PID_COMMAND}. Then reply with its output and \
                     the answer to 2+2."
                ),
            )
            .await?;
        let next = next.finish(300).await??;
        println!("  next reply: {:?}", next.text);
        let (before, after) = (s.claude_pids("work"), s.claude_pids("next"));
        println!("  claude pid before {before:?}, after {after:?}");
        check(
            &mut failed,
            "same Claude process after the interrupt",
            !before.is_empty() && before == after,
        );

        if kind == Background::AgentDuringForeground {
            // Claude's own Stop ends background subagents along with the
            // foreground work; the SDK must report it rather than hide it.
            if let Err(error) = s
                .wait_for(60, "the subagent's stop", |_, event| {
                    matches!(event, TurnEvent::TaskActivity { activity }
                    if activity.task_id == task && activity.kind == AgentTaskActivityKind::Stopped)
                })
                .await
            {
                println!("  {error}");
            }
            let stopped = s.task_activity("work", &task, AgentTaskActivityKind::Stopped)
                || s.task_activity("next", &task, AgentTaskActivityKind::Stopped);
            check(
                &mut failed,
                "Claude's stop of the subagent is reported",
                stopped,
            );
            tokio::time::sleep(Duration::from_secs(30)).await;
            check(
                &mut failed,
                "stopped subagent never finished",
                !marker.exists(),
            );
        } else {
            check(
                &mut failed,
                "background work finished",
                wait_for_file(&marker, 60).await,
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
            let last = s.start("last", "Reply with exactly OK.".into()).await?;
            let last = last.finish(240).await??;
            println!("  last reply: {:?}", last.text);
            let completed = s.task_activity("next", &task, AgentTaskActivityKind::Completed)
                || s.task_activity("last", &task, AgentTaskActivityKind::Completed);
            check(
                &mut failed,
                "a later turn received the completion",
                completed,
            );
        }
        s.drain();
        for (label, event) in &s.seen {
            if let TurnEvent::TaskActivity { activity } = event {
                if activity.task_id == task {
                    println!(
                        "  [{label}] task {:?} status={:?}",
                        activity.kind, activity.status
                    );
                }
            }
        }
        s.dispose().await?;
        Ok(failed)
    }

    async fn approval(client: &InProcessRuntimeClient, model: &str) -> Result<Vec<String>, Error> {
        let mut failed = Vec::new();
        let mut s = Session::new(client, "e2e-approval", model, PermissionMode::Default).await?;
        let marker = s.path("approved-write.txt");
        let asked = Arc::new(Mutex::new(Vec::new()));
        let handler = |turn| -> Arc<dyn InteractionHandler> {
            Arc::new(Approver {
                turn,
                asked: Arc::clone(&asked),
            })
        };
        let turn = s
            .start_with(
                "work",
                format!(
                    "Use the Agent tool with run_in_background set to true and subagent_type \
                     general-purpose, giving it exactly this task: 'First run this with the Bash \
                     tool: python3 -c 'import time; time.sleep(20)'. After it finishes, run this with the Bash tool: echo done > \
                     {}. Then reply BGDONE.' Do not wait for it. Reply LAUNCHED and end your turn.",
                    marker.display()
                ),
                handler("work"),
            )
            .await?;
        let task = s.background_task_started().await?;
        println!("  background task {task}");
        // Let the turn answer (and the subagent start sleeping), then stop it:
        // the subagent's write will need an approval while no turn runs.
        tokio::time::sleep(Duration::from_secs(10)).await;
        let outcome = turn.interrupt.interrupt().await;
        check(
            &mut failed,
            "interrupt confirmed",
            outcome == InterruptOutcome::Interrupted,
        );
        let _ = turn.finish(30).await?;
        let during_work = asked.lock().map(|a| a.len()).unwrap_or_default();

        // Long enough for the subagent to ask; nobody can answer yet.
        tokio::time::sleep(Duration::from_secs(40)).await;
        check(&mut failed, "write waits for an approval", !marker.exists());
        check(
            &mut failed,
            "no approval answered while no turn runs",
            asked.lock().map(|a| a.len()).unwrap_or_default() == during_work,
        );

        let next = s
            .start_with("next", "Reply with exactly OK.".into(), handler("next"))
            .await?;
        let next = next.finish(300).await??;
        println!("  next reply: {:?}", next.text);
        let answered_by_next = asked.lock().is_ok_and(|a| {
            a.iter()
                .any(|(turn, summary)| turn == "next" && summary.contains("approved-write"))
        });
        check(
            &mut failed,
            "next turn answered the held approval",
            answered_by_next,
        );
        check(
            &mut failed,
            "approved write happened",
            wait_for_file(&marker, 60).await,
        );
        s.drain();
        let request_event = s.seen.iter().any(|(l, e)| {
            l == "next"
                && matches!(e, TurnEvent::ApprovalRequested(r) if r.input.to_string().contains("approved-write"))
        });
        check(
            &mut failed,
            "next turn showed the request event",
            request_event,
        );
        s.dispose().await?;
        Ok(failed)
    }

    pub async fn run() -> Result<(), Error> {
        let mut args = std::env::args().skip(1);
        let model = args.next().unwrap_or_else(|| "haiku".into());
        let selected: Vec<String> = args.collect();
        let selected: Vec<&str> = if selected.is_empty() {
            SCENARIOS.to_vec()
        } else {
            selected.iter().map(String::as_str).collect()
        };

        let mut builder =
            AgentRuntime::builder().provider_process_retention(ProviderProcessRetention {
                max_processes: 2,
                idle_timeout: Duration::from_secs(180),
                initialization_timeout: Duration::from_secs(60),
                active_inactivity_timeout: Some(Duration::from_secs(240)),
            });
        builder.register(Claude::default());
        let client = InProcessRuntimeClient::new(builder.build()?);

        let mut summary = Vec::new();
        for scenario in selected {
            println!("== {scenario} ({model})");
            let started = std::time::Instant::now();
            let outcome = match scenario {
                "fold" => fold(&client, &model).await,
                "multi" => multi(&client, &model).await,
                "queued" => queued(&client, &model).await,
                "idle" => idle(&client, &model).await,
                "bg-shell" => background(&client, &model, Background::Shell).await,
                "bg-agent" => background(&client, &model, Background::AgentDuringForeground).await,
                "bg-agent-answered" => {
                    background(&client, &model, Background::AgentAfterAnswer).await
                }
                "approval" => approval(&client, &model).await,
                other => Err(format!("unknown scenario {other}; known: {SCENARIOS:?}").into()),
            };
            // A scenario that failed partway left its runtime (and any turn
            // still running on it) behind; never let it overlap the next one.
            if let Ok(runtime_id) = RuntimeId::new(format!("e2e-{scenario}")) {
                let _ = client.dispose(&runtime_id).await;
            }
            let verdict = match outcome {
                Ok(failed) if failed.is_empty() => "PASS".to_owned(),
                Ok(failed) => format!("FAIL: {}", failed.join("; ")),
                Err(error) => format!("ERROR: {error}"),
            };
            println!("== {scenario}: {verdict} ({:?})", started.elapsed());
            summary.push((scenario, verdict));
        }
        println!();
        for (scenario, verdict) in &summary {
            println!("{scenario:>9}: {verdict}");
        }
        if summary.iter().all(|(_, verdict)| verdict == "PASS") {
            println!("PASS");
            Ok(())
        } else {
            Err("some scenarios failed".into())
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
