# Stream Claude native subagents

Claude Code can create native Task/Agent subagents. The Claude adapter projects
their lifecycle into provider-neutral `TurnEvent` values without requiring a
second SDK or parser in the host application.

## Consume snapshots and activity

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{EventSink, Result, TurnEvent};

struct Events;

#[async_trait]
impl EventSink for Events {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        match event {
            TurnEvent::TasksChanged { tasks } => {
                // Replace the materialized task list for this run.
                println!("{} active or completed tasks", tasks.len());
            }
            TurnEvent::TaskActivity { activity } => {
                // Append this transition to the run timeline.
                println!("{}: {:?}", activity.task_id, activity.kind);
            }
            TurnEvent::ToolCall { name, task_id, .. } => {
                // A non-null task_id nests the call under its native subagent.
                println!("{name} belongs to {task_id:?}");
            }
            _ => {}
        }
        Ok(())
    }
}
```

`TasksChanged` is a bounded, replaceable snapshot. `TaskActivity` is an
append-only transition carrying progress, summary, agent type, nesting depth,
latest tool, and task-local usage when Claude reports them. Tool events use the
native task ID even though Claude's `parent_tool_use_id` belongs to a separate
identifier namespace.

## Show Claude workflows

A run of Claude Code's `Workflow` tool is a task with kind `workflow`. Its
`AgentTask::workflow` carries the run's structure, replaced on every update:

- `phases`, in declaration order;
- `agents`, each with its label, phase, normalized `state` (queued, running,
  completed, failed, blocked), model, tokens, tool calls, timings, latest tool
  (`last_tool_name`, `last_tool_summary`), and bounded prompt and result
  previews;
- recent script `logs`, and `omitted_agents` beyond the bound of 100 agents.

Render it as a live card rather than a log: phases as sections, one row per
agent. A tick that only moves counters (tokens, tool calls, durations, latest
tool) is re-emitted at most every fifth tick, and a tick that changes nothing is
not re-emitted; any other change is emitted at once. For a workflow task,
`TaskActivity` records only agent state changes;
`activity.workflow_agent` is the agent that changed, and `summary` reads such
as `scan:read completed`. Per-tick progress updates only the snapshot.

Claude does not stream a workflow agent's own tool calls. It writes them to a
transcript on the execution host instead:

```rust,ignore
if let (Some(workflow), Some(agent)) = (&task.workflow, task_agent) {
    if let Some(path) = workflow.agent_transcript_path(agent) {
        let transcript = std::fs::read_to_string(path)?; // on the execution host
        for entry in Claude::default().transcript_activity(&transcript, 200) {
            // entry.event is a TextDelta, ReasoningDelta or ToolCall, exactly
            // as a live turn would emit it; entry.timestamp is RFC 3339.
        }
    }
}
```

`agent_transcript_path` only returns a path for a plain agent identifier, so a
provider-reported value cannot point outside the transcript directory. The
transcript grows while the agent runs; read it again to refresh, and a line
still being written is skipped. Keep reading local to the host that runs
Claude: the transcript directory is a path on that host.

Entries are not redacted. Tool inputs and outputs are returned as Claude
recorded them, so they can contain secrets a tool read or printed. Filter what
you store or log, as you would for live `ToolCall` events.

## Preserve ordering around background completion

Claude may emit its parent `result` while background subagents are still
running. The adapter keeps the bidirectional stdin channel available and keeps
reading native task progress. It closes interactive input only after Claude's
background-task set becomes empty. `AgentRuntime::run` returns after the Claude
process itself exits, so the caller does not receive a false terminal result
while the adapter can still receive subagent approvals or task events.

Do not infer a run's terminal state from assistant text. Use the returned
`TurnResult` or `RuntimeError`. Task completion and turn completion are separate
state machines.

## Send a new prompt while subagents keep working

A background subagent lives inside the Claude process. With
`ProviderProcessRetention` enabled, nothing a follow-up does ends that process:
send the user's message into the running turn, or start a new turn once the
running one has answered, and interrupt only when the user asks to stop.

### Message the running turn

While a turn runs, deliver the user's next message into it:

```rust,ignore
let turn = handle.start_turn(input).await?;
let messages = turn.message_handle(); // cloneable, outlives the stream split
// ... later, while the turn still runs:
match messages.send("also update the changelog").await {
    Ok(()) => {} // answered on this turn's stream; the turn ends after it
    Err(failure) if failure.kind == RuntimeFailureKind::InvalidRequest => {
        // The turn has ended: start a new turn with the message instead.
    }
    Err(failure) => return Err(failure),
}
```

Claude queues the message and answers it within the same exchange, usually
folding it into the reply it is writing. Its output arrives on the running
turn's stream, and the turn completes only once every message sent into it
has been answered. `RuntimeDriverCapabilities::live_messages` reports support;
without it `send` fails with `CapabilityUnavailable`. Remote protocol clients
always report it as unsupported.

Resend a failed message, as a new turn or later, only when the failure carries
`DeliveryState::NotSent`. That covers a turn that has ended, missing support,
and a turn that did not take the message within ten seconds (`Timeout`; the
message is withdrawn and will never be written). Any other delivery state, such
as a write that failed partway, means Claude may have received it: reconcile
with the conversation before sending it again.

### Interrupt only foreground work

On a retained Claude runtime, `TurnHandle::interrupt` is cooperative. The turn
ends `Cancelled` and the Claude process is kept. What else stops depends on
what Claude is doing:

- **Claude is still working in the foreground.** The SDK sends Claude's
  `interrupt` control request with `cancel_queued`. It stops the reply being
  written, its foreground tools and any messages queued behind them. Claude
  treats this as a stop of the whole session's work, so it also stops
  *background subagents*; each one is reported as a `TaskActivity` with kind
  `Stopped`. Background shells keep running.
- **Claude has answered and only background work is running.** The SDK sends
  Claude nothing; it just ends the turn. Background subagents and shells keep
  running.

So interrupt only when the user asks to stop. To say something else while
background subagents work, send a message into the turn or start a new turn
instead; neither stops anything.

Until the next turn arrives the SDK keeps reading the process: their events, Claude's answers to their completion, and
any approval they request are held (bounded) and delivered at the start of
the next turn. A request event stays in the buffer for as long as its approval
is held. Requests beyond the bound are denied and reported as a warning on the
next turn. If Claude does not confirm the interruption within a few
seconds, the process is retired as before. A parked process with no background
work expires after the configured idle timeout.

### Start a new turn

- If the active turn has already produced its answer and is only waiting on
  background tasks, it hands its live process to the new turn. The active turn
  completes with its answer, the new prompt is written to the same Claude
  process, and the subagents keep running.
- The new turn inherits the running tasks: their `TaskActivity`, nested
  `ToolCall` events and Claude's answer to their completion are delivered on
  the new turn's stream, keyed by the original task IDs. It completes once its
  own answer is in and the inherited work has finished, or hands off again.
- Otherwise `start_turn` still fails with `RuntimeBusy`, for example while the
  active turn is still answering or while Claude is composing its answer to a
  finished task. Send the message into the running turn instead, or retry after
  the advised delay.

```rust,ignore
let follow_up = match handle.start_turn(input).await {
    Ok(turn) => turn,
    Err(failure) if failure.kind == RuntimeFailureKind::RuntimeBusy => {
        // Still answering: retry per `failure.retry`, or interrupt on request.
        return retry_later(failure);
    }
    Err(failure) => return Err(failure),
};
```

When background work drains, Claude answers each task notification with a
follow-up exchange. A retained turn stays open for that answer and completes a
few seconds after the work drains if Claude stays silent.

Without process retention every turn is its own process, so overlap is still
`RuntimeBusy` and background subagents end with the turn that started them.

A turn may change the model, effort or permission mode: the retained Claude
process switches them in place (`set_model`, `apply_flag_settings`,
`set_permission_mode`) before the prompt, and background work keeps running.
Other changes need a new process: the working directory, MCP servers or launch
context, sandbox, harness options, turning thinking off or ultracode on, going
back to the CLI's default model or effort after launching with one, and
entering bypass mode on a process launched without it. While background work
runs, such a turn fails with `RuntimeError::RestartWouldStopBackgroundWork`
(`RuntimeBusy`, `RequiresUserAction`, prompt not sent), naming the work it
would stop; nothing is stopped. Send it again once the work finishes, or stop
the work first. A process that refuses a switch is handled the same way.

To check the hand-off against an installed Claude CLI, run
`cargo run --example claude_background_handoff_smoke -- haiku`. It launches a
background subagent, sends a second prompt while it works, and passes once the
subagent finishes and its completion reaches the second turn.
`cargo run --example claude_live_messages_smoke -- haiku [scenario...]` checks
messages and interrupts end to end: messages answered by the running turn,
queued messages stopped with it, the same Claude process kept across an
interrupt, background shells and answered-turn subagents surviving it, a
background approval held until the next turn, and background work surviving a
change of model (`cfg-model`, `cfg-model-agent`), effort (`cfg-effort`) or
permission mode (`cfg-permission`).

## Persist and replay

Persist both event forms in the same ordered event journal as text, tools, and
approvals:

- upsert the task materialization from `TasksChanged`;
- append every `TaskActivity` to the timeline;
- associate `ToolCall` records by `task_id` when present;
- retain unknown provider-native task status strings for forward compatibility.

The adapter bounds the native task set to 32 and each text field to 4,000
characters; a workflow's text fields to 240 characters, its agents to 100,
phases to 32 and logs to the last 10. The public event enum is non-exhaustive, so consumers still need a
fallback match arm.

Codex and OpenCode do not currently emit these native task variants through
their bundled adapters. Their ordinary tool and text streams continue to use
the same `TurnEvent` enum.
