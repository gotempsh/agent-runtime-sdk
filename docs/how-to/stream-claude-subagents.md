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

A background subagent lives inside the Claude process, so interrupting the turn
that started it kills it. With `ProviderProcessRetention` enabled, do not
interrupt: call `RuntimeHandle::start_turn` for the new prompt first.

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
  finished task. Retry after the advised delay, or interrupt only if the user
  asked to stop.

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
`RuntimeBusy` and background subagents end with the turn that started them. A
configuration change that replaces the retained process (model, permissions,
sandbox) also ends them.

To check the hand-off against an installed Claude CLI, run
`cargo run --example claude_background_handoff_smoke -- haiku`. It launches a
background subagent, sends a second prompt while it works, and passes once the
subagent finishes and its completion reaches the second turn.

## Persist and replay

Persist both event forms in the same ordered event journal as text, tools, and
approvals:

- upsert the task materialization from `TasksChanged`;
- append every `TaskActivity` to the timeline;
- associate `ToolCall` records by `task_id` when present;
- retain unknown provider-native task status strings for forward compatibility.

The adapter bounds the native task set to 32 and each text field to 4,000
characters. The public event enum is non-exhaustive, so consumers still need a
fallback match arm.

Codex and OpenCode do not currently emit these native task variants through
their bundled adapters. Their ordinary tool and text streams continue to use
the same `TurnEvent` enum.
