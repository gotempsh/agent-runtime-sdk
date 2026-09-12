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
