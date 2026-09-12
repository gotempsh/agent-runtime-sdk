# Run your first agent turn

This tutorial runs one installed coding-agent CLI through the shared Rust
runtime and consumes its normalized event stream. It is written for maintainers
embedding the crate in a Tokio application.

## 1. Add the dependency

During local development, place `temps-agent-runtime` beside your application
repository and use a path dependency:

```toml
[dependencies]
async-trait = "0.1"
temps-agent-runtime = { path = "../temps-agent-runtime" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Default features include Claude Code, Codex, OpenCode, and Nono. Use
`default-features = false` with a provider feature when you need a smaller
dependency surface.

## 2. Check readiness

Readiness confirms that the executable can be resolved and obtains a
best-effort version. Authentication is still checked by the provider when the
first turn starts.

```rust,no_run
use temps_agent_runtime::{AgentRuntime, Provider};

# async fn check() -> Result<(), Box<dyn std::error::Error>> {
let runtime = AgentRuntime::builder().build()?;
let readiness = runtime.readiness(Provider::Claude).await?;
if !readiness.installed {
    return Err(readiness.detail.into());
}
# Ok(())
# }
```

## 3. Receive events

An `EventSink` is backpressured: the runtime waits for `emit` before reading and
delivering more provider events. Keep this method bounded. For durable product
state, enqueue or store the event before returning.

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{EventSink, Result, TurnEvent};

struct Events;

#[async_trait]
impl EventSink for Events {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        match event {
            TurnEvent::TextDelta { text } => print!("{text}"),
            TurnEvent::ToolCall { name, status, .. } => {
                eprintln!("tool {name}: {status:?}");
            }
            _ => {}
        }
        Ok(())
    }
}
```

Tool inputs and outputs can contain user data or secrets. Treat them as
sensitive when persisting or logging events.

## 4. Run a turn

```rust,no_run
# use temps_agent_runtime::{AgentRuntime, Provider, TurnRequest};
# struct Events;
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let runtime = AgentRuntime::builder()
    .concurrency_limit(2)
    .build()?;

let mut request = TurnRequest::new(
    Provider::Claude,
    std::env::current_dir()?,
    "Explain the main modules. Do not edit files.",
);
request.timeout = std::time::Duration::from_secs(15 * 60);

let result = runtime.run(request, &Events, None).await?;
println!("\nstatus: {:?}", result.status);
# Ok(())
# }
```

The runtime bounds concurrent child processes globally. A natural provider
exit preserves intentionally detached tool processes by default, so a
development server can remain available after the turn. Cancellation, timeout,
event-sink failure, and dropping the in-flight future still terminate the
provider tree. See [Keep tool processes running](../how-to/keep-tool-processes-running.md)
for the explicit policy and detachment requirements.

## 5. Add interactive approvals

Passing `None` fails closed. To support Claude approval and question frames,
implement `InteractionHandler` and connect it to the host application's durable
workflow:

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{
    ApprovalDecision, ApprovalRequest, InteractionHandler, QuestionAnswer,
    QuestionRequest,
};

struct ProductApprovals;

#[async_trait]
impl InteractionHandler for ProductApprovals {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        // Replace with your database + UI approval bridge.
        ApprovalDecision::Deny {
            reason: Some(format!("{} requires explicit review", request.tool_name)),
        }
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}
```

The runtime applies a separate interaction deadline. A timed-out approval is
denied; a timed-out question is unanswered.

## Next steps

- Use [Nono management](../how-to/nono.md) for an outer OS sandbox.
- Implement a [custom sandbox backend](../how-to/custom-sandbox.md) for another
  isolation system.
- Consult the [capability matrix](../reference/api.md) before choosing an
  adapter or replacing an existing integration.
- Use the [managed process supervisor](../how-to/manage-background-processes.md)
  for servers, watchers, and commands that must outlive a turn.
- Consume [Claude native subagent events](../how-to/stream-claude-subagents.md)
  when the UI should show Task/Agent progress and nested tools.
- Follow the [adoption guide](../MIGRATION.md) to introduce the runtime behind
  an existing application boundary.
