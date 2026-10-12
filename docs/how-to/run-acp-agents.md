# Run Agent Client Protocol agents

`providers::acp::Acp` runs any agent that speaks the
[Agent Client Protocol](https://agentclientprotocol.com) (ACP) over stdio:
Gemini CLI, GitHub Copilot, Cursor, Kimi, goose, Qwen Code and the rest of the
[ACP registry](https://agentclientprotocol.com/registry). One adapter,
`Provider::Acp`, serves them all; the application registers each agent's
command and picks one per turn.

```rust
use temps_agent_runtime::providers::acp::AGENT_OPTION;
use temps_agent_runtime::providers::{Acp, AcpAgent, AcpAgents};
use temps_agent_runtime::{AgentRuntime, PermissionMode, Provider, TurnRequest};

let agents = AcpAgents::new();
agents.insert(AcpAgent::new("gemini", "Gemini CLI", "gemini", ["--acp"]))?;

let mut builder = AgentRuntime::builder();
builder.register(Acp::new(agents.clone()));
let runtime = builder.build()?;

let mut request = TurnRequest::new(Provider::Acp, "/path/to/project", "Fix the failing test");
request.harness_options.insert(AGENT_OPTION.into(), "gemini".into());
request.permission_mode = PermissionMode::Default;
let result = runtime.run(request, &events, Some(&approvals)).await?;
```

The working directory must be absolute: ACP sends it to the agent as `cwd`,
and the agent starts inside it, so a relative path would resolve a second time.
A relative one fails the turn before the agent starts.

`AcpAgents` is shared: inserting, replacing or removing an agent on any clone
takes effect for the next turn of every runtime holding it.

## What a turn does

1. `initialize` with protocol version 1. The client offers no `fs` or
   `terminal` capability, so the agent uses its own tools in the working
   directory, inside whatever sandbox wraps the process.
2. `session/new`, or for `TurnRequest::session_id`: `session/resume` when the
   agent advertises it, else `session/load` (its history replay is dropped).
   An agent with neither starts a new session and emits a warning. A resumed
   session the agent no longer knows fails with `acp::session_not_found`.
3. `PermissionMode::Custom(mode)` selects that session mode
   (`session/set_mode`); `TurnRequest::model` selects a model through the
   session's `model` config option (or `session/set_model`). An unknown mode
   or model fails before the prompt, naming the choices.
4. `session/prompt`. `session/update` notifications become `TextDelta`,
   `ReasoningDelta`, `ToolCall`, `PlanUpdated`, `Usage` and `Warning` events;
   the prompt's `stopReason` ends the turn.

`launch_context.system_prompt_append` is sent as a leading text block of a new
session's first prompt (ACP has no system prompt). Stdio and HTTP MCP servers
are passed in the session request with their referenced variables resolved
from `TurnRequest::environment`; HTTP servers are dropped with a warning when
the agent does not support them.

## Permissions

| `PermissionMode` | `session/request_permission` |
| --- | --- |
| `Default`, `Custom(_)` | Every request reaches the `InteractionHandler` |
| `AcceptEdits` | `read`, `edit`, `delete`, `move` and `search` kinds are allowed; others reach the handler |
| `FullAccess` | Every request is allowed |
| `Plan` | Rejected: ACP has no client-enforceable read-only mode |

An agent asks only when it decides to; an agent configured to run tools
without asking never sends a request.

## Check an agent

```rust
let probe = Acp::probe_agent(&agent, &scratch_dir, true, Duration::from_secs(30)).await?;
match probe.authentication {
    AcpAuthentication::Authenticated => { /* ready */ }
    AcpAuthentication::Required { message } => { /* show the agent's sign-in hint */ }
    _ => { /* unknown */ }
}
```

`probe_agent` starts the agent locally, runs `initialize` and, with
`check_session`, `session/new` in the given (absolute) directory, then stops
it. It
reports the protocol version, agent name and version, sign-in methods and
session capabilities. Agents may record that empty session in their history,
so run it on an explicit user action.

## Limits

- One process per turn; the process is not retained across turns.
- An agent that keeps running after the turn ends is stopped after
  `terminal_exit_grace` (3 seconds), which also stops tool processes it left
  running.
- Questions (elicitation) and client-side file system and terminal methods
  are not offered.
