# temps-agent-runtime

`temps-agent-runtime` is a provider-neutral Rust runtime for installed coding
agents. It starts Claude Code, Codex, or OpenCode as a supervised child process,
normalizes their JSON event streams, bridges approvals, and can place the whole
turn inside a managed [Nono](https://nono.sh/) sandbox.

The crate is an independent runtime library. Any Rust application can embed it
without adopting a particular HTTP API, database, job queue, UI framework, or
authorization model; those concerns remain in the host application.

> Status: early `0.1` API. Claude's bidirectional CLI protocol is the most
> complete adapter. See the [capability matrix](docs/reference/api.md) before
> replacing an existing integration.

## Why this exists

Agent SDKs and headless CLIs still expose different commands and event shapes.
Applications otherwise end up duplicating process supervision, cancellation,
permission mapping, JSON parsing, secret handling, and sandbox setup.

This crate provides one small boundary:

```text
Host application
       │ TurnRequest + TurnEvent
       ▼
temps-agent-runtime
        │ optional Nono supervisor
        ▼
Claude Code / Codex / OpenCode executable
```

It deliberately invokes installed executables. It does not reimplement model
APIs, own credentials, or silently fall back to an unsandboxed process.

## Features

- Typed requests, streaming events, terminal results, and errors
- Claude Code, Codex, and OpenCode adapters behind independent Cargo features
- Live approvals with enforced per-turn permission policy: Codex through
  `codex app-server`, OpenCode through `opencode serve`
- Bounded concurrent turns and bounded provider output
- Deadlines, cooperative cancellation, and process-tree cleanup
- Long-running tool processes preserved after natural turn completion by
  default, with explicit turn-scoped cleanup when desired
- Owned background commands and managed services with bounded logs, typed live
  events, restart policies, and explicit stop/restart/delete control
- Claude native Task/Agent subagents as task snapshots, activity events, and
  nested tool-call attribution
- Claude context-window occupancy, native automatic-compaction boundaries, and
  durable provider-native manual compaction
- Fetch-on-demand provider account quota with session/weekly reset windows,
  plan metadata, credits, and typed unavailable states on local or remote targets
- Fail-closed approval and question handling
- Sanitized child environments and redacted secret wrappers
- Pluggable `SandboxBackend` interface with per-turn capability requirements
- Nono discovery, capability reporting, strict profile validation, atomic
  managed-profile activation, per-turn path grants, and credential proxy names
- Trait-based managed profile resolution and optimistic revision updates, with
  approved, bounded same-session retry after a classified sandbox denial
- Public adapter trait for richer protocols or additional providers
- Built-in `LocalTransport` and interactive `SshTransport`, optional
  `TempsSandboxTransport`, plus the public transport trait for hosted microVMs
- Transport-aware aggregate harness discovery for bounded local, SSH, and
  hosted-sandbox onboarding
- Explicit host-scoped skill search and sanitized MCP inventory, with typed
  management access or a concrete denial reason
- Provider-neutral launch context for host-owned system instructions, exact
  tool availability, and secret-safe MCP capability injection in Claude Code
- Opt-in Agent Relay contracts and a scoped MCP bridge for application-owned,
  durable messaging between independent top-level agents
- Typed SSH/API/provider failures for authentication, host keys, connectivity,
  permissions, missing remote paths, models, rate limits, and process control

## Install

Requires Rust 1.88 or newer. Normal library tests use fake provider processes and
do not require provider accounts or a running Temps installation.

Until the first crates.io release, place the crate beside your application and
use a path dependency:

```toml
[dependencies]
temps-agent-runtime = { path = "../temps-agent-runtime" }
```

After publication:

```toml
[dependencies]
temps-agent-runtime = "0.1"
```

Default features enable every bundled provider adapter, Nono, and SSH. The
Temps sandbox integration is opt-in through the `temps-sandbox` feature. A host
can select only what it uses:

```toml
temps-agent-runtime = { version = "0.1", default-features = false, features = ["codex", "nono"] }
```

The provider CLI must be installed and authenticated for the same OS user as
the host process. Nono is required only when a request includes a Nono policy.

## Quick start

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{
    AgentRuntime, EventSink, Provider, Result, TurnEvent, TurnRequest,
};

struct Events;

#[async_trait]
impl EventSink for Events {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        // Tool payloads can contain secrets; do not log the entire event.
        if let TurnEvent::TextDelta { text } = event {
            print!("{text}");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = AgentRuntime::builder()
        .concurrency_limit(2)
        .build()?;
    let request = TurnRequest::new(
        Provider::Claude,
        std::env::current_dir()?,
        "Summarize this repository without changing files.",
    );
    let result = runtime.run(request, &Events, None).await?;
    println!("{}", result.text);
    Ok(())
}
```

`None` uses the fail-closed interaction handler: approval requests are denied
and questions are left unanswered. Production applications should implement
`InteractionHandler` and connect it to their durable approval workflow.

For a complete walkthrough, read the [quickstart](docs/tutorials/quickstart.md).
For long Codex threads, read [Resume large Codex conversations](docs/how-to/resume-large-codex-conversations.md).

For sandbox setup, read [Manage Nono sandboxes](docs/how-to/nono.md) or
[Implement a sandbox backend](docs/how-to/custom-sandbox.md). For durable
profile updates and retry, read
[Recover a denied sandbox step](docs/how-to/recover-sandbox-denials.md).
To give an agent one Tailscale or Headscale network without changing the
host's own login, read [Give an agent its own tailnet](docs/how-to/tailnets.md).
For target onboarding, read
[Discover provider harnesses](docs/how-to/discover-harnesses.md).
For host-scoped skills and MCP servers, read
[Discover and manage harness extensions](docs/how-to/discover-harness-extensions.md).
For per-runtime system instructions, tool restrictions, and MCP capabilities,
read [Configure launch context](docs/how-to/configure-launch-context.md).
For durable, explicitly authorized communication between independent agents,
read [Relay messages between independent agents](docs/how-to/agent-relay.md).
For context meters and automatic/manual compaction in a durable host, read
[Integrate context usage and compaction with Fleet](docs/how-to/integrate-context-compaction-with-fleet.md).

## Design boundaries

- Provider and user-supplied executable arguments stay separated. The SSH
  transport and bounded host-extension helpers use fixed POSIX shell scripts;
  untrusted values remain positional arguments and managed paths are derived
  from validated names.
- Prompts are not logged. Claude and Codex prompts use stdin, and OpenCode's
  `Serve` turn mode posts the prompt in an HTTP body. OpenCode's headless
  `Run` mode accepts the message as an argument, so it may be visible to local
  process inspection; see the capability matrix.
- The runtime keeps no database and emits no telemetry. The embedding
  application owns both.
- Agent Relay never connects agents directly. The application owns logical
  addresses, grants, approval, durable queues, scheduling, idempotency, and
  at-least-once delivery; the SDK supplies contracts and an opt-in MCP bridge.
- Provider readiness and working-directory validation occur inside the selected
  execution transport. Remote execution never requires the CLI or workspace to
  exist on the Rust host.
- Sandbox capability or preparation errors stop the turn. There is no automatic
  unsandboxed retry.
- `run_with_sandbox_recovery` is an explicit managed-profile flow. It updates a
  validated revision before resuming the same provider session; ordinary
  `run` never mutates profiles.
- `ToolProcessPolicy` preserves detached tools after a natural provider exit by
  default. Cancellation, timeout, sink failure, and a dropped in-flight future
  always retain process-tree cleanup.
- `ManagedProcessSupervisor` is the provider-independent path for processes
  that must outlive a turn. The host retains its supervisor or handle and owns
  durable persistence; dropping the final owner stops the process tree.
- Nono `run` and `wrap` do not enforce the same controls. The library reports
  their actual capabilities rather than treating them as interchangeable.

## Documentation

- [Documentation portal source](site/)
- [Embedding a retained runtime daemon](docs/daemon-stream.md)
- [Quickstart tutorial](docs/tutorials/quickstart.md)
- [How to manage Nono sandboxes](docs/how-to/nono.md)
- [How to give an agent its own tailnet](docs/how-to/tailnets.md)
- [How to implement a sandbox backend](docs/how-to/custom-sandbox.md)
- [How to implement a custom execution transport](docs/how-to/custom-transport.md)
- [How to discover harnesses on a target](docs/how-to/discover-harnesses.md)
- [How to display provider account usage](docs/how-to/display-account-usage.md)
- [How to discover and manage harness extensions](docs/how-to/discover-harness-extensions.md)
- [How to configure system instructions, tools, and MCP servers](docs/how-to/configure-launch-context.md)
- [How to relay messages between independent agents](docs/how-to/agent-relay.md)
- [How to integrate context usage and compaction with Fleet](docs/how-to/integrate-context-compaction-with-fleet.md)
- [How to adopt SDK security hardening in Temps Fleet](docs/how-to/adopt-security-hardening-in-fleet.md)
- [How to recover a denied sandbox step](docs/how-to/recover-sandbox-denials.md)
- [How to keep tool processes running](docs/how-to/keep-tool-processes-running.md)
- [How to manage background commands and services](docs/how-to/manage-background-processes.md)
- [How to stream Claude native subagents](docs/how-to/stream-claude-subagents.md)
- [How to persist approvals](docs/how-to/persist-approvals.md)
- [How to persist command execution](docs/how-to/persist-command-execution.md)
- [How to operate sandboxed turns](docs/how-to/operate-sandboxes.md)
- [API and capability reference](docs/reference/api.md)
- [Architecture and security model](docs/explanation/architecture.md)
- [Adoption guide for existing applications](docs/MIGRATION.md)
- [Full-stack Axum + React example](examples/fullstack-chat/README.md)

Build the branded portal and compiler-generated Rust API together:

```bash
cd site
bun install
bun run build
```

The static output is written to `site/dist`; generated Rustdoc is available
under `site/dist/api/temps_agent_runtime/`.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo test --test nono_live --all-features -- --ignored
```

The live Nono test is optional and requires `nono` on `PATH`. Provider unit and
runtime tests do not make model API calls.

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md) before
submitting changes.
Maintainers can use the [source-readiness and release checklist](docs/open-source-readiness.md)
to separate local verification from publishing and hosting decisions.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at
your option.

- [Measure session startup](docs/how-to/measure-session-startup.md)
