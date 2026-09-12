# Run agents through a custom execution transport

Use `ExecutionTransport` when Claude Code, Codex, OpenCode, or a managed
background command must execute somewhere other than the SDK host. Typical
targets include an SSH-accessible worker, container service, hosted sandbox,
or microVM. The optional Temps integration is one implementation of the same
public transport contract.

## Decide where readiness belongs

The provider executable belongs to the execution environment. A local
`claude` installation is irrelevant when the selected transport runs
`/usr/local/bin/claude` inside a sandbox.

Configure that path on the adapter and configure the transport on the runtime:

```rust,no_run
# use temps_agent_runtime::{AgentRuntime, SshHostKeyPolicy, SshTransport, providers::Claude};
# async fn build() -> Result<(), Box<dyn std::error::Error>> {
let transport = SshTransport::builder("worker.example.com")
    .user("agent")
    .identity_file("/run/secrets/worker-key")
    .known_hosts_file("/etc/agent-runtime/known_hosts")
    .host_key_policy(SshHostKeyPolicy::Strict)
    .build()?;
let mut builder = AgentRuntime::builder().transport(transport);
builder.register(Claude::with_executable("/usr/local/bin/claude"));
let runtime = builder.build()?;

let readiness = runtime
    .readiness(temps_agent_runtime::Provider::Claude)
    .await?;
# let _ = readiness;
# Ok(())
# }
```

`AgentRuntime::readiness` asks the configured transport to probe the adapter's
executable. It never requires the same binary to exist on the Rust host.

For onboarding, call `runtime.discover_harnesses().await` instead. It probes
every registered provider concurrently and combines executable readiness,
permission support, transport capabilities, compatibility limitations, and
typed per-provider failures. See [Discover provider harnesses](discover-harnesses.md).

## Implement the process boundary

An agent transport is a byte-stream process API, not a human PTY. JSON provider
protocols require exact stdout framing and, for Claude, writable stdin after
startup for approvals, questions, and background-subagent activity.

Implement these operations:

```rust,ignore
#[async_trait]
impl ExecutionTransport for HostedTransport {
    fn name(&self) -> &'static str { "hosted-sandbox" }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: true,
            interactive_stdin: true,
            reconnect: true,
            managed_processes: true,
            process_tree_termination: true,
            sandbox: SandboxCapabilities {
                filesystem: true,
                process_isolation: true,
                audit: true,
                ..SandboxCapabilities::NONE
            },
        }
    }

    async fn readiness(&self, request: TransportReadinessRequest)
        -> TransportResult<ProviderReadiness>
    {
        // Run request.program --version inside the selected sandbox.
    }

    async fn validate_working_directory(&self, path: &Path)
        -> TransportResult<()>
    {
        // Inspect the sandbox filesystem, not the SDK host filesystem.
    }

    async fn spawn(&self, request: TransportSpawnRequest)
        -> TransportResult<TransportProcess>
    {
        // Send separated program, argv, environment, cwd, and stdin metadata
        // to the sandbox process bridge. Return streaming byte pipes plus a
        // TransportProcessControl implementation.
    }
}
```

`TransportProcessControl::terminate` must terminate the complete remote process
tree. Its `Drop` implementation must initiate best-effort cleanup. Because an
async network request cannot run in Rust `Drop`, remote systems should also use
bounded process leases that expire if the SDK host disappears.

## Use the built-in SSH transport

`SshTransport` uses the local OpenSSH client, key-only batch authentication,
strict or accept-new host-key verification, bounded connection deadlines, and
POSIX-safe quoting for every remote command component:

```rust,no_run
use std::time::Duration;
use temps_agent_runtime::{SshHostKeyPolicy, SshTransport};

let transport = SshTransport::builder("worker.example.com")
    .user("agent")
    .port(22)
    .identity_file("/run/secrets/worker-key")
    .known_hosts_file("/etc/agent-runtime/known_hosts")
    .host_key_policy(SshHostKeyPolicy::Strict)
    .connect_timeout(Duration::from_secs(10))
    .build()?;
# Ok::<(), temps_agent_runtime::TransportError>(())
```

Each command writes its remote process-group ID to a mode-0600 lease file.
Cancellation, timeout, managed-service stop, and dropped process ownership use
a second authenticated SSH control command to terminate the complete remote
group. Merely killing the local `ssh` process is insufficient because many SSH
servers leave the remote session orphaned.

The transport preserves writable stdin, so Claude approval and question frames
can flow through the ordinary `InteractionHandler`. `reconnect` is false: use
a sidecar or another framed transport when process adoption after SDK-host
restart is required.

SSH executable discovery and execution share one cached remote login
environment. The transport selects the user's Zsh or Bash and captures only
`HOME`, `PATH`, and `SHELL`; it does not copy arbitrary variables or credentials
from the login shell. A missing supported shell or malformed environment probe
is returned as a typed transport error.

## Optional: connect to a Temps sandbox

`TempsSandboxTransport` targets an existing sandbox through
`/api/v1/sandboxes`. It supports structured argv/env/cwd, staged initial stdin,
incremental output, native job handles, `attach`, cancellation, and managed
services. It advertises filesystem and process isolation by default; override
the intrinsic capability set only when the selected sandbox profile actually
enforces more.

The base URL must not contain embedded credentials, a query, or a fragment.
HTTPS is required for non-loopback destinations by default. If a private
encrypted network such as WireGuard or Tailscale protects cleartext HTTP end to
end, opt in explicitly with `allow_insecure_http(true)`; never enable it for an
ordinary routed network carrying bearer or session credentials.

This transport is an optional vendor integration. Local execution, SSH, custom
transports, managed processes, persistence traits, and sandbox backends do not
depend on Temps. Enable it explicitly with `cargo add temps-agent-runtime
--features temps-sandbox`.

The current Temps HTTP command API does **not** expose writable stdin after
spawn. Therefore this transport reports `interactive_stdin = false`: Codex and
OpenCode non-interactive turns can run, but Claude live approvals fail before
spawn with `RuntimeError::TransportCapabilityUnavailable { capability:
"interactive_stdin", .. }`.

Temps' `/terminal` WebSocket is a human PTY. It merges stdout/stderr, applies
terminal line discipline, and may replay scrollback, so it must not be used as
a byte-exact NDJSON agent transport. Full live approvals over Temps need a raw
exec WebSocket with separate stdout/stderr, writable stdin, exit status, and a
stable process handle.

## Adapt a hosted sandbox

When a hosted sandbox has no native Rust client, put its supported SDK behind a
small authenticated sidecar. The Rust transport talks to the sidecar using the
same framed process protocol. This keeps provider parsing, approvals, task
events, and storage projections in Rust without reproducing the vendor SDK.

For reusable images, install and authenticate the provider CLI inside a base
snapshot. Restore the workspace into that environment before readiness and
execution. Preserve the provider's home/session files when session resume must
work across processes.

## Require sandbox controls

Declare controls enforced by the transport and require them on the turn:

```rust,no_run
# use temps_agent_runtime::{Provider, SandboxCapabilities, TurnRequest};
let mut request = TurnRequest::new(
    Provider::Claude,
    "/workspace/repository",
    "Review the current changes.",
);
request.required_sandbox_capabilities = SandboxCapabilities {
    filesystem: true,
    network_allowlist: true,
    process_isolation: true,
    audit: true,
    ..SandboxCapabilities::NONE
};
```

The runtime combines intrinsic transport controls with an optional
`SandboxBackend`. Missing required controls fail before the provider starts.
There is no local or unsandboxed fallback.

## Use the transport for managed commands

Use the same transport instance for services and background commands:

```rust,ignore
let transport: Arc<dyn ExecutionTransport> = Arc::new(configured_transport);

let runtime = AgentRuntime::builder()
    .transport_from_arc(transport.clone())
    .build()?;

let processes = ManagedProcessSupervisor::builder()
    .transport_from_arc(transport)
    .build()?;
```

`ManagedProcessSnapshot::transport_handle` exposes the latest native process
identity for persistence and reconciliation. A custom transport can implement
`attach` for its own consumers; the current in-process supervisor does not
automatically adopt stored processes after a host restart.

## React to typed failures

Every transport operation returns `TransportError`. Match `kind` to drive UI:

- `AuthenticationFailed`: replace or refresh the SSH/API credential.
- `HostKeyVerificationFailed`: verify the fingerprint, then update
  `known_hosts` explicitly.
- `ConnectionRefused`, `ConnectionTimedOut`, or `RemoteUnavailable`: offer a
  bounded retry.
- `WorkingDirectoryNotFound` or `ExecutableNotFound`: repair the remote image
  or workspace before retrying the original step.
- `PermissionDenied`: change the remote identity or sandbox policy.

Unsuccessful provider exits additionally expose `ProviderProcessErrorKind`,
including authentication, permission, model, rate-limit, and network failures.
