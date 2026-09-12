# Keep tool processes running after a turn

Use the default `ToolProcessPolicy::PreserveOnCompletion` when an agent may
start a development server, watcher, worker, or another intentionally
long-running tool process. A natural provider exit does not make the SDK kill
the remaining provider process group.

```rust,no_run
use temps_agent_runtime::{Provider, ToolProcessPolicy, TurnRequest};

let mut request = TurnRequest::new(
    Provider::Claude,
    std::env::current_dir()?,
    "Start the development server and report its URL.",
);

// This is already the default. Set it explicitly when the product contract
// should be obvious at the call site.
request.tool_process_policy = ToolProcessPolicy::PreserveOnCompletion;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The provider process itself still exits at the end of a turn. Conversation
continuity uses the provider `session_id`; process preservation applies only to
descendants created by provider tools.

## Make the tool truly detachable

The child tool must not keep the provider's standard input, output, or error
pipes open. A process that retains one of those streams can prevent the
provider command from reaching EOF and therefore prevent the turn from
finishing.

Ask the provider to redirect output to an application-owned log and write
durable identity such as a PID, port, or service record. The host application
should own that record and expose explicit stop/restart actions.

`PreserveOnCompletion` controls SDK cleanup; it cannot stop a provider CLI from
killing its own tool children before the provider exits. Provider behavior can
change between CLI versions, so direct shell detachment is not a portable
lifecycle guarantee. Launch the process through the SDK's
`ManagedProcessSupervisor` and retain its handle or supervisor in application
state. See
[Manage background commands and services](manage-background-processes.md).

Do not work around provider cleanup by merely escaping the SDK's process group
with `setsid` or an equivalent. An untracked process that escapes the group also
escapes the SDK's cancellation and timeout cleanup. A real supervisor must own
both start and stop.

## Opt into turn-scoped cleanup

Use `TerminateOnCompletion` for short-lived automation where any surviving
tool child would be a leak:

```rust,no_run
# use temps_agent_runtime::{Provider, ToolProcessPolicy, TurnRequest};
# let mut request = TurnRequest::new(Provider::Codex, std::env::current_dir()?, "Run checks");
request.tool_process_policy = ToolProcessPolicy::TerminateOnCompletion;
# Ok::<(), Box<dyn std::error::Error>>(())
```

This terminates remaining descendants after the provider exits naturally.

## Understand the hard-stop cases

The preservation policy does not weaken cancellation safety. The SDK still
terminates the supervised process tree when:

- the caller cancels the turn;
- the turn times out;
- the event sink fails;
- the in-flight `run` future is dropped.

Once `run` has returned under `PreserveOnCompletion`, dropping `AgentRuntime`
does not kill preserved descendants. They are intentionally unmanaged by the
SDK. Prefer `ManagedProcessSupervisor` when the application must retain
authority over their lifecycle; dropping its final owner stops its managed
trees.

In particular, an outer `SandboxBackend` owns the wrapped command's containment
semantics. Confirm that the selected Nono mode or custom backend permits a
detached process to outlive its supervisor before promising persistent servers
to users. Never fall back to an unsandboxed launch just to keep a process alive.
