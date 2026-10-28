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

## Background commands of a one-shot Codex turn

A background command the Codex app-server started with its own shell tool
belongs to the app-server process, not to the SDK. In a one-shot turn the
SDK closes the app-server's stdin at `turn/completed`, so the app-server exits
and takes such a command with it, whatever the policy says. This was observed
with `codex app-server` 0.162: a `sleep 15` command started by a turn that
had already ended never wrote its finish marker.

Opt into process retention when a command must outlive the turn. The retained
app-server keeps owning it, and the SDK keeps the process alive while
`thread/backgroundTerminals/list` reports a live terminal, past the idle
timeout. [Retained runtimes](../reference/retained-runtimes.md) shows how to
enable it. For a command whose
lifetime the application must control, use `ManagedProcessSupervisor`. The
`background_terminal_retention_smoke` example checks the retained case against
a real Codex.

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
with `setsid` or an equivalent. Local Linux/macOS launches retain SDK ownership
across session changes and reparenting through an inherited environment marker.
A real supervisor must own both start and stop.

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

For local Linux and macOS launches, the SDK injects a fresh random
`TEMPS_AGENT_PROCESS_SCOPE` value for each provider process. Cleanup first kills
the provider group, then discovers processes with that exact inherited marker,
including daemons that called `setsid` and were reparented before cleanup began.
It rechecks the marker and start time before signaling each PID, with up to
three discovery passes to catch children forked during cleanup. Separate SDK
launches receive separate scopes, including managed services. No process is
selected by executable name, and environment values are never logged.

This is best-effort lifecycle cleanup, not an OS containment boundary. A tool
that clears the marker, changes user, or runs where process environments cannot
be inspected can escape it. Environment inspection and PID signaling are not
atomic on macOS; identity rechecks reduce, but cannot eliminate, PID reuse
races. Arbitrary forking during the bounded cleanup can also escape. Remote
transports retain their existing remote cleanup behavior; Windows and other
Unix platforms retain their existing tree/group cleanup. The SDK cannot run
cleanup after the embedding host is killed or crashes. Use an outer supervisor,
container/cgroup, or explicit service registration for those guarantees.

`PreserveOnCompletion` still leaves these daemons running after successful
completion. Applications using that policy must close ephemeral browser
sessions themselves, or choose `TerminateOnCompletion` for that workload.
Already-running unmarked daemons cannot be adopted retroactively.

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
