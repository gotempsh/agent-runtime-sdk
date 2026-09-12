# Manage background commands and services

Use `ManagedProcessSupervisor` when a command must have a lifecycle independent
from an agent turn. The supervisor starts a direct executable, captures bounded
stdout and stderr, streams typed lifecycle events, applies a restart policy,
and owns the entire process tree.

## Keep the supervisor in application state

```rust,no_run
use temps_agent_runtime::{ManagedProcessSpec, ManagedProcessSupervisor};

# async fn start() -> Result<(), Box<dyn std::error::Error>> {
let processes = ManagedProcessSupervisor::new();

let web = processes.start(
    ManagedProcessSpec::service(
        "docs server",
        "bun",
        std::env::current_dir()?.join("site"),
    )
    .args(["run", "dev"]),
).await?;

println!("managed id: {}", web.id());
# Ok(())
# }
```

`service` defaults to `RestartPolicy::OnFailure`. Use
`ManagedProcessSpec::background` for a one-shot command; it defaults to
`RestartPolicy::Never`. No shell parses the program or arguments.

Store the supervisor in long-lived application state. A process survives the
agent turn that requested it because `AgentRuntime` does not own it. It remains
owned while either the supervisor or a returned handle exists. Dropping the
last owner stops the complete process tree, so an accidental orphan is not the
default.

To run the process in a remote environment, configure the supervisor with the
same `ExecutionTransport` used by `AgentRuntime`. This can be an SSH worker,
hosted sandbox, container service, or optional vendor-specific transport. The
executable and working directory are then resolved inside that transport, and
snapshots expose its native process handle.

## Stream status and logs

`ManagedProcessHandle::recv` returns future `ManagedProcessEvent` values:

- `StatusChanged` contains a complete snapshot;
- `Log` contains one bounded stdout or stderr line;
- `RestartScheduled` identifies the attempt and delay.

```rust,no_run
# use temps_agent_runtime::{ManagedProcessEvent, ManagedProcessHandle};
# async fn follow(mut process: ManagedProcessHandle) -> Result<(), Box<dyn std::error::Error>> {
while let Ok(event) = process.recv().await {
    match event {
        ManagedProcessEvent::Log { line, .. } => {
            println!("[{}] {}", line.stream, line.text);
        }
        ManagedProcessEvent::StatusChanged { snapshot } => {
            println!("{:?}: {}", snapshot.status, snapshot.detail);
        }
        _ => {}
    }
}
# Ok(())
# }
```

The live channel is bounded and does not replay. On reconnect, subscribe first,
then read `snapshot(id)` and `logs(id)`, and discard duplicate log sequence
numbers. This closes the gap between state hydration and live delivery.

## Control and retain records

The supervisor exposes `list`, `snapshot`, `logs`, `subscribe`, `stop`,
`restart`, and `delete`. `stop` preserves the record and its log tail;
`restart` uses the original specification; `delete` stops the process before
removing its in-memory record.

Default safety bounds are 32 retained process records, 1,000 log lines per
record, 4,000 characters per line, and 256 pending stream events. Customize
them through `ManagedProcessSupervisor::builder()`.

Environment values use `SecretString`, are omitted from snapshots and events,
and are redacted from `Debug`. Arguments are not secret storage: keep
credentials in the environment or another OS-native secret channel.

## Persist the product view

The supervisor is deliberately in-process. Persist snapshots and events in the
host application's database when the UI must survive a host restart. After a
host crash, mark previously running SDK-owned processes `interrupted`; the SDK
does not claim it can safely reattach using only a PID. If survival across host
restart is required, implement an OS service-manager adapter above this API and
store its native handle.

Run the complete example with:

```bash
cargo run --example managed_service
```

For agent-launched descendants, see
[Keep tool processes running](keep-tool-processes-running.md). For durable
projection rules, see
[Persist command and tool execution](persist-command-execution.md).
