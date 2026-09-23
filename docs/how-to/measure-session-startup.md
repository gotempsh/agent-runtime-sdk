# Measure session startup

Enable an optional observer when constructing the runtime to find where a turn
spends time. This does not change process lifetime, add model requests, or change
provider events or the remote protocol.

```rust
use std::sync::Arc;
use temps_agent_runtime::{AgentRuntime, StartupObserver, StartupTiming};

struct Timings;
impl StartupObserver for Timings {
    fn observe(&self, timing: StartupTiming) {
        // For production exporters, use a bounded channel's try_send here.
        // Keep this callback quick: it runs on the execution task.
        eprintln!("{} {:?} {:?} {:?}", timing.observation_id,
                  timing.provider, timing.stage, timing.elapsed);
    }
}

let runtime = AgentRuntime::builder()
    .startup_observer(Arc::new(Timings))
    .build()?;
# Ok::<(), temps_agent_runtime::RuntimeError>(())
```

Samples contain only a process-local observation ID, provider, stage and monotonic
elapsed duration. No prompt, path, provider session ID, account, environment value
or error diagnostic is included. IDs distinguish concurrent runs, but do not
survive a host restart and are not durable invocation identifiers.

For `run`, elapsed time begins before validation. Subtract consecutive timestamps
to locate waits:

- `WaitingForPermit` to `PermitAcquired`: process concurrency queue.
- `CommandPrepared` to `SandboxPrepared`: sandbox preparation, when configured.
- The last preparation boundary to `ProcessSpawned`: transport process launch.
- `ProcessSpawned` to `InitialInputWritten`: initial stdin flush, when present.
- `StreamsAttached` to `FirstOutput`: waiting for the first provider output line.
- `FirstOutput` to `FirstText`: protocol initialization and provider/model work.

`FirstOutput` may be a handshake, warning or error. `InitialInputWritten` means
bytes were flushed, not that a prompt was accepted. `StreamsAttached` is not
universal provider readiness. Claude and Codex may perform initialization after
these boundaries. Only the first nonempty normalized text delta triggers
`FirstText`, and a tool-only or failed turn may never reach it. These observations
do not claim to separate MCP readiness from model latency yet.

Exactly one terminal observation closes a polled run: `Succeeded`, `Failed`,
`Cancelled`, `TimedOut`, or `Abandoned` when its future is dropped. Observation
callbacks are synchronous and must not block. Use bounded export with dropped
samples under overload; observer panics are contained under unwinding builds
(the standard panic hook can still report them). As with other Rust panics,
`panic=abort` terminates the process.

For `run_with_sandbox_recovery`, each provider attempt receives a separate
observation ID after sandbox policy resolution and concurrency admission. Its
elapsed time excludes those outer steps. Attempts can therefore be compared for
process startup, but should not be used as whole-operation latency.

No observer is installed by default. Normalized provider events and invocation
journals stay unchanged. Built-in retained clients still report
`retained_process: false`; keeping a logical session does not avoid process
startup. See [the preparation proposal](../adr/0004-prepared-provider-processes.md)
for the opt-in process-retention work needed before early preparation can help.
