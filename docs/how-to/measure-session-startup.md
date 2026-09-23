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
survive a host restart and are not durable invocation identifiers. Treat observers
as trusted: timing metadata can still reveal provider choice and activity. Do not
expose a shared observer stream across tenants.

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

## Optional live baseline

With an authenticated CLI installed, run:

```sh
cargo run --example startup_timings -- claude
cargo run --example startup_timings -- codex
```

These commands send two small real prompts and can incur model usage. They use a
temporary working directory, deny interactive approvals through the default
handler, and print timing metadata only. Provider-native session persistence can
still write to the CLI's normal home directory. The second turn resumes the first
session; the current driver still spawns a second process. Neither command
measures a prepared process, and the result is specific to the configured model,
provider account, network and host load.

## Initial live sample (2026-09-23)

One pair per provider on macOS, with normal installed CLI configuration, an
isolated temporary workspace and the prompt from the example. Provider pairs ran
concurrently on the same host, so these are diagnostic samples, not percentiles or
a controlled performance comparison. Compilation time is outside these samples.

| Provider / turn | Process spawned | First output | First assistant text |
| --- | ---: | ---: | ---: |
| Claude, new session | 4 ms | 666 ms | 5,274 ms |
| Claude, resumed session | 3 ms | 689 ms | 3,548 ms |
| Codex, new session | 6 ms | 122 ms | 5,236 ms |
| Codex, resumed session | 1 ms | 67 ms | 4,719 ms |

Both resumed turns launched a fresh process. Most observed first-text latency was
after process creation, but these samples cannot separate CLI initialization, MCP
setup, authentication, provider network latency and model computation. They do not
include Fleet's own context resolution, sandbox preparation or UI rendering. A
prepared process is not a promise of an immediate model response. Measure explicit
provider-ready and turn-accepted acknowledgements before assigning the remaining
latency to the model or predicting a speedup.
