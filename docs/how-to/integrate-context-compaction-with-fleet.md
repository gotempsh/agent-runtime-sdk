# Integrate context usage and compaction with Fleet

This guide describes the application boundary for Temps Fleet and similar durable hosts. The SDK
owns harness configuration, Claude stream parsing, normalized events, and the manual compaction
operation. Fleet owns authorization, durable operation IDs, event persistence, replay, and UI.

Claude Code is currently the implementing driver. Inspect
`RuntimeHandle::driver_capabilities()` before exposing controls; do not infer support from the
provider name in application code.

## Configure automatic compaction

Set a default on the retained runtime:

```rust,no_run
use temps_agent_runtime::{AutoCompactionPolicy, Provider};
use temps_agent_runtime::lifecycle::RuntimeId;
use temps_agent_runtime::retained::RuntimeSpec;

let mut spec = RuntimeSpec::new(
    RuntimeId::new("fleet-conversation-42")?,
    Provider::Claude,
    "/workspace/project",
);
spec.auto_compaction = AutoCompactionPolicy::Automatic;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The supported policies are:

- `ProviderDefault`: do not override the harness configuration;
- `Automatic`: ask Claude Code to choose its automatic threshold;
- `TokenThreshold { tokens }`: use an explicit Claude threshold from 100,000 through 1,000,000.

For a one-invocation override, set `TurnInput::auto_compaction`. `None` inherits the runtime
default. The retained driver reports `configurable_auto_compaction`; unsupported settings fail
closed rather than being silently ignored.

Automatic compaction does not create a separate Fleet message. It occurs inside an ordinary turn
and emits `TurnEvent::CompactionCompleted` at the exact provider boundary.

## Persist context-window usage

`TurnEvent::Usage` and `TurnResult::usage` now include:

```text
input_tokens
output_tokens
cache_creation_input_tokens
cache_read_input_tokens
context_window.used_tokens
context_window.limit_tokens
context_window.model
context_window.estimated
cost_usd
```

`context_window` is a point-in-time occupancy snapshot, not cumulative billing usage. For Claude,
the SDK derives occupancy from the input, cache-creation, cache-read, and output components in the
assistant frame. Cached input still occupies context even when billing treats it differently, so
Fleet must not display only `input_tokens` as context usage.

Persist the latest context snapshot per conversation or provider session and retain each `Usage`
event in the invocation journal. Compute display values without rounding stored data:

```rust
# use temps_agent_runtime::ContextWindowUsage;
fn percentage(context: &ContextWindowUsage) -> Option<f64> {
    let used = context.used_tokens? as f64;
    let limit = context.limit_tokens? as f64;
    (limit > 0.0).then_some((used / limit * 100.0).clamp(0.0, 100.0))
}
```

`HarnessModel::context_window_tokens` carries the model limit when the harness catalog advertises
one. A usage snapshot can also carry `limit_tokens` when its resolved model identifies the limit.
Fleet should merge a missing usage limit from the selected catalog entry. If neither source
provides a limit, show the token count without a percentage; never hard-code a guessed limit.

The `estimated` flag applies to occupancy only. After a native compact boundary Claude reports
the post-compaction token count directly, so the resulting snapshot has `estimated: false`.

## Persist automatic compact boundaries

Handle these events as append-only activity:

```rust,no_run
# use temps_agent_runtime::TurnEvent;
# fn persist(_: &str, _: &impl std::fmt::Debug) {}
# let event = TurnEvent::Warning { message: String::new() };
match event {
    TurnEvent::CompactionStarted { trigger } => persist("compaction_started", &trigger),
    TurnEvent::CompactionCompleted { compaction } => {
        persist("compaction_completed", &compaction)
    }
    TurnEvent::Usage(usage) => persist("usage", &usage),
    _ => {}
}
```

`ContextCompaction` contains the trigger and the provider-reported pre/post/dropped token counts,
cumulative dropped tokens, and duration. Provider-private transcript identifiers are intentionally
not exposed.

Commit every retained `EventEnvelope` before publishing it to Fleet clients. Preserve its
`runtime_id`, `invocation_id`, and gap-free sequence. A compaction boundary belongs at its actual
position in the activity stream; do not reconstruct one from a later usage total.

## Request manual compaction

Manual compaction is a retained operation, not a synthetic chat message. It therefore gets a
durable Fleet operation ID and the same acceptance, replay, cancellation, and recovery semantics
as a turn:

```rust,no_run
use temps_agent_runtime::lifecycle::InvocationId;
use temps_agent_runtime::retained::CompactionInput;
# async fn example(runtime: &temps_agent_runtime::retained::RuntimeHandle) -> Result<(), Box<dyn std::error::Error>> {

if !runtime.driver_capabilities().manual_compaction {
    return Err("the selected harness cannot compact this session".into());
}

let mut request = CompactionInput::new(InvocationId::new(
    "fleet-compact-operation-0194f2",
)?);
request.instructions = Some("Preserve open decisions and unresolved failures".into());

let turn = runtime.compact(request).await?;
let (mut events, completion) = turn.into_parts();
while let Some(envelope) = events.next().await {
    // Fleet transaction: insert envelope, update projections, commit, then publish.
    persist_envelope(envelope).await?;
}
let result = completion.wait().await?;
persist_terminal_result(result).await?;
# Ok(())
# }
# async fn persist_envelope(_: temps_agent_runtime::retained::EventEnvelope) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
# async fn persist_terminal_result(_: temps_agent_runtime::TurnResult) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

The runtime must already have a provider session. A sessionless request fails with
`DeliveryState::NotSent`. The SDK emits this ordered lifecycle:

1. `RuntimeEvent::InvocationStarted`;
2. `TurnEvent::CompactionStarted { trigger: Manual }`;
3. any provider progress or usage events;
4. `TurnEvent::CompactionCompleted` when Claude reports the native boundary;
5. `RuntimeEvent::InvocationCompleted` or `InvocationFailed`.

Claude repeats the attached session ID in the startup handshake for a resumed
compaction process. The SDK treats that as transport bookkeeping and does not
emit a second `SessionStarted` event for the same ID. Consumers should keep the
operation visibly labeled as queued or compacting from `CompactionStarted`
until the invocation reaches a terminal envelope; it is not a normal agent
turn and should not be presented as “agent working.”

Fleet should disable the action while the retained runtime is busy. It may show the manual
operation in activity history, but should not add `/compact` to the visible user transcript.

## Retry and recovery contract

Use one durable `InvocationId` for the lifetime of the manual operation.

- `NotSent`: the operation is safe to submit again with the same ID after correcting the failure.
- `Accepted`: attach and replay; do not submit another compaction.
- `PossiblySent`: reconnect and attach/reconcile before deciding anything. Never create a new ID
  automatically, because the first compaction may already have happened.

The in-process accepted-ID ledger is memory-only. Fleet must preserve its durable operation and
event journal across daemon restarts. Remote protocol version four transports auto-compaction and
manual-compaction semantics; an older negotiated peer rejects them instead of dropping them.

## Fleet implementation checklist

1. Update the SDK dependency to a revision containing protocol version four.
2. Add context and compaction variants to Fleet's generated API/event schema and exhaustive Rust
   mappings.
3. Persist cache-token components and the latest context snapshot separately from cumulative run
   usage.
4. Merge catalog limits only when the usage snapshot omits its limit.
5. Store `CompactionCompleted` as ordered activity before WebSocket/SSE publication.
6. Add an authorized manual-compaction endpoint backed by a durable operation record and
   `RuntimeHandle::compact`.
7. Recover accepted or indeterminate operations by attach/replay using the original invocation ID.
8. Test automatic boundaries, manual success, unsupported harnesses, missing sessions, runtime
   busy, disconnect after acceptance, daemon restart, and duplicate button submissions.
