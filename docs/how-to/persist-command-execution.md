# Persist command and tool execution

Use normalized `ToolCall` events to build a durable execution timeline for a
conversation. Persistence makes tool status replayable; it does not make a
provider process magically resumable after a host crash.

## Model attempts, not mutable commands

Store each execution attempt independently. A retry should point to the
original attempt instead of overwriting it.

```rust,no_run
use serde::{Deserialize, Serialize};
use temps_agent_runtime::ToolCallStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredToolAttempt {
    conversation_id: String,
    run_id: String,
    command_key: String,
    provider_call_id: Option<String>,
    ordinal: u64,
    name: String,
    status: ToolCallStatus,
    started_at_unix_ms: Option<u64>,
    finished_at_unix_ms: Option<u64>,
    redacted_input: Option<serde_json::Value>,
    redacted_output: Option<String>,
    redacted_error: Option<String>,
    retry_of: Option<String>,
}
```

Prefer the provider call ID when present. Because IDs are optional, allocate a
stable per-run ordinal and derive `command_key` from `(run_id, ordinal)`. Keep a
unique constraint on both the synthesized key and any non-null provider ID.

## Apply monotonic transitions

`ToolCallStatus` has `Started`, `Succeeded`, and `Failed`. Accept only these
transitions:

```text
missing ──► started ──► succeeded
   │            └────► failed
   ├─────────────────► succeeded
   └─────────────────► failed
```

Some provider streams report only a completed record, so insertion directly
into `Succeeded` or `Failed` must be valid. Never move a terminal attempt back
to `Started`. Treat duplicate terminal events as idempotent only when their
stored outcome matches.

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::TurnEvent;

#[async_trait]
trait CommandStore: Send + Sync {
    async fn apply_tool_event(
        &self,
        run_id: &str,
        sequence: u64,
        event: &TurnEvent,
    ) -> Result<(), StoreError>;

    async fn mark_running_as_interrupted(
        &self,
        run_id: &str,
        reason: &str,
    ) -> Result<(), StoreError>;
}

# struct StoreError;
```

Call `apply_tool_event` from the same transaction that appends the event
envelope. This keeps the replay log and materialized command view consistent.

## Preserve ordering and user-visible status

For every tool event, persist:

- the run sequence number;
- provider call ID when available;
- the normalized status and tool name;
- bounded, redacted input, output, and error;
- timestamps assigned by the application;
- the sandbox profile revision used for the attempt, when applicable.

Publish the committed sequence to streaming clients. A reconnecting client
first replays events after its cursor, then follows the live stream. The UI can
derive `queued`, `running`, `approval_needed`, `failed`, `succeeded`, and
`interrupted` without depending on an in-memory worker.

## Reconcile a worker restart

On worker startup, inspect leased runs whose terminal state was never written:

1. mark their `Started` tool attempts `interrupted` in the application model;
2. release or expire open approvals;
3. record that the provider process was lost;
4. resume only with a provider session ID and a new run/attempt ID;
5. never claim that the original command is still executing without an
   independently verified process or service handle.

The normalized SDK enum intentionally does not contain `Interrupted`; that is
an application-level reconciliation state, not a provider event.

## Track long-running processes separately

`ToolProcessPolicy::PreserveOnCompletion` prevents the SDK from killing
remaining descendants after a natural provider exit. A PID written by a tool
is still not a durable service contract: it can be reused, killed by the
provider, or owned by an outer sandbox.

For a server, watcher, or worker that must survive a turn reliably, start it
through `ManagedProcessSupervisor` and store its `ManagedProcessId` alongside
the tool attempt:

```text
service_id · supervisor · native_handle · command_key · desired_state
observed_state · endpoint · started_at · stopped_at
```

Reconcile that ID with `snapshot` before showing `running`, and persist
`ManagedProcessEvent` records for replay. Stop it through the same supervisor;
do not rely on process-group escape tricks. The in-process supervisor does not
reattach after a host crash, so persisted running records become `interrupted`
unless an external OS supervisor can verify its native handle. See
[Manage background commands and services](manage-background-processes.md) and
[Keep tool processes running](keep-tool-processes-running.md).

## Protect command data

Provider-native input and output may contain credentials, source code, or
private paths. Bound field sizes, encrypt sensitive payloads, redact logs and
telemetry, and apply retention independently from the conversation transcript.

See [Persist approvals](persist-approvals.md) for waiting interactions and
[Operate sandboxed turns](operate-sandboxes.md) for associating command attempts
with exact sandbox revisions.
