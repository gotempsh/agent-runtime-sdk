# Event and status catalog

`TurnEvent` is a non-exhaustive, provider-neutral stream. Consumers must keep a
fallback match arm so minor releases can add variants.

## Conversation output

| Event | Meaning | Durable projection |
| --- | --- | --- |
| `SessionStarted` | A provider-native resumable ID and optional display title became available | Save the session ID and replace any provisional conversation title when `title` is present. A resumed provider startup handshake that repeats the already-known ID does not emit this event. |
| `TextDelta` | Incremental assistant prose | Append to the active assistant message |
| `ReasoningDelta` | Provider-reported reasoning text | Store only when product policy permits it |
| `PermissionModeChanged` | The harness's effective permission mode changed, including native entry to or exit from plan mode | Replace the materialized effective mode without rewriting the originally requested turn configuration |
| `Usage` | Billing-token totals, cache-token components, cost, or active context occupancy changed | Merge billing totals and replace the latest context snapshot |
| `AccountUsageUpdated` | Provider-account quota windows or credits were refreshed | Replace the latest account snapshot for that provider; never add percentages together |
| `CompactionStarted` | A provider-native manual compaction invocation began | Add visible running activity keyed by the retained invocation ID |
| `CompactionCompleted` | Claude reported a native automatic or manual compact boundary | Append the boundary and replace context occupancy with `post_tokens` when present |
| `Warning` | Recoverable diagnostic | Add a visible diagnostic without ending the run |
| `AgentRelayActivity` | Content-free relay operation, receipt, rejection, or limit result | Append to the relay timeline and reconcile by message ID |

`Usage.context_window` is a point-in-time occupancy snapshot, not a cumulative
billing counter. It includes optional used and limit tokens, the resolved model,
and whether occupancy was estimated from provider components. Cached input still
occupies context. Keep an unknown limit as `None` and avoid presenting a guessed
percentage.

`AccountUsageUpdated` is deliberately separate from `Usage`. It reports account
allocation, not tokens consumed by one turn. Every `AccountUsageWindow` carries
a provider-native stable ID, a provider-neutral `session`/`weekly`/`other`
scope, percentage used, optional duration, and an absolute Unix reset time.
Percentages can exceed 100 and should be clamped only when drawing a progress
bar. Claude currently reports a five-hour session window despite some product
copy historically describing this as four hours. Render the returned duration
instead of hard-coding one.

`AgentRuntime::fetch_account_usage(provider)` is the explicit refresh API. It
returns `AccountUsageReport`, whose status distinguishes available data from a
temporary failure and an unsupported adapter. Fetch when the usage surface is
opened and cache briefly in the application; the SDK does not poll account
endpoints.

`HarnessReadiness.account_usage` may contain the same replaceable snapshot from
the bounded discovery probe. It is optional: `None` means the executable or
authentication mode did not expose quota metadata, never zero usage. Claude
also emits snapshots from `rate_limit_event`; Codex discovery reads
`account/rateLimits/read` because `codex exec --json` does not reliably include
the account windows.

`ContextCompaction` contains bounded pre/post/dropped token counts, cumulative
dropped tokens, duration, and a normalized automatic/manual/unknown trigger.
Provider-private transcript identifiers are not part of the normalized event.

## Tool execution

`ToolCall` carries a provider-native ID when available, a name, status, input,
output, error, and optional native `task_id`. Inputs and outputs may contain
user data or secrets. Redact them before logs or telemetry.

`ToolCallStatus` is `Started`, `Succeeded`, or `Failed`. Use the provider ID to
update one durable tool-call record rather than inserting a new record for each
status transition.

## Native tasks and subagents

| Event | Meaning | Durable projection |
| --- | --- | --- |
| `TasksChanged` | Complete bounded task snapshot for the current run | Replace the materialized task collection |
| `TaskActivity` | One ordered native task transition | Append to the run timeline |

Claude currently emits these events for native Task/Agent work. `AgentTask`
contains the stable native ID, provider-neutral kind, description,
provider-native status, optional agent type, error, and summary.
`AgentTaskActivity` distinguishes started, updated, progress, completed, failed,
and stopped transitions and can include nesting depth, last tool, and task-local
usage. A nested `ToolCall.task_id` uses the same native ID as the task snapshot.

Claude can finish the parent response before background subagents finish. The
adapter keeps reading the stream and servicing interactions until the native
background set clears and Claude exits. See
[Stream Claude native subagents](../how-to/stream-claude-subagents.md).

## Managed process stream

`ManagedProcessEvent` is separate from `TurnEvent` because its lifetime is not
bound to a conversation turn. `StatusChanged` carries a complete snapshot,
`Log` carries a bounded sequenced line, and `RestartScheduled` reports the
attempt and delay. The broadcast stream is bounded and future-only; hydrate
from `snapshot` and `logs` before following it.

## Human interaction

| Event | Meaning | Recommended run state |
| --- | --- | --- |
| `ApprovalRequested` | A tool needs an explicit decision | `approval_needed` |
| `PlanApprovalRequested` | The agent proposed a plan and is waiting for an explicit accept/reject decision | `approval_needed` |
| `QuestionRequested` | The agent needs user input; `QuestionRequest.prompts` is the normalized UI shape | `input_needed` |

Both approval events resolve through `InteractionHandler::approve`: `Allow`
accepts the tool or proposed plan, while `Deny { reason }` rejects it and can
carry revision feedback back to the agent. Persist the request before waiting,
and make decision writes idempotent. A successful native `EnterPlanMode` or
`ExitPlanMode` is followed by `PermissionModeChanged`; do not update the
effective mode merely because an exit approval was requested. See
[Persist approvals](../how-to/persist-approvals.md) for expiration, reconnect,
and restart behavior.

## Sandbox recovery

| Event | Meaning | Recommended projection |
| --- | --- | --- |
| `SandboxAccessDenied` | A configured backend identified a denied resource | Record the failed step and proposed remediation |
| `SandboxProfileUpdated` | The manager committed a validated revision | Store the exact profile revision and approved change |
| `SandboxStepRetrying` | The runtime is resuming the provider session | Set the run back to `running` and increment its retry count |

The original failed `ToolCall` remains in the stream. Recovery events supplement
it; they do not rewrite history.

See [Operate sandboxed turns](../how-to/operate-sandboxes.md) for the complete
profile-resolution and same-session retry sequence.

## Terminal status

`TurnResult.status` is currently `Succeeded` or `Failed`. Its optional
`session_title` mirrors the provider-native display title when the harness
reports one, so a durable conversation can reconcile titles even if it missed
the streaming `SessionStarted` event. Cancellation, timeout,
spawn failures, protocol failures, non-zero process exits, and native failed
terminal frames are typed `RuntimeError` values returned by the run future.
Native provider failures do not emit their raw diagnostic as a `Warning`; the
runtime first redacts and bounds it in `RuntimeError::ProcessFailed`.

Applications building persistent chat should append their own terminal event
for both paths. This makes replay independent of whether completion came from a
provider frame or a Rust error.

## Ordering and backpressure

The runtime awaits each `EventSink::emit` call before delivering the next event.
Events are ordered within one run. Ordering across concurrent runs belongs to
the application and should use separate run IDs and per-run sequences.

See [Persist command and tool execution](../how-to/persist-command-execution.md)
for legal tool-state transitions and restart reconciliation.
