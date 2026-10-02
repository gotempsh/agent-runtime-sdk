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
| `CompactionStarted` | The provider began compacting the active context: a manual compaction invocation, or an automatic compaction inside an ordinary turn | Show a visible in-progress state until the matching completion or failure |
| `CompactionCompleted` | The provider reported a native automatic or manual compact boundary | Close the in-progress state, append the boundary, and replace context occupancy with `post_tokens` when present |
| `CompactionFailed` | An open compaction ended without compacting (provider error, skipped compaction, or the turn ended first) | Close the in-progress state and show the optional provider message; the turn may continue |
| `Warning` | Recoverable diagnostic | Add a visible diagnostic without ending the run |
| `AgentRelayActivity` | Content-free relay operation, receipt, rejection, or limit result | Append to the relay timeline and reconcile by message ID |

`Usage.context_window` is a point-in-time occupancy snapshot, not a cumulative
billing counter. It includes optional used and limit tokens, the resolved model,
and whether occupancy was estimated from provider components. Cached input still
occupies context. Keep an unknown limit as `None` and avoid presenting a guessed
percentage.

Claude and Codex in `app-server` mode report occupancy natively: Codex's
`thread/tokenUsage/updated` carries the latest model request (`tokenUsage.last`)
and the model's `modelContextWindow`, labelled with the resolved or requested
model. A later snapshot replaces the earlier one, so a compaction correctly
shrinks the reported window. `codex exec --json` reports turn token totals only;
`AgentRuntime::turn_capabilities(provider).context_window_usage` tells the two
apart before a product surfaces a context meter.

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

### Compaction lifecycle by provider

At most one compaction is open at a time. Every `CompactionStarted` is followed
by exactly one `CompactionCompleted` or `CompactionFailed` before the turn
ends. When the provider never confirms (it exits, the turn is cancelled, or a
manual `/compact` finishes without a boundary) the runtime emits
`CompactionFailed` itself; a retained invocation delivers a single start even when the runtime
announces a manual compaction and the provider then reports its own start.
`TurnCapabilities::compaction_lifecycle` and
`RuntimeDriverCapabilities::compaction_lifecycle` report whether an adapter
emits start signals for automatic compaction. Without it, an application only
sees the completed boundary.

| Provider | Start signal | Completion | Failure | Token counts |
| --- | --- | --- | --- | --- |
| Claude Code (stream-JSON) | `system`/`status` with `status: "compacting"` (repeated as a keepalive; deduplicated) | `system`/`compact_boundary` | `status: null` with `compact_result: "failed"` and `compact_error`, or the terminal `result` arriving first | `pre_tokens`, `post_tokens`, and derived `dropped_tokens` from `compact_metadata` |
| Codex (`app-server`) | `item/started` with a `contextCompaction` item | `item/completed` for that item; the deprecated `thread/compacted` only when no item was reported | `turn/completed` or `turn/failed` while the item is open | `pre_tokens` from the latest `thread/tokenUsage/updated` snapshot; the next snapshot carries the post-compaction occupancy |
| OpenCode (`serve`) | `message.part.updated` with a `compaction` part (`auto: false` means manual) | `session.compacted` | the summary message (`summary: true`, agent `compaction`) reports an `error`, or `session.idle` arrives first | `pre_tokens` from the latest assistant message's token counts |

`codex exec --json` and `opencode run --format json` do not expose compaction.
OpenCode's compaction summary is harness-internal and is not streamed as
assistant `TextDelta`.

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
The result of a background shell task's `Bash` call carries that task's ID too.

A task of kind `workflow` also carries `AgentTask::workflow`: the run's phases,
agents and recent logs, replaced on every update. Its `TaskActivity` records
agent state changes, with the changed agent in `workflow_agent`, rather than
every progress tick. `Claude::transcript_activity` rebuilds a workflow agent's
own activity, including recorded reasoning, from the transcript at
`AgentWorkflow::agent_transcript_path`; its tool inputs and outputs are not
redacted. See
[Show Claude workflows](../how-to/stream-claude-subagents.md#show-claude-workflows).

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
| `QuestionRequested` | The agent needs user input and the turn is waiting; `QuestionRequest.prompts` is the normalized UI shape | `input_needed` |
| `AsyncQuestionRequested` | The agent asked a question without waiting for it; the turn keeps running | unchanged; show the question as open |

Both approval events resolve through `InteractionHandler::approve`: `Allow`
accepts the tool or proposed plan, `AllowForSession` additionally grants
comparable operations for the rest of the session on providers that support it
(Codex app-server's `acceptForSession`; other adapters treat it as `Allow`),
while `Deny { reason }` rejects it and can carry revision feedback back to the
agent.

`AsyncQuestionRequested` is never resolved through `InteractionHandler::answer`:
the runtime already told the provider that no answer exists yet, which is what
keeps the turn from stalling (Codex app-server's `isBlocking: false`
`requestUserInput`). Render it as an open question and send the user's eventual
answer as the prompt of a follow-up turn. Persist the request before waiting,
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
