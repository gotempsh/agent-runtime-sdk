# Retained runtimes

`InProcessRuntimeClient` adds stable lifecycle and provider-session continuity
on top of `AgentRuntime`. Applications acquire a `RuntimeHandle`, submit turns,
consume ordered `EventEnvelope` values, and persist whatever projections their
product needs.

`RuntimeHandle::driver_capabilities` distinguishes logical provider-session
resume from native process retention. The compatibility `AgentRuntime` driver
reports `session_resume: true` and `retained_process: false`: Claude, Codex, and
OpenCode can continue their provider-native sessions even though the CLI is
currently relaunched for each turn.

Use `RuntimeHandle::configuration_impact` before presenting a live setting
change. The compatibility driver applies per-turn model, reasoning, permission,
harness, launch-context, environment, and timeout changes live. Provider,
working-directory, and sandbox changes require acquiring a new runtime.

The driver also reports `configurable_auto_compaction`, `manual_compaction`,
`context_window_usage`, and `native_image_attachments`. Gate product controls on these capabilities. Automatic
policy can be set on `RuntimeSpec::auto_compaction` and overridden by
`TurnInput::auto_compaction`.

`RuntimeSpec::launch_context` supplies the default system instructions, tool
availability, and MCP definitions. `TurnInput::launch_context` is an optional
complete replacement for one invocation; `None` inherits the runtime default.

## Resource limits

`InProcessRuntimeClient` retains at most 1,024 acquired runtimes by default.
Applications with a smaller tenant or host budget should construct it with
`InProcessRuntimeClient::with_limits` or
`InProcessRuntimeClient::from_executor_with_limits` and a
`RetainedRuntimeLimits` value. Capacity is released only by `dispose`; a limit
failure is `RuntimeBusy` with `DeliveryState::NotSent`, so the application can
dispose an inactive runtime and retry safely.

`RemoteRuntimeHost` separately bounds its idempotency cache. By default it
retains 1,024 completed responses and permits 256 distinct pending requests.
Use `RemoteRuntimeHost::with_limits` and `RemoteRuntimeHostLimits` to match the
authenticated carrier's connection and tenant budgets. Dropping a dispatch
future releases its pending request claim and wakes duplicate waiters, so a
cancelled carrier request cannot permanently consume cache capacity.

## Ownership

The SDK owns runtime and invocation execution. The application still owns chats,
queues, users, authorization, file storage, and databases. A durable queue item
maps to a fresh `InvocationId`; the SDK does not prescribe its schema.

## Retry safety

Accepted invocation identifiers are retained in a bounded in-memory ledger.
Submitting the same identifier again returns
`RuntimeFailureKind::InvocationAlreadyExists` with `DeliveryState::Accepted`
instead of repeating provider side effects. A remote host or application that
needs deduplication across process restarts must durably retain the same
contract; the in-process ledger is not durable storage.

Always inspect `RuntimeFailure.delivery` before retrying:

- `NotSent`: the provider did not receive the request;
- `PossiblySent`: reconcile before retrying with a new invocation;
- `Accepted`: treat the invocation as delivered and attach/recover its events.

## Attachments

`TurnAttachment` references a file that is already present on the selected
execution host. The SDK does not upload bytes, infer authorization, or persist
the file. Stage uploads through an application-owned boundary, then submit the
execution-host path. References are bounded and validated before the invocation
is accepted.

The compatibility executor adds file references to the provider prompt. Future
native retained drivers can map the same typed references to provider-native
attachment mechanisms without changing application persistence.

## Interactions

Set `TurnInput::interaction_policy` to `RequireHandler` when a turn must be able
to surface approvals or questions. Starting without a handler then fails with
`DeliveryState::NotSent`. `Deny` remains the fail-closed default.

## Manual compaction

`RuntimeHandle::compact(CompactionInput)` runs provider-native manual compaction
through the ordinary retained invocation pipeline. It requires a resumable
provider session and a fresh durable `InvocationId`, emits ordered start and
completion activity, and inherits normal busy, interruption, replay, and retry
semantics. It is not an ordinary user chat message.

See [Integrate context usage and compaction with Fleet](../how-to/integrate-context-compaction-with-fleet.md).
