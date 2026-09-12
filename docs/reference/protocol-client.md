# Remote runtime client

`RemoteRuntimeClient` implements the retained `RuntimeClient` contract over the
versioned protocol. Applications provide an authenticated
`RemoteRuntimeConnector`; the SDK owns request correlation, event routing,
per-invocation replay cursors, interaction responses, and typed delivery state.

The carrier exposes ordered `send` and `receive` operations and may be backed by
a WebSocket, an SSH tunnel, a Unix socket, or another authenticated full-duplex
channel. It must not log complete frames because prompts, tool inputs, and
explicit protocol secrets may be present.

Protocol version two adds structured launch context. Protocol version three
adds typed user-versus-agent turn provenance. Protocol version four adds
automatic/manual compaction controls and context metadata. The client emits version four,
and older frames are accepted only when fields they cannot enforce are absent.
Version-one frames are accepted only when system instructions, exact
tool availability, and MCP launch configuration are empty. This prevents an
older peer from silently dropping execution-policy fields.

Active invocations recover automatically after a connection failure. The SDK
reconnects, attaches with the last sequence observed for each invocation,
replays the gap, and moves future live events onto the replacement carrier.
`RemoteRecoveryPolicy` bounds exponential backoff; exhausting the configured
attempts completes affected turns with an actionable typed transport failure
instead of leaving them pending forever. Authentication failures fail closed
without retrying.

Attach replay is lossless across page boundaries. The host sends the runtime
descriptor, then only the requested invocations' replay events, then a
`ReplayComplete` frame. If that frame is marked `truncated`, the client advances
its per-invocation cursors and follows the next page, up to a bounded 1,024-page
limit. A page that claims truncation without cursor progress is rejected as a
provider-protocol failure. Per-runtime delivery gates keep live events behind
the complete replay, while client-side sequence checks trigger recovery instead
of accepting a gap or duplicate.

For a newly started turn, the host delivers `TurnAccepted` before releasing its
event pump. This makes acceptance the observable boundary even for executors
that emit immediately. Direct responses and live writes are independently
bounded so a stalled carrier cannot hold the host forever.

Drain each turn's event stream concurrently with awaiting its completion. Each
invocation has a bounded 128-event delivery queue. If a consumer fills or closes
that queue, only that invocation fails with a contextual transport error and
`RequiresUserAction`; the SDK requests a best-effort remote interrupt without
blocking control responses or other sessions. The failure explicitly states that
remote interruption is unconfirmed. It is not permission to retry a mutating
turn automatically. Previously delivered events remain available to the consumer;
the SDK does not report a successful turn after losing an event.

Host disposal serializes against acquire, attach, and start for the same runtime.
It cancels the retained invocation and stops its event pump before removing
journal data, so old events cannot recreate a disposed runtime's journal after
its ID is reused. Lifecycle locks are weakly retained and pruned to avoid growth
from requests for unknown runtime IDs.

Approval decisions and question answers use the same recovery discipline. The
application handler is called once; the client retains the resulting response
and one request identity while it performs bounded reconnect attempts. A lost
acknowledgement therefore returns the host's idempotency-cache result instead of
asking the user again or resolving the provider interaction twice. Exhaustion,
authorization failure, or a malformed acknowledgement completes the local turn
with a typed, accepted-delivery failure. The answer is never silently dropped
while the UI continues to show a running turn.

Applications may also call `recover(runtime_id)` explicitly when restoring
their own durable runtime inventory. Mutating requests use unique request IDs,
and transient retries preserve the original ID so the host can return its
cached response without executing accepted work twice. If a start response is
lost after acceptance, the client attaches and recognizes the invocation from
its replayed lifecycle before reporting failure.

`RuntimeSpec` values without a sandbox can use the ordinary `RuntimeClient`
`acquire` method. A remote sandbox is a host-authorized name rather than an
in-process backend object, so use `acquire_protocol` with a
`ProtocolSandboxSelection` when one is required.
