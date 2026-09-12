# Relay messages between independent agents

Agent Relay lets application-owned, top-level agents exchange durable messages
without turning `temps-agent-runtime` into a database, directory, broker, or
scheduler. It is opt-in: a turn has no relay tools unless the application both
creates a scoped bridge and installs its MCP endpoint into that turn's
`LaunchContext`.

This is not Claude-native subagent support. Native subagents share one provider
process and parent turn. Relay agents have distinct logical `AgentAddress`
values and may use different projects, runtimes, hosts, or providers.

## Keep the application authoritative

Implement `AgentMessageRouter` in the host application. Its methods are the
durable authorization boundary:

- `discover` queries the application's agent directory;
- `send` and `reply` authorize, index the sender-scoped idempotency key, persist
  the envelope, and enqueue delivery before returning `Queued`;
- `delivery_status` reconciles by canonical message ID or idempotency key.

The router receives `AgentRelayContext` separately from tool input. Its sender
was bound by the host capability and cannot be selected by the harness. Treat
the context's `capability_id` as an audit/index key into application-owned
authorization state, not as a secret credential.

Delivery is at least once. Recipient work must be idempotent where side effects
matter. Never project `Delivered` as proof of exactly-once effects.

## Issue a bounded grant

`MessagingGrant::default()` grants nothing. Add only the address ranges and
limits required for one runtime or turn:

```rust
use temps_agent_runtime::relay::{
    AgentAddressPattern, AgentDiscoveryGrant, AgentMessagingLimits,
    AgentReceiveGrant, AgentSendGrant, MessagingGrant,
};

let project_b = AgentAddressPattern::prefix("tenant/project-b/")?;
let project_a = AgentAddressPattern::prefix("tenant/project-a/")?;
let grant = MessagingGrant {
    discovery: Some(AgentDiscoveryGrant {
        addresses: vec![project_b.clone()],
        max_results: 20,
    }),
    send: Some(AgentSendGrant {
        recipients: vec![project_b],
    }),
    receive: Some(AgentReceiveGrant {
        senders: vec![project_a],
    }),
    limits: AgentMessagingLimits {
        max_hops: 4,
        max_messages_per_turn: 8,
        max_messages_per_minute: Some(12),
        ..AgentMessagingLimits::default()
    },
    ..MessagingGrant::default()
};
grant.validate()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Set `approval` to `Required` when every send/reply needs an application approval
record, or `HostPolicy` when the application decides per operation. The router
remains responsible for checking that approval; the SDK does not persist it.

## Bind the tool bridge to a sender

Create one bridge for one scoped runtime/turn capability. The tool schemas have
no `sender`, `hop_count`, or `hop_limit` argument:

```rust,no_run
use std::sync::Arc;
use temps_agent_runtime::relay::{
    AgentAddress, AgentMessageRouter, AgentRelayMcpBridge, MessagingGrant,
};

# fn build(
#   router: Arc<dyn AgentMessageRouter>,
#   grant: MessagingGrant,
# ) -> Result<(), Box<dyn std::error::Error>> {
let bridge = AgentRelayMcpBridge::for_turn(
    AgentAddress::new("tenant/project-a/reviewer")?,
    "relay-capability-audit-id",
    grant,
    router,
)?;

// Mount bridge.handle_json_rpc(request) behind an authenticated MCP route.
# let _ = bridge;
# Ok(())
# }
```

`handle_json_rpc` is the reusable minimal MCP method adapter. It handles
`initialize`, `ping`, `tools/list`, and `tools/call`; the application supplies
HTTP/stdio framing, authentication, listeners, and MCP session policy. It lists
only tools enabled by the grant:

- `agent_relay_discover`
- `agent_relay_send`
- `agent_relay_reply`
- `agent_relay_status`

Only `send` or `reply` crosses an agent boundary. Assistant text produced after
an inbound message remains in the recipient's own chat.

## Expose the bridge to a local turn

For a local provider process, mount the scoped bridge on a loopback MCP route.
Authenticate the request first, then select the bridge associated with that
opaque token. Install the route explicitly:

```rust,no_run
use temps_agent_runtime::relay::AgentRelayMcpExposure;
use temps_agent_runtime::{SecretString, TurnRequest};

# fn configure(mut request: TurnRequest) -> Result<TurnRequest, Box<dyn std::error::Error>> {
let exposure = AgentRelayMcpExposure::http(
    "http://127.0.0.1:8787/mcp/agent-relay",
    "AGENT_RELAY_TURN_AUTHORIZATION",
)?;
exposure.install(&mut request.launch_context)?;
request.environment.insert(
    "AGENT_RELAY_TURN_AUTHORIZATION".into(),
    SecretString::new("Bearer short-lived-opaque-token"),
);

// Exact tool lists must include each desired MCP tool explicitly.
request.launch_context.allowed_tools = Some(vec![
    exposure.claude_tool_name("agent_relay_send"),
    exposure.claude_tool_name("agent_relay_reply"),
    exposure.claude_tool_name("agent_relay_status"),
]);
# Ok(request)
# }
```

The URL and command arguments are not secret locations. Tokens belong only in
`SecretString` environment values. The SDK validates that every MCP header/env
source exists and redacts its value from `Debug`.

For stdio, use `AgentRelayMcpExposure::stdio`. The application-provided
executable reads an opaque capability token from its environment, authenticates
to the durable application router, constructs the correctly scoped bridge, and
passes MCP JSON-RPC to `handle_json_rpc`. Do not accept sender identity in the
executable's tool payload.

## Expose it when the harness runs over SSH

`LaunchContext` is interpreted on the execution host. With `SshTransport`,
`127.0.0.1` means the remote host, not the Rust application host. Choose one
application-owned deployment:

1. Establish a bounded SSH reverse port forward before starting the turn, then
   configure the remote loopback URL that forwards to the authenticated local
   MCP route.
2. Use an authenticated TLS MCP endpoint reachable from the remote host.
3. Install an application bridge executable on the remote host and configure a
   stdio exposure. The executable connects back to the durable application
   service using its short-lived environment token.

The SDK's `SshTransport` does not create or persist a relay tunnel. Tunnel
lifecycle, endpoint authorization, retry, and token rotation remain application
responsibilities. If SSH or the harness disappears after send, reconcile with
the same idempotency key instead of inventing a new message.

## Dispatch an inbound envelope safely

After a worker leases a durable queued envelope, validate the recipient's
receive grant and build the recipient turn:

```rust,no_run
use temps_agent_runtime::relay::{
    inbound_turn_request, AgentMessageEnvelope, MessagingGrant,
};
use temps_agent_runtime::Provider;

# fn dispatch(
#   envelope: &AgentMessageEnvelope,
#   receive_grant: &MessagingGrant,
#   now_unix_ms: u64,
# ) -> Result<(), Box<dyn std::error::Error>> {
let request = inbound_turn_request(
    Provider::Claude,
    "/projects/recipient",
    envelope,
    receive_grant,
    now_unix_ms,
)?;
// request.provenance is TurnProvenance::Agent(...), but the prompt uses user role.
# let _ = request;
# Ok(())
# }
```

`inbound_turn_input` provides the same integration for retained runtimes. Both
helpers reject unauthorized senders, expired messages, oversized payloads, and
hop violations. They render the complete envelope inside escaped, explicit
untrusted-message delimiters. Relay content is never added to
`system_prompt_append`.

When the recipient is busy or offline, keep the envelope queued. Mark it
`Dispatching` only under the application's lease, then `Delivered` after the
recipient runtime acknowledges the inbound turn. On a lost acknowledgement,
retry may schedule the turn again; use message/invocation idempotency to avoid
duplicating durable side effects.

## Reconcile failures

- `UnauthorizedRecipient`, `UnauthorizedSender`, and `RecipientNotFound` are
  permanent unless application authorization/directory state changes.
- `BudgetExceeded`, `RateLimited`, and `HopLimitExceeded` stop the operation and
  produce an actionable content-free `AgentRelayActivity` when a sink is
  configured.
- `Indeterminate` plus `PossiblyAccepted` requires `agent_relay_status` by
  message ID or the original idempotency key before any retry.
- `Unavailable` with `NotAccepted` can be retried using the same idempotency key
  according to its typed retry advice.

`AgentRelayActivity` can also be projected as `TurnEvent::AgentRelayActivity`.
It intentionally omits message bodies, attachment URIs, idempotency keys, and
capability tokens.

## Run the verification fixtures

The deterministic test covers two logical projects, a busy recipient, durable
queue state, explicit reply, idempotent replay, ambiguous-response
reconciliation, and permanent unauthorized rejection:

```bash
cargo test --test agent_relay --all-features
```

With an authenticated Claude Code CLI, run the bounded live fixture:

```bash
cargo run --example agent_relay_claude_live --features claude
```

It creates two temporary projects and a temporary application-owned JSON store.
Agent A sends through the stdio relay MCP tool, the fixture host dispatches the
envelope to Agent B as typed user-level input, and B explicitly replies through
the tool. The fixture prints durable queued/delivered receipts and exits with
all MCP child processes; it does not leave a server running. The file router is
test-only and deliberately not a production database or locking design.
