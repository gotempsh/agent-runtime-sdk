# ADR 0003: Application-owned agent relay

- Status: Accepted
- Date: 2026-09-03

## Context

Applications need independent top-level coding agents, sometimes running in
different projects or over different execution transports, to exchange work.
This is separate from Claude-native Task/Agent subagents: each relay recipient
has its own application identity, runtime, chat, workspace, authorization, and
scheduling lifecycle.

Putting a directory, durable queue, database, or distributed scheduler in this
SDK would violate the runtime ownership boundary in ADR 0001. Connecting agent
processes directly would also bypass application authorization, audit, retry,
and retention policy. A provider prompt cannot safely assert which logical
agent sent a message.

## Decision

Agent relay is an opt-in host capability:

- `AgentAddress` is a stable logical application address and is not a
  `RuntimeId`, invocation ID, provider session, hostname, or network endpoint.
- The SDK defines versioned `AgentMessageEnvelope` values, thread/reply
  correlation, opaque attachment references, bounded metadata, TTL/hop fields,
  delivery receipts, retry advice, and typed errors.
- `MessagingGrant` scopes discovery, send, and receive permissions and carries
  mandatory hop, message, rate, TTL, and per-turn budget ceilings.
- The application implements `AgentMessageRouter`. It remains authoritative
  for its directory, authorization, approval records, durable idempotency
  index, queue, dispatch scheduling, persistence, and retention.
- A reusable MCP tool adapter binds a host-authenticated
  `AgentRelayContext`. Sender identity is never accepted in tool arguments.
  The host exposes this bridge only for configured runtimes or turns through
  `LaunchContext`.
- The host dispatches an accepted envelope as an ordinary user-level provider
  turn with typed agent provenance. Agent content is safely delimited and is
  never appended to the system prompt.
- Only an explicit relay tool call sends or replies. Ordinary assistant prose
  remains in the current chat.

Delivery is **at least once**. The application must durably index the
sender-scoped idempotency key before reporting a queued receipt. If an MCP,
SSH, harness, or broker response is ambiguous, the caller reconciles by
canonical message ID or idempotency key. The SDK never promises exactly-once
recipient side effects.

## Consequences

- A busy or offline recipient does not lose accepted work; the application
  keeps the durable message queued and schedules it when possible.
- An unauthorized sender or recipient produces a typed permanent rejection.
- Hop, message, rate, or byte exhaustion terminates the relay operation with an
  actionable typed failure/activity event instead of allowing an agent loop.
- Applications must provide an MCP-serving boundary and a durable router. The
  SDK provides the contracts and adapter, not a database or daemon.
- Relay credentials remain short-lived `SecretString` environment values.
  They are never placed in URLs, command arguments, tool inputs, or debug
  output.
