# ADR 0001: Runtime ownership and lifecycle contracts

- Status: Accepted
- Date: 2026-09-02

## Context

Applications embedding `temps-agent-runtime` need durable chats, retries,
remote execution, and retained provider sessions. Those applications differ in
their persistence models, tenancy rules, authorization systems, and user
interfaces. Encoding any one application's concepts in the SDK would make the
runtime difficult to reuse and would split execution behavior across layers.

The existing one-turn API is useful as a compatibility surface, but it does not
name the lifecycle boundaries required by long-lived and remote runtimes.

## Decision

The SDK owns provider execution and exposes provider-neutral lifecycle
contracts:

- a `RuntimeId` identifies one retained provider runtime;
- an `InvocationId` identifies one turn within that runtime;
- runtime health, configuration impact, and interruption outcomes are explicit;
- failures carry retry advice and delivery state so callers never assume that
  an ambiguous request is safe to replay.

The SDK does **not** own chats, users, workspaces, database schemas, channels,
schedules, HTTP routes, or presentation state. An embedding application maps
its own records to SDK identifiers and persists normalized SDK events as needed.

The runtime host owns provider-process lifetime. An execution transport owns
how commands and streams cross a local, SSH, sandbox, or future remote boundary.
Provider drivers own provider-native protocol details. These responsibilities
remain separate even when an in-process client composes all three.

The existing `AgentRuntime::run` API remains available while retained-runtime
APIs are introduced. Compatibility code delegates inward; new lifecycle code
does not depend on the compatibility facade.

## Consequences

- Callers can implement durable queues and retries without treating an
  uncertain delivery as definitely failed.
- Remote protocols can version lifecycle messages independently of application
  APIs.
- Provider drivers can gain persistent-process support incrementally.
- Applications must continue to own persistence and authorization decisions.
- Identifiers and failure contracts become public compatibility commitments and
  require additive evolution.
