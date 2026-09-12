# Adopt the runtime in an existing application

Use this guide to introduce `temps-agent-runtime` without changing your
application's public API, persistence model, or authorization rules. The
library owns provider execution and normalization; the host keeps product and
operational state.

## Keep application responsibilities at the boundary

Move these responsibilities behind the runtime:

- provider executable discovery and readiness;
- provider command construction and JSON parsing;
- child stdin, stdout, and stderr supervision;
- timeouts, cancellation, and process-tree cleanup;
- normalized turn, tool, task, usage, and interaction events;
- provider permission mapping;
- optional execution transports and sandbox preparation.

Keep these responsibilities in the host application:

- durable jobs, messages, events, and retention;
- HTTP, WebSocket, desktop, or CLI interfaces;
- approval records and user authorization;
- telemetry and user-facing error presentation;
- provider installation, authentication, and update workflows;
- scheduling above the runtime's bounded process concurrency.

## 1. Describe the existing behavior

Before replacing an integration, record the behavior its callers depend on:

- partial text and reasoning delivery;
- tool start, completion, and failure states;
- approvals and user questions;
- session creation and resume;
- usage and rate-limit reporting;
- cancellation, timeout, and process cleanup;
- background command ownership;
- sandbox policy and denial recovery.

Treat this behavior as the compatibility contract. A migration is complete
only when each required behavior is represented by a runtime event, result,
error, or explicitly retained application adapter.

## 2. Add an application adapter

Introduce one module that translates between application types and the SDK:

1. map the existing request into `TurnRequest`;
2. forward `TurnEvent` values through an `EventSink`;
3. connect `InteractionHandler` to the existing approval workflow;
4. translate `TurnResult` and `RuntimeError` into the application's public
   result and error types;
5. preserve the application's existing persistence order and status model.

Keep this adapter narrow. Business rules should remain on the application side
of the boundary, and provider-specific protocol details should remain inside
the SDK adapter.

## 3. Select execution independently

Local execution is the default. Replace it with `SshTransport`,
`TempsSandboxTransport`, or another `ExecutionTransport` when the provider must
run elsewhere. The provider executable, workspace, credentials, and session
files belong to the selected execution environment.

`TempsSandboxTransport`, enabled through the opt-in `temps-sandbox` feature, is
an integration with the Temps sandbox API; using the library does not require
Temps. Implement the public transport trait for another container platform,
remote worker, or hosted sandbox.

Require transport and sandbox capabilities explicitly. A missing capability
must stop before provider spawn, and a failed sandbox must never trigger a
local or unsandboxed fallback.

## 4. Migrate one provider at a time

Switch a single provider behind the application adapter while leaving the
others unchanged. Compare event order, terminal state, session identity,
usage, interaction behavior, and cleanup with the compatibility contract from
step 1.

If the bundled CLI adapter cannot preserve required behavior, implement a
richer `AgentAdapter` or `ExecutionTransport` behind the same normalized event
model. Do not weaken permissions or collapse typed failures to make an adapter
appear compatible.

Remove the replaced provider parser and process code only after the new path
has met the application's compatibility contract.

## 5. Connect durable state

The SDK intentionally has no database. Implement storage in the host by
projecting the ordered event stream into the application's existing schema:

- persist events before publishing them to reconnecting clients;
- store the provider session ID with the conversation;
- keep approval decisions separate from sandbox-profile changes;
- store native transport handles when remote process reattachment is enabled;
- reconcile running attempts after worker or application restart;
- retain the `ManagedProcessSupervisor` for services that outlive a turn.

See the persistence, approvals, command-execution, and background-process
how-to guides for concrete trait boundaries.

## 6. Retire duplicate code

After every enabled provider uses the shared boundary:

- delete duplicate provider parsers and process supervision;
- keep application-specific storage, scheduling, authorization, and UI;
- pin compatible provider CLI versions in deployment configuration;
- roll back by selecting the previous application adapter, never by retrying
  outside the requested sandbox or with broader permissions.

The result is an application-independent runtime integration: provider and
process behavior lives in the library, while each host retains full ownership
of its product contract.
