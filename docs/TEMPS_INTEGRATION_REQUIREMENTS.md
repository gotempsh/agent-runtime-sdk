<!-- SCOPE: Requirements and boundary decisions for adopting temps-agent-runtime in Temps AI workspaces. -->

# Temps integration requirements

Status: in progress

Audience: `temps-agent-runtime` and Temps maintainers

Last reviewed: 2026-09-03

Related documentation: [migration guide](MIGRATION.md),
[runtime architecture](explanation/architecture.md),
[launch context](how-to/configure-launch-context.md), and
[custom transports](how-to/custom-transport.md).

## Decision

Temps should adopt `temps-agent-runtime` as its provider-process runtime, but
the SDK is not yet a drop-in replacement for the current Claude Code, Codex,
and OpenCode integrations.

The SDK should remain application-independent. It should own provider command
construction, protocol parsing, normalized events, process supervision, and
transport capability checks. Temps should continue to own credentials,
credential relays, sandbox lifecycle, authorization, platform tools,
conversation storage, approvals, and user-facing recovery.

Migration should be incremental and provider-by-provider. The existing
integration must remain available as a rollback path until the compatibility
requirements in this document pass.

## Current failure motivating this work

The current Temps workspace UI can advertise Codex based on the host
environment while executing the turn in a persistent sandbox. In the observed
failure:

- Codex was installed in the sandbox but was not logged in there.
- Temps' secure model relay and credential resolver supported only Claude.
- The Codex turn failed before a useful assistant response was produced.
- The concrete unsupported-provider/authentication reason was reduced to the
  generic message `AI harness returned no reply`.

This is a boundary mismatch, not an arithmetic or model problem. Discovery,
readiness, execution, and diagnostics must describe the same execution target.

## Ownership boundary

| Responsibility | Owner | Rationale |
| --- | --- | --- |
| Provider executable arguments and native protocol parsing | SDK | Reusable across every embedding application |
| Normalized turn, tool, usage, session, and failure events | SDK | This is the SDK's core compatibility contract |
| Process lifetime, cancellation, timeouts, output bounds, and cleanup | SDK | Transport-independent process correctness |
| Transport interface and capability negotiation | SDK | Enables local, SSH, Temps, and third-party transports |
| Concrete Temps sandbox creation, recovery, networking, and persistence | Temps | Platform resource policy and infrastructure ownership |
| Provider credential storage and encryption | Temps | Product security and operator configuration |
| Short-lived model/API credential relay | Temps | Provider accounts and tenant authorization are application state |
| Platform MCP tools and their authorization | Temps | Tools operate Temps resources and must use current user permissions |
| Conversation/message/event persistence | Temps | Existing database, retention, pagination, and product semantics |
| Approval audit records and decision authorization | Temps | Decisions must re-check the current operator and role |
| Project topology, database links, previews, and deployments | Temps | Platform domain concepts must not enter the runtime SDK |
| Chat attachments and workspace file materialization | Temps | Upload policy and persistent workspace paths are application concerns |

## Required SDK changes

### SDK-1: Provider-capability-aware launch context

Priority: MUST before migrating Codex or OpenCode workspace turns.

The built-in adapters must report which launch-context fields they support.
Validation must reject only an unsupported field, rather than rejecting the
entire launch context for every provider except Claude.

At minimum, Codex must support a turn-scoped HTTP MCP server with credentials
referenced from `TurnRequest::environment`. OpenCode should support the same
when its native configuration format can preserve secret boundaries.

The SDK must keep URLs, arguments, and secret values separate. Secret values
must not be serialized into command arguments or diagnostic output.

Why this belongs in the SDK: HTTP/stdio MCP configuration is a provider adapter
concern useful to any host application. Implementing it in Temps would require
a second Codex/OpenCode command builder and defeat the purpose of the common
runtime.

Acceptable temporary workaround: Temps may register its own `AgentAdapter` for
one provider during migration. This is a bridge only; it must not become the
permanent architecture.

Implementation status: field-level `LaunchContextCapabilities` are exposed by
adapters, direct runtime inspection, and harness discovery. Validation rejects
only the unsupported field. Codex supports additive turn-scoped HTTP MCP with
header values referenced from the redacted turn environment. System-prompt
additions, exact tool allowlists, stdio MCP, and strict MCP isolation remain
unsupported for Codex and are rejected before spawn.

### SDK-2: Lossless structured terminal failures

Priority: MUST before routing production turns through the SDK.

Provider-native terminal failure frames such as Codex `turn.failed` must be
stored in adapter state and returned as a typed terminal failure. A warning
event alone is insufficient because stderr can be empty.

`RuntimeError::ProcessFailed` must preserve:

- the normalized failure kind;
- the bounded, redacted provider diagnostic;
- the native exit code when available;
- whether the provider accepted the turn far enough to make retry safety
  uncertain.

Why this belongs in the SDK: interpreting provider-native failure frames is a
provider protocol responsibility. Every embedding application needs the same
lossless result.

Implementation status: complete. Claude, Codex, and OpenCode retain native
terminal failure frames in adapter state. `RuntimeError::ProcessFailed`
preserves the normalized kind, redacted and bounded diagnostic, native exit
code, optional bounded provider code, and `DeliveryState`. An explicit native
failure is returned even when the provider process exits with code zero.
Retained runtimes carry the same provider code and delivery certainty into
`RuntimeFailure`, so applications can make retry decisions without parsing
diagnostic text.

### SDK-3: Context-bearing, transport-scoped discovery

Priority: SHOULD; required for a fully accurate model/authentication picker in
ephemeral-credential environments.

Add a bounded discovery request that can carry explicit `SecretString`
environment values and a working directory into provider catalog and account
probes. Keep the current no-argument discovery method as a convenience for
targets with ambient credentials.

The result should distinguish at least:

- executable unavailable;
- executable available, authentication unknown;
- authenticated and ready;
- authentication required or rejected;
- model catalog unavailable while basic execution remains usable.

Why this belongs in the SDK: local, container, SSH, and hosted environments can
all use ephemeral credentials. Transport-scoped probes prevent an application
from accidentally displaying host readiness for a remote execution target.

Temps may still combine SDK readiness with its own server-side credential
configuration. The SDK must not read the Temps database or understand Temps
credential records.

Implementation status: complete. `ProviderProbeContext` carries a
transport-local working directory and bounded `SecretString` environment into
catalog and account-usage probes. The original no-argument discovery and usage
methods remain ambient-context conveniences. Supplied values are omitted from
debug output and redacted from probe diagnostics. Catalog failures now
distinguish provider authentication, permission, model availability, rate
limiting, and network failures without making an installed harness unavailable.
`HarnessReadiness::authentication` independently reports `authenticated`,
`required`, `rejected`, `unavailable`, or `unknown`. Claude and Codex use their
bounded native authentication-status commands with the same probe context.
OpenCode remains `unknown` because its models may belong to independently
authenticated providers; Temps must combine that state with its own selected
provider/relay readiness.

### SDK-4: Built-in adapter compatibility controls

Priority: SHOULD.

Provider adapters should expose typed native controls for behavior required to
run through an application-owned relay, including transport selection when a
provider can choose WebSocket or HTTP. Unsupported controls must fail before
spawn.

This should use the existing harness-control mechanism rather than a
Temps-specific relay type. The SDK should not implement or host the relay.

Implementation status: satisfied for currently supported native controls.
Adapters expose provider-native behavior through `HarnessControlGroup`, and
unknown keys or values fail before spawn. Endpoint URLs and short-lived relay
credentials use the bounded, redacted `TurnRequest::environment` boundary.
The currently tested Codex CLI no longer exposes its former Responses
WebSocket feature flags, so the SDK deliberately does not advertise a
fictional HTTP/WebSocket selector. If a future pinned CLI exposes a supported
transport choice, add it as another discovered control group rather than a
Temps-specific request field.

### SDK-5: Feature-matrix build correctness

Priority: MUST before adding the dependency to Temps CI.

Every documented Cargo feature combination must compile and test. Currently,
the default `cargo test --lib` build fails because a test references
`is_loopback_endpoint` while that function is gated behind `temps-sandbox`.
The all-features library suite passes.

Required verification:

| Command | Expected result |
| --- | --- |
| `cargo test --lib` | Pass with default features |
| `cargo test --lib --all-features` | Pass |
| Provider-only feature builds | Pass for Claude, Codex, and OpenCode independently |
| `cargo clippy --all-targets --all-features` | No new warnings |

Implementation status: complete. Default, all-feature, and independent
Claude, Codex, and OpenCode library suites pass, as do public API/doc tests and
strict all-target Clippy. The feature-gated loopback test was corrected without
making the core discovery or launch-context types depend on an optional
transport feature.

## Changes that should remain in Temps

### TEMPS-1: Internal SDK execution transport

Temps should implement `ExecutionTransport` over its internal
`SandboxProvider` primitive. The embedded server should not call its own public
sandbox API using a reusable user or platform token.

The adapter should translate sandbox process handles, stdout, stderr, wait,
termination, and reattachment into the SDK transport types. It should be
created or cached per selected sandbox so multiple chats can intentionally
share one persistent filesystem while turns remain independently tracked.

No SDK core change is required: `ExecutionTransport` is already the correct
extension point.

### TEMPS-2: Duplex sandbox process primitive

Temps' sandbox API/provider must support writing to a running process after its
initial stdin payload. This can be a WebSocket, a bounded stdin-write endpoint,
or an equivalent authenticated process channel.

The primitive must provide:

- process-scoped authorization;
- bounded stdin writes;
- separate stdout and stderr streams;
- cursored reattachment;
- complete process-tree termination;
- lease or reconciliation behavior after server restart.

This belongs to Temps because it is a sandbox process capability useful beyond
AI. Once available, the optional SDK `TempsSandboxTransport` may consume it.
The SDK's core transport abstraction already models interactive stdin.

### TEMPS-3: OpenAI/Codex model relay

Extend the existing host-side, turn-scoped relay to Codex. The sandbox should
receive only a short-lived bearer scoped to one principal, provider, selected
model, and deadline. It must never receive a reusable Temps platform token.

The relay must explicitly handle the authentication flavor:

- OpenAI API key;
- Codex/ChatGPT subscription credentials, including refresh and account
  identity requirements;
- HTTP and, only if necessary, WebSocket Responses transport.

Prefer forcing bounded HTTP transport when the native CLI supports it. If a
WebSocket proxy is required, it must enforce the same path, request, model,
concurrency, and expiry limits as the HTTP relay.

This must not be added to the SDK. The SDK should receive only provider-native
endpoint configuration and redacted per-turn environment values.

### TEMPS-4: Composite readiness

The UI must advertise a harness only when all required layers agree:

1. the executable exists in the selected sandbox;
2. the sandbox transport has the required capabilities;
3. Temps has a usable credential configuration for that provider;
4. the selected provider has a supported secure relay or approved credential
   delivery mode;
5. the selected model is available or the catalog is explicitly marked as a
   fallback.

Host installation must never imply sandbox readiness. Labels must name the
actual execution target.

### TEMPS-5: SDK-to-chat adapter

Add one narrow adapter that maps:

- `ChatTurnRequest` to `TurnRequest`;
- `TurnEvent` to `ChatStreamDelta` and durable chat events;
- `InteractionHandler` to the existing approval workflow;
- `TurnResult::session_id` to conversation continuation state;
- `RuntimeError` to typed, actionable public failures.

Temps must persist an event before publishing it to browsers. Existing
authorization and conversation ownership checks remain unchanged.

### TEMPS-6: Provider-version parity

Provider discovery and execution must use the same executable in the same
transport. Temps should pin tested CLI versions in sandbox images and include
the actual execution-target version in diagnostics.

Compatibility fixtures should cover the pinned native event formats. A host
CLI version must not populate the sandbox model catalog.

### TEMPS-7: Attachments remain application input

Temps should upload files and images into an authorized path in the persistent
workspace, store attachment metadata in its chat schema, and add bounded file
references to the turn input. Provider-native multimodal input can be added to
the SDK later as a general `TurnInputPart` capability, but it is not required
for the first runtime migration.

## Rejected coupling and unsafe workarounds

| Option | Decision | Reason |
| --- | --- | --- |
| Put Temps project/application IDs in SDK request types | Reject | Platform domain concepts do not belong in a generic agent runtime |
| Let the SDK read Temps settings or database tables | Reject | Couples storage, encryption, and tenancy to one application |
| Move the Temps MCP tool registry into the SDK | Reject | Tool authorization and resource semantics are platform-owned |
| Seed reusable Codex `auth.json` into the sandbox | Reject | The harness and its shell tools could read or exfiltrate the account credential |
| Run the provider CLI on the host while tools operate in the sandbox | Reject | Breaks the execution boundary and risks host filesystem/process access |
| Use the public Temps sandbox API from the embedded Temps server | Avoid | Requires unnecessary self-HTTP authentication and a reusable token |
| Keep a permanent Temps-specific Codex command builder | Reject | Restores the duplicated harness architecture the SDK is intended to remove |
| Use `TurnRequest::environment` for a short-lived relay bearer | Accept | Already generic, bounded, redacted, and application-controlled |
| Implement an internal `ExecutionTransport` in Temps | Accept | This is the intended SDK extension point and preserves SDK independence |
| Use a static model catalog while discovery is unavailable | Temporary | Must be labeled as fallback and never represented as account-verified |

## Migration sequence

### Phase 0: Correct the current behavior

- Mark unsupported sandbox/provider combinations unavailable before turn
  creation.
- Surface the concrete relay/authentication failure rather than `no reply`.
- Stop mixing host discovery with sandbox execution metadata.

### Phase 1: Prepare the SDK

- SDK-1 through SDK-5 are complete for the currently supported native CLI
  capabilities.
- Use `ProviderProbeContext` and `HarnessReadiness::authentication` for
  execution-target discovery; keep Temps relay readiness as a separate input.
- Add provider protocol fixtures for the CLI versions pinned by Temps.

### Phase 2: Build the Temps boundary

- Implement the internal sandbox `ExecutionTransport`.
- Implement the OpenAI/Codex relay.
- Add the SDK-to-chat event and error adapter.
- Preserve the existing implementation behind a rollback flag.

### Phase 3: Migrate providers

1. Migrate Codex after HTTP MCP and relay support pass end-to-end tests.
2. Migrate OpenCode after its launch-context and credential path pass.
3. Migrate Claude after the sandbox provides duplex stdin or an equivalent
   SDK-supported interaction mode.

### Phase 4: Remove duplication

Delete the old provider command construction, JSON parsing, process
supervision, and host/sandbox catalog split only after all compatibility and
recovery tests pass.

## Acceptance criteria

- A Codex turn in a persistent sandbox returns an answer using a short-lived
  host relay capability and never writes reusable provider credentials there.
- Claude, Codex, and OpenCode resume their provider-native session when the
  conversation has a compatible session ID.
- The platform MCP server is available only for the active turn and carries a
  scoped capability rather than a reusable platform token.
- Discovery, model selection, and execution identify the same sandbox and CLI
  version.
- Authentication, model, rate-limit, network, timeout, permission, and process
  failures produce distinct persisted statuses and actionable UI messages.
- A failed turn preserves the user message and can be retried in the same
  conversation after the underlying issue is corrected.
- Cancelling or timing out a turn terminates the provider process tree without
  deleting the persistent workspace.
- Two chats may share a sandbox filesystem without sharing turn credentials,
  approval decisions, event streams, or provider session IDs.
- SDK integration tests cover local and custom transport execution; Temps
  integration tests cover its sandbox, credential relay, authorization, and
  persistence boundaries.

## Non-goals

- Making the SDK aware of Temps applications, projects, databases,
  deployments, or alert rules.
- Replacing Temps' database-backed chat model with the SDK's optional storage
  traits during the initial migration.
- Giving a sandbox a reusable Temps API token or unrestricted provider
  credential.
- Requiring every provider to implement identical native features. Capability
  discovery and explicit unsupported states are preferred over false parity.

---

Maintenance: update this document when an SDK provider adapter, transport
contract, Temps credential relay, or AI workspace execution boundary changes.
Owners: Temps AI workspace and `temps-agent-runtime` maintainers.
