# API and capability reference

This reference describes the `0.1` contract. Provider CLIs evolve independently;
pin and test the CLI versions used in production.

## Cargo features

| Feature | Default | Adds |
| --- | --- | --- |
| `claude` | yes | `providers::Claude` stream-JSON adapter |
| `codex` | yes | `providers::Codex` `exec --json` and `app-server` adapter |
| `opencode` | yes | `providers::OpenCode` JSON adapter |
| `nono` | yes | profile management and per-turn Nono execution |
| `tailnet` | yes | per-agent userspace Tailscale daemons and split proxy |
| `ssh` | yes | OpenSSH execution transport |
| `temps-sandbox` | no | optional Temps sandbox HTTP transport |

The core types and public `AgentAdapter` trait compile without provider
features.

## Provider capabilities

| Capability | Claude Code | Codex | OpenCode |
| --- | --- | --- | --- |
| Installed CLI discovery | yes | yes | yes |
| Normalized text events | yes | yes | yes |
| Reasoning events when reported | yes | yes | yes |
| Tool lifecycle events | yes | yes | yes |
| Native task/subagent events | yes | no | no |
| Usage when reported | yes | yes | yes |
| Account quota windows and reset times | explicit fetch + live turn + optional discovery snapshot | explicit fetch + optional discovery snapshot; live if emitted | no |
| Context-window occupancy | yes, estimated from native usage components and direct after compaction | no | no |
| Configurable automatic compaction | yes | no | no |
| Provider-native manual compaction | yes, retained runtime with an existing session | no | no |
| Session identifier | yes | yes | when reported |
| Resume by session identifier | yes | yes | yes |
| `Default` | yes | yes, static workspace sandbox | yes, asks auto-reject in headless mode |
| `AcceptEdits` | yes | yes, static workspace sandbox | no; rejected rather than broadening access |
| `Plan` | yes | yes, read-only sandbox | yes, built-in `plan` agent |
| `FullAccess` | yes | yes | yes, `--auto`; explicit configured denies remain |
| Custom mode | yes | yes, native approval policy | yes, configured agent |
| Live approvals | yes | app-server mode only (`exec` is configured non-interactively) | no (`run` is non-interactive) |
| Live user questions | yes | app-server mode only, blocking and async | no |
| Cooperative interrupt on cancellation | no | app-server mode only (`turn/interrupt`) | no |
| Structured launch context | system prompt, exact tools, stdio/HTTP MCP, strict MCP | additive stdio/HTTP MCP | rejected |
| Native image attachments | no; described as prompt paths | yes (`--image`, `localImage` input) | no; described as prompt paths |
| Prompt kept out of argv | yes | yes | no; current `run` CLI uses message args |
| Current backend | CLI stream JSON | `codex exec --json` (default) or `codex app-server` | `opencode run --format json` |

The public adapter trait is the extension point for OpenCode server or
SDK-backed adapters. Such adapters should preserve the normalized contract and
establish compatibility coverage before replacing a CLI adapter.

### Codex turn modes

`providers::CodexTurnMode` selects how a Codex turn runs. `Exec` (the default)
keeps the one-way `codex exec --json` behavior. `AppServer`, selected with
`Codex::app_server()` or `Codex::default().with_turn_mode(CodexTurnMode::AppServer)`,
drives `codex app-server` over JSON-RPC and adds:

- live approvals for `item/commandExecution/requestApproval`,
  `item/fileChange/requestApproval` and `item/permissions/requestApproval`,
  answered through `InteractionHandler::approve`. `ApprovalDecision::Allow`,
  `ApprovalDecision::AllowForSession` and `ApprovalDecision::Deny` map to the
  native `accept`, `acceptForSession` and `decline` decisions;
- `item/tool/requestUserInput` questions. A blocking question
  (`isBlocking: true`) becomes `TurnEvent::QuestionRequested` and waits for
  `InteractionHandler::answer`. A non-blocking question becomes
  `TurnEvent::AsyncQuestionRequested`, is answered immediately on the wire with
  a note that no answer exists yet, and never stalls the turn; deliver the
  user's eventual answer as a follow-up prompt;
- incremental text and reasoning deltas, thread token usage, and a cooperative
  `turn/interrupt` when the turn's `CancellationToken` fires.

The app-server mode requires a transport whose capabilities include
`interactive_stdin`. Model, sandbox, approval policy, service tier and the
resumed thread identifier travel in `thread/start`/`thread/resume` and
`turn/start` instead of argv; turn-scoped stdio and HTTP MCP servers and the
model relay still use `--config` overrides.

## Private-network providers

`NetworkProviderRegistry` stores heterogeneous, trusted in-process
`NetworkProvider` implementations under validated `NetworkProviderId` values.
It returns a `ManagedNetworkSession` whose provider identity is assigned by the
registry rather than repeated by the implementation. `NetworkSession` exposes
status, authentication, teardown, restart, and launch access through an
object-safe interface. Providers without interactive authentication return
`NetworkError::Unsupported` from that operation.

`NetworkAccess` supplies launch environment, agent guidance, and
`NetworkSandboxRequirements`. Capability values only select UI and onboarding;
they are self-reported and must never authorize privileged operations or prove
traffic isolation. Provider startup is cancellation-safe, session teardown
revokes connectivity before success, and the final session owner cleans up
ephemeral provider resources. Authentication URLs and access configuration are
sensitive and should not be persisted or logged. `ManagedNetworkSession::shutdown`
awaits provider teardown before releasing its process-wide state-directory
reservation. Dropping the wrapper without shutdown keeps the directory
reserved until process exit; separate host processes must use distinct private
state roots.

The `tailnet` feature implements this contract with `TailscaleProvider` and
`HeadscaleProvider` while preserving the concrete `TailnetDaemon` and
`TailnetAccess` APIs. Headscale uses the same userspace Tailscale data plane,
with a validated HTTPS coordination-server URL supplied to `tailscale up`.

## Primary types

### `AgentRuntimeBuilder`

Registers adapters and sets process-wide limits. `concurrency_limit` defaults
to 2. Prompt and event-line limits are validated before or while reading a
turn. A zero limit is rejected. `transport` and `transport_from_arc` replace
the default `LocalTransport` for both readiness and execution.

### `AgentRuntime`

`readiness(provider)` performs bounded executable inspection inside the
configured execution transport.
`discover_harnesses()` concurrently probes every registered adapter inside that
same transport and returns `HarnessInventory`. Each `HarnessReadiness` preserves
its executable metadata, independent `HarnessAuthentication`, backwards-compatible
`PermissionSupport`, provider-native `control_groups`, transport-fetched
`models`, compatibility limitations, and a typed error without failing the rest
of the inventory. It may also include an
`account_usage` snapshot from the same provider-native metadata exchange.
Catalog probes are
metadata-only and bounded; they do not start an agent turn.
`discover_harnesses_with(ProviderProbeContext)` additionally applies an
execution-target-local working directory and explicit `SecretString`
environment. The context is bounded before spawn, its debug representation
contains only environment keys, and diagnostics are redacted against supplied
values. The same context type is accepted by `fetch_account_usage_with`.
`permission_support(provider)` reports static modes and live interaction
support without starting a turn.
`launch_context_capabilities(provider)` reports field-level support for system
instructions, tool restrictions, stdio/HTTP MCP, and strict MCP isolation.
`turn_capabilities(provider)` reports optional per-turn behaviors, currently
`native_image_attachments`: whether the adapter delivers `TurnRequest`
attachments with an `image/*` media type as native provider image inputs
instead of leaving them described as host paths in the prompt. The retained
runtime mirrors the same flag on `RuntimeDriverCapabilities` and stops
appending those paths to the prompt when the driver reads them natively.
`fetch_account_usage(provider)` performs a bounded, fetch-on-demand quota query
inside the configured execution transport. It returns an `AccountUsageReport`
with `available`, `unavailable`, or `unsupported` status; unavailable reports
carry a bounded reason and retryability hint rather than representing missing
data as zero. Account usage belongs to the authenticated provider identity on
the target and is independent from conversation context usage.
`run(request, events, interactions)` acquires a concurrency permit, prepares
the optional sandbox, supervises the provider, and returns `TurnResult`.

`HarnessCatalogErrorKind` distinguishes provider authentication, permission,
model availability, rate limiting, and network failures from transport,
protocol, timeout, and generic command failures. A failed catalog remains
separate from executable readiness because basic execution may still be usable.
`HarnessAuthenticationStatus` is independently `unknown`, `authenticated`,
`required`, `rejected`, or `unavailable`. Claude and Codex use bounded native
status commands; adapters that cannot prove a single provider identity return
`unknown` rather than inferring credentials from executable discovery.

### `TurnRequest`

Contains provider, working directory, prompt, typed `TurnProvenance`, optional
model/reasoning/session, permission intent, provider-native `harness_options`,
`LaunchContext`, `AutoCompactionPolicy`, deadlines, explicit environment secrets, cancellation, and an
optional `SandboxRequest`.
`harness_options` accepts the group keys and values returned by discovery; it
keeps approval, sandbox, collaboration/agent mode, and Codex service tier
orthogonal. `tool_process_policy` defaults to
`ToolProcessPolicy::PreserveOnCompletion`, so a natural provider exit does not
kill intentionally detached tool processes. Set `TerminateOnCompletion` for
turn-scoped automation. Cancellation, timeout, sink failure, and a dropped
in-flight future always terminate the supervised tree. Its `Debug`
implementation prints prompt length, environment keys, and non-secret sandbox
capabilities—never prompt or secret values.

Explicit environment input is bounded to 128 variables, 64 KiB per value, and
256 KiB across names and values. These values are injected into the harness;
they are not isolated from tools launched by that harness.

### `LaunchContext`

Carries host-resolved standing instructions, exact tool availability, and named
MCP server definitions without introducing application concepts such as personas.
`allowed_tools: None` preserves provider defaults, while an empty list disables
all tools. MCP credentials are referenced by environment-variable name and must
be supplied separately through the redacted request environment. Support is
field-level and exposed through `LaunchContextCapabilities`; unsupported fields
are rejected before spawn. Claude supports every current field, while Codex
supports additive stdio and HTTP MCP definitions. Codex forwards stdio MCP
credentials through `mcp_servers.<name>.env_vars`, which selects harness
variables by name, so each `environment_from` entry must map a variable to a
source variable of the same name. See
[Configure launch context](../how-to/configure-launch-context.md).

### Agent Relay

The `relay` module defines stable logical `AgentAddress` values, bounded
`AgentMessageEnvelope` records, thread/reply correlation, opaque attachment
references, delivery receipts, `MessagingGrant`, typed retry/delivery errors,
and the application-implemented `AgentMessageRouter` boundary.

`AgentRelayMcpBridge` binds sender identity and hop state outside tool input,
enforces the grant's per-turn/rate/byte/hop ceilings, and implements the minimal
MCP JSON-RPC method surface. `AgentRelayMcpExposure` explicitly adds an
application-hosted HTTP or stdio endpoint to `LaunchContext`; merely depending
on the crate exposes no relay tools. See
[Relay messages between independent agents](../how-to/agent-relay.md).

### `TurnEvent`

The non-exhaustive event enum currently includes session start, text and
reasoning deltas, tool lifecycle, Claude native task snapshots/activity,
context compaction boundaries,
content-free Agent Relay activity, approval, question, turn usage, provider
account-usage snapshots, and warnings. A `ToolCall` can carry the native
`task_id` that owns it. Consumers must include a fallback match arm so minor
releases can add events.

### `EventSink`

An async, backpressured destination. Returning an error ends the turn. The
application should redact provider-native tool payloads before logs or
telemetry.

### `InteractionHandler`

An application-owned approval/question bridge. `DenyAll` is the default when
none is supplied. Approval timeouts deny; question timeouts return no answer.
`InteractionBroker` is the SDK-owned, bounded coordination implementation for
web and desktop hosts: the runtime awaits it while an authorized boundary calls
`resolve_approval`, `resolve_question`, or `decline_question`. It safely stages
a response that arrives between event publication and waiter registration and
removes abandoned waiters automatically. Persist and authorize the response
before resolving the broker; the broker coordinates live delivery and does not
replace durable storage.

`QuestionRequest::prompts` exposes provider-neutral `QuestionPrompt` and
`QuestionOption` values ready for a UI while retaining the provider-native
`questions` payload for auditing. `QuestionAnswer::selected` constructs the
answer map expected by the harness.

For durable implementation and restart semantics, see
[Persist approvals](../how-to/persist-approvals.md).

### `ChatStore`

An application-supplied persistence boundary for durable chats. `Chat` stores
the latest status and provider session; `ChatMessage` stores the transcript;
`ChatEvent` stores replayable normalized activity; and `ChatApproval` stores an
invocation-scoped approval audit record.

`ChatStore::create_chat` atomically creates a chat and its first user message.
`commit` uses `ChatCommit::expected_revision` for optimistic concurrency and
atomically appends messages, events, and approvals with the materialized chat
update. `load_chat`, cursor-based `list_chats`, and bounded `events_after`
support persistent chat UIs and replay-then-follow streaming.

A provider invocation ID disambiguates events and repeated native approval IDs
inside one chat. It is not a second public conversation entity. See
[Persist chats and streams](../how-to/persist-conversations.md).

### `AgentAdapter`

A public provider extension point with command construction, one-line parsing,
and interaction-response encoding. `CommandSpec` keeps program, args, stdin,
and environment separate.

Adapters can implement `account_usage_probe` and `parse_account_usage_probe`
to support explicit quota refresh without starting an agent turn. Probe output
is bounded and executed through the selected transport. Provider credentials
must stay private to that command; return only normalized `AccountUsageReport`
data.

Adapters can independently implement `authentication_probe` and
`parse_authentication_probe`. The runtime supplies bounded stdout, bounded
stderr, and the native exit status, then redacts any returned reason before it
enters `HarnessReadiness`. Returning no probe is a supported capability state
and produces `HarnessAuthenticationStatus::Unknown`.

`executable` returns a name or path meaningful inside the selected transport.
An adapter does not require that path to exist locally when constructing a
command. The older adapter-local `readiness` method remains for compatibility;
applications should call `AgentRuntime::readiness`.

### `ExecutionTransport`

Owns executable readiness, working-directory validation, process spawning,
byte-stream stdin/stdout/stderr, native process identity, termination, and
optional reattachment. `LocalTransport` is the default.

`TransportCapabilities` declares interactive stdin, remote execution,
reattachment, managed-process support, process-tree termination, and intrinsic
`SandboxCapabilities`. Claude requires interactive stdin. Every agent turn
requires process-tree termination so cancellation cannot knowingly orphan tool
descendants.

`TransportSpawnRequest` preserves `CommandSpec` program/argument boundaries and
treats its working directory as transport-local. `TransportProcess` exposes
the streams, `TransportProcessHandle`, optional local PID, `wait`, `terminate`,
and `disarm`. See
[Run agents through a custom execution transport](../how-to/custom-transport.md).

`AgentRuntime::suggest_working_directories(input, limit)` provides bounded,
non-recursive autocomplete inside the configured transport. It returns the
transport user's home directory, at most 50 matching directories, and an
`exact_match` flag that applications can use before enabling a turn. Absolute
paths, `~` paths, and relative prefixes are resolved by the target—not by the
SDK host. `LocalTransport` and `SshTransport` implement this operation. Custom
transports can implement `ExecutionTransport::suggest_working_directories`;
the default is a typed `Unsupported` error.

`SshTransport` is the interactive remote implementation. It supports writable
stdin and remote process-group termination. `TempsSandboxTransport` is the
reattachable HTTP implementation for an existing Temps sandbox; it supports
staged initial stdin but not live interactive stdin.

### Harness discovery

`HarnessStatus` is `Ready`, `NotInstalled`, `Incompatible`, or `Unavailable`.
`HarnessLimitation` distinguishes a missing interactive-stdin channel from
missing process-tree termination. Discovery is bounded to registered adapters;
it never searches for hosts, sandboxes, workspaces, or session history. See the
[onboarding guide](../how-to/discover-harnesses.md).

Provider catalog sources are native and transport-local: Claude Code's
prompt-free stream-JSON initialization response, Codex app-server model and
collaboration lists, and `opencode models`. `HarnessModel::description` keeps
the provider's own generation/context description, while `id` remains the
exact value to send on a turn.

### `ManagedProcessSupervisor`

Owns provider-independent background commands and long-running services. A
`ManagedProcessSpec::background` command defaults to no restart; a
`ManagedProcessSpec::service` defaults to restart on failure. The supervisor
uses direct executable and argv boundaries, a sanitized environment, one
process group/tree per record, and bounded stdout/stderr capture.

`start` returns a `ManagedProcessHandle`. Both handle and supervisor expose
typed state/log access and control. `subscribe` and `ManagedProcessHandle::recv`
deliver future `ManagedProcessEvent` values. `list`, `snapshot`, and `logs`
hydrate reconnecting consumers. `stop` retains history, `restart` reuses the
original specification, and `delete` stops before removing the record.
Configure the same `ExecutionTransport` through the supervisor builder to run
services beside remote agent processes. Snapshots retain the latest native
transport handle even after exit.

The default limits are 32 retained records, 1,000 log lines per record, 4,000
characters per line, and 256 queued live events. Dropping the final owning
supervisor/handle stops active process trees. Retaining application state lets
these processes outlive any individual agent turn. See
[Manage background commands and services](../how-to/manage-background-processes.md).

### `SandboxBackend`

An async extension point that receives `SandboxContext` and a separated
`CommandSpec`. A configured backend advertises the `SandboxCapabilities` it
supports and returns a prepared command or `SandboxError`.

`SandboxRequest` combines a backend with per-turn required capabilities.
Missing controls fail before the provider starts. `CommandSpec::wrap_with`
helps wrappers preserve the inner command's stdin and environment contract.

A backend may implement `classify_event` to turn a provider tool failure into
a structured `SandboxViolation`. Nono recognizes bounded filesystem-denial
diagnostics and leaves ambiguous failures unclassified.

### Managed sandbox profiles and retry

`SandboxProfileManager` is the persistence/materialization boundary. `resolve`
returns an exact `SandboxProfileRef` plus its backend. `update` receives that
revision as a compare-and-swap token and must durably commit, validate, and
return the new exact revision.

`SandboxRecoveryHandler` is the application authorization boundary. It returns
`Deny` or one explicit `SandboxProfileChange`; the runtime never invents or
widens a grant itself.

`AgentRuntime::run_with_sandbox_recovery` emits `SandboxAccessDenied`, waits for
the handler, calls the manager, emits `SandboxProfileUpdated` and
`SandboxStepRetrying`, then resumes the same provider session. A missing session
ID, denied approval, profile failure, or exhausted retry bound stops recovery.
There is never an unsandboxed retry. See
[Recover a denied sandbox step](../how-to/recover-sandbox-denials.md).

## Error behavior

`TransportErrorKind` distinguishes invalid configuration, SSH/API
authentication, host-key verification, DNS, refusal, timeout, remote
availability, missing executables/directories, permission, stream, process
control, protocol, and unsupported operations. `retryable` tells an
application whether retrying without a configuration change can be useful.

`RuntimeError` preserves typed transport failures and reports missing transport
capabilities separately. `RuntimeError::ProcessFailed` represents both
non-zero exits and provider-native failed terminal frames, including providers
that report failure before exiting successfully. It carries a normalized
`ProviderProcessErrorKind`, native exit code when available, optional bounded
provider code, `DeliveryState`, and a bounded diagnostic redacted against the
request environment. Provider process failures are classified as
authentication, permission, model availability, rate limiting, network, or
unknown failures. Variants are non-exhaustive.

Applications should use the typed kind, provider code, and delivery state for
recovery policy. Diagnostic text is for display and support context, not
control flow. `DeliveryState::NotSent` permits a fresh dispatch;
`PossiblySent` requires reconciliation before replay; `Accepted` means the
provider acknowledged or began the turn.

No error triggers a different provider, an unsandboxed retry, or a permission
upgrade. Managed retry occurs only through the explicit recovery API after an
approved and successfully committed profile revision.

For the end-to-end operational sequence, see
[Operate sandboxed turns](../how-to/operate-sandboxes.md). For materialized
tool execution state, see
[Persist command and tool execution](../how-to/persist-command-execution.md).

## Nono capabilities

| Control | `NonoMode::Run` | `NonoMode::Wrap` |
| --- | --- | --- |
| Filesystem boundary | yes | yes |
| Block all network | yes | yes |
| Destination allowlist | yes | no |
| Named credential proxy | yes | no |
| Supervisor audit | yes | no |
| Explicit macOS proxy CA trust | yes | no |

Use `NonoMode::capabilities()` instead of hard-coding this table.

Custom backends can advertise the same capability type. Capability claims are
trusted declarations by backend implementers, not runtime attestation.

## Limits and compatibility

- Default maximum prompt: 256 KiB.
- Default maximum provider event line: 2 MiB.
- Captured stderr tail: 32 KiB.
- Default turn deadline: 30 minutes.
- Default interaction deadline: 10 minutes.
- Unix cancellation uses a dedicated process group. Windows cancellation uses
  `taskkill /T /F` to terminate the child tree.
- Natural provider exit preserves detached tool descendants by default. This
  behavior is configurable with `ToolProcessPolicy` and does not override a
  provider, service manager, or sandbox backend that enforces its own cleanup.
- Managed-process state and log buffers are in memory. Persist their events in
  the embedding application and reconcile a host crash as an interruption.
- Minimum supported Rust version: 1.88 (checked in CI with the committed lockfile).
