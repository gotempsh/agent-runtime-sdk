# Changelog

All notable changes are documented here. The project follows Semantic
Versioning and Keep a Changelog conventions.

## [Unreleased]

### Added

- Messages into a running turn. On a retained Claude process,
  `TurnHandle::send_message` and the cloneable `TurnHandle::message_handle`
  deliver further user input into the active invocation, which answers it on
  its own stream and completes only once every message has been answered. A
  message to an invocation that has ended fails with `InvalidRequest` and
  `DeliveryState::NotSent`. Support is reported by the new `live_messages`
  flag on `TurnCapabilities` and `RuntimeDriverCapabilities`; executors and
  adapters opt in through `RuntimeTurnExecutor::send_retained_message` and
  `AgentAdapter::encode_user_message`, both with defaults that decline.

### Fixed

- Interrupting a retained Claude turn no longer kills its process. The
  interrupt is now cooperative: Claude's `interrupt` control request (with
  `cancel_queued`) stops the foreground reply, its tools and any queued
  messages, and the process is kept. Background shells keep running. Claude
  itself stops background subagents when it is interrupted mid-work, reported
  as `Stopped` task activity. A turn that has already answered is ended without
  sending Claude anything, so all of its background work keeps running. Output,
  Claude's answers and approval requests from the surviving work are buffered
  (bounded) for the next turn. An unconfirmed interrupt still retires the process after three
  seconds. Adapter hooks `retained_interrupt_settled` and
  `retained_background_work` default to the previous behavior.
- A retained Claude turn now ends on the completion of the commands it sent,
  correlated through Claude's `command_lifecycle` frames, instead of on the
  first `result`. A turn whose reply finished before a queued message, or an
  unrelated follow-up answer, no longer completes early.

- Claude background subagents survive a new prompt on a retained process.
  A retained Claude turn that has answered and is only running background
  work now hands its live process to the next turn instead of rejecting it as
  `RuntimeBusy`, so applications no longer have to interrupt the turn (killing
  the subagents) to deliver a follow-up message. The next turn inherits the
  running tasks and receives their events and Claude's answer to their
  completion. A retained turn also no longer ends the moment its background
  work drains: it waits for Claude's follow-up answer (bounded by a short quiet
  grace), which previously reached the idle process and retired it.
  `RuntimeTurnExecutor::request_retained_handoff` and the
  `AgentAdapter::retained_handoff_ready`, `inherit_retained_handoff` and
  `retained_completion_grace` hooks are additive with no-op defaults.
- OpenCode's compaction summary is no longer streamed as assistant text. Its
  cost still counts: OpenCode turn cost is now the sum of every assistant
  message in the turn instead of the last message's cost.
- Codex resumes and active-writer forks omit historical turns from their replies,
  so long conversations can continue without exceeding the protocol frame limit.
  Saved provider context is preserved.

### Changed

- Add opt-in bounded native process reuse for Claude streaming and OpenCode serve,
  alongside Codex app-server reuse. Retained processes use provider-specific
  preflight checks, strict session/configuration isolation, idle expiry, and
  no-replay delivery boundaries. Default one-process-per-turn behavior is unchanged.

- Preserve Windows system and profile environment variables when launching providers,
  without inheriting unrelated credentials. Hide provider and cleanup console windows.
  Add native process fixtures for arguments, stdin, failures, cancellation, and
  Windows Claude/Codex/OpenCode `.cmd` wrappers.

- Corrected the minimum supported Rust version to 1.88 to match locked
  dependencies, with an all-targets/all-features compiler check in CI.
- Crate archive paths are root-anchored so nested third-party README/license
  files and local application dependencies cannot enter the package through
  root-document patterns. Added package-boundary regression checks and a
  standalone archive build to CI.
- Codex model relays reject non-loopback HTTP endpoints. Existing integrations
  using a remote plaintext relay must switch to HTTPS or a loopback tunnel.
  Relay destinations and credential references remain trusted host settings.

### Added

- Report the context-compaction lifecycle inside ordinary turns for Claude
  (`status: compacting` → `compact_boundary`), Codex app-server
  (`contextCompaction` items, with `thread/compacted` as a fallback), and
  OpenCode serve (`compaction` part → `session.compacted`). Adapters emit
  `TurnEvent::CompactionStarted` when the harness begins compacting and the
  new `TurnEvent::CompactionFailed` when an open compaction ends without
  compacting, so applications can show an in-progress state for automatic
  compaction. New `TurnCapabilities::compaction_lifecycle` and
  `RuntimeDriverCapabilities::compaction_lifecycle` flags advertise support.
  Retained invocations deliver one start per compaction even when the provider
  repeats its signal.
  A turn or retained invocation that ends with a compaction still open (the
  provider exits, is cancelled, or never confirms a manual `/compact`) emits
  `CompactionFailed`, so no compaction is left running.

- Add opt-in, bounded Codex app-server process reuse for in-process retained
  runtimes, with strict runtime/configuration isolation, cold fallback at pool
  capacity, idle expiry, and process-tree cleanup on cancellation or failure.
- Contain observer panic-payload cleanup failures and disable failed observers
  across runtime clones. Report event-delivery wait separately from observed
  first-text latency, preserving bounded backpressure.
- Opt-in payload-free startup timing observers for validation, concurrency admission,
  sandbox preparation, process spawn, first output/text and terminal outcomes.
  Existing process lifetime and wire events are unchanged.

- Bidirectional OpenCode support through `opencode serve`. `OpenCodeTurnMode`
  selects the transport; `OpenCode::serve()` starts the server on a reserved
  loopback port and drives it over HTTP and Server-Sent Events, adding live
  approvals (`once`/`always`/`reject`), incremental text and reasoning deltas,
  tool lifecycle events, and a cooperative `session/abort` on cancellation.
  `PermissionSupport` reports `live_approvals` in that mode. The default
  `opencode run --format json` transport is unchanged and still reports
  `live_approvals: false`.
- Enforced per-turn permissions for OpenCode. `Serve` mode supplies the policy
  through `OPENCODE_CONFIG_CONTENT`, which the server reads instead of the
  ambient configuration, so the requested policy is the one the harness runs
  under. `PermissionMode` maps onto OpenCode's `edit`/`bash` axes as
  `Default`/`Custom` = ask/ask, `AcceptEdits` = allow/ask, `FullAccess` =
  allow/allow and `Plan` = deny/deny. An empty
  `LaunchContext::allowed_tools` becomes a `{"*": "deny"}` wildcard. A plan
  turn and an empty allowlist additionally refuse any permission that reaches
  the adapter without consulting the application. `Run` mode had no
  enforcement an application could rely on: it accepts only `--auto` and
  `--agent plan`, leaving every other policy to the machine's own
  configuration.
- Turn-scoped stdio and HTTP MCP servers for OpenCode in `Serve` mode,
  translated into native `local` and `remote` `mcp` entries with credentials
  referenced as `{env:NAME}` rather than serialized.
  `LaunchContextCapabilities` now advertises `stdio_mcp`, `http_mcp`,
  `system_prompt_append` and `allowed_tools` for that mode; the latter two are
  carried as a prompt prefix, which is the only channel OpenCode offers.
- `AgentAdapter::attach`, returning optional `ProtocolStreams`, lets an adapter
  carry a turn on streams of its own instead of the child's stdout and stdin,
  for a provider whose protocol is not on its own stdio. The frame contract is
  unchanged, so `parse_line` stays one synchronous state machine and
  cancellation, interrupts, interaction timeouts and line bounding are shared
  by both kinds of provider. `AgentAdapter::command_for_turn` exposes the state
  `prepare_turn` seeded, which now runs before the command is built.

- Bidirectional Codex support through `codex app-server`. `CodexTurnMode`
  selects the transport; `Codex::app_server()` drives JSON-RPC over stdio with
  live approvals (`accept`/`acceptForSession`/`decline`),
  `item/tool/requestUserInput` questions, incremental text and reasoning
  deltas, thread token usage, thread resume and fork, and a cooperative
  `turn/interrupt` on cancellation. `PermissionSupport` reports
  `live_approvals`/`live_questions` in that mode. The default
  `codex exec --json` transport is unchanged.
- Turn-scoped stdio MCP servers for Codex. `McpServerConfig::Stdio` entries are
  translated into native `-c mcp_servers.<name>.command/args/env_vars`
  overrides in both the `exec --json` and `app-server` turn modes, and
  `LaunchContextCapabilities::stdio_mcp` is now advertised for Codex. Codex
  forwards MCP environment variables by name, so each `environment_from` entry
  must name a source variable identical to the child variable; anything else is
  rejected before spawn.
- Native image attachments for Codex. `TurnRequest::attachments` carries the
  execution-host file references an adapter can read itself, `TurnCapabilities`
  (`AgentAdapter::turn_capabilities`, `AgentRuntime::turn_capabilities`) reports
  `native_image_attachments`, and `RuntimeDriverCapabilities` mirrors the same
  flag. Codex sends `image/*` attachments as `codex exec --image` arguments or
  `localImage` `turn/start` inputs, and the retained runtime no longer appends
  their host paths to the prompt. Providers without native support keep the
  existing path-text behavior.
- Context-window fidelity for Codex `app-server` turns. Token-usage
  notifications are attributed to the active thread, the emitted
  `ContextWindowUsage` is labelled with the resolved (or requested) model, and
  `TurnCapabilities::context_window_usage` plus the existing
  `RuntimeDriverCapabilities::context_window_usage` now report Codex
  app-server support so applications can gate a context meter.
- `TurnEvent::AsyncQuestionRequested` for a question the turn did not wait on
  (Codex `isBlocking: false`). The runtime answers the provider immediately so
  the turn keeps running; hosts show the question as open and deliver the
  answer as a follow-up prompt.
- `ApprovalDecision::AllowForSession` for providers with a session-scoped
  grant. Adapters without one treat it as `Allow`.
- `AgentAdapter::prepare_turn` (seed per-turn parser state from the validated
  request), `AgentAdapter::interrupt_request` (encode a cooperative interrupt
  the runtime writes before terminating a cancelled turn), and
  `AdapterOutput::writes` (provider frames the runtime writes to stdin without
  waiting for an application decision). All three are additive with defaults.
- Object-safe private-network provider, session, access, and registry contracts
  with validated provider IDs, explicit cancellation/teardown requirements,
  provider-neutral sandbox requirements, and built-in `TailscaleProvider` and
  `HeadscaleProvider` implementations. Headscale control servers require a
  credential-free HTTPS URL and use the Tailscale userspace data plane.
- Optional `tailnet` feature: supervised per-agent userspace `tailscaled`
  daemons (`TailnetDaemon`) with a loopback split proxy, browser login URL
  reporting, and a `TailnetAccess` that applies proxy and `TEMPS_TAILNET_*`
  environment to any provider command. `NonoExecution::tailnet` opens the
  daemon's ports, socket, and ssh config inside the sandbox and can chain
  Nono's filtering proxy into the split proxy. The daemon relaunches
  itself when its control socket disappears and never reports a daemon
  that stopped answering as connected.
- Explicit `ProviderProbeContext` for transport-local catalog and account-usage
  discovery with bounded secret environment injection, redacted diagnostics,
  and typed provider authentication and availability failures.
- Independent harness authentication readiness with bounded Claude and Codex
  native status probes; unsupported or multi-provider adapters remain
  explicitly `unknown` instead of inheriting executable readiness.
- Bounded, transport-local `fetch_account_usage` queries with typed
  available/unavailable/unsupported reports, explicit Claude OAuth usage and
  Codex app-server quota fetchers, reset windows, plan metadata, and scoped
  display labels.
- Opt-in application-owned Agent Relay contracts, scoped MCP tool bridge,
  typed inbound provenance, bounded loop/budget enforcement, at-least-once
  delivery reconciliation, deterministic integration coverage, and a live
  two-project Claude verification fixture.

- Transport-aware `discover_harnesses` inventory with typed readiness,
  permission support, compatibility limitations, and per-provider errors.
- Full-stack target onboarding for local, SSH, and optional Temps sandbox
  transports, plus a trait-based SQLite run/event/approval store with restart
  recovery.
- Full-stack Axum + React/Tailwind/shadcn runtime-console example with bounded
  SSE history, approvals, cancellation, reload recovery, deterministic failure
  coverage, optional installed-provider mode, and Playwright E2E tests.
- Built-in SSH and optional Temps sandbox transports with typed failures, remote
  readiness, streaming, managed services, reattachment where supported, and
  remote process-tree cleanup.
- Stable transport and provider-process error categories for user recovery UI.

- Pluggable local or remote execution transports shared by provider turns and
  managed processes, with transport-scoped readiness, streaming byte pipes,
  remote working directories, native handles, and intrinsic sandbox controls.
- Provider-independent background commands and managed services with bounded
  status/log streaming, restart policies, and owned process-tree cleanup.
- Claude native Task/Agent subagent snapshots, lifecycle activity, nested tool
  attribution, and post-result background-task streaming.
- Branded React/Tailwind documentation portal with client-side search,
  Diátaxis navigation, generated Rustdoc under `/api/`, and GitHub Pages
  deployment.
- Durable conversation and normalized event reference guides.
- Explicit `ToolProcessPolicy`: detached tool processes survive natural turn
  completion by default, while cancellation, timeout, and dropped futures
  retain hard-stop cleanup.
- Portal guides for durable approvals, command execution projections, worker
  restart reconciliation, and managed sandbox operation.
- Trait-based managed sandbox profile resolution and optimistic revision
  updates.
- Structured sandbox denial, profile-updated, and step-retrying events.
- Opt-in bounded recovery that commits an approved profile update before
  resuming the same provider session, with Nono filesystem-denial
  classification and portable profile-change helpers.

### Fixed

- Claude, Codex, and OpenCode native terminal failure frames now survive
  successful process exit as typed, redacted failures with provider codes and
  delivery certainty; retained runtimes preserve the same recovery metadata.
- Turn cancellation now interrupts pending approval and question handlers and
  terminates the supervised provider instead of waiting for the interaction
  timeout.
- A turn carried on adapter-supplied protocol streams now terminates its child
  as the normal shutdown instead of waiting for an exit that never comes:
  `opencode serve` is a server and does not stop because a turn ended. The turn
  loop also stops reading at a terminal frame in that mode, so a carrier that
  never closes its reader cannot hang a turn.

## [0.1.0] - 2026-08-31

### Added

- Provider-neutral runtime, events, interactions, and typed failures.
- Claude Code, Codex, and OpenCode CLI adapters.
- Bounded concurrency, deadlines, cancellation, environment sanitization, and
  process-tree cleanup.
- Nono capability reporting, managed profiles, strict validation, per-turn
  grants, and named credential proxy arguments.
- Pluggable sandbox backends with explicit required-capability checks and Nono
  as the first implementation.
- Provider permission-support inspection and explicit OpenCode permission-mode
  mappings.
- Tutorials, reference documentation, security policy, migration plan, and CI.

[Unreleased]: https://github.com/gotempsh/agent-runtime-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gotempsh/agent-runtime-sdk/releases/tag/v0.1.0
