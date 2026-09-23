# Changelog

All notable changes are documented here. The project follows Semantic
Versioning and Keep a Changelog conventions.

## [Unreleased]

### Changed

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
