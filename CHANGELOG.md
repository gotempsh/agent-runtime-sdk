# Changelog

All notable changes are documented here. The project follows Semantic
Versioning and Keep a Changelog conventions.

## [Unreleased]

### Changed

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
