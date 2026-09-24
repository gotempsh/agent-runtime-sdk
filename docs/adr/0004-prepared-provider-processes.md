# ADR 0004: Prepared provider processes

- Status: Accepted (Claude, Codex, and OpenCode retained turns implemented; explicit prewarming deferred)
- Date: 2026-09-23

## Problem

`InProcessRuntimeClient` retains logical session identity, not the provider process.
The built-in executor reports `retained_process: false` and calls `AgentRuntime::run`
for each invocation. Both Claude and Codex therefore pay process startup again on
subsequent turns. Calling `acquire` earlier cannot remove this cost.

The first implementation step is stage timing so an embedder can distinguish
validation, concurrency wait, sandbox setup, process spawn, protocol output and
first assistant text. A first output frame is not evidence of provider readiness
or model request submission. Provider-specific readiness requires an explicit
handshake acknowledgement, not a sleep or an empty model turn.

## Ownership

The SDK owns a bounded process supervisor and provider protocol state. Fleet owns
when a user has selected enough configuration to prepare, durable conversation
records, authorization, feature rollout, and presentation. Listing projects or
conversations must never spawn provider processes.

Existing `AgentRuntime::run` and default retained-client constructors preserve
lazy, one-process-per-turn behavior. `provider_process_retention` opts the
in-process retained client into bounded reuse for the built-in Claude streaming,
Codex app-server, and OpenCode serve protocols. `codex_process_retention` remains
the compatibility opt-in for Codex alone. Custom adapters remain disabled unless
they implement the lifecycle contract.

## Lifecycle

1. Acquire a logical runtime with project, provider, sandbox and launch settings.
2. Send the first real turn. This lazily starts and initializes the app server,
   then submits the prompt exactly once.
3. On a successful turn, keep the provider connection and continue draining its
   bounded event stream. Idle tool/background events belong to the runtime and
   must not be attached to the next invocation.
4. Dispose or expire the idle process, confirming process-tree teardown. Preserve
   session identity so later work can explicitly resume from provider persistence.

Acquiring a logical runtime does not start a provider process or claim provider
readiness. Failure before submission remains `delivery=not_sent`; failure after
submission preserves accepted/possibly-sent semantics and never replays a prompt.

Retention has bounded turn concurrency, separate global process capacity and idle
expiration. Capacity exhaustion falls back to the ordinary cold-turn path.
Cancellation, timeout, dropped futures, disposal and ambiguous idle output poison
the connection and terminate its process tree before it can be reused.

## Configuration and credentials

Use one launch-configuration representation for preparation and sending. Fleet
currently provides MCP definitions, capability credentials, system instructions,
allowed tools and tailnet environment at turn time. Preparing without these and
then spawning again on send would provide no benefit and could misconfigure the
sandbox. Fleet must resolve that context before requesting preparation.

Never share prepared processes across users, projects, sandboxes or credential
contexts. Working-directory, environment, MCP, sandbox revision and credential
changes require disposal/repreparation unless the driver has a tested live-update
operation. Compare full launch configuration without logging secret values.
Runtime and per-turn settings must have explicit precedence.

Fleet's MCP capability lifetime must be reconciled with idle process retention:
retaining a process must neither keep a revoked turn token valid nor silently
broaden it to an unbounded credential. Use scoped runtime credentials with explicit
revocation, or safely replace the child when credentials rotate. This contract is
a prerequisite to Fleet enabling retention.

## Provider implementation

Claude keeps a streaming input channel open and separates initialization from user
messages. Codex keeps one app-server connection and thread alive, issues one
initialize handshake per process, and starts later turns on that connection.
OpenCode retains its loopback serve process while creating a fresh health-checked
HTTP/SSE bridge for each turn. All three use bounded initialization and inactivity
deadlines, crash detection, cancellation, late-event isolation, and process-tree
cleanup. One-shot modes and unsupported providers remain lazy.

## Fleet adoption

1. Upgrade to the verified Windows baseline.
2. Collect startup stage timings without changing process behavior.
3. Add and verify retained-process drivers and preparation in the SDK.
4. Add a feature-gated Fleet preparation endpoint using the generated API client.
5. Trigger preparation only after explicit project/provider selection; display
   queued/running/ready/failed feedback while the composer stays usable.
6. Measure cold/prepared first-turn and subsequent-turn latency, process count,
   idle memory, abandoned preparation cost and failure rate before wider rollout.

The dependency upgrade alone must not start extra processes. Existing sessions,
legacy adapters, remote execution and unattended work retain their behavior until
that path explicitly opts in and has equivalent lifecycle coverage.

## Acceptance evidence

- No prompt or model request during preparation; one process for prepare+send.
- Concurrent prepare/send is deduplicated; exactly one submitted invocation.
- Two turns reuse the same native PID and preserve provider session identity.
- Sandbox/MCP/environment/credential changes never reuse a stale process.
- Deadline, cancellation, disposal, crash and failed initialization release slots.
- Capacity remains bounded; idle expiration does not interrupt active work.
- Background events are drained and never misattributed to a later turn.
- Existing non-opt-in callers and all provider permission tests continue to pass.
- Native Windows/macOS/Linux lifecycle tests, plus authenticated Claude/Codex
  smoke measurements with an isolated workspace before enabling Fleet by default.
