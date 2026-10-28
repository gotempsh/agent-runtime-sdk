# Changelog

All notable changes are documented here. The project follows Semantic
Versioning and Keep a Changelog conventions.

## [Unreleased]

### Security

- `opencode serve` now requires HTTP Basic credentials
  (`OPENCODE_SERVER_USERNAME`/`OPENCODE_SERVER_PASSWORD`, a per-process
  password derived from a random SDK secret and the port), and the SDK's HTTP
  bridge sends them. Another local process can no longer drive a served
  session through its loopback port, including a port forwarded to the SDK
  host for an SSH target.

### Added

- `AgentRuntime::subscribe_parked_process_ends`: announces a parked retained
  process that ended while background work it started was still running (it
  exited or was killed, it wrote output the runtime could not read, or a turn
  resuming a different session replaced it) as `ParkedProcessEnded {
  runtime_id, task_ids, reason }`. Before, those tasks ended silently: the
  provider never reports them, so an application kept showing them as running
  and an agent waiting on one never heard back. Each end is announced once.
  Disposing a runtime, or a process a turn has already claimed (that turn's
  result and `parked_background_tasks` report it), is not announced.
- `TurnInput::keep_background_work` / `TurnRequest::keep_background_work`:
  instead of `RestartWouldStopBackgroundWork`, a turn whose model, reasoning
  or auto-compaction policy would need a new process (for Claude, going back
  to the default model or effort, or turning thinking off or back on) keeps
  the retained process's values for just those, still switching what the
  process can switch in place, and reports what it kept as the new
  `TurnEvent::SettingsDeferred { change, tasks }`. If the work has ended, or
  the process has gone, by the time the turn claims it, the turn runs as
  asked instead. Changes to the working
  directory, launch context, sandbox, permission mode or other harness options
  are still refused. Carried over the remote host protocol as an optional
  field.

### Changed

- A retained Claude process switches ultracode (reasoning `ultracode`) on and
  off in place with `apply_flag_settings`, like effort and Fast mode, instead
  of being replaced. A turn on another effort after a workflow started under
  ultracode is no longer refused with `RestartWouldStopBackgroundWork`, and the
  workflow keeps running.
- A retained Claude process switches Fast mode (`harness_options.fast_mode`)
  in place, like the model and effort, instead of being replaced. A turn that
  only changes Fast mode is no longer refused with
  `RestartWouldStopBackgroundWork` while background work runs.
- **Breaking:** `ProviderProcessRetention` has a new field,
  `held_task_quiet_timeout: Option<Duration>`. A retained turn that has answered
  and is held open only by background work that reports nothing now completes
  successfully with a `TurnEvent::Warning` after that period instead of staying
  open until the inactivity limit. Set `None` for the previous behavior.
- A reused retained process that has already answered a turn is no longer held
  to `initialization_timeout`; it is judged by `active_inactivity_timeout`, so a
  process busy with earlier work is not failed as "starting".
- `RuntimeError::StartupTimeout`: a process that never answered within
  `initialization_timeout` now fails with this variant ("did not respond within N
  seconds of starting") instead of `Timeout` ("turn timed out"), so applications
  can tell a failed start from a turn that ran too long. Both map to
  `RuntimeFailureKind::Timeout`.
- Codex turns (`exec` and `app-server`) now run with `--config notify=[]`, so
  the `notify` command in the user's `~/.codex/config.toml` no longer runs when
  an SDK turn ends. The Codex desktop app installs one that calls its "Codex
  Computer Use" helper, which sends an Apple Event; macOS blamed the host
  application and asked "wants access to control Codex Computer Use" on every
  host. A user's own `notify` hook (a desktop notifier or webhook, for example)
  stops firing for SDK turns too; turn events still reach the host through the
  protocol. There is no opt-out yet.

### Added

- `TurnRequest::request_session_title`, `TurnEvent::SessionTitle` and
  `TurnResult::session_title`: ask the harness to title a session that has
  none yet, from the user's own description of the task, the way its own
  client does. Claude Code answers `generate_session_title`, the Codex
  app-server replays its TUI's hidden title thread, and `opencode serve`
  titles through its session API. Generation runs alongside the turn and
  never delays or fails it; a turn that ends first reports no title, so keep
  setting the field until one arrives. The request is carried over the
  protocol (`ProtocolTurnInput::request_session_title`) and is never logged.
- `AdapterOutput::optional_writes` and `AgentAdapter::idle_frame_writes`:
  provider frames the runtime writes without ever waiting for them. A frame
  the provider's stdin does not accept at once is dropped and a failed write
  is ignored, so housekeeping such as the session-title request and the
  cleanup of its hidden Codex thread can neither delay a turn nor replace a
  completed turn's success with a write error. Code that constructs
  `AdapterOutput` with struct syntax must now set `optional_writes`.
- `Provider::Acp` and `providers::acp::Acp` (default feature `acp`): one
  adapter for any agent that speaks the Agent Client Protocol over stdio —
  Gemini CLI (`gemini --acp`), GitHub Copilot (`copilot --acp`), Cursor
  (`cursor-agent acp`) and the rest of the ACP registry. Agents live in a
  shared, updatable `AcpAgents` registry and a turn selects one with the
  `agent` harness option, advertised as a `HarnessControlKind::Agent`
  control. Turns run `initialize`, `session/new` (or `session/resume`, else
  `session/load` without its replay), optional mode and model selection, then
  `session/prompt`; `session/request_permission` reaches the interaction
  handler unless `AcceptEdits`/`FullAccess` decide it, cancellation sends
  `session/cancel`, and `-32000` fails as an authentication error before the
  prompt. ACP requires an absolute working directory; a relative one is
  refused before the agent starts. `Acp::probe_agent` runs the handshake (and
  optionally opens a session) to report capabilities and sign-in state. See
  `docs/how-to/run-acp-agents.md`.
- ACP agents appear in harness discovery and readiness. `discover_harnesses`
  now includes `Provider::Acp` when the adapter is registered and
  `AgentRuntime::readiness(Provider::Acp)` resolves each registered agent's
  own command inside the configured transport instead of a placeholder `acp`
  executable: the harness is `Ready` when at least one agent is installed,
  `readiness.detail` names every agent, and the `Agent` control group lists
  only installed agents. New `AgentAdapter::selectable_executables` (default
  `None`, so existing adapters are unchanged) and `SelectableExecutable`
  expose that per-turn choice to the runtime.
- `TurnEvent::PlanUpdated` with `PlanEntry`/`PlanEntryStatus`: a complete
  replacement of an agent's execution plan, emitted for ACP `plan` updates.
- `AgentAdapter::interaction_answered`: called by the turn loop right after
  an approval or question reply is encoded, so an adapter that tracks
  unanswered requests stops refusing one the application already answered
  when the turn is cancelled. The default does nothing.
- `AgentAdapter::terminal_exit_grace`: a stdio provider that keeps running
  after its terminal frame and end-of-input is stopped after this grace
  period instead of holding the finished turn until its deadline. The default
  (`None`) keeps reading to end-of-output as before.
- `AgentTask::model`: the model a Claude Code subagent runs on, as its own
  messages report it (for example `claude-haiku-5-5`), including a subagent
  that inherits the parent's model. It is `None` until the subagent's first
  message, after which a `TasksChanged` snapshot carries it. A subagent's
  model never becomes the turn's model. The field is omitted from serialized
  tasks when unknown, so recorded events still deserialize. Code that
  constructs `AgentTask` with struct syntax must now set `model`.
- Background follow-ups: `AgentRuntime::subscribe_background_follow_ups`
  announces a runtime whose parked process started answering background work
  on its own, and `RuntimeInvocationKind::Continuation` (`TurnRequest::continuation`)
  streams that answer as an invocation without submitting a prompt. A
  continuation with nothing to deliver fails with `NothingToContinue`
  (`InvalidRequest`, `NotSent`) before writing to the provider. Adapters opt
  in through `AgentAdapter::retained_follow_up_pending`; Claude does.
- `AgentRuntime::parked_background_tasks` lists the provider task ids still
  running in a runtime's parked process (`AgentAdapter::retained_background_task_ids`),
  so an application can tell the tasks that outlived a finished or
  interrupted turn from those that ended with it.

- `TurnEvent::ToolCall { images }` and `ToolImage`: images a tool returned
  to the model (Claude's Read of an image file, an MCP screenshot) arrive as
  structured JPEG/PNG/GIF/WebP payloads with a SHA-256 of their base64,
  instead of base64 text in `output`, which the output bound cut mid-image.
  Unsupported, oversized or non-base64 images, and images past eight per
  result, are named in `output` after its bound, even when a shell's output
  or a file's content is the reported text. Adapters without image results report an empty list,
  and the field is omitted from serialized events when empty, so recorded
  events still deserialize. Code that constructs `TurnEvent::ToolCall` with
  struct syntax must now set `images` (usually `Vec::new()`).

- Claude Fast mode: `harness_options.fast_mode` (`true`/`false`) sets Claude
  Code's `fastMode`, merged with `ultracode` into a single `--settings`
  argument. Any other value is an `InvalidRequest`.
- `TurnEvent::ReplyFinishedWithBackgroundTasks { task_ids }`: Claude finished
  its reply but background tasks it started keep the turn open; the turn
  completes when the last one ends.

- Manual compaction for Codex and OpenCode. `RuntimeHandle::compact` now runs
  `thread/compact/start` on the Codex app server and `/session/{id}/summarize`
  on `opencode serve` (which needs a pinned model), reporting the same manual
  compaction lifecycle as Claude and ending the invocation when the provider
  confirms it. `TurnCapabilities::manual_compaction` states which adapters
  support it; the retained driver's `manual_compaction` capability now follows
  it instead of naming Claude alone.
- `TurnCapabilities::compaction_instructions` and
  `RuntimeDriverCapabilities::compaction_instructions`. Only Claude's native
  compaction takes summary instructions; `RuntimeHandle::compact` with
  instructions on Codex or OpenCode fails with `CapabilityUnavailable` instead
  of compacting without them.
- `DockerTransport` (default `docker` feature) runs provider CLIs inside an
  existing, running container through `docker exec -i`, with interactive stdin,
  secrets kept out of argv by the same private launcher as SSH, and
  termination of the whole process tree inside the container. The application
  owns the container (image, mounts, environment, limits). Processes that
  declare `CommandSpec::loopback_ports` are refused with `Unsupported`, so
  `OpenCode::serve()` does not run in a container yet.
  With `.user(...)` the selected user's home comes from the container's
  `/etc/passwd`, and a status is classified as Docker's only with Docker's own
  diagnostic, so a provider's exit 125 stays its own. The container's login
  environment is resolved once per transport; build a new transport after
  replacing the container.
- `CommandSpec::loopback_ports`: loopback ports a provider process serves that
  the SDK host must reach. `SshTransport` forwards each one (`ssh -L`, with
  `ExitOnForwardFailure`), so `OpenCode::serve()` works on SSH targets; local
  execution ignores it.
- A pi adapter. `providers::Pi`, behind the new default `pi` feature, drives
  `pi --mode rpc` (tested with pi 1.0.0): text and reasoning deltas, tool
  lifecycle events, usage with cost and context-window occupancy, the
  compaction lifecycle, session start and resume, cooperative `abort` on
  cancellation, a model catalog with per-model thinking levels, and a
  credential check through `pi --list-models`. pi runs tools without asking,
  so the adapter accepts only `FullAccess` and `Plan` (pi's read-only tools);
  approvals and questions come from pi extensions' UI requests. A resume of a
  session pi does not have fails as `pi::session_not_found`, and one pi cannot
  confirm as `pi::session_unconfirmed`, before the prompt is sent. `Provider::Pi` is a new variant of the non-exhaustive `Provider`
  enum, and default discovery now reports four harnesses. Skill discovery and
  management cover pi's skill directories; pi MCP discovery and management are
  reported as unsupported.
- `AgentAdapter::accepts_oversized_frame`, `AgentAdapter::parse_oversized_frame`
  and `OversizedFrame`. An adapter may accept a provider line longer than the
  event-line limit from its first 512 bytes instead of failing the turn; the
  runtime then consumes the line without buffering it and passes its first and
  last 512 bytes to the adapter. The default still fails the turn as soon as
  the line crosses the limit.
- `PI_CODING_AGENT_DIR` and `PI_CODING_AGENT_SESSION_DIR` pass through the
  local, SSH and Temps sandbox environment allowlists, like `CLAUDE_HOME`.

- Structured Claude workflows. A `workflow` task's `AgentTask::workflow`
  carries the run's phases, agents (label, phase, state, model, tokens, tool
  calls, timings, latest tool, prompt and result previews) and recent logs,
  parsed from Claude's `workflow_progress` and replaced on every update (a
  tick that only moves counters is re-emitted every fifth tick), plus
  the run's name, identifier and transcript directory from the `Workflow`
  tool's result. Its `TaskActivity` now records agent state changes, with the
  changed agent in the new `AgentTaskActivity::workflow_agent`, instead of one
  `Progress` per tick. `AgentWorkflow::agent_transcript_path` and
  `Claude::transcript_activity` rebuild a workflow agent's own text, reasoning
  and tool calls from its transcript; ticks that only move counters are sent
  at most every fifth tick. `AgentTask` and `AgentTaskActivity` gain fields,
  so code that builds them literally must set `workflow` and `workflow_agent`;
  stored values without them still deserialize.

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

- Local Linux/macOS process cleanup now discovers detached, reparented tools
  through a random inherited launch marker, in addition to killing the provider
  process group. Cleanup rechecks process identity before signaling and keeps
  separate launches isolated. `PreserveOnCompletion` remains unchanged; remote
  cleanup and host-crash recovery still require an outer supervisor.

- A Claude turn that Claude Code ended without a finished reply no longer shows
  its debug line (`[ede_diagnostic] result_type=user last_content_type=n/a
  stop_reason=null`) as the failure. The diagnostic now names what happened,
  for example that Claude stopped without answering the last message it
  received, keeps that line at the end, and a real error listed after it is
  shown instead. It makes no claim that earlier output was finished.

- Cancelling a turn no longer waits for a provider that has stopped reading
  its stdin. A provider busy inside a tool call can leave its pipe full, and
  the adapter-encoded interrupt (`turn/interrupt` for the Codex app-server)
  was written without a bound, so cancellation and the process-tree cleanup
  after it waited until the tool ended on its own. The interrupt write now
  gives up after 1 second, for one-shot and retained turns alike, and the
  provider is terminated as it already was when the interrupt was not
  accepted.
- A finished turn, probe or metadata query no longer waits for a descendant
  of the provider that kept its stderr open after the process ended, which
  turned a completed ACP turn into a timeout when the agent's terminal grace
  stopped it. The runtime waits a bounded 2 seconds for stderr to close and
  keeps the tail that had arrived for diagnostics. A turn keeps draining the
  pipe in the background past that point, so a tool left running by
  `ToolProcessPolicy::PreserveOnCompletion` never gets a broken pipe; probes,
  which have already stopped the process, stop reading.
- Changing a Claude turn's model, effort or permission mode no longer
  replaces the retained process and stops its background subagents and
  shells. The process switches them in place with `set_model`,
  `apply_flag_settings` and `set_permission_mode` before the prompt. A change
  that does need a new process, while background work runs, now fails with the
  new `RuntimeError::RestartWouldStopBackgroundWork` naming that work, and
  stops nothing; the new `AgentAdapter::retained_background_summary` describes
  it.
- Provider event lines are held to `max_event_line_bytes` exactly. A
  CRLF-terminated line at the limit is no longer rejected, and a final line
  one byte over it without a terminator is no longer accepted.
- The result of a Claude background shell's `Bash` call carries the shell
  task's ID, so it is grouped with that task instead of the parent turn.
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

- A retained Claude turn now ends at its answer when only background shells
  are still running (a dev server or watcher never ends), instead of staying
  open until they finish. The process is parked with the shells; Claude's
  answer when one ends is announced for a continuation, or delivered at the
  start of the next turn. Background subagents still hold the turn open, and
  one-shot processes are unchanged.

- Claude account usage honors `CLAUDE_CONFIG_DIR` (then `CLAUDE_HOME`): it
  reads that directory's credentials file and the Keychain entry Claude Code
  keys to it (`Claude Code-credentials-<path hash>`), never the host's
  default login, so a selected account reports its own quota.
- Claude: consecutive assistant text blocks are separated by a blank line;
  background tasks ending as `cancelled` or `interrupted` count as finished;
  a task's first real description replaces its fallback description.
- Claude account usage on macOS prefers the Keychain login over a
  credentials file, which can hold an expired token from an older login.
- Only JPEG, PNG, GIF and WebP attachments are native image inputs. Other
  image types (SVG, TIFF, ...) are described as host files instead.

- `SshTransport` and `DockerTransport` share one remote-shell implementation.
  Remote termination now also signals descendants it finds under `/proc`, so a
  process tree is stopped even when the launcher is not a process-group leader;
  with neither, termination fails instead of leaving children running.
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
